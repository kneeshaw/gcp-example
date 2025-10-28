# !/usr/bin/env python3
"""
Daily Schedule Builder
Builds a complete transit schedule for a given date from GTFS data stored in BigQuery.

Usage:
    python daily_schedule.py --date 20250914
    python daily_schedule.py --project my-project --dataset my-dataset --date 20250914
"""

import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import Optional, List
import pandas as pd
import pytz
import os
from google.cloud import bigquery

from common.logging_utils import logger
from schemas.schema_registry import get_schema_class
from schemas.common.schema_utils import clean_and_validate_dataframe
from big_query.batch_insert import insert_batch


def get_globals():

    # project variables
    project_id = os.getenv("PROJECT_ID")
    bucket = os.getenv("BUCKET")
    bq_dataset = os.getenv("BQ_DATASET")
    timezone = os.getenv("TIMEZONE")
    service_date = os.getenv("SERVICE_DATE")

    # dataset variables
    dataset = os.getenv("DATASET")
    spec = os.getenv("SPEC")
    return {
        "project_id": project_id,
        "bucket": bucket,
        "bq_dataset": bq_dataset,
        "dataset": dataset,
        "spec": spec,
        "timezone": timezone,
        "service_date": service_date
    }


def convert_gtfs_time_to_utc(service_date: int, time_str: str) -> Optional[datetime]:
    """Convert GTFS time string (HH:MM:SS) to UTC datetime, handling times > 24:00"""
    if not time_str or pd.isna(time_str):
        return None

    # Parse the time string
    hours, minutes, seconds = map(int, time_str.split(':'))

    # Calculate days to add (for times > 24:00)
    extra_days = hours // 24
    hours = hours % 24

    # Create base datetime for service date
    base_date = datetime(service_date // 10000, (service_date % 10000) // 100, service_date % 100)

    # Add the time
    dt = base_date + timedelta(days=extra_days, hours=hours, minutes=minutes, seconds=seconds)

    # Assume local timezone (adjust based on your GTFS feed's timezone)
    local_tz = pytz.timezone('Pacific/Auckland')
    dt_local = local_tz.localize(dt)

    # Convert to UTC
    dt_utc = dt_local.astimezone(pytz.UTC)

    return dt_utc


def get_applicable_feed_hash(client, project_id: str, dataset: str, service_date: int) -> Optional[str]:
    """Get the applicable GTFS feed hash for the given service date."""
    logger.info(f"Finding feed for service date: {service_date}")

    query = f"""
    SELECT feed_hash FROM `{project_id}.{dataset}.sc_feed_info`
    WHERE feed_start_date <= {service_date}
    AND feed_end_date >= {service_date}
    LIMIT 1
    """

    try:
        feed_df = client.query(query).to_dataframe()

        if feed_df.empty:
            logger.warning(f"No applicable feed found for date {service_date}")
            return None

        feed_hash = feed_df.iloc[0]['feed_hash']
        logger.info(f"Found feed hash: {feed_hash}")
        return feed_hash

    except Exception as e:
        logger.error(f"Error getting feed hash: {e}")
        return None


def get_active_services(client, project_id: str, dataset: str, feed_hash: str, service_date: int) -> List[str]:
    """Get list of active service IDs for the given date."""
    logger.info("Getting active services...")

    # Convert service date to day of week
    date_obj = datetime(service_date // 10000, (service_date % 10000) // 100, service_date % 100)
    day_name = date_obj.strftime('%A').lower()

    try:
        # Get regular services
        calendar_query = f"""
        SELECT service_id FROM `{project_id}.{dataset}.sc_calendar`
        WHERE feed_hash = '{feed_hash}'
        AND start_date <= {service_date} AND end_date >= {service_date}
        AND {day_name} = 1
        """
        calendar_df = client.query(calendar_query).to_dataframe()
        regular_services = set(calendar_df['service_id'].tolist())

        # Get added services (exception_type = 1)
        added_query = f"""
        SELECT service_id FROM `{project_id}.{dataset}.sc_calendar_dates`
        WHERE feed_hash = '{feed_hash}'
        AND date = {service_date} AND exception_type = 1
        """
        added_df = client.query(added_query).to_dataframe()
        added_services = set(added_df['service_id'].tolist())

        # Get removed services (exception_type = 2)
        removed_query = f"""
        SELECT service_id FROM `{project_id}.{dataset}.sc_calendar_dates`
        WHERE feed_hash = '{feed_hash}'
        AND date = {service_date} AND exception_type = 2
        """
        removed_df = client.query(removed_query).to_dataframe()
        removed_services = set(removed_df['service_id'].tolist())

        # Combine: (regular + added) - removed
        active_services = (regular_services | added_services) - removed_services
        active_services = list(active_services)

        logger.info(f"Found {len(active_services)} active services")
        return active_services

    except Exception as e:
        logger.error(f"Error getting active services: {e}")
        return []


def get_trips(client, project_id: str, dataset: str, feed_hash: str, active_services: List[str]) -> pd.DataFrame:
    """Get trips for active services."""
    logger.info("Getting trips...")

    if not active_services:
        logger.warning("No active services provided")
        return pd.DataFrame()

    try:
        # Get ALL trips for this feed (no filtering in SQL)
        trips_query = f"""
        SELECT
            t.service_id, t.route_id, r.route_short_name, r.route_type,
            t.trip_id, t.trip_headsign, t.direction_id, t.shape_id
        FROM `{project_id}.{dataset}.sc_trips` t
        LEFT JOIN `{project_id}.{dataset}.sc_routes` r
        ON t.route_id = r.route_id AND t.feed_hash = r.feed_hash
        WHERE t.feed_hash = '{feed_hash}'
        """
        all_trips_df = client.query(trips_query).to_dataframe()

        # Filter by active services in pandas
        trips_df = all_trips_df[all_trips_df['service_id'].isin(active_services)]

        logger.info(f"All trips: {len(all_trips_df)}, Filtered: {len(trips_df)}")
        return trips_df

    except Exception as e:
        logger.error(f"Error getting trips: {e}")
        return pd.DataFrame()


def get_stop_times_and_convert(client, project_id: str, dataset: str, feed_hash: str,
                              trips_df: pd.DataFrame, service_date: int) -> pd.DataFrame:
    """Get stop times and convert to UTC format."""
    logger.info("Getting stop times and converting times...")

    if trips_df.empty:
        logger.warning("No trips data provided")
        return pd.DataFrame()

    try:
        # Get ALL stop times for this feed (no filtering in SQL)
        stop_times_query = f"""
        SELECT
            st.trip_id, st.stop_id, st.stop_sequence, s.stop_code, s.stop_name,
            st.stop_headsign, st.arrival_time, st.departure_time,
            s.stop_lat, s.stop_lon, st.shape_dist_traveled
        FROM `{project_id}.{dataset}.sc_stop_times` st
        LEFT JOIN `{project_id}.{dataset}.sc_stops` s
        ON st.stop_id = s.stop_id AND st.feed_hash = s.feed_hash
        WHERE st.feed_hash = '{feed_hash}'
        """
        stop_times_df = client.query(stop_times_query).to_dataframe()

        # Rename raw GTFS time strings to scheduled_*_time
        stop_times_df = stop_times_df.rename(columns={
            'arrival_time': 'scheduled_arrival_time',
            'departure_time': 'scheduled_departure_time'
        })

        # Convert scheduled times to UTC timestamps and seconds
        stop_times_df['scheduled_arrival'] = stop_times_df['scheduled_arrival_time'].apply(
            lambda x: convert_gtfs_time_to_utc(service_date, x)
        )
        stop_times_df['scheduled_departure'] = stop_times_df['scheduled_departure_time'].apply(
            lambda x: convert_gtfs_time_to_utc(service_date, x)
        )
        stop_times_df['scheduled_arrival_s'] = stop_times_df['scheduled_arrival'].apply(
            lambda x: int(x.timestamp()) if x else None
        )
        stop_times_df['scheduled_departure_s'] = stop_times_df['scheduled_departure'].apply(
            lambda x: int(x.timestamp()) if x else None
        )

        # Join with trips data
        schedule_df = stop_times_df.merge(trips_df, on='trip_id', how='inner')

        logger.info(f"Stop times: {len(stop_times_df)}, Schedule entries: {len(schedule_df)}")
        return schedule_df

    except Exception as e:
        logger.error(f"Error getting stop times: {e}")
        return pd.DataFrame()

def run(request):
    
    config = get_globals()

    if config['timezone'] is None:
        timezone = 'UTC'
    else:
        timezone = config['timezone']

    # if config['service_date'] is None then set service date to today in YYYYMMDD format in local timezone
    if config['service_date'] is None:
        service_date = int(datetime.now().astimezone(pytz.timezone(timezone)).strftime("%Y%m%d"))
    else:
        service_date = int(config['service_date'])

    logger.info(f"Using service date: {service_date} in timezone {timezone}")

    try:
        # Set up BigQuery client
        client = bigquery.Client(project=config['project_id'])

        # Step 1: Get feed hash
        feed_hash = get_applicable_feed_hash(client, config['project_id'], config['bq_dataset'], service_date)
        if not feed_hash:
            logger.error("No feed found - exiting")
            return "no-feed", 404

        # Step 2: Get active services
        active_services = get_active_services(client, config['project_id'], config['bq_dataset'], feed_hash, service_date)
        if not active_services:
            logger.warning("No active services found")

        # Step 3: Get trips
        trips_df = get_trips(client, config['project_id'], config['bq_dataset'], feed_hash, active_services)

        # Step 4: Get stop times and convert
        schedule_df = get_stop_times_and_convert(
            client, config['project_id'], config['bq_dataset'], feed_hash, trips_df, service_date
        )

        # Step 5: Build final schedule
        schedule_df['service_date'] = service_date
        schedule_df["service_date_dt"] = pd.to_datetime(schedule_df["service_date"].astype(str), format="%Y%m%d").dt.date
        schedule_df['feed_hash'] = feed_hash

        if not schedule_df.empty:
            logger.info("=== Schedule Summary ===")
            logger.info(f"Total entries: {len(schedule_df)}")
            logger.info(f"Routes: {schedule_df['route_short_name'].nunique()}")
            logger.info(f"Trips: {schedule_df['trip_id'].nunique()}")
            logger.info(f"Stops: {schedule_df['stop_id'].nunique()}")

            # Validate dataframe using Pandera
            schema_class = get_schema_class(config['dataset'])
            df_processed = clean_and_validate_dataframe(schedule_df, schema_class)

            # Prepare table name and upload if we have rows post-validation
            table_name = f"{config['spec']}_{config['dataset'].replace('-', '_')}"
            result = None
            if not df_processed.empty:
                result = insert_batch(
                    df_processed,
                    table_name,
                    config['project_id'],
                    config['bq_dataset']
                )
                logger.info(f"BigQuery ingestion result: {result}")

            # Ingest-style response
            object_name = table_name
            row_count = int(len(df_processed)) if df_processed is not None else int(len(schedule_df))
            return ({
                "status": "ok",
                "dataset": config.get('dataset'),
                "object": object_name,
                "rows": row_count,
                "bq_ingest": result,
            }, 200)
        else:
            logger.warning("No schedule data generated")
            return "no-data", 204

    except Exception as e:
        logger.error(f"Error building schedule: {e}")
    return f"error: {str(e)}", 500