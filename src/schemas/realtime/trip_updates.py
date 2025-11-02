"""
Trip Updates Table Schema
========================

Schema for the rt_trip_updates GTFS Real-Time table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class TripUpdates(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for trip updates data.
    Table-specific field definitions with proper validation.
    """

    # Record identification
    record_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for this trip update record")
    entity_id:				Series[str]				= pa.Field(nullable=False, description="GTFS-RT entity identifier for this trip update")

    # Timestamp fields
    timestamp:				Series[pd.Timestamp]	= pa.Field(nullable=False, description="Timestamp when this trip update was recorded")
    timestamp_s:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Unix timestamp in seconds")

    # Trip and route context
    route_id:				Series[str]				= pa.Field(nullable=True,  description="Route identifier this trip belongs to")
    trip_id:				Series[str]				= pa.Field(nullable=False, description="Trip identifier this update applies to")
    start_date:				Series[str]	            = pa.Field(nullable=True,  description="Start date of the trip in YYYYMMDD format")
    start_time:				Series[str]				= pa.Field(nullable=True,  description="Scheduled start time of the trip")
    direction_id:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Direction of travel (0 or 1)")
    schedule_relationship:	Series[str]				= pa.Field(nullable=True,  description="Relationship to the scheduled trip")
    delay:				    Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Delay in seconds from scheduled time")
    
    # Stop information
    stop_sequence:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Sequence number of the stop in the trip")
    stop_id:				Series[str]				= pa.Field(nullable=True,  description="Stop identifier this update applies to")

    # Realtime arrival and departure information
    actual_arrival:			Series[pd.Timestamp]	= pa.Field(nullable=True,  description="Actual arrival time at the stop (UTC)")
    actual_arrival_s:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Actual arrival time in Unix seconds")
    arrival_delay:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Arrival delay in seconds")
    arrival_uncertainty:	Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Uncertainty of arrival time in seconds")
    actual_departure:		Series[pd.Timestamp]	= pa.Field(nullable=True,  description="Actual departure time from the stop (UTC)")
    actual_departure_s:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Actual departure time in Unix seconds")
    departure_delay:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Departure delay in seconds")
    departure_uncertainty:	Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Uncertainty of departure time in seconds")

    # Vehicle information
    vehicle_id:				Series[str]				= pa.Field(nullable=True,  description="Vehicle identifier serving this trip")
    vehicle_label:			Series[str]				= pa.Field(nullable=True,  description="User-visible vehicle label")
    vehicle_license_plate:	Series[str]				= pa.Field(nullable=True,  description="Vehicle license plate number")

    # Metadata and tracking (warehouse timestamps provided by TimestampMixin)

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types

# Source columns for dataframe transformation
COLS_TIMESTAMP = ['timestamp', 'actual_arrival', 'actual_departure', 'created_at', 'updated_at']
COLS_VOLATILE = ['record_id', 'timestamp', 'timestamp_s', 'created_at', 'updated_at']
COLS_ENTITY = ['vehicle_id', 'trip_id', 'stop_id']

# Source field mappings for trip updates data
COLS_MAPPING = {
    'timestamp': ['timestamp'],
    'timestamp_s': ['trip_update.timestamp', 'timestamp'],
    'route_id': ['trip_update.trip.route_id'],
    'trip_id': ['trip_update.trip.trip_id', 'tripUpdate.trip.tripId'],
    'start_date': ['trip_update.trip.start_date', 'tripUpdate.trip.startDate'],
    'start_time': ['trip_update.trip.start_time', 'tripUpdate.trip.startTime'],
    'direction_id': ['trip_update.trip.direction_id'],
    'schedule_relationship': ['trip_update.trip.schedule_relationship', 'tripUpdate.trip.scheduleRelationship'],
    'delay': ['trip_update.delay'],
    'stop_sequence': ['trip_update.stop_time_update.stop_sequence', 'tripUpdate.stopTimeUpdate.stopSequence'],
    'stop_id': ['trip_update.stop_time_update.stop_id', 'tripUpdate.stopTimeUpdate.stopId'],
    'actual_arrival': ['trip_update.stop_time_update.arrival.time', 'tripUpdate.stopTimeUpdate.arrival.time'],
    'actual_arrival_s': ['trip_update.stop_time_update.arrival.time', 'tripUpdate.stopTimeUpdate.arrival.time'],
    'arrival_delay': ['trip_update.stop_time_update.arrival.delay'],
    'arrival_uncertainty': ['trip_update.stop_time_update.arrival.uncertainty'],
    'actual_departure': ['trip_update.stop_time_update.departure.time', 'tripUpdate.stopTimeUpdate.departure.time'],
    'actual_departure_s': ['trip_update.stop_time_update.departure.time', 'tripUpdate.stopTimeUpdate.departure.time'],
    'departure_delay': ['trip_update.stop_time_update.departure.delay'],
    'departure_uncertainty': ['trip_update.stop_time_update.departure.uncertainty'],
    'vehicle_id': ['trip_update.vehicle.id'],
    'vehicle_label': ['trip_update.vehicle.label'],
    'vehicle_license_plate': ['trip_update.vehicle.license_plate'],
    'created_at': ['created_at'],
    'updated_at': ['updated_at']
}

# BigQuery-specific table configuration
TripUpdates._bigquery_table_name = "rt_trip_updates"
TripUpdates._description = "Real-time trip update data from GTFS-RT feeds"
TripUpdates._bigquery_clustering = ["record_id", "entity_id"]
TripUpdates._bigquery_partitioning = {
    "type": "DAY",
    "field": "timestamp"
}

__all__ = [
    'TripUpdates'
]