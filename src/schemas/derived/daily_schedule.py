"""
Daily Schedule Schema
=====================

Derived schema for daily schedule data combining fields from multiple schedule schemas.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class DailySchedule(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for daily schedule data.
    Derived schema combining fields from multiple schedule tables.
    """

    # Identity fields
    service_date:			    Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Date for which this schedule applies")
    service_id:				    Series[str]				= pa.Field(nullable=False, description="Unique identifier for the service")
    route_id:				    Series[str]				= pa.Field(nullable=False, description="Unique identifier for the route")
    trip_id:				    Series[str]				= pa.Field(nullable=False, description="Unique identifier for the trip")
    stop_id:				    Series[str]				= pa.Field(nullable=False, description="Unique identifier for the stop")

    # Route information
    route_short_name:		    Series[str]				= pa.Field(nullable=True,  description="Short name of the route")
    route_type:				    Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Type of transportation used on the route")

    # Trip information
    direction_id:			    Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Indicates the direction of travel (0=outbound, 1=inbound)")
    trip_headsign:			    Series[str]				= pa.Field(nullable=True,  description="Text that appears on signage identifying the trip's destination")
    stop_headsign:			    Series[str]				= pa.Field(nullable=True,  description="Text that appears on signage identifying the trip's destination at this stop")
    shape_id:				    Series[str]				= pa.Field(nullable=True,  description="Shape identifier for the trip")
    shape_dist_traveled:	    Series[float]			= pa.Field(nullable=True,  description="Distance traveled along the shape from the first stop")


    # Stop information
    stop_sequence:			    Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Order of stops for this trip")
    stop_code:				    Series[str]				= pa.Field(nullable=True,  description="Code of the stop")
    stop_name:				    Series[str]				= pa.Field(nullable=True,  description="Name of the stop")
    stop_lat:				    Series[float]			= pa.Field(nullable=True,  description="Latitude of the stop")
    stop_lon:				    Series[float]			= pa.Field(nullable=True,  description="Longitude of the stop")

    # Scheduled time information (from GTFS static)
    scheduled_arrival_time:	    Series[str]				= pa.Field(nullable=True,  description="Scheduled arrival time HH:MM:SS (may exceed 24h)")
    scheduled_departure_time:	Series[str]				= pa.Field(nullable=True,  description="Scheduled departure time HH:MM:SS (may exceed 24h)")
    scheduled_arrival:		    Series[pd.Timestamp]	= pa.Field(nullable=True,  description="Scheduled arrival timestamp (derived to UTC)")
    scheduled_departure:		Series[pd.Timestamp]	= pa.Field(nullable=True,  description="Scheduled departure timestamp (derived to UTC)")
    scheduled_arrival_s:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Scheduled arrival in seconds since service day midnight")
    scheduled_departure_s:	    Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Scheduled departure in seconds since service day midnight")

    # Metadata
    service_date_dt:            Series[pd.Timestamp]    = pa.Field(nullable=False, description="Service date as a date object")
    feed_hash:				    Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
DailySchedule._bigquery_table_name = "ds_daily_schedule"
DailySchedule._description = "Derived daily schedule combining route, trip, stop, and timing information"
DailySchedule._bigquery_clustering = ['feed_hash', 'route_id', 'trip_id', 'stop_sequence']
DailySchedule._bigquery_partitioning = {
    "type": "DAY",
    "field": "service_date_dt"
}

# Force BigQuery type overrides for fields where Pandera annotation isn't 1:1 with BQ
# Here we want service_date_dt to be a DATE column, even though the DataFrame carries a Timestamp
DailySchedule._bigquery_field_types = {
    "service_date_dt": "DATE"
}

__all__ = [
    'DailySchedule'
]
