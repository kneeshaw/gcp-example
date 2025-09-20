"""
Stop Times Table Schema
======================

Schema for the sc_stop_times GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class StopTimes(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for stop times data.
    Table-specific field definitions with proper validation.
    """

    # Trip and stop identification
    trip_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the trip")
    stop_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the stop")
    stop_sequence:			Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Order of stops for this trip")

    # Time information
    arrival_time:			Series[str]				= pa.Field(nullable=True,  description="Arrival time at the stop")
    departure_time:			Series[str]				= pa.Field(nullable=True,  description="Departure time from the stop")

    # Stop attributes
    stop_headsign:			Series[str]				= pa.Field(nullable=True,  description="Text that appears on signage identifying the trip's destination")
    pickup_type:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Indicates pickup method (0=regular, 1=no pickup, 2=must phone, 3=must coordinate)")
    drop_off_type:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Indicates drop off method (0=regular, 1=no drop off, 2=must phone, 3=must coordinate)")
    timepoint:				Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Indicates if arrival/departure times are exact (0=approximate, 1=exact)")

    # Shape information
    shape_dist_traveled:	Series[float]			= pa.Field(nullable=True,  description="Distance traveled along the shape from the first stop")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
StopTimes._bigquery_table_name = "sc_stop_times"
StopTimes._description = "Stop times and sequences for GTFS schedule feeds"
StopTimes._bigquery_clustering = ['feed_hash', 'trip_id', 'stop_sequence']

__all__ = [
    'StopTimes'
]
