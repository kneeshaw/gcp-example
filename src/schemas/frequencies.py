"""
Frequencies Table Schema
=======================

Schema for the stg_frequencies GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Frequencies(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for frequencies data.
    Table-specific field definitions with proper validation.
    """

    # Frequency identification
    trip_id:				Series[str]				= pa.Field(nullable=False, description="Trip identifier for this frequency")

    # Frequency timing
    start_time:				Series[str]				= pa.Field(nullable=False, description="Start time for this frequency period")
    end_time:				Series[str]				= pa.Field(nullable=False, description="End time for this frequency period")
    headway_secs:			Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Time between departures in seconds")

    # Frequency attributes
    exact_times:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Indicates if frequencies are exact (0=frequency-based, 1=exact schedule)")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Frequencies._bigquery_table_name = "stg_frequencies"
Frequencies._description = "Trip frequencies and headways from GTFS schedule feeds"
Frequencies._bigquery_clustering = ['feed_hash', 'trip_id']

__all__ = [
    'Frequencies'
]
