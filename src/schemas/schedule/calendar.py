"""
Calendar Table Schema
====================

Schema for the sc_calendar GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Calendar(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for calendar data.
    Table-specific field definitions with proper validation.
    """

    # Service identification
    service_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the service")

    # Day of week flags
    monday:					Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Mondays (1=yes, 0=no)")
    tuesday:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Tuesdays (1=yes, 0=no)")
    wednesday:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Wednesdays (1=yes, 0=no)")
    thursday:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Thursdays (1=yes, 0=no)")
    friday:					Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Fridays (1=yes, 0=no)")
    saturday:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Saturdays (1=yes, 0=no)")
    sunday:					Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Service available on Sundays (1=yes, 0=no)")

    # Service period
    start_date:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Start date of the service in YYYYMMDD format")
    end_date:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="End date of the service in YYYYMMDD format")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Calendar._bigquery_table_name = "sc_calendar"
Calendar._description = "Service calendar information from GTFS schedule feeds"
Calendar._bigquery_clustering = ['feed_hash', 'service_id']

__all__ = [
    'Calendar'
]
