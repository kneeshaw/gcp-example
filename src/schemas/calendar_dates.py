"""
Calendar Dates Table Schema
==========================

Schema for the stg_calendar_dates GTFS table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class CalendarDates(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for calendar dates data.
    Table-specific field definitions with proper validation.
    """

    # Service exception details
    service_id:				Series[str]				= pa.Field(nullable=False, description="Service identifier this exception applies to")
    date:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Date of the service exception in YYYYMMDD format")
    exception_type:			Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Type of exception (1=added, 2=removed)")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
CalendarDates._bigquery_table_name = "stg_calendar_dates"
CalendarDates._description = "Service exception dates from GTFS schedule feeds"
CalendarDates._bigquery_clustering = ['feed_hash', 'service_id', 'date']

__all__ = [
    'CalendarDates'
]
