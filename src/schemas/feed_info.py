"""
Feed Info Table Schema
=====================

Schema for the stg_feed_info GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class FeedInfo(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for feed info data.
    Table-specific field definitions with proper validation.
    """

    # Feed publisher information
    feed_publisher_name:	Series[str]				= pa.Field(nullable=False, description="Full name of the organization that publishes the feed")
    feed_publisher_url:		Series[str]				= pa.Field(nullable=False, description="URL of the feed publisher's website")
    feed_lang:				Series[str]				= pa.Field(nullable=False, description="Primary language used in the feed")

    # Feed metadata
    feed_start_date:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Start date for the feed validity period")
    feed_end_date:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="End date for the feed validity period")
    feed_version:			Series[str]				= pa.Field(nullable=True,  description="Version of the feed")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")
    #ingestion_timestamp:	Series[pd.Timestamp]	= pa.Field(nullable=False, description="Timestamp when this record was ingested")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
FeedInfo._bigquery_table_name = "stg_feed_info"
FeedInfo._description = "Feed metadata and publisher information from GTFS schedule feeds"
FeedInfo._bigquery_clustering = ['feed_hash']

__all__ = [
    'FeedInfo'
]
