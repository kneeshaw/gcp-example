"""
Transfers Table Schema
=====================

Schema for the sc_transfers GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Transfers(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for transfers data.
    Table-specific field definitions with proper validation.
    """

    # Transfer stops
    from_stop_id:			Series[str]				= pa.Field(nullable=False, description="Stop identifier where the transfer originates")
    to_stop_id:				Series[str]				= pa.Field(nullable=False, description="Stop identifier where the transfer arrives")

    # Transfer rules
    transfer_type:			Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Type of transfer (0=recommended, 1=timed, 2=min time, 3=not possible)")
    min_transfer_time:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Minimum time in seconds required to transfer")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Transfers._bigquery_table_name = "stg_transfers"
Transfers._description = "Transfer rules between stops from GTFS schedule feeds"
Transfers._bigquery_clustering = ['feed_hash', 'from_stop_id', 'to_stop_id']

__all__ = [
    'Transfers'
]
