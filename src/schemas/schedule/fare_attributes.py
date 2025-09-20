"""
Fare Attributes Table Schema
===========================

Schema for the sc_fare_attributes GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class FareAttributes(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for fare attributes data.
    Table-specific field definitions with proper validation.
    """

    # Fare identification
    fare_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the fare")
    agency_id:				Series[str]				= pa.Field(nullable=True,  description="Agency identifier for the fare")

    # Fare pricing
    price:					Series[float]			= pa.Field(nullable=False, description="Fare price")
    currency_type:			Series[str]				= pa.Field(nullable=False, description="Currency type for the fare price")

    # Fare rules
    payment_method:			Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Payment method (0=onboard, 1=before boarding)")
    transfers:				Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Number of transfers allowed (0=no, 1=once, 2=twice, empty=unlimited)")
    transfer_duration:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Time in seconds until transfer expires")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
FareAttributes._bigquery_table_name = "sc_fare_attributes"
FareAttributes._description = "Fare pricing and rules from GTFS schedule feeds"
FareAttributes._bigquery_clustering = ['feed_hash', 'fare_id']

__all__ = [
    'FareAttributes'
]
