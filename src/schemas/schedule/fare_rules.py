"""
Fare Rules Table Schema
======================

Schema for the sc_fare_rules GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class FareRules(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for fare rules data.
    Table-specific field definitions with proper validation.
    """

    # Fare rule identification
    fare_rules_fare_id:		Series[str]				= pa.Field(nullable=False, description="Fare identifier for this rule")

    # Fare rule scope
    route_id:				Series[str]				= pa.Field(nullable=True,  description="Route identifier for this fare rule")
    origin_id:				Series[str]				= pa.Field(nullable=True,  description="Origin zone identifier for this fare rule")
    destination_id:			Series[str]				= pa.Field(nullable=True,  description="Destination zone identifier for this fare rule")
    contains_id:			Series[str]				= pa.Field(nullable=True,  description="Zone identifier that must be contained in the trip")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
FareRules._bigquery_table_name = "sc_fare_rules"
FareRules._description = "Fare rules and zone restrictions from GTFS schedule feeds"
FareRules._bigquery_clustering = ['feed_hash', 'fare_rules_fare_id', 'route_id']

__all__ = [
    'FareRules'
]
