"""
Routes Table Schema
==================

Schema for the sc_routes GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Routes(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for routes data.
    Table-specific field definitions with proper validation.
    """

    # Route identification
    route_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the route")
    agency_id:				Series[str]				= pa.Field(nullable=True,  description="Agency identifier this route belongs to")

    # Route names and descriptions
    route_short_name:		Series[str]				= pa.Field(nullable=True,  description="Short name of the route")
    route_long_name:		Series[str]				= pa.Field(nullable=True,  description="Full name of the route")
    route_desc:				Series[str]				= pa.Field(nullable=True,  description="Description of the route")
    route_type:				Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Type of transportation used on the route")

    # Route presentation
    route_url:				Series[str]				= pa.Field(nullable=True,  description="URL of a web page about the route")
    route_color:			Series[str]				= pa.Field(nullable=True,  description="Route color designation (hex color)")
    route_text_color:		Series[str]				= pa.Field(nullable=True,  description="Route text color designation (hex color)")
    route_sort_order:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Order for sorting routes in lists")
    contract_id:			Series[str]				= pa.Field(nullable=True,  description="Identifier for the contract associated with the route")
    # Pickup/drop-off behavior
    #continuous_pickup:		Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Continuous pickup behavior along the route")
    #continuous_drop_off:	Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Continuous drop-off behavior along the route")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Routes._bigquery_table_name = "stg_routes"
Routes._description = "Route information from GTFS schedule feeds"
Routes._bigquery_clustering = ['feed_hash', 'route_id']

__all__ = [
    'Routes'
]
