"""
Trips Table Schema
=================

Schema for the stg_trips GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Trips(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for trips data.
    Table-specific field definitions with proper validation.
    """

    # Trip identification
    trip_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the trip")
    route_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the route")
    service_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the service")

    # Trip information
    trip_headsign:			Series[str]				= pa.Field(nullable=True,  description="Text that appears on signage identifying the trip's destination")
    trip_short_name:		Series[str]				= pa.Field(nullable=True,  description="Short text or number that identifies the trip to passengers")
    direction_id:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Indicates the direction of travel (0=outbound, 1=inbound)")

    # Trip attributes
    block_id:				Series[str]				= pa.Field(nullable=True,  description="Block identifier for the trip")
    shape_id:				Series[str]				= pa.Field(nullable=True,  description="Shape identifier for the trip")
    wheelchair_accessible:	Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Wheelchair accessibility (0=no, 1=yes, 2=unknown)")
    bikes_allowed:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Bicycle allowance (0=no, 1=yes, 2=unknown)")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Trips._bigquery_table_name = "stg_trips"
Trips._description = "Trip information from GTFS schedule feeds"
Trips._bigquery_clustering = ['feed_hash', 'route_id', 'trip_id']

__all__ = [
    'Trips'
]
