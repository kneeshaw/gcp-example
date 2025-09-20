"""
Shapes Table Schema
==================

Schema for the sc_shapes GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Shapes(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for shapes data.
    Table-specific field definitions with proper validation.
    """

    # Shape identification
    shape_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the shape")

    # Shape point information
    shape_pt_lat:			Series[float]			= pa.Field(nullable=False, description="Latitude of the shape point")
    shape_pt_lon:			Series[float]			= pa.Field(nullable=False, description="Longitude of the shape point")
    shape_pt_sequence:		Series[pd.Int64Dtype]	= pa.Field(nullable=False, description="Sequence number of the shape point")

    # Shape attributes
    shape_dist_traveled:	Series[float]			= pa.Field(nullable=True,  description="Distance traveled along the shape from the first point")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Shapes._bigquery_table_name = "sc_shapes"
Shapes._description = "Shape geometry points for GTFS schedule feeds"
Shapes._bigquery_clustering = ['feed_hash', 'shape_id', 'shape_pt_sequence']

__all__ = [
    'Shapes'
]
