"""
Stops Table Schema
=================

Schema for the sc_stops GTFS Schedule table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Stops(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for stops data.
    Table-specific field definitions with proper validation.
    """

    # Stop identification
    stop_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for the stop")
    stop_code:				Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Short text or number that identifies the stop")

    # Stop information
    stop_name:				Series[str]				= pa.Field(nullable=False, description="Name of the stop")
    stop_desc:				Series[str]				= pa.Field(nullable=True,  description="Description of the stop")
    stop_lat:				Series[float]			= pa.Field(nullable=False, description="Latitude of the stop")
    stop_lon:				Series[float]			= pa.Field(nullable=False, description="Longitude of the stop")

    # Stop attributes
    zone_id:				Series[str]				= pa.Field(nullable=True,  description="Fare zone identifier for the stop")
    stop_url:				Series[str]				= pa.Field(nullable=True,  description="URL of a web page about the stop")
    location_type:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Type of location (0=stop, 1=station, 2=entrance, 3=generic)")
    parent_station:			Series[str]				= pa.Field(nullable=True,  description="Identifier of the parent station")
    platform_code:			Series[str]				= pa.Field(nullable=True,  description="Platform identifier for the stop")
    stop_timezone:			Series[str]				= pa.Field(nullable=True,  description="Timezone of the stop")
    wheelchair_boarding:	Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Wheelchair boarding availability (0=no, 1=yes, 2=unknown)")
    start_date:				Series[str]	            = pa.Field(nullable=True,  description="Start date of service availability at the stop")
    end_date:				Series[str]	            = pa.Field(nullable=True,  description="End date of service availability at the stop")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Stops._bigquery_table_name = "sc_stops"
Stops._description = "Stop and station information from GTFS schedule feeds"
Stops._bigquery_clustering = ['feed_hash', 'stop_id']

__all__ = [
    'Stops'
]
