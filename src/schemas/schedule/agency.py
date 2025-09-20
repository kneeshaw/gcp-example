"""
Agency Table Schema
==================

Schema for the sc_agency GTFS table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class Agency(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for agency data.
    Table-specific field definitions with proper validation.
    """

    # Agency identification
    agency_id:				Series[str]				= pa.Field(nullable=True,  description="Unique identifier for the agency")
    agency_name:			Series[str]				= pa.Field(nullable=False, description="Full name of the agency")
    agency_url:				Series[str]				= pa.Field(nullable=False, description="URL of the agency's website")
    agency_timezone:		Series[str]				= pa.Field(nullable=False, description="Timezone of the agency")
    agency_lang:			Series[str]				= pa.Field(nullable=True,  description="Primary language used by the agency")
    agency_phone:			Series[str]				= pa.Field(nullable=True,  description="Voice telephone number of the agency")
    agency_fare_url:		Series[str]				= pa.Field(nullable=True,  description="URL of a web page with fare information")
    agency_email:			Series[str]				= pa.Field(nullable=True,  description="Email address of the agency")

    # System fields
    feed_hash:				Series[str]				= pa.Field(nullable=False, description="Hash of the GTFS feed for data lineage")

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types


# BigQuery-specific table configuration
Agency._bigquery_table_name = "sc_agency"
Agency._description = "Transit agency information from GTFS schedule feeds"
Agency._bigquery_clustering = ['feed_hash']

__all__ = [
    'Agency'
]
