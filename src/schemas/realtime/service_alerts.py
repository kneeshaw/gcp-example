"""
Service Alerts Table Schema
==========================

Schema for the rt_service_alerts GTFS Real-Time table.
Table-specific DataFrameModel definition with proper validation.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class ServiceAlerts(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for service alerts data.
    Table-specific field definitions with proper validation.
    """

    # Record identification
    record_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for this service alert record")
    entity_id:				Series[str]				= pa.Field(nullable=False, description="GTFS-RT entity identifier for this service alert")
    alert_id:				Series[str]				= pa.Field(nullable=True,  description="Alert identifier")

    # Timestamp fields
    timestamp:				Series[pd.Timestamp]	= pa.Field(nullable=False, description="Timestamp when this alert was recorded")
    timestamp_s:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Unix timestamp in seconds")

    # Alert content
    header:				    Series[str]				= pa.Field(nullable=True,  description="Alert header text")
    description:			Series[str]				= pa.Field(nullable=True,  description="Alert description text")
    url:				    Series[str]				= pa.Field(nullable=True,  description="URL with additional alert information")

    # Alert classification
    cause:				    Series[str]				= pa.Field(nullable=True,  description="Cause of the service disruption")
    effect:				    Series[str]				= pa.Field(nullable=True,  description="Effect of the service disruption")
    effect_detail:			Series[str]				= pa.Field(nullable=True,  description="Detailed effect description")
    severity_level:			Series[str]				= pa.Field(nullable=True,  description="Severity level of the alert")

    # Active periods
    period_start:			Series[pd.Timestamp]	= pa.Field(nullable=True,  description="Start time of the alert period")
    period_end:				Series[pd.Timestamp]	= pa.Field(nullable=True,  description="End time of the alert period")
    period_start_s:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Start time in Unix seconds")
    period_end_s:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="End time in Unix seconds")

    # Affected entities
    route_id:				Series[str]				= pa.Field(nullable=True,  description="Affected route identifier")
    agency_id:				Series[str]				= pa.Field(nullable=True,  description="Affected agency identifier")
    trip_id:				Series[str]				= pa.Field(nullable=True,  description="Affected trip identifier")
    stop_id:				Series[str]				= pa.Field(nullable=True,  description="Affected stop identifier")
    direction_id:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Affected direction (0 or 1)")

    # Metadata and tracking (warehouse timestamps provided by TimestampMixin)

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types

# Source columns for dataframe transformation
COLS_TIMESTAMP = ['timestamp', 'period_start', 'period_end', 'created_at', 'updated_at']
COLS_VOLATILE = ['record_id', 'timestamp', 'timestamp_s', 'created_at', 'updated_at']
COLS_ENTITY = ['alert_id']

# Source field mappings for service alerts data
COLS_MAPPING = {
    'alert_id': ['id'],
    'timestamp': ['timestamp'],
    'timestamp_s': ['alert.timestamp', 'timestamp'],
    'header': ['alert.header_text.translation.text'],
    'description': ['alert.description_text.translation.text'],
    'url': ['alert.url.translation.text'],
    'cause': ['alert.cause'],
    'effect': ['alert.effect'],
    'effect_detail': ['alert.effect_detail.translation.text'],
    'severity_level': ['alert.severity_level'],
    'period_start': ['alert.active_period.start'],
    'period_end': ['alert.active_period.end'],
    'period_start_s': ['alert.active_period.start'],
    'period_end_s': ['alert.active_period.end'],
    'route_id': ['alert.informed_entity.route_id'],
    'agency_id': ['alert.informed_entity.agency_id'],
    'trip_id': ['alert.informed_entity.trip.trip_id'],
    'stop_id': ['alert.informed_entity.stop_id'],
    'direction_id': ['alert.informed_entity.direction_id'],
    'created_at': ['created_at'],
    'updated_at': ['updated_at']
}

# BigQuery-specific table configuration
ServiceAlerts._bigquery_table_name = "rt_service_alerts"
ServiceAlerts._description = "Real-time service alert data from GTFS-RT feeds"
ServiceAlerts._bigquery_clustering = ["entity_id"]
ServiceAlerts._bigquery_partitioning = {
    "type": "DAY",
    "field": "timestamp"
}

__all__ = [
    'ServiceAlerts',
    'COLS_TIMESTAMP',
    'COLS_VOLATILE',
    'COLS_ENTITY',
    'COLS_MAPPING'
]