"""
Vehicle Positions Table Schema
=============================

Schema for the rt_vehicle_positions GTFS Real-Time table.
Table-specific DataFrameModel definition with BigQuery optimizations.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from schemas.common.columns import TimestampMixin


class VehiclePositions(TimestampMixin, pa.DataFrameModel):
    """
    Pandera DataFrameModel for vehicle positions data.
    Table-specific field definitions with proper validation.
    """

    # Record identification
    record_id:				Series[str]				= pa.Field(nullable=False, description="Unique identifier for this vehicle position record")
    entity_id:				Series[str]				= pa.Field(nullable=False, description="GTFS-RT entity identifier for this vehicle")

    # Timestamp fields
    timestamp:				Series[pd.Timestamp]	= pa.Field(nullable=False, description="Timestamp when this position was recorded")
    timestamp_s:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Unix timestamp in seconds")

    # Vehicle identification
    vehicle_id:				Series[str]				= pa.Field(nullable=True,  description="Unique identifier for the vehicle")
    vehicle_label:			Series[str]				= pa.Field(nullable=True,  description="User-visible label for the vehicle")
    vehicle_license_plate:	Series[str]				= pa.Field(nullable=True,  description="License plate number of the vehicle")

    # Vehicle status
    latitude:				Series[float]			= pa.Field(nullable=True,  description="Latitude coordinate in decimal degrees", metadata={"precision": 6})
    longitude:				Series[float]			= pa.Field(nullable=True,  description="Longitude coordinate in decimal degrees", metadata={"precision": 6})
    bearing:				Series[float]			= pa.Field(nullable=True,  description="Bearing/direction of travel in degrees", metadata={"precision": 1})
    speed:				    Series[float]			= pa.Field(nullable=True,  description="Speed of the vehicle in m/s", metadata={"precision": 1})
    odometer:				Series[float]			= pa.Field(nullable=True,  description="Odometer reading of the vehicle", metadata={"precision": 1})
    occupancy_status:		Series[str]				= pa.Field(nullable=True,  description="Current occupancy status of the vehicle")

    # Trip and route context
    route_id:				Series[str]				= pa.Field(nullable=True,  description="Route identifier this vehicle is serving")
    trip_id:				Series[str]				= pa.Field(nullable=True,  description="Trip identifier this vehicle is serving")
    direction_id:			Series[pd.Int64Dtype]	= pa.Field(nullable=True,  description="Direction of travel (0 or 1)")
    start_date:				Series[str]	            = pa.Field(nullable=True,  description="Start date of the trip in YYYYMMDD format")
    start_time:				Series[str]				= pa.Field(nullable=True,  description="Scheduled start time of the trip")
    schedule_relationship:	Series[str]				= pa.Field(nullable=True,  description="Relationship to the scheduled trip")

    # Metadata and tracking (warehouse timestamps provided by TimestampMixin)

    class Config:
        strict = False  # Allow extra columns during development
        coerce = True   # Attempt to coerce data types

# Source columns for dataframe transformation
COLS_TIMESTAMP = ['timestamp', 'created_at', 'updated_at']
COLS_VOLATILE = ['record_id', 'created_at', 'updated_at']
COLS_ENTITY = ['vehicle_id']
COLS_FILTERNA = ['vehicle_id']

# Source column mappings for vehicle positions data
COLS_MAPPING = {
    'vehicle_id': ['vehicle.vehicle.id'],
    'vehicle_label': ['vehicle.vehicle.label'],
    'vehicle_license_plate': ['vehicle.vehicle.license_plate'],
    'latitude': ['vehicle.position.latitude'],
    'longitude': ['vehicle.position.longitude'],
    'bearing': ['vehicle.position.bearing'],
    'speed': ['vehicle.position.speed'],
    'odometer': ['vehicle.position.odometer'],
    'occupancy_status': ['vehicle.occupancy_status', 'vehicle.occupancyStatus'],
    'route_id': ['vehicle.trip.route_id', 'vehicle.trip.routeId'],
    'trip_id': ['vehicle.trip.trip_id', 'vehicle.trip.tripId'],
    'direction_id': ['vehicle.trip.directionId', 'vehicle.trip.direction_id'],
    'start_date': ['vehicle.trip.start_date', 'vehicle.trip.startDate'],
    'start_time': ['vehicle.trip.start_time', 'vehicle.trip.startTime'],
    'schedule_relationship': ['vehicle.trip.schedule_relationship', 'vehicle.trip.scheduleRelationship'],
    'timestamp': ['vehicle.timestamp', 'timestamp'],
    'timestamp_s': ['vehicle.timestamp', 'timestamp'],
    'created_at': ['created_at'],
    'updated_at': ['updated_at']
}

# GTFS-RT Categorical Field Mappings (following GTFS specification)
CATEGORICAL_MAPPING = {
    'occupancy_status': {
        # GTFS standard numeric codes to text
        0: 'EMPTY',
        1: 'MANY_SEATS_AVAILABLE',
        2: 'FEW_SEATS_AVAILABLE',
        3: 'STANDING_ROOM_ONLY',
        4: 'CRUSHED_STANDING_ROOM_ONLY',
        5: 'FULL',
        6: 'NOT_ACCEPTING_PASSENGERS',
        7: 'NO_DATA_AVAILABLE',
        8: 'NOT_BOARDABLE',
    },
    'schedule_relationship': {
        # GTFS standard numeric codes to text
        0: 'SCHEDULED',
        1: 'ADDED',
        2: 'UNSCHEDULED',
        3: 'CANCELED',
        4: 'REPLACEMENT',
        5: 'DUPLICATED',
        6: 'DELETED',
    }
}

# BigQuery-specific table configuration
VehiclePositions._bigquery_table_name = "stg_vehicle_positions"
VehiclePositions._description = "Real-time vehicle position data from GTFS-RT feeds"
VehiclePositions._bigquery_clustering = ["record_id", "entity_id"]
VehiclePositions._bigquery_partitioning = {
    "type": "DAY",
    "field": "timestamp"
}

__all__ = [
    'VehiclePositions',
    'COLS_TIMESTAMP',
    'COLS_VOLATILE',
    'COLS_ENTITY',
    'COLS_FILTERNA',
    'COLS_MAPPING',
    'CATEGORICAL_MAPPING'
]
