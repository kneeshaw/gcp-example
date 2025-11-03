"""
Central registry for all schema classes and their mappings.
This provides a single source of truth for dataset-to-schema relationships.
"""

# Realtime schemas
from schemas.vehicle_positions import VehiclePositions
from schemas.trip_updates import TripUpdates
from schemas.service_alerts import ServiceAlerts

# Schedule schemas
from schemas.agency import Agency
from schemas.calendar import Calendar
from schemas.calendar_dates import CalendarDates
from schemas.fare_attributes import FareAttributes
from schemas.fare_rules import FareRules
from schemas.feed_info import FeedInfo
from schemas.frequencies import Frequencies
from schemas.routes import Routes
from schemas.shapes import Shapes
from schemas.stop_times import StopTimes
from schemas.stops import Stops
from schemas.transfers import Transfers
from schemas.trips import Trips
from schemas.daily_schedule import DailySchedule

# Dataset to Schema Class Mapping
DATASET_SCHEMA_MAPPING = {
    # Realtime datasets
    'vehicle-positions': VehiclePositions,
    'trip-updates': TripUpdates,
    'service-alerts': ServiceAlerts,

    # Schedule datasets
    'agency': Agency,
    'calendar': Calendar,
    'calendar_dates': CalendarDates,
    'fare_attributes': FareAttributes,
    'fare_rules': FareRules,
    'feed_info': FeedInfo,
    'frequencies': Frequencies,
    'routes': Routes,
    'shapes': Shapes,
    'stop_times': StopTimes,
    'stops': Stops,
    'transfers': Transfers,
    'trips': Trips,

    # Derived datasets
    'daily-schedule': DailySchedule,
}

def get_schema_class(dataset: str):
    """
    Get the schema class for a dataset.

    Args:
        dataset: Dataset name (e.g., 'vehicle-positions', 'stops')

    Returns:
        Schema class or None if not found
    """
    return DATASET_SCHEMA_MAPPING.get(dataset)

# Export essential items
__all__ = [
    'DATASET_SCHEMA_MAPPING',
    'get_schema_class',
]