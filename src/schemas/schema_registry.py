"""
Central registry for all schema classes and their mappings.
This provides a single source of truth for dataset-to-schema relationships.
"""

# Realtime schemas
from schemas.realtime.vehicle_positions import VehiclePositions
from schemas.realtime.trip_updates import TripUpdates
from schemas.realtime.service_alerts import ServiceAlerts

# Schedule schemas
from schemas.schedule.agency import Agency
from schemas.schedule.calendar import Calendar
from schemas.schedule.calendar_dates import CalendarDates
from schemas.schedule.fare_attributes import FareAttributes
from schemas.schedule.fare_rules import FareRules
from schemas.schedule.feed_info import FeedInfo
from schemas.schedule.frequencies import Frequencies
from schemas.schedule.routes import Routes
from schemas.schedule.shapes import Shapes
from schemas.schedule.stop_times import StopTimes
from schemas.schedule.stops import Stops
from schemas.schedule.transfers import Transfers
from schemas.schedule.trips import Trips
from schemas.derived.daily_schedule import DailySchedule

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