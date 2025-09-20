"""
Schedule GTFS Table Schemas
===========================

These schemas define the structure of raw GTFS Schedule tables as they exist in BigQuery.
Each schema corresponds to a standard GTFS Schedule table.
"""

from .agency import Agency
from .calendar import Calendar
from .calendar_dates import CalendarDates
from .fare_attributes import FareAttributes
from .fare_rules import FareRules
from .feed_info import FeedInfo
from .frequencies import Frequencies
from .routes import Routes
from .shapes import Shapes
from .stops import Stops
from .stop_times import StopTimes
from .transfers import Transfers
from .trips import Trips

__all__ = [
    # DataFrameModel versions (pandera models)
    'Agency',
    'Calendar',
    'CalendarDates',
    'FareAttributes',
    'FareRules',
    'FeedInfo',
    'Frequencies',
    'Routes',
    'Shapes',
    'Stops',
    'StopTimes',
    'Transfers',
    'Trips'
]
