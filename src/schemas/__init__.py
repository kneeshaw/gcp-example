"""
Centralized Schema Definitions
============================

This module provides centralized schema definitions for GTFS data processing.

Schedule schemas define the structure of raw GTFS Schedule tables.
Derived schemas define the structure of processed/analytics datasets.
"""

from .schedule import *
from .derived import *

__version__ = "1.0.0"
