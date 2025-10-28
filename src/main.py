#  Centralized Entry Point for All Cloud Functions
"""
Unified entry point for all GTFS processing functions:
- Workers: Real-time data ingestion (vehicle-positions, trip-updates, etc.)
- Enqueuers: High-frequency task scheduling
- Future: Daily schedule analytics

This single entry point dispatches to appropriate handlers based on function type.
"""

import os
import sys
from typing import Dict, Any

# Add current directory to path for imports
sys.path.append('.')

def main(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Centralized dispatcher for all Cloud Functions.

    Determines function type from environment variables and routes accordingly.
    """
    function = os.environ.get('FUNCTION')
    if function in ('ingest', 'fetch'):
        return _handle_fetch(request)
    elif function == 'transform':
        return _handle_transform(request)
    elif function == 'generate':
        return _handle_daily_schedule(request)
    else:
        return _handle_enqueuer(request)

def _handle_fetch(request: Dict[str, Any]) -> Dict[str, Any]:
    """Handle fetch functions (HTTP-triggered fetch + cache to GCS)"""
    from fetch.main import run
    return run(request)

def _handle_transform(request: Dict[str, Any]) -> Dict[str, Any]:
    """Handle transform functions (batch process cached files to BigQuery)"""
    from transform.main import run
    return run(request)

def _handle_enqueuer(request: Dict[str, Any]) -> Dict[str, Any]:
    """Handle enqueuer functions (task scheduling)"""
    from enqueuer.main import enqueue
    return enqueue(request)

def _handle_daily_schedule(request: Dict[str, Any]) -> Dict[str, Any]:
    """Handle daily schedule generation functions"""
    from daily_schedule.main import run
    return run(request)