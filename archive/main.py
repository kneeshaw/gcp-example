"""
Cloud Function entrypoint for decoupled GTFS transforms.

This module simply re-exports the shared runner in ``transform.main`` so the
deployment signature remains ``main.run``.
"""

from transform.main import run  # noqa: F401
