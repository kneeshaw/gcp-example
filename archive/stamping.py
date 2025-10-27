"""
Timestamp stamping helpers for ingestion.
"""

from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone


def stamp_created_updated(df: pd.DataFrame, now: datetime | None = None) -> pd.DataFrame:
    """
    Ensure created_at and updated_at columns exist and are populated.

    - created_at: set only if missing/null (preserve existing on updates)
    - updated_at: set to current time (always)
    """
    if df is None or df.empty:
        return df

    ts = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    result = df.copy()

    if 'created_at' not in result.columns:
        result['created_at'] = pd.NaT

    if 'updated_at' not in result.columns:
        result['updated_at'] = pd.NaT

    # Only fill created_at where null
    result['created_at'] = pd.to_datetime(result['created_at'], utc=True, errors='coerce')
    result['created_at'] = result['created_at'].fillna(ts)

    # Always update updated_at
    result['updated_at'] = ts

    return result


__all__ = [
    'stamp_created_updated',
]
