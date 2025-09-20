"""
Common Columns Mixins
=====================

Defines shared Pandera columns to be mixed into DataFrameModel schemas.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class TimestampMixin(pa.DataFrameModel):
    """
    Mixin that adds warehouse timestamps to all tables.

    - created_at: when row was first written to the warehouse
    - updated_at: when row was last updated in the warehouse
    """

    created_at: Series[pd.Timestamp] = pa.Field(nullable=True, description="Row creation timestamp (warehouse)")
    updated_at: Series[pd.Timestamp] = pa.Field(nullable=True, description="Last update timestamp (warehouse)")

    class Config:
        strict = False
        coerce = True


__all__ = [
    "TimestampMixin",
]
