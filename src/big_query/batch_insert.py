# src/big_query/batch_insert.py
"""Batch insert helper for BigQuery without merge/update semantics."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd
from google.cloud import bigquery

from common.logging_utils import logger


def insert_batch(
    df: pd.DataFrame,
    table_name: str,
    project_id: str,
    dataset: str,
) -> Dict[str, Any]:
    """Append the provided ``df`` into the target BigQuery table.

    Unlike :func:`batch_upsert.upsert_batch`, this helper performs a straight
    ``WRITE_APPEND`` load and does not attempt to update existing rows. Use
    when the upstream pipeline guarantees deduplication or when historical
    inserts are desired.
    """

    table_id = f"{project_id}.{dataset}.{table_name}"

    if df.empty:
        logger.warning("DataFrame is empty, skipping insert to %s", table_id)
        return {
            "table": table_id,
            "rows_processed": 0,
            "method": "insert",
            "job_id": None,
        }

    client = bigquery.Client(project=project_id)

    try:
        target_table = client.get_table(table_id)
    except Exception as exc:
        logger.error("Failed to fetch BigQuery table %s: %s", table_id, exc)
        raise

    target_columns = [field.name for field in target_table.schema]

    df_to_load = df.copy()
    extra_columns = set(df_to_load.columns) - set(target_columns)
    if extra_columns:
        logger.debug(
            "Dropping columns not present in target schema: %s",
            ", ".join(sorted(extra_columns)),
        )
        df_to_load = df_to_load.drop(columns=list(extra_columns))

    for column in target_columns:
        if column not in df_to_load.columns:
            df_to_load[column] = None

    df_to_load = df_to_load[target_columns]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_dataframe(df_to_load, table_id, job_config=job_config)
    load_job.result()

    return {
        "table": table_id,
        "rows_processed": len(df_to_load),
        "method": "insert",
        "job_id": load_job.job_id,
    }
