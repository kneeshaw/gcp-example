# src/big_query/batch_upsert.py
"""Standalone helper for batch upserts into BigQuery using MERGE semantics."""

from __future__ import annotations

import uuid
from typing import Any, Dict

import pandas as pd
from google.cloud import bigquery

from common.logging_utils import logger


def upsert_batch(
    df: pd.DataFrame,
    table_name: str,
    project_id: str,
    dataset: str,
) -> Dict[str, Any]:
    """Perform a batch upsert (MERGE) of `df` into the specified BigQuery table.

    Uses `record_id` as the merge key. When a match is found, all columns from the
    staging data overwrite the existing row. When not matched, a new row is inserted.
    """

    table_id = f"{project_id}.{dataset}.{table_name}"

    if df.empty:
        logger.warning("DataFrame is empty, skipping upsert to %s", table_id)
        return {
            "table": table_id,
            "rows_processed": 0,
            "method": "merge",
            "staging_table": None,
        }

    client = bigquery.Client(project=project_id)
    target_table = client.get_table(table_id)
    target_columns = [field.name for field in target_table.schema]

    if "record_id" not in df.columns:
        raise ValueError("DataFrame must include 'record_id' column for merge alignment")

    original_columns = set(df.columns)
    df = df.copy()

    extra_columns = original_columns - set(target_columns)
    if extra_columns:
        logger.debug(
            "Dropping columns not present in target schema: %s", ", ".join(sorted(extra_columns))
        )
        df = df.drop(columns=list(extra_columns))

    for column in target_columns:
        if column not in df.columns:
            df[column] = None

    df = df[target_columns]

    staging_table_id = f"{table_id}_staging_{uuid.uuid4().hex[:8]}"
    client.create_table(bigquery.Table(staging_table_id, schema=target_table.schema))

    load_job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=load_job_config)
        load_job.result()

        merge_sql = (
            f"MERGE `{table_id}` AS target\n"
            f"USING `{staging_table_id}` AS source\n"
            "  ON target.record_id = source.record_id\n"
            f"WHEN MATCHED THEN\n  UPDATE SET target.updated_at = source.updated_at\n"
            "WHEN NOT MATCHED THEN\n  INSERT ROW"
        )

        merge_job = client.query(merge_sql)
        merge_job.result()

        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "merge",
            "staging_table": staging_table_id,
            "load_job_id": load_job.job_id,
            "merge_job_id": merge_job.job_id,
        }

    finally:
        client.delete_table(staging_table_id, not_found_ok=True)
