# src/big_query/batch_upsert.py
"""Standalone helper for batch upserts into BigQuery using MERGE semantics."""

from __future__ import annotations

import uuid
from typing import Any, Dict
from datetime import datetime, timedelta, timezone

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

        # Determine partition window bounds from the batch to enable partition pruning on target.
        # Expect a 'timestamp' column in df per schema. Coerce to UTC-aware datetimes.
        if "timestamp" in df.columns and len(df) > 0:
            ts_series = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            ts_min = ts_series.min()
            ts_max = ts_series.max()
        else:
            ts_min = None
            ts_max = None

        # Compute [lb, ub) window at day granularity; default to current day if unknown
        now_utc = datetime.now(timezone.utc)
        if pd.isna(ts_min) or pd.isna(ts_max):
            lb = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)
            ub = lb + timedelta(days=1)
        else:
            lb = datetime(ts_min.year, ts_min.month, ts_min.day, tzinfo=timezone.utc)
            # If all within same day, choose next day; else use day after max
            if ts_max.tzinfo is None:
                ts_max = ts_max.replace(tzinfo=timezone.utc)
            next_day_after_max = datetime(ts_max.year, ts_max.month, ts_max.day, tzinfo=timezone.utc) + timedelta(days=1)
            ub = next_day_after_max

        logger.debug("Partition window for MERGE: lb=%s, ub=%s (UTC)", lb.isoformat(), ub.isoformat())

        # Build dynamic MERGE SQL
        # For WHEN MATCHED: update all columns except created_at (preserve existing created_at)
        updatable_columns = [col for col in target_columns if col != "created_at"]
        update_clauses = [f"target.{col} = source.{col}" for col in updatable_columns]

        # Note: We reference @lb/@ub both in USING filter and ON clause to allow partition pruning
        merge_sql = (
            f"MERGE `{table_id}` AS target\n"
            f"USING (\n"
            f"  SELECT * FROM `{staging_table_id}`\n"
            f"  WHERE timestamp >= @lb AND timestamp < @ub\n"
            f") AS source\n"
            "  ON target.record_id = source.record_id\n"
            " AND target.timestamp >= @lb AND target.timestamp < @ub\n"
            f"WHEN MATCHED THEN\n"
            f"  UPDATE SET {', '.join(update_clauses)}\n"
            "WHEN NOT MATCHED THEN\n"
            "  INSERT ROW"
        )

        logger.debug("Executing MERGE SQL with parameters lb/ub:\n%s", merge_sql)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lb", "TIMESTAMP", lb),
                bigquery.ScalarQueryParameter("ub", "TIMESTAMP", ub),
            ]
        )

        merge_job = client.query(merge_sql, job_config=job_config)
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
