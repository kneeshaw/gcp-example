# src/transform/transform_realtime.py
"""Realtime transform helpers for cached GTFS-RT payloads."""

from __future__ import annotations

import json
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from google.cloud import storage

from common.logging_utils import logger
from schemas.common.schema_utils import clean_and_validate_dataframe
from schemas.common.schema_registry import get_schema_class
from big_query.batch_upsert import upsert_batch
from .transform_utils import (
    Timer,
    build_response,
    move_to_final,
    normalize_nested_json,
    read_blob_bytes,
)


def _to_utc_timestamp(value) -> pd.Timestamp:
    """Convert a variety of timestamp representations to UTC."""

    if isinstance(value, pd.Timestamp):
        return value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")

    try:
        return pd.to_datetime(value, utc=True)
    except Exception:  # pragma: no cover - defensive fallback
        return pd.Timestamp.utcnow().tz_localize("UTC")


def _extract_entities(payload: Dict[str, object]) -> List[Dict[str, object]]:
    """Return GTFS-RT entities from either top-level or response wrapper."""

    response = payload.get("response")
    if isinstance(response, dict) and "entity" in response:
        entities = response.get("entity", [])
    else:
        entities = payload.get("entity", [])

    if not isinstance(entities, list):
        return []

    return entities


def _transform_blobs(dataset: str, blobs: Sequence[storage.Blob]) -> Tuple[pd.DataFrame, int]:
    """Download, decode, and normalize a batch of realtime cache blobs."""

    frames: List[pd.DataFrame] = []
    processed = 0

    for blob in blobs:

        try:
            payload_bytes = read_blob_bytes(blob, log_details=True)
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as exc:
            logger.error("Failed to load realtime blob %s: %s", blob.name, exc)
            continue

        entities = _extract_entities(payload)
        if not entities:
            logger.info("Realtime blob %s contains no entities", blob.name)
            continue

        df_raw = normalize_nested_json(entities)
        if df_raw.empty:
            logger.info("Realtime blob %s normalized to empty dataframe", blob.name)
            continue

        updated_ts = _to_utc_timestamp(blob.updated or blob.time_created).floor("s")
        df_raw["created_at"] = updated_ts
        df_raw["updated_at"] = updated_ts
        df_raw["cache_blob"] = blob.name

        header = payload.get("header", {})
        if isinstance(header, dict) and "timestamp" in header:
            df_raw["header.timestamp"] = header.get("timestamp")

        frames.append(df_raw)
        processed += 1

    if not frames:
        return pd.DataFrame(), processed

    combined = pd.concat(frames, ignore_index=True)

    return combined, processed


def process_realtime_batch(
    cfg: Dict[str, str],
    storage_client: storage.Client,
    batch: Sequence[storage.Blob],
) -> Tuple[Dict[str, object], int]:
    """Process cached realtime blobs into BigQuery."""

    dataset = cfg["dataset"]

    with Timer() as download_timer:
        raw_df, processed = _transform_blobs(dataset, batch)

    raw_rows = len(raw_df)
    logger.info(
        "Realtime batch download complete: processed_blobs=%d raw_rows=%d duration=%.2fs",
        processed,
        raw_rows,
        download_timer.duration,
    )

    if raw_df.empty:
        logger.info("Realtime batch produced no rows; skipping upload.")
        return build_response(dataset, "no_rows", rows=0, processed=processed, moved=0)

    schema_class = get_schema_class(dataset)
    if schema_class is None:
        logger.error("No schema registered for realtime dataset %s", dataset)
        return build_response(dataset, "error", http_code=500, error="schema_not_found")

    with Timer() as transform_timer:
        df = clean_and_validate_dataframe(raw_df, schema_class)

    logger.info(
        "Realtime transformation complete: rows=%d duration=%.2fs",
        len(df),
        transform_timer.duration,
    )

    if df.empty:
        logger.info("Realtime batch had no rows after validation; skipping upload.")
        return build_response(dataset, "no_rows", rows=0, processed=processed, moved=0)

    table_name = f"stg_{dataset.replace('-', '_')}"

    try:
        with Timer() as upload_timer:
            result = upsert_batch(
                df,
                table_name,
                cfg["project_id"],
                cfg["bq_dataset"],
            )
        logger.info(
            "Realtime BigQuery upsert complete: table=%s rows=%d duration=%.2fs",
            table_name,
            len(df),
            upload_timer.duration,
        )
    except Exception as exc:  # pragma: no cover - upload safety net
        logger.exception("Realtime BigQuery upsert failed for %s", table_name)
        return build_response(dataset, "error", http_code=500, error=str(exc))

    with Timer() as move_timer:
        moved = move_to_final(
            storage_client,
            cfg["bucket"],
            batch,
            cache_prefix=cfg["cache_prefix"],
            final_prefix=cfg["final_prefix"],
        )

    logger.info(
        "Realtime cache move complete: moved=%d duration=%.2fs",
        moved,
        move_timer.duration,
    )

    logger.info(
        "Realtime batch complete: dataset=%s rows=%d processed_blobs=%d moved=%d",
        dataset,
        len(df),
        processed,
        moved,
    )

    return build_response(
        dataset,
        "ok",
        rows=len(df),
        processed=processed,
        moved=moved,
        bq=result,
    )
