# src/transform/transform_schedule.py
"""Schedule transform helpers for cached GTFS static feeds."""

from __future__ import annotations

import io
import zipfile
from time import monotonic
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from google.cloud import storage

from common.logging_utils import logger
from schemas.common.schema_utils import clean_and_validate_dataframe
from schemas.schema_registry import get_schema_class
from big_query.batch_insert import insert_batch
from .transform_utils import Timer, build_response, read_blob_bytes


def _extract_feed_hash(blob: storage.Blob) -> str:
    metadata = blob.metadata or {}
    if "hash" in metadata and metadata["hash"]:
        return metadata["hash"]

    name = blob.name.rsplit("/", 1)[-1]
    if name.endswith(".zip"):
        name = name[:-4]
    return name


def _transform_schedule_df(dataset: str, df: pd.DataFrame) -> pd.DataFrame:
    schema_class = get_schema_class(dataset)

    if schema_class is None:
        logger.error("No schema class registered for dataset: %s", dataset)
        return pd.DataFrame()

    try:
        return clean_and_validate_dataframe(df, schema_class)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("Schema transformation/validation failed: %s", exc)
        return pd.DataFrame()


def _process_schedule_blob(cfg: Dict[str, str], blob: storage.Blob, zip_bytes: bytes) -> Dict[str, object]:
    """Unpack a cached schedule ZIP and load each table, logging timings."""

    blob_start = monotonic()
    feed_hash = _extract_feed_hash(blob)
    tables_loaded: List[Dict[str, object]] = []
    files_skipped: List[str] = []
    errors: List[Dict[str, str]] = []
    rows_inserted = 0

    logger.info("Schedule blob start: name=%s hash=%s", blob.name, feed_hash)

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            for filename in archive.namelist():
                if filename.endswith("/"):
                    continue

                read_start = monotonic()
                try:
                    with archive.open(filename) as file_handle:
                        df_raw = pd.read_csv(file_handle)
                    read_seconds = monotonic() - read_start
                except Exception as exc:
                    read_seconds = monotonic() - read_start
                    logger.error(
                        "Schedule file read failed: blob=%s file=%s error=%s read=%.2fs",
                        blob.name,
                        filename,
                        exc,
                        read_seconds,
                    )
                    errors.append({"file": filename, "error": f"read_csv_failed: {exc}"})
                    continue

                if df_raw.empty:
                    logger.info(
                        "Schedule file skipped (empty): blob=%s file=%s read=%.2fs",
                        blob.name,
                        filename,
                        read_seconds,
                    )
                    files_skipped.append(filename)
                    continue

                df_raw["feed_hash"] = feed_hash
                dataset_key = filename.split(".")[0]

                transform_start = monotonic()
                df_processed = _transform_schedule_df(dataset_key, df_raw)
                transform_seconds = monotonic() - transform_start
                if df_processed.empty:
                    logger.info(
                        "Schedule file skipped (post-transform empty): blob=%s file=%s read=%.2fs transform=%.2fs",
                        blob.name,
                        filename,
                        read_seconds,
                        transform_seconds,
                    )
                    files_skipped.append(filename)
                    continue

                table_name = f"{cfg['spec']}_{dataset_key.replace('-', '_')}"
                insert_seconds = 0.0
                try:
                    insert_start = monotonic()
                    result = insert_batch(
                        df_processed,
                        table_name,
                        cfg["project_id"],
                        cfg["bq_dataset"],
                    )
                    insert_seconds = monotonic() - insert_start
                except Exception as exc:  # pragma: no cover - upload safety net
                    insert_seconds = monotonic() - insert_start if insert_seconds == 0.0 else insert_seconds
                    logger.error(
                        "Schedule file insert failed: blob=%s file=%s table=%s error=%s read=%.2fs transform=%.2fs insert=%.2fs",
                        blob.name,
                        filename,
                        table_name,
                        exc,
                        read_seconds,
                        transform_seconds,
                        insert_seconds,
                    )
                    errors.append({"file": filename, "error": f"bq_upload_failed: {exc}"})
                    continue

                tables_loaded.append(
                    {
                        "file": filename,
                        "rows": len(df_processed),
                        "table": table_name,
                        "result": result,
                        "timings": {
                            "read_seconds": read_seconds,
                            "transform_seconds": transform_seconds,
                            "insert_seconds": insert_seconds,
                        },
                    }
                )
                rows_inserted += len(df_processed)

                logger.info(
                    "Schedule file loaded: blob=%s file=%s table=%s rows=%d read=%.2fs transform=%.2fs insert=%.2fs",
                    blob.name,
                    filename,
                    table_name,
                    len(df_processed),
                    read_seconds,
                    transform_seconds,
                    insert_seconds,
                )

    except zipfile.BadZipFile as exc:
        errors.append({"file": blob.name, "error": f"bad_zip: {exc}"})
        logger.error("Schedule blob invalid ZIP: name=%s error=%s", blob.name, exc)

    total_seconds = monotonic() - blob_start
    logger.info(
        "Schedule blob complete: name=%s hash=%s tables=%d rows=%d skipped=%d errors=%d duration=%.2fs",
        blob.name,
        feed_hash,
        len(tables_loaded),
        rows_inserted,
        len(files_skipped),
        len(errors),
        total_seconds,
    )

    return {
        "feed_hash": feed_hash,
        "tables_loaded": tables_loaded,
        "files_skipped": files_skipped,
        "errors": errors,
        "rows": rows_inserted,
    }


def process_schedule_batch(
    cfg: Dict[str, str],
    storage_client: storage.Client,
    batch: Sequence[storage.Blob],
) -> Tuple[Dict[str, object], int]:
    """Process a batch of cached schedule blobs."""

    logger.info(
        "Processing schedule batch: feeds=%d cache_prefix=%s",
        len(batch),
        cfg["cache_prefix"],
    )

    processed = 0
    deleted = 0
    total_rows = 0
    aggregate_tables: List[Dict[str, object]] = []
    skipped_files: List[str] = []
    feed_hashes: List[str] = []
    run_errors: List[Dict[str, str]] = []

    for blob in batch:
        logger.info("Processing schedule feed blob=%s", blob.name)
        zip_bytes = read_blob_bytes(blob, log_details=True)

        with Timer() as blob_timer:
            result = _process_schedule_blob(cfg, blob, zip_bytes)

        feed_hashes.append(result["feed_hash"])
        aggregate_tables.extend(result["tables_loaded"])
        skipped_files.extend(result["files_skipped"])
        run_errors.extend(result["errors"])
        total_rows += result["rows"]
        processed += 1

        logger.info(
            "Schedule feed processed: hash=%s tables=%d rows=%d duration=%.2fs errors=%d",
            result["feed_hash"],
            len(result["tables_loaded"]),
            result["rows"],
            blob_timer.duration,
            len(result["errors"]),
        )

        if not result["errors"]:
            try:
                blob.delete()
                deleted += 1
            except Exception as exc:  # pragma: no cover - defensive delete
                logger.error("Failed to delete processed cache blob %s: %s", blob.name, exc)
        else:
            logger.warning(
                "Retaining cache blob %s due to errors: %s",
                blob.name,
                result["errors"],
            )

    logger.info(
        "Schedule batch summary: feeds=%d rows=%d deleted=%d skipped_files=%d errors=%d",
        processed,
        total_rows,
        deleted,
        len(skipped_files),
        len(run_errors),
    )

    status = "ok" if not run_errors else "partial"
    http_code = 200 if not run_errors else 500

    return build_response(
        cfg["dataset"],
        status,
        http_code=http_code,
        feeds=processed,
        rows=total_rows,
        table_results=aggregate_tables,
        skipped_files=skipped_files,
        feed_hashes=feed_hashes,
        deleted=deleted,
        errors=run_errors,
    )
