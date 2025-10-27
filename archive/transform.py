"""Common runner for schedule and realtime transform batches."""

from __future__ import annotations

import os
from typing import Dict, List, Tuple

from google.cloud import storage

from common.logging_utils import logger
from .transform_realtime import process_realtime_batch
from .transform_schedule import process_schedule_batch


def _get_config() -> Dict[str, str]:
    project_id = os.getenv("PROJECT_ID")
    bucket = os.getenv("BUCKET")
    dataset = os.getenv("DATASET")
    spec = os.getenv("SPEC")
    bq_dataset = os.getenv("BQ_DATASET")

    prefix_root = f"{spec}-{dataset}" if spec else dataset
    cache_prefix = f"{prefix_root}/cache/"
    final_prefix = f"{prefix_root}/"

    batch_size = 120

    return {
        "project_id": project_id,
        "bucket": bucket,
        "dataset": dataset,
        "spec": spec,
        "bq_dataset": bq_dataset,
        "cache_prefix": cache_prefix,
        "final_prefix": final_prefix,
        "batch_size": batch_size,
    }


def _list_cache_blobs(client: storage.Client, bucket: str, prefix: str) -> List[storage.Blob]:
    blobs = list(client.bucket(bucket).list_blobs(prefix=prefix))
    blobs.sort(key=lambda b: (b.time_created or 0, b.name))
    return blobs


def run(request) -> Tuple[Dict[str, object], int]:
    """Entry point orchestrating cached dataset transforms."""

    cfg = _get_config()
    dataset = cfg["dataset"]

    if dataset == "schedule":
        logger.info(
            "Starting schedule transform batch (bucket=%s, cache_prefix=%s)",
            cfg["bucket"],
            cfg["cache_prefix"],
        )
    else:
        logger.info(
            "Starting transform batch for %s (bucket=%s, cache_prefix=%s, batch_size=%d)",
            dataset,
            cfg["bucket"],
            cfg["cache_prefix"],
            cfg["batch_size"],
        )

    try:
        storage_client = storage.Client(project=cfg["project_id"])
        all_blobs = _list_cache_blobs(storage_client, cfg["bucket"], cfg["cache_prefix"])

        if not all_blobs:
            if dataset == "schedule":
                logger.info("No cached schedule feeds to process.")
                return {"status": "empty", "dataset": dataset, "feeds": 0, "rows": 0}, 200

            logger.info("No cached files to process.")
            return {"status": "empty", "dataset": dataset, "rows": 0, "processed": 0, "moved": 0}, 200

        batch = all_blobs[: cfg["batch_size"]]

        if dataset == "schedule":
            return process_schedule_batch(cfg, storage_client, batch)

        return process_realtime_batch(cfg, storage_client, batch)

    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Transform batch failed")
        return {"status": "error", "dataset": dataset, "error": str(exc)}, 500