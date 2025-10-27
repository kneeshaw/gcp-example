# src/fetch/main.py

import base64
import json
import os
from time import monotonic
from typing import Tuple

from common.logging_utils import logger
from fetch.fetch_utils import get_data
from gcs.storage_utils import upload_data_response, upload_schedule_response


def _get_env() -> dict:
    project_id = os.getenv("PROJECT_ID")
    bucket = os.getenv("BUCKET")
    dataset = os.getenv("DATASET")
    spec = os.getenv("SPEC")

    url = os.getenv("URL")
    headers_b64 = os.getenv("HEADERS")
    headers = {}
    if headers_b64:
        try:
            headers = json.loads(base64.b64decode(headers_b64).decode("utf-8"))
        except Exception:
            headers = {}
    response_type = (os.getenv("RESPONSE_TYPE") or "").lower()

    return {
        "project_id": project_id,
        "bucket": bucket,
        "dataset": dataset,
        "spec": spec,
        "url": url,
        "headers": headers,
        "response_type": response_type,
    }


def run(request) -> Tuple[dict, int]:
    """Fetch from agency API and store to GCS cache prefix only.

    This entrypoint aligns with the decoupled architecture: Fetch/Store here;
    a separate minutely job performs Transform + BigQuery upload.
    """
    cfg = _get_env()
    dataset = cfg["dataset"]
    logger.info(f"Starting Fetch and Store process for dataset={dataset}")

    try:
        fetch_start = monotonic()
        data_bytes = get_data(cfg["url"], cfg["headers"], cfg["response_type"])  # may be bytes or str-encoded JSON
        fetch_seconds = monotonic() - fetch_start
        payload_size = len(data_bytes) if data_bytes else 0
        logger.info(
            "Fetch complete: bytes=%d duration=%.2fs",
            payload_size,
            fetch_seconds,
        )

        if dataset == "schedule":
            store_start = monotonic()
            object_name, feed_hash, is_new = upload_schedule_response(
                cfg["bucket"],
                dataset,
                data_bytes,
                cfg["response_type"],
                cfg["spec"],
                use_cache_prefix=True,
            )

            if not is_new:
                return {
                    "status": "duplicate",
                    "dataset": dataset,
                    "object_name": object_name,
                    "feed_hash": feed_hash,
                }, 200

            store_seconds = monotonic() - store_start

        else:
            store_start = monotonic()
            object_name = upload_data_response(
                cfg["bucket"],
                dataset,
                data_bytes,
                cfg["response_type"],
                cfg["spec"],
                use_cache_prefix=True,
            )
            store_seconds = monotonic() - store_start
            
        logger.info(
            "Cache upload complete: object=%s duration=%.2fs",
            object_name,
            store_seconds,
        )

        response_body = {"status": "cached", "dataset": dataset, "object": object_name}

        return response_body, 200

    except Exception as e:
        logger.exception("Fetch+Store run failed")
        return {"status": "error", "dataset": dataset, "error": str(e)}, 500
