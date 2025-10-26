# src/fetch/main.py

import base64
import json
import os
from typing import Tuple

from common.logging_utils import logger
from fetch import get_data
from gcs.store import upload_data_response


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
    logger.info(f"Fetch+Store start for dataset={dataset}")

    try:
        data_bytes = get_data(cfg["url"], cfg["headers"], cfg["response_type"])  # may be bytes or str-encoded JSON
        logger.info(
            f"Fetched {len(data_bytes) if data_bytes else 0} bytes for {dataset}"
        )

        cache_dataset = f"{dataset}/cache"
        object_name = upload_data_response(
            cfg["bucket"],
            cache_dataset,
            data_bytes,
            cfg["response_type"],
            cfg["spec"],
        )
        logger.info(f"Cached payload to GCS: {object_name}")
        return {"status": "cached", "dataset": dataset, "object": object_name}, 200

    except Exception as e:
        logger.exception("Fetch+Store run failed")
        return {"status": "error", "dataset": dataset, "error": str(e)}, 500
