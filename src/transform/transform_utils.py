"""Shared helpers for transform pipelines."""

from __future__ import annotations

import gzip
from time import monotonic
from typing import Callable, Dict, Iterable, Tuple, TypeVar

import pandas as pd
from google.cloud import storage

from common.logging_utils import logger

T = TypeVar("T")


def read_blob_bytes(blob: storage.Blob, *, log_details: bool = False) -> bytes:
    """Download a blob, optionally logging timing and compression details."""

    download_start = monotonic()
    raw_payload = blob.download_as_bytes()
    download_seconds = monotonic() - download_start

    payload = raw_payload
    decompressed = False
    decompress_seconds = 0.0

    if blob.content_encoding == "gzip" or blob.name.endswith(".gz"):
        try:
            decompress_start = monotonic()
            payload = gzip.decompress(raw_payload)
            decompress_seconds = monotonic() - decompress_start
            decompressed = True
        except OSError:
            payload = raw_payload

    return payload


def move_to_final(
    client: storage.Client,
    bucket: str,
    blobs: Iterable[storage.Blob],
    *,
    cache_prefix: str,
    final_prefix: str,
) -> int:
    """Copy processed blobs out of the cache prefix and delete originals."""

    moved = 0
    src_bucket = client.bucket(bucket)

    for blob in blobs:
        new_name = blob.name
        if cache_prefix in new_name:
            new_name = new_name.replace(cache_prefix, final_prefix, 1)
        else:
            new_name = new_name.replace("/cache/", "/", 1)

        try:
            src_bucket.copy_blob(blob, src_bucket, new_name)
            blob.delete()
            moved += 1
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Failed moving %s -> %s: %s", blob.name, new_name, exc)

    return moved


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self) -> None:
        self.duration: float = 0.0
        self._start: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = monotonic()
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # pragma: no cover - trivial
        self.duration = monotonic() - self._start


def run_with_timing(fn: Callable[[], T]) -> Tuple[T, float]:
    """Execute ``fn`` and return (result, seconds)."""

    start = monotonic()
    result = fn()
    return result, monotonic() - start


def build_response(dataset: str, status: str, http_code: int = 200, **payload) -> Tuple[Dict[str, object], int]:
    """Construct a standard transform response tuple."""

    body: Dict[str, object] = {"status": status, "dataset": dataset}
    body.update(payload)
    return body, http_code


def normalize_nested_json(entities) -> pd.DataFrame:
    """Flatten nested GTFS-RT entity payloads into a tabular DataFrame."""

    df_result = pd.json_normalize(entities, sep=".")
    max_iterations = 10

    for _ in range(max_iterations):
        list_columns = []
        for col in df_result.select_dtypes(include=["object"]):
            sample_values = df_result[col].dropna().head(5)
            if len(sample_values) > 0 and any(isinstance(val, list) for val in sample_values):
                list_columns.append(col)

        if not list_columns:
            break

        col = list_columns[0]
        df_result = df_result.explode(col).reset_index(drop=True)

    dict_columns = []
    for col in df_result.select_dtypes(include=["object"]):
        sample_values = df_result[col].dropna().head(5)
        if len(sample_values) > 0 and any(isinstance(val, dict) for val in sample_values):
            dict_columns.append(col)

    for col in dict_columns:
        non_null_dicts = df_result[col].dropna()
        if non_null_dicts.empty:
            continue

        try:
            normalized = pd.json_normalize(non_null_dicts).add_prefix(f"{col}.")
            normalized.index = df_result[df_result[col].notna()].index
            df_result = df_result.drop(columns=[col])
            df_result = pd.concat([df_result, normalized], axis=1)
        except Exception:  # pragma: no cover - defensive
            continue

    return df_result
