"""Streaming uploader with Redis-backed snapshot deduplication."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import redis
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

DEFAULT_SNAPSHOT_KEYS = ("entity_id", "record_id")
CAPTURED_AT_SUFFIX = ":captured_at"

logger = logging.getLogger(__name__)


def upload(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table_name: str,
    *,
    use_snapshot: bool = False,
    snapshot_key_columns: Iterable[str] = DEFAULT_SNAPSHOT_KEYS,
    redis_url: Optional[str] = None,
    redis_key: Optional[str] = None,
    redis_client: Optional[redis.Redis] = None,
    snapshot_ttl_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    table_id = f"{project_id}.{dataset}.{table_name}"
    client = bigquery.Client(project=project_id)

    if df is None or df.empty:
        return {
            "table": table_id,
            "rows_processed": 0,
            "method": "streaming",
            "errors": 0,
            "skipped_duplicates": 0,
        }

    try:
        table = client.get_table(table_id)
    except NotFound as exc:
        raise RuntimeError(
            f"BigQuery table not found: {table_id}. Ensure it exists before streaming uploads."
        ) from exc

    key_columns: List[str] = list(snapshot_key_columns)
    snapshot_keys: Set[Tuple[Any, ...]] = set()
    skipped_duplicates = 0
    snapshot_resource: Optional[str] = None

    snapshot_source_df = df

    if use_snapshot:
        _validate_key_columns(table, key_columns)
        redis_client = _ensure_redis_client(redis_client, redis_url)
        redis_key = redis_key or f"{project_id}:{dataset}:{table_name}:snapshot"
        snapshot_resource = f"redis://{redis_key}"

        schema_map = {field.name: field for field in table.schema}
        snapshot_keys = _load_snapshot_keys(redis_client, redis_key, key_columns, schema_map)

        df, skipped_duplicates = _drop_seen_rows(df, snapshot_keys, key_columns)

        if df.empty:
            _overwrite_snapshot(
                redis_client,
                redis_key,
                snapshot_source_df,
                key_columns,
                snapshot_ttl_seconds,
            )
            return {
                "table": table_id,
                "rows_processed": 0,
                "method": "streaming",
                "errors": 0,
                "skipped_duplicates": skipped_duplicates,
                "snapshot_uri": snapshot_resource,
            }

    rows = _dataframe_to_rows(df)

    errors = client.insert_rows(table, rows)
    if not errors:
        if use_snapshot and redis_client and redis_key:
            _overwrite_snapshot(
                redis_client,
                redis_key,
                snapshot_source_df,
                key_columns,
                snapshot_ttl_seconds,
            )
        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "streaming",
            "errors": 0,
            "skipped_duplicates": skipped_duplicates,
            "snapshot_uri": snapshot_resource,
        }

    return {
        "table": table_id,
        "rows_processed": len(df),
        "method": "streaming",
        "errors": errors,
        "error_count": len(errors),
        "skipped_duplicates": skipped_duplicates,
        "snapshot_uri": snapshot_resource,
    }


def _ensure_redis_client(
    provided_client: Optional[redis.Redis], redis_url: Optional[str]
) -> redis.Redis:
    if provided_client:
        return provided_client
    if not redis_url:
        raise ValueError("redis_url or redis_client must be supplied when use_snapshot is True")
    return redis.Redis.from_url(redis_url)


def _validate_key_columns(table: bigquery.Table, key_columns: List[str]) -> None:
    table_fields = {field.name for field in table.schema}
    missing = [col for col in key_columns if col not in table_fields]
    if missing:
        raise ValueError(
            "Snapshot key columns missing from table schema: " + ", ".join(missing)
        )


def _load_snapshot_keys(
    client: redis.Redis,
    redis_key: str,
    key_columns: List[str],
    schema_map: Dict[str, bigquery.SchemaField],
) -> Set[Tuple[Any, ...]]:
    if not key_columns:
        return set()

    members = client.smembers(redis_key)
    if not members:
        return set()

    keys: Set[Tuple[Any, ...]] = set()
    for raw_member in members:
        try:
            decoded = raw_member.decode("utf-8") if isinstance(raw_member, bytes) else raw_member
            serialized_values = json.loads(decoded)
            if len(serialized_values) != len(key_columns):
                logger.warning(
                    "Ignoring snapshot member with mismatched column count for key %s", redis_key
                )
                continue
            typed_values = [
                _deserialize_snapshot_value(value, schema_map[col].field_type)
                for col, value in zip(key_columns, serialized_values)
            ]
            keys.add(tuple(_normalize_key_value(value) for value in typed_values))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to load snapshot member from Redis: %s", exc)
    return keys


def _drop_seen_rows(
    df: pd.DataFrame,
    existing_keys: Set[Tuple[Any, ...]],
    key_columns: List[str],
) -> Tuple[pd.DataFrame, int]:
    if not key_columns:
        return df, 0

    seen = set(existing_keys)
    mask: List[bool] = []
    skipped = 0
    for values in df[key_columns].itertuples(index=False, name=None):
        key = tuple(_normalize_key_value(value) for value in values)
        if key in seen:
            mask.append(False)
            skipped += 1
        else:
            mask.append(True)
            seen.add(key)

    filtered = df[mask]
    return filtered, skipped


def _overwrite_snapshot(
    client: redis.Redis,
    redis_key: str,
    source_df: pd.DataFrame,
    key_columns: List[str],
    ttl_seconds: Optional[int],
) -> None:
    if not key_columns:
        return

    if source_df is None or source_df.empty:
        key_frame = pd.DataFrame(columns=list(key_columns))
    else:
        key_frame = (
            source_df[list(key_columns)]
            .copy()
            .applymap(_normalize_key_value)
            .drop_duplicates()
        )

    members: List[str] = []
    for values in key_frame.itertuples(index=False, name=None):
        serialized = [_serialize_snapshot_value(value) for value in values]
        members.append(json.dumps(serialized, separators=(",", ":")))

    captured_at_key = f"{redis_key}{CAPTURED_AT_SUFFIX}"
    timestamp = datetime.utcnow().isoformat()

    pipeline = client.pipeline(transaction=True)
    pipeline.delete(redis_key)
    pipeline.delete(captured_at_key)
    if members:
        pipeline.sadd(redis_key, *members)
    pipeline.set(captured_at_key, timestamp)
    if ttl_seconds is not None:
        pipeline.expire(redis_key, ttl_seconds)
        pipeline.expire(captured_at_key, ttl_seconds)
    pipeline.execute()


def _dataframe_to_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        obj: Dict[str, Any] = {}
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                obj[col] = None
            elif hasattr(val, "to_pydatetime"):
                obj[col] = val.to_pydatetime()
            elif hasattr(val, "item"):
                obj[col] = val.item()
            else:
                obj[col] = val
        rows.append(obj)
    return rows


def _normalize_key_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _serialize_snapshot_value(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def _deserialize_snapshot_value(value: str, field_type: str) -> Any:
    if value == "" or value is None:
        return None

    field_type = field_type.upper()
    if field_type in {"INT64", "INTEGER"}:
        return int(value)
    if field_type in {"FLOAT64", "FLOAT"}:
        return float(value)
    if field_type in {"BOOL", "BOOLEAN"}:
        return value.lower() in {"true", "1", "t", "yes"}
    if field_type == "TIMESTAMP":
        return pd.to_datetime(value, utc=True)
    if field_type == "DATE":
        return pd.to_datetime(value).date()
    if field_type == "DATETIME":
        return pd.to_datetime(value)
    return value
