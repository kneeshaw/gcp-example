"""
Streaming uploader (legacy tabledata.insertAll via client.insert_rows)

Adds a lightweight snapshot-based deduplication option suitable for sequential
duplicate bursts. The snapshot table stores the key columns from the previous
cycle so the next run can skip any immediate repeats.
"""
import io
from datetime import datetime
from typing import Dict, Any, Iterable, List, Optional, Set, Tuple

import pandas as pd
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

DEFAULT_SNAPSHOT_KEYS = ("entity_id", "record_id")


def upload(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table_name: str,
    *,
    use_snapshot: bool = False,
    snapshot_key_columns: Iterable[str] = DEFAULT_SNAPSHOT_KEYS,
    snapshot_bucket: Optional[str] = None,
    snapshot_object: Optional[str] = None,
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

    # Ensure table exists (schema managed by Terraform)
    try:
        table = client.get_table(table_id)
    except NotFound:
        raise RuntimeError(
            f"BigQuery table not found: {table_id}. Create it via Terraform "
            "using the generated JSON schema files before running uploads."
        )

    snapshot_keys: Set[Tuple[Any, ...]] = set()
    key_columns: List[str] = list(snapshot_key_columns)
    skipped_duplicates = 0
    snapshot_uri: Optional[str] = None

    snapshot_source_df = df

    if use_snapshot:
        _validate_key_columns(table, key_columns)
        if not snapshot_bucket:
            raise ValueError("snapshot_bucket must be provided when use_snapshot is True")
        snapshot_object = snapshot_object or f"{dataset}/{table_name}_snapshot.csv"
        snapshot_uri = f"gs://{snapshot_bucket}/{snapshot_object}"

        schema_map = {field.name: field for field in table.schema}
        snapshot_keys = _load_snapshot_keys(snapshot_bucket, snapshot_object, key_columns, schema_map)

        df, skipped_duplicates = _drop_seen_rows(df, snapshot_keys, key_columns)

        if df.empty:
            # Ensure snapshot still reflects most recent keys (original data)
            _overwrite_snapshot(snapshot_bucket, snapshot_object, snapshot_source_df, key_columns)
            return {
                "table": table_id,
                "rows_processed": 0,
                "method": "streaming",
                "errors": 0,
                "skipped_duplicates": skipped_duplicates,
                "snapshot_uri": snapshot_uri,
            }

    # Convert DataFrame rows
    rows = _dataframe_to_rows(df)

    errors = client.insert_rows(table, rows)
    if not errors:
        if use_snapshot:
            _overwrite_snapshot(snapshot_bucket, snapshot_object, snapshot_source_df, key_columns)
        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "streaming",
            "errors": 0,
            "skipped_duplicates": skipped_duplicates,
            "snapshot_uri": snapshot_uri,
        }

    return {
        "table": table_id,
        "rows_processed": len(df),
        "method": "streaming",
        "errors": errors,
        "error_count": len(errors),
        "skipped_duplicates": skipped_duplicates,
        "snapshot_uri": snapshot_uri,
    }


def _validate_key_columns(table: bigquery.Table, key_columns: List[str]) -> None:
    """Ensure key columns exist on the target table."""

    table_fields = {field.name for field in table.schema}
    missing = [col for col in key_columns if col not in table_fields]
    if missing:
        raise ValueError(
            "Snapshot key columns missing from table schema: " + ", ".join(missing)
        )


def _load_snapshot_keys(
    bucket: str,
    object_name: str,
    key_columns: List[str],
    schema_map: Dict[str, bigquery.SchemaField],
) -> Set[Tuple[Any, ...]]:
    """Load existing snapshot key tuples from GCS."""

    if not key_columns:
        return set()

    storage_client = storage.Client()
    blob = storage_client.bucket(bucket).blob(object_name)
    if not blob.exists():
        return set()

    data = blob.download_as_text()
    if not data:
        return set()

    dataframe = pd.read_csv(io.StringIO(data), dtype=str, keep_default_na=False)
    if dataframe.empty:
        return set()

    missing = [col for col in key_columns if col not in dataframe.columns]
    if missing:
        raise ValueError(
            "Snapshot CSV missing required key columns: " + ", ".join(missing)
        )

    keys = set()
    for values in dataframe[key_columns].itertuples(index=False, name=None):
        typed_values = [
            _deserialize_snapshot_value(value, schema_map[col].field_type)
            for col, value in zip(key_columns, values)
        ]
        keys.add(tuple(_normalize_key_value(value) for value in typed_values))
    return keys


def _drop_seen_rows(
    df: pd.DataFrame,
    existing_keys: Set[Tuple[Any, ...]],
    key_columns: List[str],
) -> Tuple[pd.DataFrame, int]:
    """Filter out rows whose key tuple already exists."""

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
    bucket: str,
    object_name: str,
    source_df: pd.DataFrame,
    key_columns: List[str],
) -> None:
    """Replace snapshot contents with the latest key set in GCS."""

    if not key_columns:
        return

    if source_df is None or source_df.empty:
        key_frame = pd.DataFrame(columns=list(key_columns))
    else:
        key_frame = source_df[list(key_columns)].copy().applymap(_normalize_key_value).drop_duplicates()

    payload = key_frame.applymap(_serialize_snapshot_value)
    payload["captured_at"] = datetime.utcnow().isoformat()

    csv_buffer = io.StringIO()
    payload.to_csv(csv_buffer, index=False)

    storage_client = storage.Client()
    blob = storage_client.bucket(bucket).blob(object_name)
    blob.cache_control = "no-store"
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")


def _dataframe_to_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DataFrame rows to BigQuery compatible dictionaries."""

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
    """Standardise key values for hashing comparisons."""

    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _serialize_snapshot_value(value: Any) -> str:
    """Serialize a snapshot key into a CSV-friendly string."""

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def _deserialize_snapshot_value(value: str, field_type: str) -> Any:
    """Deserialize a CSV string back into the expected BigQuery type."""

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
