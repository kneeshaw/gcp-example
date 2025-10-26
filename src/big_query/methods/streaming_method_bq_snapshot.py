"""
Streaming uploader (legacy tabledata.insertAll via client.insert_rows)

Adds a lightweight snapshot-based deduplication option suitable for sequential
duplicate bursts. The snapshot table stores the key columns from the previous
cycle so the next run can skip any immediate repeats.
"""
from typing import Dict, Any, Iterable, List, Optional, Set, Tuple

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

SNAPSHOT_SUFFIX = "_snapshot"
DEFAULT_SNAPSHOT_KEYS = ("entity_id", "record_id")


def upload(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table_name: str,
    *,
    use_snapshot: bool = False,
    snapshot_key_columns: Iterable[str] = DEFAULT_SNAPSHOT_KEYS,
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

    snapshot_table: Optional[bigquery.Table] = None
    snapshot_keys: Set[Tuple[Any, ...]] = set()
    key_columns: List[str] = list(snapshot_key_columns)
    skipped_duplicates = 0

    snapshot_source_df = df

    if use_snapshot:
        _validate_key_columns(table, key_columns)
        snapshot_table = _ensure_snapshot_table(
            client,
            f"{project_id}.{dataset}.{table_name}{SNAPSHOT_SUFFIX}",
            table,
            key_columns,
        )
        snapshot_keys = _load_snapshot_keys(client, snapshot_table, key_columns)

        df, skipped_duplicates = _drop_seen_rows(df, snapshot_keys, key_columns)

        if df.empty:
            # Ensure snapshot still reflects most recent keys (original data)
            if snapshot_table is not None:
                _overwrite_snapshot(client, snapshot_table, snapshot_source_df, key_columns)
            return {
                "table": table_id,
                "rows_processed": 0,
                "method": "streaming",
                "errors": 0,
                "skipped_duplicates": skipped_duplicates,
            }

    # Convert DataFrame rows
    rows = _dataframe_to_rows(df)

    errors = client.insert_rows(table, rows)
    if not errors:
        if use_snapshot and snapshot_table is not None:
            _overwrite_snapshot(client, snapshot_table, snapshot_source_df, key_columns)
        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "streaming",
            "errors": 0,
            "skipped_duplicates": skipped_duplicates,
        }

    return {
        "table": table_id,
        "rows_processed": len(df),
        "method": "streaming",
        "errors": errors,
        "error_count": len(errors),
        "skipped_duplicates": skipped_duplicates,
    }


def _validate_key_columns(table: bigquery.Table, key_columns: List[str]) -> None:
    """Ensure key columns exist on the target table."""

    table_fields = {field.name for field in table.schema}
    missing = [col for col in key_columns if col not in table_fields]
    if missing:
        raise ValueError(
            "Snapshot key columns missing from table schema: " + ", ".join(missing)
        )


def _ensure_snapshot_table(
    client: bigquery.Client,
    snapshot_table_id: str,
    source_table: bigquery.Table,
    key_columns: List[str],
) -> bigquery.Table:
    """Create the snapshot table if it does not exist."""

    try:
        table = client.get_table(snapshot_table_id)
        _validate_snapshot_schema(table, key_columns)
        return table
    except NotFound:
        schema_map = {field.name: field for field in source_table.schema}
        snapshot_schema = []
        for col in key_columns:
            snapshot_schema.append(
                bigquery.SchemaField(
                    name=col,
                    field_type=schema_map[col].field_type,
                    mode="NULLABLE",
                )
            )
        snapshot_schema.append(bigquery.SchemaField("captured_at", "TIMESTAMP", mode="NULLABLE"))

        snapshot_table = bigquery.Table(snapshot_table_id, schema=snapshot_schema)
        return client.create_table(snapshot_table)


def _load_snapshot_keys(
    client: bigquery.Client,
    snapshot_table: bigquery.Table,
    key_columns: List[str],
) -> Set[Tuple[Any, ...]]:
    """Load existing snapshot key tuples."""

    if not key_columns:
        return set()

    _validate_snapshot_schema(snapshot_table, key_columns)

    rows = client.list_rows(
        snapshot_table,
        selected_fields=[field for field in snapshot_table.schema if field.name in key_columns],
    )

    dataframe = rows.to_dataframe()
    if dataframe.empty:
        return set()

    keys = set()
    for values in dataframe[key_columns].itertuples(index=False, name=None):
        keys.add(tuple(_normalize_key_value(value) for value in values))
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
    client: bigquery.Client,
    snapshot_table: bigquery.Table,
    source_df: pd.DataFrame,
    key_columns: List[str],
) -> None:
    """Replace snapshot contents with the latest key set."""

    if not key_columns:
        return

    if source_df is None or source_df.empty:
        key_frame = pd.DataFrame(columns=list(key_columns))
    else:
        key_frame = source_df[list(key_columns)].copy().applymap(_normalize_key_value).drop_duplicates()

    key_frame["captured_at"] = pd.Timestamp.utcnow().replace(tzinfo=None)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=snapshot_table.schema,
    )

    job = client.load_table_from_dataframe(key_frame, snapshot_table.reference, job_config=job_config)
    job.result()


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


def _validate_snapshot_schema(snapshot_table: bigquery.Table, key_columns: List[str]) -> None:
    """Ensure the snapshot table exposes all required key columns."""

    if not key_columns:
        return

    snapshot_fields = {field.name for field in snapshot_table.schema}
    missing = [col for col in key_columns if col not in snapshot_fields]
    if missing:
        raise ValueError(
            "Snapshot table missing required key columns: " + ", ".join(missing)
        )
