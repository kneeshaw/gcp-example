"""
Streaming uploader (legacy tabledata.insertAll via client.insert_rows)

Minimal overhead streaming insert without server-side insertId dedupe.
Relies on an existing table managed by Terraform; does not create tables.
"""
from typing import Dict, Any
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def upload(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table_name: str,
) -> Dict[str, Any]:
    
    table_id = f"{project_id}.{dataset}.{table_name}"
    client = bigquery.Client(project=project_id)

    if df is None or df.empty:
        return {"table": table_id, "rows_processed": 0, "method": "streaming", "errors": 0}

    # Ensure table exists (schema managed by Terraform)
    try:
        table = client.get_table(table_id)
    except NotFound:
        raise RuntimeError(
            f"BigQuery table not found: {table_id}. Create it via Terraform "
            "using the generated JSON schema files before running uploads."
        )

    # Convert DataFrame rows
    rows: list[dict] = []
    for _, row in df.iterrows():
        obj: dict = {}
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

    errors = client.insert_rows(table, rows)
    if not errors:
        return {"table": table_id, "rows_processed": len(df), "method": "streaming", "errors": 0}

    return {"table": table_id, "rows_processed": len(df), "method": "streaming", "errors": errors, "error_count": len(errors)}
