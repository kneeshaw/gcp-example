"""
Batch uploader using load_table_from_dataframe.

Appends data to an existing BigQuery table. This method does not create tables or
manage schemas; use Terraform (generated from Pandera models) to provision tables
with the desired schema, partitioning, and clustering. If the table is missing,
an explicit error is raised with guidance to apply infrastructure first.
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
        return {"table": table_id, "rows_processed": 0, "method": "batch"}

    # Ensure table exists (schema is managed by Terraform)
    try:
        client.get_table(table_id)
    except NotFound:
        raise RuntimeError(
            f"BigQuery table not found: {table_id}. Create it via Terraform "
            "using the generated JSON schema files before running uploads."
        )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Schema is not provided here; the existing table schema is authoritative.
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    return {"table": table_id, "rows_processed": len(df), "method": "batch", "job_id": job.job_id}

