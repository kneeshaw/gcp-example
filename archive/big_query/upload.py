""" 
Simplified BigQuery Upload Utilities

This module provides a unified, simple interface for uploading data to BigQuery.
It consolidates multiple ingestion methods into one smart function while maintaining
streaming capabilities for real-time data.
"""

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery_storage import BigQueryWriteClient
from google.cloud.bigquery_storage_v1 import types
from typing import Optional, List, Literal, Dict, Any, Union
from datetime import datetime, timezone
import uuid
import time
import pandera.pandas as pa

from common.logging_utils import logger
from common.stamping import stamp_created_updated


def upload_to_bigquery(
    df: pd.DataFrame,
    table_name: str,
    project_id: str,
    bq_dataset: str,
    schema: Optional[List[bigquery.SchemaField]] = None,
    upload_method: str = "auto",
    deduplication_mode: str = "none"
) -> Dict[str, Any]:
    """
    Unified BigQuery upload function with separate transport and deduplication control.

    Args:
        df: DataFrame to upload
        table_name: Table name (without project/dataset prefix)
        project_id: BigQuery project ID
        bq_dataset: BigQuery dataset name
        schema: BigQuery schema (auto-detected if None)
        upload_method: Transport method - "streaming", "batch", "storage_api", "auto"
        deduplication_mode: Duplicate handling - "none", "skip_duplicates", "merge_tracking"

    Returns:
        Dict with upload results and metadata
    """
    # Start timing as soon as we enter the function
    start_time = time.time()
    
    if df.empty:
        table_id = f"{project_id}.{bq_dataset}.{table_name}"
        logger.warning(f"DataFrame is empty, skipping upload to {table_id}")
        return {
            "table": table_id, 
            "rows_processed": 0, 
            "method": "skipped",
            "processing_time": round(time.time() - start_time, 3)
        }

    # Construct full table ID
    table_id = f"{project_id}.{bq_dataset}.{table_name}"

    # Create BigQuery client for the specified project
    client = bigquery.Client(project=project_id)
    logger.debug(f"Created BigQuery client for project: {project_id}")

    # Stamp warehouse timestamps
    df = stamp_created_updated(df)

    # Auto-detect schema if not provided
    if schema is None:
        schema = _dataframe_to_bigquery_schema(df)

    # Determine transport method
    if upload_method == "auto":
        transport = _choose_optimal_mode(df)
    elif upload_method in ["streaming", "batch", "storage_api"]:
        transport = upload_method
    else:
        raise ValueError(f"Unknown upload_method: {upload_method}")

    logger.info(f"Uploading {len(df)} rows to {table_id} using {transport} transport, {deduplication_mode} deduplication")

    # Route based on deduplication needs
    if deduplication_mode == "merge_tracking":
        result = _upload_with_tracking(client, df, table_id, schema)
        result["processing_time"] = round(time.time() - start_time, 3)
        return result
    
    elif deduplication_mode == "skip_duplicates":
        # Deduplicate DataFrame first, then use regular streaming
        deduplicated_df, skipped_count = _deduplicate_dataframe(client, df, table_id)
        if deduplicated_df.empty:
            return {
                "table": table_id,
                "rows_processed": 0,
                "method": "streaming_dedup",
                "skipped_existing": skipped_count,
                "processing_time": round(time.time() - start_time, 3)
            }
        # Use regular streaming for the deduplicated data
        result = _upload_streaming(client, deduplicated_df, table_id, schema)
        # Add deduplication info to result
        result["skipped_existing"] = skipped_count
        result["method"] = "streaming_dedup"
        result["processing_time"] = round(time.time() - start_time, 3)
        return result
    
    elif deduplication_mode == "none":
        if transport == "streaming":
            result = _upload_streaming(client, df, table_id, schema)
            result["processing_time"] = round(time.time() - start_time, 3)
            return result
        else:
            result = _upload_batch(client, df, table_id, schema)
            result["processing_time"] = round(time.time() - start_time, 3)
            return result
    
    else:
        raise ValueError(f"Unknown deduplication_mode: {deduplication_mode}")


def _choose_optimal_mode(df: pd.DataFrame) -> str:
    """Choose optimal upload mode based on data characteristics."""
    row_count = len(df)
    data_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

    # Streaming for small, real-time data
    if row_count < 1000 and data_size_mb < 10:
        return "streaming"

    # Batch for large datasets
    if row_count > 10000 or data_size_mb > 100:
        return "batch"

    # Default to streaming for moderate real-time data
    return "streaming"


def _dataframe_to_bigquery_schema(df: pd.DataFrame) -> List[bigquery.SchemaField]:
    """Convert pandas DataFrame to BigQuery schema."""
    schema = []
    for col in df.columns:
        dtype = df[col].dtype

        if pd.api.types.is_integer_dtype(dtype):
            bq_type = "INT64"
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = "FLOAT64"
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = "BOOL"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            bq_type = "TIMESTAMP"
        else:
            bq_type = "STRING"

        schema.append(bigquery.SchemaField(col, bq_type))

    return schema

def _upload_with_tracking(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    schema: List[bigquery.SchemaField]
) -> Dict[str, Any]:
    """
    Handle uploads with created_at/updated_at tracking via MERGE.

    - When MATCHED: update updated_at only (payload change detection can be added later)
    - When NOT MATCHED: insert full row (created_at/updated_at already stamped)
    """
    # Convert timezone-aware datetimes to naive for Pandera validation
    df_copy = df.copy()
    
    # Create unique staging table name
    staging_table_id = f"{table_id}_staging_{uuid.uuid4().hex[:8]}"

    try:
        # Step 1: Create staging table
        staging_table = bigquery.Table(staging_table_id, schema=schema)
        client.create_table(staging_table)

        # Step 2: Load data to staging table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = client.load_table_from_dataframe(df_copy, staging_table_id, job_config=job_config)
        load_job.result()

        # Step 3: MERGE from staging to target table
        source_spec = f"`{staging_table_id}`"

        # Build explicit column lists to avoid positional mismatches during INSERT
        target_table = client.get_table(table_id)
        target_columns = [field.name for field in target_table.schema]
        # Quote identifiers for safety
        insert_columns_sql = ", ".join([f"`{col}`" for col in target_columns])
        insert_values_sql = ", ".join([f"source.`{col}`" for col in target_columns])

        # Execute MERGE query for created_at/updated_at tracking
        merge_query = f"""
        MERGE `{table_id}` target
        USING {source_spec} source
        ON target.record_id = source.record_id
        WHEN MATCHED THEN
            UPDATE SET target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_sql})
            VALUES ({insert_values_sql})
        """

        job_config = bigquery.QueryJobConfig()
        query_job = client.query(merge_query, job_config=job_config)
        query_job.result()

        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "merge_tracking",
            "staging_table": staging_table_id,
            "load_job_id": load_job.job_id,
            "merge_job_id": query_job.job_id
        }

    finally:
        # Step 4: Clean up staging table
        try:
            client.delete_table(staging_table_id, not_found_ok=True)
            logger.debug(f"Cleaned up staging table: {staging_table_id}")
        except Exception as e:
            logger.warning(f"Failed to clean up staging table {staging_table_id}: {e}")


def _upload_streaming(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    schema: List[bigquery.SchemaField]
) -> Dict[str, Any]:
    """Handle simple streaming uploads without deduplication."""
    table = client.get_table(table_id)
    
    # Convert timezone-aware datetimes to naive for Pandera validation
    df_copy = df.copy()
   
    # Convert DataFrame to list of dictionaries for streaming insert
    rows_data = []
    for idx, (_, row) in enumerate(df_copy.iterrows()):
        row_dict = {}
        for col in df_copy.columns:
            value = row[col]
            # Handle NaN values and data type conversions
            if pd.isna(value):
                row_dict[col] = None
            elif hasattr(value, 'to_pydatetime'):  # pandas Timestamp
                row_dict[col] = value.to_pydatetime()
            elif hasattr(value, 'item'):  # numpy types
                row_dict[col] = value.item()
            else:
                row_dict[col] = value

        rows_data.append(row_dict)

    errors = client.insert_rows(table, rows_data)

    # Check if the insert was successful
    if not errors:
        # Empty list means success
        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "streaming",
            "errors": 0
        }

    # If we get here, there were actual errors
    logger.error(f"BigQuery streaming insert failed with {len(errors)} error batches")

    # Just return the raw errors for debugging
    return {
        "table": table_id,
        "rows_processed": len(df),
        "method": "streaming",
        "errors": errors,
        "error_count": len(errors)
    }


def _upload_batch(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    schema: List[bigquery.SchemaField]
) -> Dict[str, Any]:
    """Handle batch uploads."""
    try:
        # Check if table exists
        try:
            table = client.get_table(table_id)
            table_exists = True
            validation_schema = table.schema
            logger.debug(f"Table {table_id} exists, using existing schema")
        except Exception:
            table_exists = False
            validation_schema = schema
            logger.debug(f"Table {table_id} does not exist, using provided schema")

        # Convert timezone-aware datetimes to naive for Pandera validation
        df_copy = df.copy()

        # Configure job based on whether table exists
        if table_exists:
            # For existing tables, don't provide schema - use existing table schema
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # Let BigQuery infer schema from existing table
            )
        else:
            # For new tables, provide the schema
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

        job = client.load_table_from_dataframe(df_copy, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        return {
            "table": table_id,
            "rows_processed": len(df),
            "method": "batch",
            "job_id": job.job_id,
            "table_existed": table_exists
        }
    except Exception as e:
        logger.error(f"Batch upload failed for {table_id}: {e}")
        return {
            "table": table_id,
            "rows_processed": 0,
            "method": "batch",
            "error": str(e),
            "job_id": getattr(job, 'job_id', None) if 'job' in locals() else None
        }


def _deduplicate_dataframe(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str
) -> tuple[pd.DataFrame, int]:
    """
    Remove records that already exist in BigQuery table.
    Returns (deduplicated_df, skipped_count)
    """
    if df.empty or 'record_id' not in df.columns:
        return df, 0

    # Extract record_ids from new data
    new_record_ids = df['record_id'].dropna().unique().tolist()

    if not new_record_ids:
        return df, 0

    # Check which records already exist
    existing_query = f"""
    SELECT record_id
    FROM `{table_id}`
    WHERE record_id IN ({','.join([f"'{rid}'" for rid in new_record_ids])})
    """

    try:
        existing_results = client.query(existing_query).result()
        existing_ids = {row['record_id'] for row in existing_results}
    except Exception as e:
        logger.warning(f"Could not check existing records: {e}")
        existing_ids = set()

    if not existing_ids:
        return df, 0

    # Filter to only new records
    original_count = len(df)
    deduplicated_df = df[~df['record_id'].isin(existing_ids)]
    skipped_count = original_count - len(deduplicated_df)

    logger.info(f"Deduplicated DataFrame: {original_count} -> {len(deduplicated_df)} records ({skipped_count} duplicates removed)")

    return deduplicated_df, skipped_count