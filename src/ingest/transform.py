# src/transform/realtime.py
import pandas as pd
import json
from datetime import datetime, timezone

from common.logging_utils import logger
from schemas.schema_registry import get_schema_class
from schemas.common.schema_utils import clean_and_validate_dataframe


def normalize_nested_json(entities) -> pd.DataFrame:
    '''
    Flatten nested JSON structures in a DataFrame.

    Phase 1: explode list columns (adds rows)
    Phase 2: expand dict columns (adds columns)

    Args:
        df: Input DataFrame
        max_iterations: Safety cap for repeated list explosions

    Returns:
        Flattened DataFrame
    '''
    
    # Normalize the extracted entities into a DataFrame
    df_result = pd.json_normalize(entities, sep='.')
    max_iterations = 10
    
    # Phase 1: Explode all list columns
    for iteration in range(max_iterations):
        # Find columns containing lists
        list_columns = []
        for col in df_result.select_dtypes(include=['object']):
            sample_values = df_result[col].dropna().head(5)
            if len(sample_values) > 0 and any(isinstance(val, list) for val in sample_values):
                list_columns.append(col)
        
        if not list_columns:
            break  # No more list columns to explode
            
        # Explode the first list column found
        col = list_columns[0]
        df_result = df_result.explode(col).reset_index(drop=True)
    
    # Phase 2: Normalize all dictionary columns
    dict_columns = []
    for col in df_result.select_dtypes(include=['object']):
        sample_values = df_result[col].dropna().head(5)
        if len(sample_values) > 0 and any(isinstance(val, dict) for val in sample_values):
            dict_columns.append(col)
    
    # Expand each dictionary column
    for col in dict_columns:
        non_null_dicts = df_result[col].dropna()
        if not non_null_dicts.empty:
            try:
                # Normalize dictionaries and add prefix
                normalized = pd.json_normalize(non_null_dicts).add_prefix(f'{col}.')
                normalized.index = df_result[df_result[col].notna()].index
                
                # Replace original column with normalized columns
                df_result = df_result.drop(columns=[col])
                df_result = pd.concat([df_result, normalized], axis=1)
                
            except Exception:
                # If normalization fails, keep the original column
                continue
    
    return df_result


def transform_realtime(dataset: str, data_bytes: bytes) -> pd.DataFrame:
    """
    Transforms raw GTFS-RT data bytes into a validated DataFrame using the appropriate schema.

    Args:
        dataset (str): Dataset key (e.g., 'vehicle-positions').
        data_bytes (bytes): Raw data bytes (JSON-encoded).

    Returns:
        pd.DataFrame: Transformed and validated DataFrame. Returns empty DataFrame if no data.
    """
    # Get schema class from registry
    schema_class = get_schema_class(dataset)

    if schema_class is None:
        logger.error(f"No schema class registered for dataset: {dataset}")
        return pd.DataFrame()

    try:
        record = json.loads(data_bytes.decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to decode data bytes: {e}")
        return pd.DataFrame()

    # Extract entities
    entities = record.get('response', {}).get('entity', record.get('entity', []))
    if not entities:
        logger.warning("No entities found in record.")
        return pd.DataFrame()

    # Normalize nested JSON
    df_raw = normalize_nested_json(entities)
    if df_raw.empty:
        logger.warning("Normalized DataFrame is empty.")
        return pd.DataFrame()

    # Note: created_at/updated_at are stamped centrally during upload; no first/last_seen here
    
    # Transform and validate
    try:
        df_processed = clean_and_validate_dataframe(df_raw, schema_class)

    except Exception as e:
        logger.error(f"Schema transformation/validation failed: {e}")
        return pd.DataFrame()

    if df_processed is None or df_processed.empty:
        logger.warning("Processed DataFrame is empty after schema validation.")
        return pd.DataFrame()

    return df_processed


def transform_schedule(dataset: str, df: pd.DataFrame) -> pd.DataFrame:
    
    # Get schema class from registry
    schema_class = get_schema_class(dataset)

    if schema_class is None:
        logger.error(f"No schema class registered for dataset: {dataset}")
        return pd.DataFrame()
    
     # Transform and validate
    try:
        df_processed = clean_and_validate_dataframe(df, schema_class)

    except Exception as e:
        logger.error(f"Schema transformation/validation failed: {e}")
        return pd.DataFrame()

    if df_processed is None or df_processed.empty:
        logger.warning("Processed DataFrame is empty after schema validation.")
        return pd.DataFrame()

    return df_processed 