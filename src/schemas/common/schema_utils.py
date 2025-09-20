"""
Schema Utilities
================

Utility functions for working with pandera schemas and DataFrames.
"""

import pandas as pd
import numpy as np
import hashlib
from typing import Dict, Any, List, Optional, Iterable
import importlib

from common.logging_utils import logger


def _get_schema_attribute(schema_class, attribute_name, default=None):
    """
    Get an attribute from the schema module.

    Args:
        schema_class: Pandera DataFrameModel class
        attribute_name: Name of the attribute to get
        default: Default value if attribute not found

    Returns:
        Attribute value or default
    """
    try:
        module = importlib.import_module(schema_class.__module__)
        if hasattr(module, attribute_name):
            return getattr(module, attribute_name)
    except ImportError:
        logger.warning(f"Could not import schema module {schema_class.__module__}")

    return default


def apply_schema_field_mappings(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Apply field mappings using COLS_MAPPING from the schema class.
    Maps source columns to target schema fields based on the schema's COLS_MAPPING.

    Args:
        df: Input DataFrame with source columns
        schema_class: Pandera DataFrameModel class with COLS_MAPPING attribute

    Returns:
        DataFrame with columns mapped according to schema COLS_MAPPING
    """
    if df.empty:
        return df

    # Get the COLS_MAPPING from the schema module
    mappings = _get_schema_attribute(schema_class, 'COLS_MAPPING', {})
    if not mappings:
        logger.debug(f"Schema class {schema_class.__name__} has no COLS_MAPPING attribute")
        return df
    schema_instance = schema_class.to_schema()

    # Start with a copy of the original DataFrame
    result_df = df.copy()

    # Track which target columns have been created
    target_columns = set()

    # Process each field in the schema
    for target_field in schema_instance.columns.keys():
        # Get source paths from COLS_MAPPING
        source_paths = mappings.get(target_field, [])
        if not source_paths:
            continue

        # Find the first source path that exists in the DataFrame
        source_column = next((path for path in source_paths if path in df.columns), None)

        if source_column:
            # Create the target column by copying from source
            result_df[target_field] = df[source_column].copy()
            target_columns.add(target_field)
            logger.debug(f"Mapped {source_column} -> {target_field}")

    # Keep only the mapped target columns and remove all original source columns
    # This ensures we don't have both source and target columns in the result
    if target_columns:
        result_df = result_df[list(target_columns)]
        logger.debug(f"Kept {len(target_columns)} mapped columns: {sorted(target_columns)}")

    return result_df


def convert_epoch_timestamps_to_utc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert epoch timestamp columns (ending in '_s') to UTC datetime columns.
    Creates new datetime columns without the '_s' suffix.

    Args:
        df: DataFrame with potential epoch timestamp columns

    Returns:
        DataFrame with new UTC datetime columns added
    """
    if df.empty:
        return df

    result_df = df.copy()

    # Find all columns ending with '_s' (epoch timestamps)
    epoch_columns = [col for col in df.columns if col.endswith('_s')]

    for epoch_col in epoch_columns:
        # Create the target column name by removing '_s' suffix
        datetime_col = epoch_col[:-2]  # Remove '_s'

        try:
            # Convert to numeric first to ensure proper epoch timestamp handling
            numeric_values = pd.to_numeric(df[epoch_col], errors='coerce')
            
            # Convert epoch to UTC datetime
            result_df[datetime_col] = pd.to_datetime(
                numeric_values,
                unit='s',  # Assume seconds (most common for GTFS)
                utc=True,
                errors='coerce'
            )
            logger.debug(f"Converted {epoch_col} to {datetime_col} (UTC datetime)")
        except Exception as e:
            logger.warning(f"Failed to convert {epoch_col} to datetime: {e}")

    return result_df


def apply_schema_precision_rounding(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Apply precision rounding to float columns based on schema field metadata.
    Rounds float columns to the precision specified in the schema's metadata.

    Args:
        df: DataFrame with float columns to round
        schema_class: Pandera DataFrameModel class with precision metadata

    Returns:
        DataFrame with float columns rounded according to schema precision
    """
    if df.empty:
        return df

    result_df = df.copy()
    schema_instance = schema_class.to_schema()

    # Process each field in the schema
    for field_name, field_config in schema_instance.columns.items():
        # Check if this field exists in the DataFrame
        if field_name not in df.columns:
            continue

        # Check if this is a float field with precision metadata
        if (field_config.dtype == 'float64' or field_config.dtype == 'float32' or
            str(field_config.dtype).startswith('float')):

            # Get precision from metadata
            precision = field_config.metadata.get('precision') if field_config.metadata else None

            if precision is not None:
                try:
                    # Round the column to the specified precision
                    result_df[field_name] = pd.to_numeric(result_df[field_name], errors='coerce').round(precision)

                    logger.debug(f"Rounded {field_name} to {precision} decimal places")
                except Exception as e:
                    logger.warning(f"Failed to round {field_name} to {precision} decimal places: {e}")

    return result_df


def apply_categorical_standardization(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Apply categorical standardization using CATEGORICAL_MAPPING from the schema class.
    Replaces numeric codes with standardized text values based on the schema's mappings.

    Args:
        df: DataFrame with categorical columns to standardize
        schema_class: Pandera DataFrameModel class with CATEGORICAL_MAPPING attribute

    Returns:
        DataFrame with categorical values standardized according to schema mappings
    """
    if df.empty:
        return df

    result_df = df.copy()

    # Get the CATEGORICAL_MAPPING from the schema module
    categorical_mappings = _get_schema_attribute(schema_class, 'CATEGORICAL_MAPPING', {})

    if not categorical_mappings:
        logger.debug(f"Schema class {schema_class.__name__} has no CATEGORICAL_MAPPING attribute")
        return df

    # Process each categorical field in the mapping
    for column_name, value_mapping in categorical_mappings.items():
        # Check if this column exists in the DataFrame
        if column_name not in df.columns:
            logger.debug(f"Column {column_name} not found in DataFrame, skipping categorical standardization")
            continue

        try:
            # Apply the mapping to standardize categorical values
            result_df[column_name] = df[column_name].map(value_mapping)

            # Count how many values were successfully mapped
            original_count = len(df[column_name].dropna())
            mapped_count = result_df[column_name].notna().sum()

            if original_count > 0:
                success_rate = mapped_count / original_count * 100
                logger.debug(f"Standardized {column_name}: {mapped_count}/{original_count} values mapped ({success_rate:.1f}%)")

                if success_rate < 100:
                    unmapped_values = df[column_name][result_df[column_name].isna() & df[column_name].notna()].unique()
                    logger.warning(f"Unmapped values in {column_name}: {list(unmapped_values)}")

        except Exception as e:
            logger.warning(f"Failed to standardize categorical values for {column_name}: {e}")

    return result_df


def coerce_integer_fields_to_nullable_int(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Coerce all integer-typed columns (per Pandera schema) to pandas nullable Int64.

    - Converts values with fractional parts to NaN to avoid silent truncation.
    - Ensures Parquet writes INT64 instead of DOUBLE for integer fields.

    Args:
        df: Input DataFrame
        schema_class: Pandera DataFrameModel class

    Returns:
        DataFrame with integer fields coerced to pandas 'Int64'
    """
    if df.empty:
        return df

    result_df = df.copy()
    schema_instance = schema_class.to_schema()

    try:
        int_cols = []
        for col_name, col_schema in schema_instance.columns.items():
            dtype = getattr(col_schema, "dtype", None)
            if dtype is None:
                continue
            alias = getattr(dtype, "str_alias", None)
            dtypes_to_check = [str(dtype).lower()]
            if alias is not None:
                dtypes_to_check.append(str(alias).lower())
            if any(s.startswith("int") for s in dtypes_to_check):
                int_cols.append(col_name)

        coerced = []
        for c in int_cols:
            if c in result_df.columns:
                ser = pd.to_numeric(result_df[c], errors='coerce')
                # If values are floats with non-zero fractional part, set to NaN
                ser = ser.where(ser.isna() | ((ser % 1) == 0))
                result_df[c] = ser.astype('Int64')
                coerced.append(c)
        if coerced:
            logger.debug(f"Coerced integer columns to Int64: {coerced}")
    except Exception as e:
        logger.warning(f"Integer coercion step skipped due to error: {e}")

    return result_df


def generate_record_id(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Generate a stable record_id that excludes volatile fields like capture_ts
    and derived timestamp_dt from COLS_VOLATILE.

    Args:
        df: DataFrame to add record_id to
        schema_class: Pandera DataFrameModel class with COLS_VOLATILE attribute

    Returns:
        DataFrame with record_id column added
    """
    if df.empty:
        return df

    result_df = df.copy()

    # Get volatile columns to exclude from hash calculation
    exclude_cols = set(_get_schema_attribute(schema_class, 'COLS_VOLATILE', []))
    if not exclude_cols:
        logger.debug("No volatile columns defined in schema; skipping record_id generation")
        return df

    cols_for_hash = [c for c in result_df.columns if c not in exclude_cols]

    if not cols_for_hash:
        logger.warning("No columns available for hash calculation after excluding volatile fields")
        return result_df

    hash_series = result_df[cols_for_hash].astype(str).sum(axis=1)
    result_df["record_id"] = hash_series.apply(lambda x: hashlib.md5(x.encode()).hexdigest()[:16])

    logger.debug(f"Generated record_id using {len(cols_for_hash)} columns (excluded {len(exclude_cols)} volatile columns)")

    return result_df


def generate_entity_id(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Generate stable entity_id by hashing key columns from COLS_ENTITY.

    Args:
        df: Input DataFrame.
        schema_class: Pandera DataFrameModel class with COLS_ENTITY attribute

    Returns:
        DataFrame with entity_id column added (no-op if COLS_ENTITY not defined or no key columns exist).
    """
    if df.empty:
        return df

    # Check if schema has COLS_ENTITY defined
    key_cols = _get_schema_attribute(schema_class, 'COLS_ENTITY', [])
    if not key_cols:
        logger.debug("generate_entity_id: no COLS_ENTITY defined in schema; skipping entity ID generation.")
        return df

    available = [c for c in key_cols if c in df.columns]

    if not available:
        logger.warning(f"generate_entity_id: no key columns present from {key_cols}; skipping.")
        return df

    def calc(row):
        composite = "|".join(str(row.get(c, "")) for c in available)
        return hashlib.md5(composite.encode("utf-8")).hexdigest()[:16]

    result_df = df.copy()
    result_df["entity_id"] = result_df.apply(calc, axis=1)

    logger.debug(f"Generated entity_id using {len(available)} key columns: {available}")

    return result_df


def deduplicate_by_record_id(df: pd.DataFrame, keep='first') -> pd.DataFrame:
    """
    Remove duplicate records based on record_id.

    Since record_id is a hash of the full record content, identical records
    will have the same record_id, making this perfect for deduplication.

    Args:
        df: Input DataFrame with record_id column
        keep: Which duplicate to keep ('first', 'last', or False to drop all duplicates)

    Returns:
        DataFrame with duplicates removed
    """
    if df.empty or 'record_id' not in df.columns:
        logger.debug("DataFrame is empty or missing record_id column - no deduplication performed")
        return df

    initial_count = len(df)
    result_df = df.drop_duplicates(subset=['record_id'], keep=keep)
    final_count = len(result_df)

    duplicates_removed = initial_count - final_count
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate records based on record_id ({initial_count} -> {final_count})")
    else:
        logger.debug("No duplicate records found")

    return result_df


def add_missing_schema_fields(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Add fields that are defined in the schema but missing from the DataFrame.
    Uses appropriate default/null values based on the field type.

    Args:
        df: Input DataFrame
        schema_class: Pandera DataFrameModel class

    Returns:
        DataFrame with missing schema fields added with appropriate defaults
    """
    if df.empty:
        return df

    result_df = df.copy()
    schema_instance = schema_class.to_schema()

    # Track which fields were added
    added_fields = []

    # Check each field in the schema
    for field_name, field_config in schema_instance.columns.items():
        if field_name not in df.columns:
            # Determine appropriate default value based on field type
            default_value = _get_default_value_for_field(field_config)

            # Add the column with the default value
            result_df[field_name] = default_value
            added_fields.append(field_name)
            logger.debug(f"Added missing field '{field_name}' with default value: {default_value}")

    if added_fields:
        logger.info(f"Added {len(added_fields)} missing schema fields: {added_fields} for schema {schema_class.__name__}")
    else:
        logger.debug("No missing schema fields found")

    return result_df


def _get_default_value_for_field(field_config) -> Any:
    """
    Determine the appropriate default value for a schema field based on its type.

    Args:
        field_config: Pandera field configuration

    Returns:
        Appropriate default value for the field type
    """
    # Get the field dtype
    dtype = getattr(field_config, 'dtype', None)
    if dtype is None:
        return None

    dtype_str = str(dtype).lower()

    # Check for nullable integer types
    if any(s.startswith('int') for s in [dtype_str, getattr(dtype, 'str_alias', '')]):
        return pd.NA  # pandas nullable integer NA

    # Check for float types
    elif any(s.startswith('float') for s in [dtype_str, getattr(dtype, 'str_alias', '')]):
        return np.nan

    # Check for boolean types
    elif 'bool' in dtype_str:
        return pd.NA  # pandas nullable boolean NA

    # Check for datetime types
    elif 'datetime' in dtype_str or 'date' in dtype_str:
        return pd.NaT  # pandas Not a Time

    # Check for string/categorical types
    elif any(s in dtype_str for s in ['str', 'string', 'object', 'category']):
        return None  # None for string fields

    # Default fallback
    else:
        logger.debug(f"Unknown dtype '{dtype_str}', using None as default")
        return None


def drop_extra_columns_not_in_schema(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Drop columns from the DataFrame that don't appear in the schema.
    Only keeps columns that are defined in the schema.

    Args:
        df: Input DataFrame
        schema_class: Pandera DataFrameModel class

    Returns:
        DataFrame with only columns that exist in the schema
    """
    if df.empty:
        return df

    result_df = df.copy()
    schema_instance = schema_class.to_schema()

    # Get the list of columns from the schema
    schema_columns = set(schema_instance.columns.keys())

    # Get columns that exist in DataFrame but not in schema
    extra_columns = [col for col in df.columns if col not in schema_columns]

    if extra_columns:
        # Drop the extra columns
        result_df = result_df.drop(columns=extra_columns)
        logger.info(f"Dropped {len(extra_columns)} extra columns not in schema: {extra_columns}")
    else:
        logger.debug("No extra columns found - all DataFrame columns are in schema")

    return result_df


def order_columns_by_schema(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Order DataFrame columns to match the order they appear in the schema.

    Args:
        df: Input DataFrame
        schema_class: Pandera DataFrameModel class

    Returns:
        DataFrame with columns ordered according to schema field order
    """
    if df.empty:
        return df

    schema_instance = schema_class.to_schema()

    # Get the ordered list of columns from the schema
    schema_columns = list(schema_instance.columns.keys())

    # Find columns that exist in both DataFrame and schema
    ordered_columns = [col for col in schema_columns if col in df.columns]

    if ordered_columns:
        # Reorder DataFrame to match schema order
        result_df = df[ordered_columns]
        logger.debug(f"Ordered {len(ordered_columns)} columns according to schema order")
        return result_df
    else:
        logger.debug("No schema columns found in DataFrame, keeping original order")
        return df


def coerce_timestamp_columns_to_utc(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Coerce columns specified in COLS_TIMESTAMP to pandas datetime64[ns, UTC] and floor to seconds.

    Args:
        df: Input DataFrame
        schema_class: Pandera DataFrameModel class with COLS_TIMESTAMP attribute

    Returns:
        DataFrame with timestamp columns coerced to UTC datetime64[ns] and floored to seconds
    """
    if df.empty:
        return df

    result_df = df.copy()

    # Get COLS_TIMESTAMP from the schema module
    timestamp_columns = _get_schema_attribute(schema_class, 'COLS_TIMESTAMP', [])

    if not timestamp_columns:
        logger.debug("COLS_TIMESTAMP is empty, no timestamp columns to coerce")
        return df

    coerced_count = 0

    # Process each timestamp column
    for col_name in timestamp_columns:
        if col_name not in df.columns:
            logger.debug(f"Timestamp column '{col_name}' not found in DataFrame, skipping")
            continue

        try:
            # Convert to datetime64[ns, UTC] and floor to seconds
            result_df[col_name] = pd.to_datetime(df[col_name], utc=True, errors='coerce').dt.floor('s')

            coerced_count += 1
            logger.debug(f"Coerced column '{col_name}' to datetime64[ns, UTC] and floored to seconds")

        except Exception as e:
            logger.warning(f"Failed to coerce column '{col_name}' to datetime: {e}")

    if coerced_count > 0:
        logger.debug(f"Coerced {coerced_count} timestamp columns to UTC datetime64[ns] and floored to seconds")
    else:
        logger.debug("No timestamp columns were successfully coerced")

    return result_df


def filter_null_rows_by_columns(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Filter out rows where any column specified in COLS_FILTERNA contains null values.
    Skip filtering if COLS_FILTERNA is empty or not defined.

    Args:
        df: Input DataFrame
        schema_class: Pandera DataFrameModel class with COLS_FILTERNA attribute

    Returns:
        DataFrame with rows filtered out where specified columns contain nulls
    """
    if df.empty:
        return df

    # Get COLS_FILTERNA from the schema module
    filter_columns = _get_schema_attribute(schema_class, 'COLS_FILTERNA', [])

    # Skip if list is empty
    if not filter_columns:
        logger.debug("COLS_FILTERNA is empty, skipping null row filtering")
        return df

    initial_count = len(df)

    # Filter out rows where any of the specified columns are null
    result_df = df.copy()

    # Build filter condition for each column
    filter_conditions = []
    for col in filter_columns:
        if col in df.columns:
            filter_conditions.append(df[col].notna())
        else:
            logger.warning(f"Filter column '{col}' not found in DataFrame, skipping")

    if filter_conditions:
        # Combine all conditions with AND (all must be non-null)
        combined_condition = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_condition = combined_condition & condition

        result_df = df[combined_condition]

    final_count = len(result_df)
    filtered_rows = initial_count - final_count

    if filtered_rows > 0:
        logger.info(f"Filtered out {filtered_rows} rows with null values in columns: {filter_columns} ({initial_count} -> {final_count})")
    else:
        logger.debug(f"No rows filtered out - all rows have non-null values in specified columns: {filter_columns}")

    return result_df


def clean_and_validate_dataframe(df: pd.DataFrame, schema_class) -> pd.DataFrame:
    """
    Clean and validate a DataFrame using a schema class with comprehensive processing pipeline.

    Args:
        df: Raw DataFrame to process
        schema_class: Pandera DataFrameModel class with processing attributes

    Returns:
        Processed and validated DataFrame
    """
    if df.empty:
        return df

    # 1. Apply field mappings using COLS_MAPPING from schema class
    df = apply_schema_field_mappings(df, schema_class)

    # 2. Convert epoch timestamps to UTC datetime columns
    df = convert_epoch_timestamps_to_utc(df)

    # 3. Apply schema precision for floats
    df = apply_schema_precision_rounding(df, schema_class)

    # 4. Apply categorical standardization
    df = apply_categorical_standardization(df, schema_class)

    # 5. Coerce integer fields to nullable Int64
    df = coerce_integer_fields_to_nullable_int(df, schema_class)

    # 6. Generate record and entity IDs
    df = generate_record_id(df, schema_class)
    df = generate_entity_id(df, schema_class)

    # 7. Deduplicate by record_id
    df = deduplicate_by_record_id(df)

    # 8. Filter out rows with nulls in specified columns
    df = filter_null_rows_by_columns(df, schema_class)

    # 9. Add missing columns and drop extra ones
    df = add_missing_schema_fields(df, schema_class)
    df = drop_extra_columns_not_in_schema(df, schema_class)

    # 10. Sort DataFrame columns to match schema order
    df = order_columns_by_schema(df, schema_class)

    # 11. Final validation using Pandera
    df = schema_class.to_schema().validate(df)

    # 12. Coerce timestamp columns to UTC datetime64[ns]
    df = coerce_timestamp_columns_to_utc(df, schema_class)

    return df