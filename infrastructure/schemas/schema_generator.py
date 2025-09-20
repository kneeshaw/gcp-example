"""
Fresh Schema Generator
======================

Clean schema generator for DataFrameModel-based Pandera schemas.
Generates BigQuery JSON schema files from DataFrameModel classes.

Usage:
    python infrastructure/schemas/simple_schema_generator.py
    python infrastructure/schemas/simple_schema_generator.py --all
"""

import json
import os
import re
import ast
from pathlib import Path
from typing import Dict, List, Any, Optional


def extract_dataframe_model_info(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Extract DataFrameModel information from a Python file.

    Args:
        file_path: Path to the schema file

    Returns:
        Dictionary containing DataFrameModel information or None if not found
    """
    with open(file_path, 'r') as f:
        content = f.read()

    # Find DataFrameModel class (allow multiple base classes, as long as pa.DataFrameModel is present)
    class_match = re.search(r'class\s+(\w+)\([^\)]*pa\.DataFrameModel[^\)]*\)\s*:', content)
    if not class_match:
        return None

    model_name = class_match.group(1)

    # Extract BigQuery table configuration
    table_name_match = re.search(r'_bigquery_table_name\s*=\s*["\']([^"\']+)["\']', content)
    description_match = re.search(r'_description\s*=\s*["\']([^"\']+)["\']', content)

    # Extract BigQuery clustering configuration
    clustering_match = re.search(r'_bigquery_clustering\s*=\s*(\[[^\]]+\])', content)
    clustering = None
    if clustering_match:
        try:
            clustering = eval(clustering_match.group(1))
        except:
            pass

    # Extract BigQuery partitioning configuration (flat dict only by design)
    partitioning_match = re.search(r'_bigquery_partitioning\s*=\s*(\{[^{}]+\})', content)
    partitioning = None
    if partitioning_match:
        try:
            partitioning = eval(partitioning_match.group(1))
        except:
            pass

    # Optional: per-field BigQuery type overrides, e.g.
    # _bigquery_field_types = { 'service_date_d': 'DATE' }
    field_type_overrides = {}
    overrides_match = re.search(r'_bigquery_field_types\s*=\s*(\{[^{}]+\})', content)
    if overrides_match:
        try:
            field_type_overrides = eval(overrides_match.group(1))
        except:
            field_type_overrides = {}

    # Extract field definitions from the class
    fields = extract_fields_from_class(content, model_name)

    return {
        'model_name': model_name,
        'table_name': table_name_match.group(1) if table_name_match else model_name.lower(),
        'description': description_match.group(1) if description_match else '',
        'fields': fields,
        'clustering': clustering,
        'partitioning': partitioning,
        'field_type_overrides': field_type_overrides
    }


def extract_fields_from_class(content: str, class_name: str) -> List[Dict[str, Any]]:
    """
    Extract field definitions from a DataFrameModel class.

    Args:
        content: File content
        class_name: Name of the DataFrameModel class

    Returns:
        List of field definitions
    """
    fields = []

    # Find the class definition - be more specific to avoid stopping at inner classes
    # Allow multiple base classes when extracting class body
    class_pattern = rf'class\s+{class_name}\([^\)]*pa\.DataFrameModel[^\)]*\)\s*:(.*?)(?=\nclass|\n@|\ndef\s|\n__|\n#|\Z)'
    class_match = re.search(class_pattern, content, re.DOTALL)

    if not class_match:
        return fields

    class_content = class_match.group(1)

    # Split into lines and process each one
    lines = class_content.split('\n')
    current_field_lines = []

    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        # Check if this line starts a field definition
        if re.match(r'\w+\s*:.*=.*pa\.Field', line):
            # If we have accumulated field lines, process them
            if current_field_lines:
                field_text = ' '.join(current_field_lines)
                field_info = parse_field_line(field_text)
                if field_info:
                    fields.append(field_info)
                current_field_lines = []

            current_field_lines = [line]
        elif current_field_lines and line:  # Continuation of current field
            current_field_lines.append(line)

    # Process any remaining field lines
    if current_field_lines:
        field_text = ' '.join(current_field_lines)
        field_info = parse_field_line(field_text)
        if field_info:
            fields.append(field_info)

    return fields


def parse_field_line(field_line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single field definition line.

    Args:
        field_line: Line containing field definition

    Returns:
        Field definition dictionary or None if parsing fails
    """
    # Clean up the line and handle tabs
    line = field_line.replace('\t', ' ').strip()

    # Extract field name - handle tabs and spaces
    name_match = re.match(r'(\w+)\s*:', line)
    if not name_match:
        return None

    field_name = name_match.group(1)

    # Extract type annotation - handle complex types with brackets
    type_match = re.search(r':\s*([^=]+)=', line)
    if not type_match:
        return None

    type_annotation = type_match.group(1).strip()

    # Extract pa.Field parameters - handle multiline and complex parameters
    field_match = re.search(r'pa\.Field\(([^)]+)\)', line)
    if not field_match:
        return None

    field_params = field_match.group(1)

    # Parse nullable parameter - check for explicit nullable=True
    nullable = 'nullable=True' in field_params

    # Extract description - handle both single and double quotes
    desc_match = re.search(r'description\s*=\s*["\']([^"\']*)["\']', field_params)
    description = desc_match.group(1) if desc_match else ''

    # Map type annotation to BigQuery type
    bq_type = map_type_to_bigquery(type_annotation)

    return {
        'name': field_name,
        'type': bq_type,
        'mode': 'NULLABLE' if nullable else 'REQUIRED',
        'description': description
    }


def map_type_to_bigquery(type_annotation: str) -> str:
    """
    Map Python type annotation to BigQuery type.

    Args:
        type_annotation: Python type annotation string

    Returns:
        BigQuery type string
    """
    type_mapping = {
        'str': 'STRING',
        'Series[str]': 'STRING',
        'int': 'INTEGER',
        'Series[pd.Int64Dtype]': 'INTEGER',
        'Series[int]': 'INTEGER',
        'float': 'FLOAT',
        'Series[float]': 'FLOAT',
        'bool': 'BOOLEAN',
        'Series[bool]': 'BOOLEAN',
        'pd.Timestamp': 'TIMESTAMP',
        'Series[pd.Timestamp]': 'TIMESTAMP',
        # Date variants (allow mapping to DATE when annotated explicitly)
        'date': 'DATE',
        'datetime.date': 'DATE',
        'Series[date]': 'DATE',
        'Series[datetime.date]': 'DATE'
    }

    # Clean up the type annotation
    clean_type = type_annotation.strip()

    return type_mapping.get(clean_type, 'STRING')


def generate_json_file(model_info: Dict[str, Any], output_dir: Path):
    """
    Generate JSON file for the DataFrameModel.

    Args:
        model_info: Dictionary containing DataFrameModel information
        output_dir: Output directory for JSON files
    """
    if not model_info['fields']:
        print(f"Warning: No fields found for {model_info['model_name']}")
        return

    # Generate filename based on table name
    filename = f"{model_info['table_name']}.table.json"
    output_path = output_dir / filename

    with open(output_path, 'w') as f:
        # Write metadata as first line
        metadata = {
            'table_name': model_info['table_name'],
            'description': model_info['description']
        }

        # Add clustering and partitioning if they exist
        if model_info.get('clustering'):
            metadata['clustering'] = model_info['clustering']
        if model_info.get('partitioning'):
            metadata['partitioning'] = model_info['partitioning']

        json.dump(metadata, f)
        f.write('\n')

        # Write each field as a separate JSON object on its own line
        overrides = model_info.get('field_type_overrides', {}) or {}

        fields = model_info['fields'][:]

        # Auto-append created_at/updated_at if missing
        existing_names = {f['name'] for f in fields}
        for name in ['created_at', 'updated_at']:
            if name not in existing_names:
                fields.append({
                    'name': name,
                    'type': 'TIMESTAMP',
                    'mode': 'NULLABLE',
                    'description': 'Auto-appended warehouse timestamp'
                })

        for field in fields:
            # Apply per-field BigQuery type override if provided
            if field['name'] in overrides:
                field = {**field, 'type': overrides[field['name']].upper()}
            json.dump(field, f)
            f.write('\n')

    print(f"Generated {output_path} with {len(fields)} fields")


def process_single_schema(schema_path: Path, output_dir: Path):
    """
    Process a single schema file.

    Args:
        schema_path: Path to the schema file
        output_dir: Output directory
    """
    print(f"Processing {schema_path.name}...")

    try:
        model_info = extract_dataframe_model_info(schema_path)
        if model_info:
            print(f"  Found DataFrameModel: {model_info['model_name']} -> {model_info['table_name']}")
            generate_json_file(model_info, output_dir)
        else:
            print(f"  No DataFrameModel found in {schema_path.name}")

    except Exception as e:
        print(f"Error processing {schema_path.name}: {e}")


def process_all_schemas(output_dir: Path):
    """
    Process all schema files in the project.

    Args:
        output_dir: Output directory
    """
    project_root = Path(__file__).parent.parent.parent

    # Define schema directories to scan
    schema_dirs = [
        'src/schemas/realtime',
        'src/schemas/schedule',
        'src/schemas/derived'
    ]

    total_processed = 0

    for schema_dir in schema_dirs:
        full_schema_dir = project_root / schema_dir

        if not full_schema_dir.exists():
            print(f"Warning: Directory {full_schema_dir} does not exist")
            continue

        print(f"\nScanning {schema_dir}...")

        # Find all Python files in the directory
        for py_file in full_schema_dir.glob('*.py'):
            if py_file.name == '__init__.py':
                continue

            try:
                process_single_schema(py_file, output_dir)
                total_processed += 1
            except Exception as e:
                print(f"Failed to process {py_file.name}: {e}")

    print(f"\nProcessed {total_processed} schema files total")


def main():
    """Main function."""
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    output_dir = project_root / 'infrastructure' / 'schemas' / 'files'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Choose processing mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        # Process all schemas
        process_all_schemas(output_dir)
    else:
        # Process single schema file (example)
        schema_file = project_root / 'src' / 'schemas' / 'schedule' / 'stops.py'

        if schema_file.exists():
            process_single_schema(schema_file, output_dir)
        else:
            print(f"Schema file not found: {schema_file}")

        print("\nTo process all schemas, run: python simple_schema_generator.py --all")


if __name__ == "__main__":
    main()
