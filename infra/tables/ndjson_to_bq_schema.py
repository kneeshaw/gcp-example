#!/usr/bin/env python3
"""
NDJSON → BigQuery Array Schema Converter
=======================================

Converts *.table.json (NDJSON: first line metadata + one field per line)
into BigQuery-compatible array schema files (*.schema.json).

The script now works with the organized dataset structure under infra/tables/{dataset}/.

Usage:
  python infra/tables/ndjson_to_bq_schema.py                    # convert all datasets
  python infra/tables/ndjson_to_bq_schema.py --all              # same as default
  python infra/tables/ndjson_to_bq_schema.py --input my.table.json --dataset vehicle_positions  # single file
  python infra/tables/ndjson_to_bq_schema.py --dataset schedule  # convert specific dataset
  python infra/tables/ndjson_to_bq_schema.py --in-dir custom/path --out-dir custom/output

Organized Structure:
- Input: infra/tables/{dataset}/*.table.json
- Output: infra/tables/{dataset}/*.schema.json and *.meta.json
- Datasets: vehicle_positions, trip_updates, service_alerts, schedule, daily_schedule

Notes:
- Writes <name>.schema.json to the same dataset directory.
- If the first NDJSON line contains partitioning/clustering, a sidecar <name>.meta.json
  is also written for optional Terraform consumption later.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any


def read_ndjson_table_file(path: Path) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Read NDJSON table file: first line is metadata, remaining lines are fields."""
    with path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    if not lines:
        raise ValueError(f"Empty file: {path}")

    try:
        metadata = json.loads(lines[0])
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid metadata JSON in {path}: {e}")

    fields: List[Dict[str, Any]] = []
    for i, line in enumerate(lines[1:], start=2):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid field JSON at line {i} in {path}: {e}")
        if not isinstance(obj, dict):
            raise ValueError(f"Field at line {i} in {path} is not an object")
        fields.append(obj)

    return metadata, fields


essential_field_keys = {"name", "type", "mode"}


def validate_fields(fields: List[Dict[str, Any]], source: Path) -> None:
    for idx, f in enumerate(fields, start=1):
        missing = essential_field_keys - set(f.keys())
        if missing:
            raise ValueError(f"Missing keys {missing} in field #{idx} of {source}")


def write_bq_array_schema(out_dir: Path, base_name: str, fields: List[Dict[str, Any]]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{base_name}.schema.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(fields, f, indent=2)
        f.write("\n")
    return out_path


def write_sidecar_meta(out_dir: Path, base_name: str, metadata: Dict[str, Any]) -> Path:
    sidecar = out_dir / f"{base_name}.meta.json"
    # Only persist interesting bits to avoid duplication
    slim = {
        k: metadata[k]
        for k in ["table_name", "description", "partitioning", "clustering"]
        if k in metadata
    }
    with sidecar.open("w", encoding="utf-8") as f:
        json.dump(slim, f, indent=2)
        f.write("\n")
    return sidecar


def convert_file(src: Path, out_dir: Path) -> tuple[Path, Path | None]:
    metadata, fields = read_ndjson_table_file(src)
    validate_fields(fields, src)

    # Choose name from metadata.table_name if present; else derive from filename
    base_name = metadata.get("table_name") or src.stem.replace(".table", "")

    schema_path = write_bq_array_schema(out_dir, base_name, fields)

    sidecar = None
    if any(k in metadata for k in ("partitioning", "clustering")):
        sidecar = write_sidecar_meta(out_dir, base_name, metadata)

    return schema_path, sidecar


def convert_all(in_dir: Path, out_dir: Path) -> int:
    """Convert all *.table.json files in input directory to schema files in output directory."""
    count = 0
    for path in sorted(in_dir.glob("*.table.json")):
        try:
            schema_path, sidecar = convert_file(path, out_dir)
            msg = f"Converted {path.name} → {schema_path.name}"
            if sidecar:
                msg += f" (+ {sidecar.name})"
            print(msg)
            count += 1
        except Exception as e:
            print(f"ERROR converting {path.name}: {e}")
    return count


def convert_all_datasets(tables_root: Path) -> int:
    """Convert all *.table.json files across all dataset directories."""
    total = 0
    datasets = [d for d in tables_root.iterdir() if d.is_dir() and d.name != "__pycache__"]
    
    if not datasets:
        print(f"No dataset directories found in {tables_root}")
        return 0
    
    for dataset_dir in sorted(datasets):
        table_files = list(dataset_dir.glob("*.table.json"))
        if not table_files:
            continue
            
        print(f"\nDataset: {dataset_dir.name}")
        print("-" * (len(dataset_dir.name) + 9))
        
        for path in sorted(table_files):
            try:
                schema_path, sidecar = convert_file(path, dataset_dir)
                msg = f"  Converted {path.name} → {schema_path.name}"
                if sidecar:
                    msg += f" (+ {sidecar.name})"
                print(msg)
                total += 1
            except Exception as e:
                print(f"  ERROR converting {path.name}: {e}")
    
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="NDJSON → BigQuery array schema converter")
    parser.add_argument("--all", action="store_true", help="Convert all *.table.json across all datasets")
    parser.add_argument("--input", type=str, help="Path to a single *.table.json file to convert")
    parser.add_argument("--dataset", type=str, help="Convert all *.table.json in specific dataset directory")
    parser.add_argument("--in-dir", type=str, help="Custom input directory (overrides dataset logic)")
    parser.add_argument("--out-dir", type=str, help="Custom output directory (overrides dataset logic)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    tables_root = (project_root / "infra" / "tables").resolve()

    # Handle single file conversion
    if args.input:
        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = project_root / input_path
        
        if not input_path.exists():
            raise SystemExit(f"Input file not found: {input_path}")
        
        # If dataset specified, output to that dataset directory
        if args.dataset:
            dataset_dir = tables_root / args.dataset
            if not dataset_dir.exists():
                raise SystemExit(f"Dataset directory not found: {dataset_dir}")
            out_dir = dataset_dir
        # If custom out-dir specified, use that
        elif args.out_dir:
            out_dir = (project_root / args.out_dir).resolve()
        # Otherwise, output to same directory as input
        else:
            out_dir = input_path.parent
        
        schema_path, sidecar = convert_file(input_path, out_dir)
        msg = f"Converted {input_path.name} → {schema_path.name}"
        if sidecar:
            msg += f" (+ {sidecar.name})"
        print(msg)
        return

    # Handle custom directory conversion (legacy mode)
    if args.in_dir and args.out_dir:
        in_dir = (project_root / args.in_dir).resolve()
        out_dir = (project_root / args.out_dir).resolve()
        
        if not in_dir.exists():
            raise SystemExit(f"Input directory not found: {in_dir}")
        
        total = convert_all(in_dir, out_dir)
        print(f"\nDone. Converted {total} file(s) from {in_dir} to {out_dir}")
        return

    # Handle specific dataset conversion
    if args.dataset:
        dataset_dir = tables_root / args.dataset
        if not dataset_dir.exists():
            raise SystemExit(f"Dataset directory not found: {dataset_dir}")
        
        total = convert_all(dataset_dir, dataset_dir)
        print(f"\nDone. Converted {total} file(s) in dataset '{args.dataset}'")
        return

    # Default: convert all datasets
    if not tables_root.exists():
        raise SystemExit(f"Tables directory not found: {tables_root}")

    total = convert_all_datasets(tables_root)
    print(f"\nDone. Converted {total} file(s) across all datasets in {tables_root}")


if __name__ == "__main__":
    main()
