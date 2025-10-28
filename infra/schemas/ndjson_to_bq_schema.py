#!/usr/bin/env python3
"""
NDJSON → BigQuery Array Schema Converter
=======================================

Converts *.table.json (NDJSON: first line metadata + one field per line)
into BigQuery-compatible array schema files (*.schema.json).

Defaults:
- Input dir: infrastructure/schemas/files
- Output dir: infra/schemas/files

Usage:
  python infra/schemas/ndjson_to_bq_schema.py                # convert all in default paths
  python infra/schemas/ndjson_to_bq_schema.py --all          # same as default
  python infra/schemas/ndjson_to_bq_schema.py --input my.table.json  # single file
  python infra/schemas/ndjson_to_bq_schema.py --in-dir infrastructure/schemas/files --out-dir infra/schemas/files

Notes:
- Writes <name>.schema.json to the output directory.
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


def main() -> None:
    parser = argparse.ArgumentParser(description="NDJSON → BigQuery array schema converter")
    parser.add_argument("--all", action="store_true", help="Convert all *.table.json in input directory")
    parser.add_argument("--input", type=str, help="Path to a single *.table.json file to convert")
    parser.add_argument("--in-dir", type=str, default="infrastructure/schemas/files", help="Input directory")
    parser.add_argument("--out-dir", type=str, default="infra/schemas/files", help="Output directory")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    in_dir = (project_root / args.in_dir).resolve()
    out_dir = (project_root / args.out_dir).resolve()

    if args.input:
        src = (project_root / args.input).resolve()
        if not src.exists():
            raise SystemExit(f"Input file not found: {src}")
        schema_path, sidecar = convert_file(src, out_dir)
        msg = f"Converted {src.name} → {schema_path.name}"
        if sidecar:
            msg += f" (+ {sidecar.name})"
        print(msg)
        return

    # Default: convert all
    if not in_dir.exists():
        raise SystemExit(f"Input directory not found: {in_dir}")

    total = convert_all(in_dir, out_dir)
    print(f"\nDone. Converted {total} file(s) to {out_dir}")


if __name__ == "__main__":
    main()
