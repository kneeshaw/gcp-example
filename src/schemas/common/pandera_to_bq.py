#!/usr/bin/env python3
"""
Minimal converter: generate BigQuery table JSONs from Pandera DataFrameModels
- Scans ONLY top-level modules in src/schemas (ignores subdirectories)
- Writes ONLY <table>.table.json files to infra/models/stg
"""

import argparse
import importlib
import inspect
import json
import sys
from pathlib import Path

# Locate project root (folder containing 'src') and ensure imports work
_here = Path(__file__).resolve()
_root = next((p for p in [_here] + list(_here.parents) if (p / "src").is_dir()), _here)
ROOT = _root
SRC = ROOT / "src"
SCHEMAS_DIR = SRC / "schemas"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    import pandera.pandas as pa_pd
except Exception as e:
    raise SystemExit("pandera and pandas are required. Install with `pip install pandera[pandas]`. ") from e


def bq_type(dtype: object) -> str:
    s = (str(dtype) or "").lower()
    if "int" in s:
        return "INT64"
    if any(x in s for x in ("float", "decimal", "numeric")):
        return "FLOAT"
    if "bool" in s:
        return "BOOL"
    if any(x in s for x in ("datetime", "timestamp", "datetime64")):
        return "TIMESTAMP"
    if s == "date":
        return "DATE"
    if s == "time":
        return "TIME"
    return "STRING"


def to_fields(schema_model: type) -> list:
    df_schema = schema_model.to_schema()
    fields = []
    for name, col in df_schema.columns.items():
        desc = getattr(col, "description", None)
        if desc is None:
            meta = getattr(col, "metadata", None)
            if isinstance(meta, dict):
                desc = meta.get("description")
        field = {
            "name": name,
            "type": bq_type(getattr(col, "dtype", None)),
            "mode": "NULLABLE" if getattr(col, "nullable", True) else "REQUIRED",
        }
        if desc:
            field["description"] = desc
        fields.append(field)
    return fields


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default=str(ROOT / "infra" / "models" / "stg"))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Discover top-level modules in src/schemas (ignore subdirectories)
    top_level_modules = [
        p.stem for p in SCHEMAS_DIR.glob("*.py")
        if p.is_file() and p.name not in {"__init__.py"}
    ]

    for mod_stem in top_level_modules:
        mod_name = f"schemas.{mod_stem}"
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for _, obj in inspect.getmembers(mod, inspect.isclass):
            if obj.__module__ != mod.__name__:
                continue
            base = getattr(pa_pd, "DataFrameModel", object)
            if not issubclass(obj, base):
                continue

            fields = to_fields(obj)
            base_name = getattr(obj, "_bigquery_table_name", None) or obj.__name__

            table_cfg: dict = {"schema": {"fields": fields}}
            desc = getattr(obj, "_description", None)
            if desc:
                table_cfg["description"] = desc
            partitioning = getattr(obj, "_bigquery_partitioning", None)
            if isinstance(partitioning, dict) and partitioning:
                table_cfg["timePartitioning"] = partitioning
            clustering = getattr(obj, "_bigquery_clustering", None)
            if isinstance(clustering, (list, tuple)) and clustering:
                table_cfg["clustering"] = {"fields": list(clustering)}

            out_file = out_dir / f"{base_name}.table.json"
            with out_file.open("w", encoding="utf-8") as f:
                json.dump(table_cfg, f, indent=2)
                f.write("\n")
            print(f"wrote {out_file}")


if __name__ == "__main__":
    main()
