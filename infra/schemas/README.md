# Infra Schemas

This folder contains BigQuery-compatible array schema files (`*.schema.json`) generated from NDJSON table files (`*.table.json`).

- Source NDJSON lives in `infrastructure/schemas/files`
- Generated array schemas are written here: `infra/schemas/files`

## Generate

Convert all schemas from source to infra format:

```bash
python infra/schemas/ndjson_to_bq_schema.py --all
```

Convert a single file:

```bash
python infra/schemas/ndjson_to_bq_schema.py --input infrastructure/schemas/files/rt_vehicle_positions.table.json
```

The converter also writes a sidecar `<name>.meta.json` (partitioning/clustering/description) for optional use in Terraform later.
