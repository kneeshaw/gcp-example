## GTFS-RT scheduled ingestion to GCS

This repo contains Terraform code and a Python Cloud Function that downloads GTFS-RT vehicle positions on a schedule, zips the payload, and stores it in GCS.

Folders:

Quick start:
1) Create `infrastructure/terraform.tfvars` with:

project_id = "your-gcp-project"
region     = "europe-west1"
gtfs_url   = "https://example.org/gtfs-rt/vehiclePositions"

2) Deploy from `infrastructure/`:

```sh
terraform init
terraform apply
```

3) Check outputs. A Cloud Scheduler job will invoke the function every 5 minutes by default.

Notes:

---

# GCP Example: Transit Analytics & OSM Integration

This repository now also includes infrastructure (Terraform) and Python code to ingest GTFS static/RT into BigQuery, build analytical views (planned vs actual, trip/stop facts), and map GTFS shapes to OSM road links. Heavy spatial joins are materialized via BigQuery scheduled queries for performance.

## Structure

- `infrastructure/` Terraform modules and region envs
	- `modules/bigquery_views` – manages views from SQL files
	- `modules/bq_scheduled_queries` – configures BigQuery Data Transfer jobs
	- `regions/auckland/envs/dev` – dev environment wiring (`dev.tfvars`)
- `src/` Python workers, schemas (Pandera), and utilities
- `notebooks/` Exploration notebooks

## Dev deploy

From `infrastructure/regions/auckland/envs/dev`:

1. Set values in `dev.tfvars` (project_id, region, gcp_region, timezone, etc.)
2. `terraform init`
3. `terraform apply -var-file=dev.tfvars`

This creates:
- BigQuery dataset and views (vw_planned_vs_actual, vw_fact_stop_events, etc.)
- Scheduled queries:
	- Materialize `shapes_geog` (01:45)
	- Materialize `trip_edges` (02:00)
	- Materialize `route_edges` (02:30)

You can run them immediately via BigQuery → Transfers → Run now.

## Notes

- Secrets and local envs are ignored by `.gitignore` (tfvars, venvs, creds).
- Python uses Application Default Credentials; ensure `gcloud auth application-default login` or set a service account.