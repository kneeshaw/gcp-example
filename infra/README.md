# Infra: Terraform operations

This folder contains the Terraform code that provisions the GCP infrastructure for agencies/environments.

- Agencies live under `infra/agencies/<agency>/<env>` (e.g., `infra/agencies/auckland/dev`).
- Reusable Terraform modules live in `infra/modules`.
- Centralized BigQuery schema files and tools live in `infra/schemas`.

The state is stored locally by default (in each agency/env folder). Consider configuring remote state later (see Optional: Remote state).

---

## Prerequisites

- Terraform 1.4+ (1.5+ recommended)
- gcloud CLI installed and authenticated with permissions to manage the target project
  - Authenticate: `gcloud auth application-default login`
  - Ensure the correct project is set in your `*.tfvars` file (we do not rely on `gcloud config set project` here)

Optional: If you use a Service Account key instead, set `GOOGLE_APPLICATION_CREDENTIALS` to a JSON key path.

---

## Quick start (per agency/env)

All commands below are run inside the agency/env directory, for example:

```bash
cd infra/agencies/auckland/dev
```

One-time init:

```bash
terraform init
```

Validate configuration:

```bash
terraform validate
```

Plan changes using the environment variables file (e.g., `dev.tfvars`):

```bash
terraform plan -var-file=dev.tfvars -compact-warnings
```

Apply changes:

```bash
terraform apply -var-file=dev.tfvars -compact-warnings
# or non-interactive
terraform apply -var-file=dev.tfvars -compact-warnings -auto-approve
```

Show outputs:

```bash
terraform output
terraform output -json
```

Destroy (CAUTION):

```bash
terraform destroy -var-file=dev.tfvars -compact-warnings
```

Format code (recommended before commits):

```bash
terraform fmt -recursive
```

---

## Variables and structure

Environment variables are provided via `dev.tfvars` (or another `*.tfvars` file). Key fields:

- `project_id`: GCP project to deploy into
- `gcp_region`: e.g., `australia-southeast1`
- `environment`: e.g., `dev`
- `agency_prefix`: e.g., `akl`
- `bq_dataset`: BigQuery dataset id, e.g., `auckland_data_dev`
- `headers`: per-dataset HTTP headers for source APIs
- `datasets`: a map of datasets with:
  - `spec`: `rt` (realtime) or `sc` (schedule)
  - `response_type`: `json` | `protobuf` | `zip`
  - `source_url`: upstream feed URL
  - `tables`: BigQuery tables to create (`schema_file` points to array schema JSON)
  - `functions`: function configurations (enqueuer, fetch, transform) with resources and triggers

Bucket name overrides (optional):

- `artifact_bucket_name`: overrides default `<project>-<region>-gcf-src`
- `data_bucket_name`: overrides default `<project>-<agency>-<env>-data`

Example overrides in `dev.tfvars`:

```hcl
artifact_bucket_name = "my-src-bucket"
data_bucket_name     = "akl-data-dev"
```

Note: GCS bucket names are global. If a name is taken, pick another.

---

## Auth and protection model

All Cloud Functions (2nd gen) are protected by default:

- No public invoker (no `allUsers` bindings)
- Cloud Scheduler service account is granted `roles/run.invoker` on functions it schedules
- The shared Functions service account is granted `roles/run.invoker` to allow internal calls (e.g., enqueuer → fetch)

Manual testing with auth:

```bash
# Replace <FUNCTION_URL> with the value from terraform output
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
     -H "Content-Type: application/json" \
     -X POST "<FUNCTION_URL>" \
     -d '{"source":"manual"}'
```

---

## BigQuery schemas

Centralized schema files live in `infra/schemas/files`. Terraform consumes BigQuery array-schema JSON files (e.g., `rt_vehicle_positions.schema.json`).

If you have NDJSON source schema (one field per line) you can convert it using the provided tool:

```bash
# From repo root
python3 infra/schemas/ndjson_to_bq_schema.py \
  --in infrastructure/schemas/files \
  --out infra/schemas/files
```

Then point your `dev.tfvars` table to the generated `*.schema.json` under `infra/schemas/files`.

---

## Common troubleshooting

- Cloud Run invoker IAM error during apply (run.services.setIamPolicy):
  - Ensure your Terraform identity has permission to set Cloud Run IAM (e.g., `roles/run.admin`), then re-run apply.
- 401/403 when calling function:
  - These services require authentication; call with an identity token as shown above (Scheduler and internal functions already have IAM bindings).
- BigQuery 403 bigquery.jobs.create:
  - Ensure the Functions service account has project-level `roles/bigquery.jobUser` in addition to dataset-level `roles/bigquery.dataEditor`.
- Bucket name already exists:
  - Set a different `data_bucket_name`/`artifact_bucket_name` override in your `*.tfvars`.

---

## Adding a new agency/environment

1) Copy an existing env folder, e.g., `infra/agencies/auckland/dev` → `infra/agencies/<new-agency>/<new-env>`
2) Update `<env>.tfvars`:
   - `project_id`, `environment`, `agency_prefix`, optional bucket overrides
   - datasets/functions as needed
3) Run:

```bash
terraform init
terraform plan -var-file=<env>.tfvars
terraform apply -var-file=<env>.tfvars -auto-approve
```

---

## Optional: Remote state (GCS)

By default, state files (`terraform.tfstate*`) are local to each env folder. To use remote state, add a backend block to the root module(s):

```hcl
terraform {
  backend "gcs" {
    bucket = "<your-tf-state-bucket>"
    prefix = "infra/agencies/auckland/dev"
  }
}
```

You must create the state bucket beforehand (outside this Terraform config or via a bootstrap step). Once configured, run `terraform init -migrate-state`.

---

## Conventions

- Keep secrets in Secret Manager (not in tfvars or code).
- Prefer centralized schema files in `infra/schemas/files` to avoid duplication per agency.
- Use `terraform fmt` and `terraform validate` before committing changes.
