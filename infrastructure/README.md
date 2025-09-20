# Infrastructure Apply Guide

This document explains how to plan and apply Terraform changes for the regional environment(s) in this repository.

## Directory Layout
```
infrastructure/
  modules/              # Reusable Terraform modules (region_env, gcs_bucket, etc.)
  regions/
    auckland/
      envs/
        dev/            # Environment root module for region "auckland" + env "dev"
          main.tf
          variables.tf
          versions.tf
          dev.tfvars     # Environment variable values (project, region, products...)
```

Each `env` directory (e.g. `dev`) is a Terraform root module. You run Terraform commands from inside that directory.

## Prerequisites
- Terraform CLI installed (>= 1.5 recommended)
- Authenticated to Google Cloud (e.g. `gcloud auth application-default login` or workload identity)
- The `project_id` in the tfvars already exists and you have permissions (project owner/editor + service account creation + IAM).

## One-Time Init
From the environment directory:
```sh
cd infrastructure/regions/auckland/envs/dev
terraform init
```
Re-run `terraform init -upgrade` if provider/module versions change.

## Planning Changes
Use the environment's tfvars file:
```sh
terraform plan -var-file=dev.tfvars -out=plan.dev
```
Common additions:
- Add `-refresh-only` to just reconcile state without proposing changes.
- Add `-target=resource.address` to isolate specific resources (diagnostics only—avoid committing targeted applies).

## Applying Changes
Preferred (two-step for safety):
```sh
terraform apply plan.dev
```
Direct (plan + apply in one step):
```sh
tf apply -var-file=dev.tfvars
# or fully spelled out
terraform apply -var-file=dev.tfvars
```
With auto-approval (CI or scripted):
```sh
terraform apply -var-file=dev.tfvars -auto-approve
```

## Changing Variables
Edit `dev.tfvars` to adjust:
- `headers` map (common per-product headers) — stored base64 in env var for functions
- `products` map (URLs, response types, cadence)
  - `rate.offsets` => enables Cloud Tasks enqueuer (sub-minute); omit to use Cloud Scheduler cron
  - `rate.cron` => standard cron (UTC) if using scheduler

After editing, re-run plan/apply.

## Adding a New Product
1. Add a new block under `products` in `dev.tfvars`, e.g.:
```hcl
  vehicle-positions = {
    url           = "https://example/api/vehicle_positions"
    response_type = "json"
    rate = { offsets = [0,5,10,15,20,25,30,35,40,45,50,55] }
  }
```
2. `terraform plan -var-file=dev.tfvars -out=plan.dev`
3. Review resources (new queue, function, scheduler job, etc.).
4. `terraform apply plan.dev`

## Destroying (Caution)
Always supply the same tfvars file:
```sh
terraform destroy -var-file=dev.tfvars
```
If queues were recently deleted, Cloud Tasks might hold names for ~7 days; queue names include a `-v2-` token to avoid collision. Increment if needed.

## State Management
Currently assumes local state. For team usage, configure a remote backend (e.g. GCS bucket):
```hcl
# Example backend block (in versions.tf or a backend.hcl)
terraform {
  backend "gcs" {
    bucket = "my-terraform-state-bucket"
    prefix = "infra/auckland/dev"
  }
}
```
Then initialize/migrate:
```sh
terraform init -migrate-state
```

## Outputs
After apply, retrieve outputs:
```sh
terraform output -json | jq
```
Key outputs:
- `data_bucket` – GCS bucket receiving ingested data
- `worker_urls` – Map of product => worker function URL
- `enqueuer_urls` – Map for products using Cloud Tasks fan-out

## Troubleshooting
| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| 403 invoking worker | Missing IAM invoker role | Ensure scheduler SA has token creator & Cloud Run invoker binding present |
| Duplicate uploads missing | Hash dedup working (schedule) | Expected; same ZIP hash short-circuits upload |
| Queue name reuse error | Cloud Tasks name retention | Bump queue version suffix |
| Plan shows recreate of buckets | Force destroy / manual deletion | Avoid manual deletions; import if needed |

## Conventions Recap
- Real-time objects: `product/year=YYYY/month=MM/day=DD/hour=HH/product-<timestamp>.<ext>[.gz]`
- Schedule objects: `schedule/year=YYYY/<md5>.zip` + `schedule/latest.zip`
- Non-zip payloads gzip-compressed; `content_encoding=gzip` set.

## Safe Workflow Summary
```sh
cd infrastructure/regions/auckland/envs/dev
terraform fmt -check
terraform validate
terraform plan -var-file=dev.tfvars -out=plan.dev
terraform show -no-color plan.dev | less
terraform apply plan.dev
```

## Optional: Drift Detection (No Changes Apply)
```sh
terraform plan -var-file=dev.tfvars -detailed-exitcode
# exit code 0 = no changes, 2 = changes, 1 = error
```

---
Questions or improvements? Add to this file to keep the runbook current.
