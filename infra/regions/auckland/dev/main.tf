locals {
  bucket_location = coalesce(var.bucket_location, var.gcp_region)

  # Normalize headers once (encode to base64 JSON for fetch function)
  headers_b64 = base64encode(jsonencode(var.headers))

  # Minimal set of required Google APIs for this stack
  required_services = [
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudtasks.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
  ]
}

# Enable core services
module "services" {
  source     = "../../../modules/project_services"
  project_id = var.project_id
  services   = local.required_services
}

# Source artifact bucket + upload
module "src_artifact" {
  source        = "../../../modules/source_artifact"
  project_id    = var.project_id
  location      = var.gcp_region
  # Allow override of artifact bucket name; default keeps project_id prefix for global uniqueness
  bucket_name   = coalesce(var.artifact_bucket_name, lower(replace("${var.project_id}-${var.gcp_region}-gcf-src", "_", "-")))
  create_bucket = true
  source_dir    = "${path.module}/../../../../src"
  object_prefix = "functions"

  depends_on = [module.services]
}

# Data bucket for cached files and final outputs
resource "google_storage_bucket" "data" {
  project  = var.project_id
  # Allow override; default includes project_id to minimize collision risk
  name     = coalesce(var.data_bucket_name, lower(replace("${var.project_id}-${var.region_prefix}-${var.environment}-data", "_", "-")))
  location = local.bucket_location
  uniform_bucket_level_access = true
}

# BigQuery dataset
resource "google_bigquery_dataset" "dataset" {
  project    = var.project_id
  dataset_id = var.bq_dataset
  location   = var.gcp_region
}

# Service accounts
resource "google_service_account" "functions" {
  account_id   = "${replace(var.region_prefix, "-", "")}-${var.environment}-functions"
  display_name = "Functions SA (${var.region_prefix}-${var.environment})"
}

resource "google_service_account" "scheduler" {
  account_id   = "${replace(var.region_prefix, "-", "")}-${var.environment}-scheduler"
  display_name = "Scheduler SA (${var.region_prefix}-${var.environment})"
}

# IAM for storage access (fetch/transform use this bucket)
resource "google_storage_bucket_iam_member" "functions_storage_admin" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.functions.email}"
}

// Datasets configuration (now sourced directly from var.datasets)
locals {
  datasets = var.datasets
}

# IAM for BigQuery edits (transform)
resource "google_bigquery_dataset_iam_member" "functions_bq_editor" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.functions.email}"
}

# Allow Cloud Functions service account to create BigQuery jobs (required for load/insert operations)
resource "google_project_iam_member" "functions_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.functions.email}"
}

# Allow Cloud Functions service account to use the BigQuery Storage Read API
resource "google_project_iam_member" "functions_bq_readsession" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.functions.email}"
}

## Unified functions loop (all non-enqueuer functions)
locals {
  function_defs = merge([
    for dname, dcfg in local.datasets : {
      for fname, fcfg in lookup(dcfg, "functions", {}) : "${dname}/${fname}" => {
        ds_name       = dname
        fn_name       = fname
        cfg           = fcfg
        spec          = dcfg.spec
        response_type = dcfg.response_type
        source_url    = dcfg.source_url
      } if fname != "enqueuer"
    }
  ]...)
}

module "functions" {
  for_each = local.function_defs
  source   = "../../../modules/cloud_function_cf2"

  project_id            = var.project_id
  region                = var.gcp_region
  function_name         = lower(replace("${var.region_prefix}-${each.value.ds_name}-${each.value.fn_name}-${var.environment}", "_", "-"))
  entry_point           = "main"
  runtime               = "python311"
  service_account_email = google_service_account.functions.email
  src_bucket            = module.src_artifact.bucket
  src_object            = module.src_artifact.object
  source_hash           = module.src_artifact.source_hash

  memory_limit    = each.value.cfg.resources.memory
  cpu_limit       = each.value.cfg.resources.cpu
  timeout_seconds = tonumber(replace(each.value.cfg.resources.timeout, "s", ""))

  env_vars = {
    FUNCTION      = each.value.fn_name
    PROJECT_ID    = var.project_id
    BUCKET        = google_storage_bucket.data.name
    DATASET       = each.value.ds_name
    SPEC          = each.value.spec
    URL           = each.value.source_url
    HEADERS       = local.headers_b64
    RESPONSE_TYPE = each.value.response_type
    BQ_DATASET    = var.bq_dataset
    TIMEZONE      = var.timezone
  }

  depends_on = [module.services, module.src_artifact]
}

# Allow the functions service account to invoke all non-enqueuer functions (service-to-service auth)
resource "google_cloud_run_v2_service_iam_member" "functions_sa_invoker" {
  for_each = module.functions
  project  = var.project_id
  location = var.gcp_region
  name     = each.value.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.functions.email}"

  depends_on = [module.functions]
}

# Enqueuer + queue (only when defined)
module "enqueuer" {
  for_each = { for name, cfg in local.datasets : name => cfg if contains(keys(cfg.functions), "enqueuer") }
  source   = "../../../modules/tasks_enqueuer"

  project_id                         = var.project_id
  region                             = var.gcp_region
  function_name                      = lower(replace("${var.region_prefix}-${each.key}-enqueuer-${var.environment}", "_", "-"))
  queue_location                     = var.gcp_region
  queue_name                         = lower(replace("${var.region_prefix}-${each.key}-queue-${var.environment}", "_", "-"))
  worker_url                         = try(module.functions["${each.key}/fetch"].uri, "")
  enqueuer_service_account_email     = google_service_account.functions.email
  scheduler_service_account_email    = google_service_account.scheduler.email
  source_bucket                      = module.src_artifact.bucket
  source_object                      = module.src_artifact.object
  source_hash                        = module.src_artifact.source_hash
  offsets                            = "0,5,10,15,20,25,30,35,40,45,50,55"
  timezone                           = var.timezone
  memory_limit                       = each.value.functions.enqueuer.resources.memory
  cpu_limit                          = each.value.functions.enqueuer.resources.cpu
  env_vars                           = {}

  depends_on = [module.functions]
}

# Scheduler jobs for enqueuer
module "enqueuer_schedule" {
  for_each = { for name, cfg in local.datasets : name => cfg if try(cfg.functions.enqueuer.trigger.type, "") == "scheduler" }
  source   = "../../../modules/scheduler_job"

  project_id                      = var.project_id
  region                          = var.gcp_region
  name                            = lower(replace("${var.region_prefix}-${each.key}-enq-${var.environment}", "_", "-"))
  schedule                        = each.value.functions.enqueuer.trigger.cron
  time_zone                       = var.timezone
  target_uri                      = module.enqueuer[each.key].enqueuer_uri
  invoker_service_account_email   = google_service_account.scheduler.email
  function_name                   = module.enqueuer[each.key].enqueuer_name

  depends_on = [module.enqueuer]
}

# Scheduler jobs for non-enqueuer scheduled functions (e.g., transform)
module "functions_schedule" {
  for_each = { for key, v in local.function_defs : key => v if try(v.cfg.trigger.type, "") == "scheduler" }
  source   = "../../../modules/scheduler_job"

  project_id = var.project_id
  region     = var.gcp_region
  name       = lower(replace("${var.region_prefix}-${replace(each.key, "/", "-")}-${var.environment}", "_", "-"))
  schedule   = each.value.cfg.trigger.cron
  time_zone  = var.timezone
  target_uri = module.functions[each.key].uri
  invoker_service_account_email = google_service_account.scheduler.email
  function_name = module.functions[each.key].name

  depends_on = [module.functions]
}

output "data_bucket" { value = google_storage_bucket.data.name }
output "source_bucket" { value = module.src_artifact.bucket }
output "source_object" { value = module.src_artifact.object }
output "source_hash" { value = module.src_artifact.source_hash }

output "fetch_urls" { value = { for k, m in module.functions : split("/", k)[0] => m.uri if endswith(k, "/fetch") } }
output "transform_urls" { value = { for k, m in module.functions : split("/", k)[0] => m.uri if endswith(k, "/transform") } }
output "enqueuer_urls" { value = { for k, m in module.enqueuer : k => m.enqueuer_uri } }

# BigQuery tables
locals {
  stg_models_dir = "${path.module}/../../../models/stg"
  dim_models_dir = "${path.module}/../../../models/dim"

  stg_table_defs = {
    for f in fileset(local.stg_models_dir, "*.table.json") : replace(basename(f), ".table.json", "") => {
      table_name        = replace(basename(f), ".table.json", "")
      table_config_file = "${local.stg_models_dir}/${f}"
    }
  }

  dim_table_defs = {
    for f in fileset(local.dim_models_dir, "*.table.json") : replace(basename(f), ".table.json", "") => {
      table_name        = replace(basename(f), ".table.json", "")
      table_config_file = "${local.dim_models_dir}/${f}"
    }
  }

  # Merge to get all table definitions from stg and dim; if duplicate keys exist, dim overrides stg
  # Also explicitly exclude dim_region from stg to avoid confusion while migrating
  table_defs = merge({ for k, v in local.stg_table_defs : k => v if k != "dim_region" }, local.dim_table_defs)
}

module "bq_tables" {
  for_each   = local.table_defs
  source     = "../../../modules/bigquery_table"

  project_id = var.project_id
  dataset_id = var.bq_dataset
  # Use the table_name derived from the .table.json filename
  table_id   = lower(replace(each.value.table_name, "-", "_"))
  table_config_file = each.value.table_config_file
  # Allow replacement to apply meta changes (dev). Consider toggling back to true after migration.
  deletion_protection = false

  depends_on = [google_bigquery_dataset.dataset]
}

# Seed dim_region from environment variables (idempotent)
resource "google_bigquery_job" "seed_dim_region" {
  project  = var.project_id
  location = var.gcp_region
  job_id   = "seed-dim-region-${var.bq_dataset}-${random_id.dim_region_seed.hex}"

  # ensure table exists first (tables are built from infra/models/stg/*.table.json)
  depends_on = [module.bq_tables["dim_region"]]

  query {
    use_legacy_sql = false
    query = <<-SQL
      MERGE `${var.project_id}.${var.bq_dataset}.dim_region` T
      USING (
        SELECT
          -- Ensure non-empty ID and sane defaults
          UPPER(NULLIF("${var.region_prefix}", "")) AS region_id,
          COALESCE(NULLIF("${coalesce(var.region_name, var.transit_authority)}", ""), UPPER(NULLIF("${var.region_prefix}", ""))) AS region_name,
          COALESCE(NULLIF("${var.timezone}", ""), "UTC") AS timezone,
          CAST(GREATEST(0, LEAST(23, ${var.service_boundary_hour})) AS INT64) AS svc_boundary_hour
      ) S
      ON T.region_id = S.region_id
      WHEN MATCHED THEN
        UPDATE SET
          region_name = S.region_name,
          timezone = S.timezone,
          svc_boundary_hour = S.svc_boundary_hour,
          updated_at = CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN
        INSERT (region_id, region_name, timezone, svc_boundary_hour, updated_at)
        VALUES (S.region_id, S.region_name, S.timezone, S.svc_boundary_hour, CURRENT_TIMESTAMP())
    SQL
  }
}

# Stable random suffix to avoid BigQuery job-id collisions across stacks/runs
resource "random_id" "dim_region_seed" {
  byte_length = 2 # 4 hex chars
  keepers = {
    project   = var.project_id
    dataset   = var.bq_dataset
    region    = var.gcp_region
  }
}

# BigQuery views (build directly from SQL files under infra/models/*)
locals {
  models_dir   = "${path.module}/../../../models"
  view_files   = distinct(concat(
    tolist(fileset(local.models_dir, "fct/**/*.sql")),
    tolist(fileset(local.models_dir, "dim/**/*.sql")),
    tolist(fileset(local.models_dir, "agg/**/*.sql")),
    tolist(fileset(local.models_dir, "mdl/**/*.sql")),
    tolist(fileset(local.models_dir, "baseline/**/*.sql"))
  ))
  folder_views = {
    for f in local.view_files : replace(basename(f), ".sql", "") => {
      sql_file    = "${local.models_dir}/${f}"
      description = "View from ${f}"
    }
  }

  # Ensure dependency order for views that reference others
  base_view_names      = ["fct_trip_update", "fct_vehicle_position", "fct_trip_plan"]
  base_folder_views    = { for k, v in local.folder_views : k => v if contains(local.base_view_names, k) }
  derived_folder_views = { for k, v in local.folder_views : k => v if !contains(local.base_view_names, k) }
}

# Create base views first
module "bq_views_base" {
  for_each = local.base_folder_views
  source   = "../../../modules/bigquery_view"

  project_id  = var.project_id
  dataset_id  = var.bq_dataset
  view_id     = lower(replace(each.key, "-", "_"))
  sql_file    = each.value.sql_file
  description = each.value.description

  depends_on = [google_bigquery_dataset.dataset, module.bq_tables]
}

# Then create derived views that depend on base views
module "bq_views" {
  for_each = local.derived_folder_views
  source   = "../../../modules/bigquery_view"

  project_id  = var.project_id
  dataset_id  = var.bq_dataset
  view_id     = lower(replace(each.key, "-", "_"))
  sql_file    = each.value.sql_file
  description = each.value.description

  depends_on = [google_bigquery_dataset.dataset, module.bq_tables, module.bq_views_base]
}

