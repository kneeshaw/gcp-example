data "google_project" "current" {}

# Locals: naming + dataset shaping (normalize keys for for_each) 
locals {
  bucket_location     = coalesce(var.bucket_location, var.gcp_region)
  env_tag             = var.environment
  src_bucket_name     = "${var.region}-fn-src-${local.env_tag}"
  data_bucket_name    = "${var.region}-data-${local.env_tag}"
  common_headers_b64  = var.headers != null ? base64encode(jsonencode(var.headers)) : null
  datasets = {
    for raw_k, raw_v in var.datasets : raw_k => {
      url           = raw_v.url
      dataset       = raw_k # The dataset key is now the canonical name
      response_type = raw_v.response_type
      spec          = try(raw_v.spec, null)
      function      = try(raw_v.function, null)
      use_enqueuer  = try(length(raw_v.rate.offsets) > 0, false)
      offsets       = try(join(",", raw_v.rate.offsets), null)
      cron          = try(raw_v.rate.cron, null)
      memory_limit  = try(raw_v.memory_limit, null)
      cpu_limit     = try(raw_v.cpu_limit, null)
    }
  }
}

# --- UNIFIED SOURCE HASH: Entire src directory ---
locals {
  # Absolute path to repository src directory
  src_dir     = abspath("${path.module}/../../../src")
  source_hash = sha256(join("", [
    for f in fileset(local.src_dir, "**") : filesha256("${local.src_dir}/${f}")
  ]))
}

# --- UNIFIED STAGING: Single directory for all functions ---
resource "null_resource" "prepare_unified_src" {
  triggers = { source_hash = local.source_hash }

  provisioner "local-exec" {
    command = <<-EOT
      set -euo pipefail
      STAGE_DIR="${path.module}/.terraform/build/unified"
      rm -rf "$STAGE_DIR"
      mkdir -p "$STAGE_DIR"

  # Mirror entire src directory (including dotfiles) into stage and remove stale files
  rsync -a --delete "${local.src_dir}/" "$STAGE_DIR/"
    EOT
  }
}

# --- UNIFIED ZIP FILE ---
data "archive_file" "unified_src_zip" {
  type        = "zip"
  output_path = "${path.module}/.terraform/build/unified.zip"
  source_dir  = "${path.module}/.terraform/build/unified"

  depends_on = [null_resource.prepare_unified_src]
}

# Upload unified zip to the source bucket
resource "google_storage_bucket_object" "unified_src_zip" {
  name       = "unified/${data.archive_file.unified_src_zip.output_md5}.zip"
  bucket     = module.src_bucket.name
  source     = data.archive_file.unified_src_zip.output_path
  depends_on = [module.services]
}

# Enable required Google Cloud APIs for this project
module "services" {
  source     = "../project_services"
  project_id = var.project_id
  services = [
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudtasks.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "bigquerydatatransfer.googleapis.com",
  ]
}

# Service account for Cloud Functions
resource "google_service_account" "function" {
  account_id   = "cf-${var.environment}-${var.region}"
  display_name = "Cloud Function Service Account"
}

# Service Account used by Cloud Scheduler to invoke workers/enqueuers via OIDC
resource "google_service_account" "scheduler" {
  account_id   = "${var.region}-scheduler-sa-${local.env_tag}"
  display_name = "${var.region} Scheduler SA ${local.env_tag}"
}

# Allow Cloud Scheduler service to mint OIDC tokens for the Scheduler SA
resource "google_service_account_iam_member" "scheduler_sa_token_creator" {
  service_account_id = google_service_account.scheduler.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}


# Source bucket for function artifacts (zips)
module "src_bucket" {
  source        = "../gcs_bucket"
  project_id    = var.project_id
  name          = local.src_bucket_name
  location      = local.bucket_location
  force_destroy = true
}

# Data bucket for data storage
module "data_bucket" {
  source     = "../gcs_bucket"
  project_id = var.project_id
  name       = local.data_bucket_name
  location   = local.bucket_location
}

# Grant worker SA permission to write objects to the data bucket
resource "google_storage_bucket_iam_member" "function_write_data" {
  bucket = module.data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.function.email}"
}

# Grant function SA all required permissions
resource "google_project_iam_member" "function_permissions" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/cloudtasks.enqueuer",
    "roles/bigquery.jobUser",
    "roles/bigquery.readSessionUser"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.function.email}"
}

# Dataset-level write (create/load/merge) access
resource "google_bigquery_dataset_access" "function_dataset_writer" {
  dataset_id = google_bigquery_dataset.region_data.dataset_id
  project    = var.project_id
  role       = "WRITER"
  user_by_email = google_service_account.function.email
}

# Deploy per-dataset worker Cloud Functions (2nd gen, HTTP)
module "workers" {
  for_each              = local.datasets
  source                = "../cloud_function_cf2"
  project_id            = var.project_id
  region                = var.gcp_region
  function_name         = "${var.region}-${each.value.spec}-${each.value.dataset}-worker-${local.env_tag}"
  entry_point           = "main"  # Changed to use unified entry point
  service_account_email = google_service_account.function.email
  src_bucket            = module.src_bucket.name
  src_object            = google_storage_bucket_object.unified_src_zip.name  # Changed to use unified zip
  source_hash           = local.source_hash
  memory_limit          = each.value.memory_limit
  cpu_limit             = each.value.cpu_limit
  # Each region has a dedicated data bucket; prefixes are per-dataset only (no region in path).
  env_vars = merge({
    BUCKET        = module.data_bucket.name
    URL           = each.value.url
    DATASET       = each.value.dataset
    SPEC          = each.value.spec
    FUNCTION      = each.value.function
    RESPONSE_TYPE = each.value.response_type
    PROJECT_ID    = var.project_id
    BQ_DATASET    = local.bq_dataset_id
    TIMEZONE      = var.timezone
  }, local.common_headers_b64 != null ? { HEADERS = local.common_headers_b64 } : {})
  depends_on = [module.services, google_storage_bucket_object.unified_src_zip]  # Changed to use unified zip
}

# Create Cloud Scheduler jobs for non-enqueuer datasets (minute+ cadence)
module "schedules" {
  for_each  = { for k, v in local.datasets : k => v if v.use_enqueuer == false }
  source    = "../scheduler_job"
  project_id = var.project_id
  region     = var.gcp_region
  name       = "${var.region}-${each.value.spec}-${each.value.dataset}-trigger-${local.env_tag}"
  schedule   = lookup(each.value, "cron", "*/1 * * * *")
  time_zone  = var.timezone
  target_uri = module.workers[each.key].uri
  invoker_service_account_email = google_service_account.scheduler.email
  function_name = module.workers[each.key].name
  depends_on   = [google_service_account_iam_member.scheduler_sa_token_creator]
}

#  Deploy Cloud Tasks enqueuer for high-cadence datasets (e.g., 5s offsets)
module "enqueuers" {
  for_each  = { for k, v in local.datasets : k => v if v.use_enqueuer == true }
  source    = "../tasks_enqueuer" # module name unchanged (could be renamed separately)
  project_id = var.project_id
  region     = var.gcp_region
  function_name = "${var.region}-${each.value.spec}-${each.value.dataset}-enqueuer-${local.env_tag}"
  queue_location = var.gcp_region
  # Queue names are retained for ~7 days after deletion in Cloud Tasks; adding a version token allows recreation after destroy.
  queue_name     = "${var.region}-${each.value.spec}-${each.value.dataset}-queue-v9-${local.env_tag}"
  worker_url     = module.workers[each.key].uri
  enqueuer_service_account_email  = google_service_account.function.email
  scheduler_service_account_email = google_service_account.scheduler.email
  source_bucket = module.src_bucket.name
  source_object = google_storage_bucket_object.unified_src_zip.name  # Changed to use unified zip
  source_hash   = local.source_hash
  offsets       = lookup(each.value, "offsets", null)
  env_vars     = {
    FUNCTION = "enqueuer"
  }
  depends_on    = [google_storage_bucket_object.unified_src_zip]  # Changed to use unified zip
}

# Allow enqueuer to invoke worker (Cloud Run invoker role)
resource "google_cloud_run_v2_service_iam_member" "enqueuer_invokes_worker" {
  for_each = module.enqueuers
  project  = var.project_id
  location = var.gcp_region
  name     = module.workers[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.function.email}"
}

# Schedule each enqueuer to trigger every minute (fan-out handles sub-minute cadence)
resource "google_cloud_scheduler_job" "minute_enqueuer" {
  for_each   = module.enqueuers
  # each.key is the dataset identifier (with hyphens) used in enqueuers map
  name       = "${var.region}-${local.datasets[each.key].spec}-${each.key}-minute-enqueuer-${local.env_tag}"
  description = "Kick ${each.key} enqueuer every minute"
  schedule   = "* * * * *"
  time_zone  = "UTC"

  http_target {
    http_method = "POST"
    uri         = module.enqueuers[each.key].enqueuer_uri
    headers     = { "Content-Type" = "application/json" }
    body        = base64encode(jsonencode({}))
    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = module.enqueuers[each.key].enqueuer_uri
    }
  }

  depends_on = [module.enqueuers]
}

# Allow unauthenticated invocations for worker functions
resource "google_cloud_run_v2_service_iam_member" "worker_allow_unauthenticated" {
  for_each = module.workers
  project  = var.project_id
  location = var.gcp_region
  name     = module.workers[each.key].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Allow unauthenticated invocations for enqueuer functions
resource "google_cloud_run_v2_service_iam_member" "enqueuer_allow_unauthenticated" {
  for_each = module.enqueuers
  project  = var.project_id
  location = var.gcp_region
  name     = module.enqueuers[each.key].enqueuer_name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# BigQuery dataset name pattern (allow override via var.bq_dataset) -> {base}_{env}
locals {
  bq_dataset_base = coalesce(var.bq_dataset, "${var.region}_data")
  bq_dataset_id   = "${local.bq_dataset_base}_${var.environment}"
}

# Create BigQuery dataset for region+env
resource "google_bigquery_dataset" "region_data" {
  dataset_id                 = local.bq_dataset_id
  project                    = var.project_id
  location                   = var.gcp_region
  delete_contents_on_destroy = false
  labels = {
    env    = var.environment
    region = var.region
  }
}

# Find all schema files in the generated directory
locals {
  schema_files = try(fileset("${path.module}/../../schemas/files/", "*.table.json"), [])
}

# Read schema files directly using local_file data source
data "local_file" "schema_files" {
  for_each = toset(local.schema_files)
  filename = "${path.module}/../../schemas/files/${each.value}"
}

# First pass: Parse all JSON objects from each file
locals {
  raw_schema_objects = {
    for filename, content in data.local_file.schema_files : filename => [
      for line in [for l in split("\n", content.content) : trimspace(l) if trimspace(l) != ""] : jsondecode(line)
    ]
  }
}

# Second pass: Extract specific data from parsed objects
locals {
  parsed_schemas = {
    for filename, objects in local.raw_schema_objects : filename => {
      # Extract partitioning (first object with partitioning key, or null)
      partitioning = try([for obj in objects : obj.partitioning if contains(keys(obj), "partitioning")][0], null)

      # Extract clustering (first object with clustering key, or null)
      clustering = try([for obj in objects : obj.clustering if contains(keys(obj), "clustering")][0], null)

      # Extract all field objects (objects with both name and type keys)
      fields = [for obj in objects : obj if contains(keys(obj), "name") && contains(keys(obj), "type")]
    }
  }
}

# Create BigQuery tables for each schema file
resource "google_bigquery_table" "schema_tables" {
  for_each = local.parsed_schemas

  project    = var.project_id
  dataset_id = google_bigquery_dataset.region_data.dataset_id
  table_id   = "${replace(trimsuffix(each.key, ".table.json"), "-", "_")}"

  # Use parsed fields from locals
  schema = jsonencode(each.value.fields)

  # Handle partitioning if present
  dynamic "time_partitioning" {
    for_each = each.value.partitioning != null ? [1] : []
    content {
      type  = each.value.partitioning.type
      field = each.value.partitioning.field
    }
  }

  # Handle clustering if present
  clustering = each.value.clustering

  deletion_protection = false

  labels = {
    env       = var.environment
    region    = var.region
    generated = "true"
  }
}

output "bq_dataset_id" {
  value       = google_bigquery_dataset.region_data.dataset_id
  description = "BigQuery dataset ID for the region env"
}

# --- Debug/Introspection Outputs ---
output "unified_source_object" {
  description = "Name of the unified source zip object uploaded to the src bucket"
  value       = google_storage_bucket_object.unified_src_zip.name
}

output "unified_source_bucket" {
  description = "Bucket holding the unified source zip"
  value       = module.src_bucket.name
}

output "source_hash" {
  description = "Hash of the src/ directory used to build the unified artifact"
  value       = local.source_hash
}

# Expose the Cloud Function service account email for other modules (e.g., Scheduled Queries)
output "function_service_account_email" {
  description = "Service account email used by workers/enqueuers"
  value       = google_service_account.function.email
}