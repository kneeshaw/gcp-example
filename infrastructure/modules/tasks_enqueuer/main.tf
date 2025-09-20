variable "project_id" { type = string }
variable "region" { type = string }
variable "function_name" { type = string }
variable "queue_location" { type = string }
variable "queue_name" { type = string }
variable "worker_url" { type = string }
variable "enqueuer_service_account_email" { type = string }
variable "scheduler_service_account_email" { type = string }
variable "source_bucket" { type = string }
variable "source_object" { type = string }
variable "offsets" {
  type    = string
  default = "0,5,10,15,20,25,30,35,40,45,50,55"
}

variable "source_hash" {
  type    = string
  default = ""
}

variable "timezone" {
  type    = string
  default = "UTC"
}

variable "function" {
  type    = string
  default = "enqueuer"
}

 

variable "memory_limit" {
  type    = string
  default = "512M"
}

variable "cpu_limit" {
  type    = string
  default = "1"
}

variable "env_vars" {
  description = "Additional environment variables to inject into the enqueuer function"
  type        = map(string)
  default     = {}
}

// memory_limit and cpu_limit are defined in variables.tf for this module

resource "google_cloud_tasks_queue" "queue" {
  name     = var.queue_name
  location = var.queue_location
  rate_limits {
    max_dispatches_per_second = 5
    max_concurrent_dispatches = 10
  }
  retry_config {
    max_attempts = 3
  }
}

resource "google_cloudfunctions2_function" "enqueuer" {
  name     = var.function_name
  location = var.region


  build_config {
    runtime     = "python311"
    entry_point = "main"  # Changed to use unified entry point
    source {
      storage_source {
        bucket = var.source_bucket
        object = var.source_object
      }
    }
  }

  service_config {
    service_account_email = var.enqueuer_service_account_email
    available_memory      = coalesce(var.memory_limit, "256M")
    available_cpu         = var.cpu_limit
    timeout_seconds       = 60
    environment_variables = merge({
      QUEUE_LOCATION        = var.queue_location
      QUEUE_NAME            = var.queue_name
      WORKER_URL            = var.worker_url
      SERVICE_ACCOUNT_EMAIL = var.enqueuer_service_account_email
      PROJECT_ID            = var.project_id
      OFFSETS               = var.offsets
      SOURCE_HASH           = var.source_hash
      FUNCTION              = var.function
      TIMEZONE              = var.timezone
    }, var.env_vars)
    ingress_settings = "ALLOW_ALL"
  }
}

resource "google_cloud_run_v2_service_iam_member" "invoke_enqueuer" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.enqueuer.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.scheduler_service_account_email}"
}

# Explicit queue-level IAM so enqueuer SA can create tasks (defense in depth if project role propagation lags)
resource "google_cloud_tasks_queue_iam_binding" "enqueuer_create_tasks" {
  project  = var.project_id
  location = var.queue_location
  name     = google_cloud_tasks_queue.queue.name
  role     = "roles/cloudtasks.enqueuer"
  members  = ["serviceAccount:${var.enqueuer_service_account_email}"]
}

output "queue_name" { value = google_cloud_tasks_queue.queue.name }
output "enqueuer_name" { value = google_cloudfunctions2_function.enqueuer.name }
output "enqueuer_uri" { value = google_cloudfunctions2_function.enqueuer.service_config[0].uri }