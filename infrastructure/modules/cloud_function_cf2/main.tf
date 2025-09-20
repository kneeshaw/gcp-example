variable "project_id" {
  type = string
}
variable "region" {
  type = string
}
variable "function_name" {
  type = string
}
variable "runtime" {
  type    = string
  default = "python311"
}
variable "entry_point" {
  type = string
}
variable "service_account_email" {
  type = string
}
variable "env_vars" {
  type    = map(string)
  default = {}
}
variable "src_bucket" {
  type = string
}
variable "src_object" {
  type = string
}

variable "source_hash" {
  type    = string
  default = ""
}

resource "google_cloudfunctions2_function" "fn" {
  name        = var.function_name
  location    = var.region
  description = "Managed by Terraform module cloud_function_cf2"

  build_config {
    runtime     = var.runtime
    entry_point = var.entry_point
    source {
      storage_source {
        bucket = var.src_bucket
        object = var.src_object
      }
    }
  }

  service_config {
    service_account_email = var.service_account_email
    available_memory      = var.memory_limit
    available_cpu         = var.cpu_limit
    timeout_seconds       = 540
    min_instance_count    = 0
    max_instance_count    = 1
    environment_variables = merge(var.env_vars, {
      SOURCE_HASH = var.source_hash
    })
    ingress_settings      = "ALLOW_ALL"
  }
}

output "name" {
  value = google_cloudfunctions2_function.fn.name
}

output "uri" {
  value = google_cloudfunctions2_function.fn.service_config[0].uri
}
