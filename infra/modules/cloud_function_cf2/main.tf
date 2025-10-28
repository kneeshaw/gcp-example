variable "project_id" { type = string }
variable "region" { type = string }
variable "function_name" { type = string }
variable "runtime" {
  type    = string
  default = "python311"
}
variable "entry_point" { type = string }
variable "service_account_email" { type = string }
variable "env_vars" {
  type    = map(string)
  default = {}
}
variable "src_bucket" { type = string }
variable "src_object" { type = string }
variable "source_hash" {
  type    = string
  default = ""
}

variable "memory_limit" {
  type    = string
  default = "512M"
}
variable "cpu_limit" {
  type    = string
  default = "1"
}
variable "timeout_seconds" {
  type    = number
  default = 540
}
variable "min_instance_count" {
  type    = number
  default = 0
}
variable "max_instance_count" {
  type    = number
  default = 1
}
variable "ingress_settings" {
  type    = string
  default = "ALLOW_ALL"
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
    timeout_seconds       = var.timeout_seconds
    min_instance_count    = var.min_instance_count
    max_instance_count    = var.max_instance_count
    environment_variables = merge(var.env_vars, {
      SOURCE_HASH = var.source_hash
    })
    ingress_settings      = var.ingress_settings
  }
}

output "name" { value = google_cloudfunctions2_function.fn.name }
output "uri"  { value = google_cloudfunctions2_function.fn.service_config[0].uri }
