variable "dataset_id" {
  description = "BigQuery dataset where the view will be created"
  type        = string
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "view_id" {
  description = "View identifier (name)"
  type        = string
}

variable "sql_file" {
  description = "Path to SQL file containing the view definition"
  type        = string
}

variable "description" {
  description = "Optional description for the view"
  type        = string
  default     = null
}

variable "labels" {
  description = "Optional labels for the view"
  type        = map(string)
  default     = {}
}

variable "timezone" {
  description = "Timezone for datetime calculations"
  type        = string
  default     = "Pacific/Auckland"
}

resource "google_bigquery_table" "view" {
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = var.view_id

  description = var.description
  labels      = var.labels

  view {
    query = templatefile(var.sql_file, {
      project_id = var.project_id
      dataset_id = var.dataset_id
      timezone   = var.timezone
    })
    use_legacy_sql = false
  }

  deletion_protection = false
}

output "view_id" {
  description = "The view ID"
  value       = google_bigquery_table.view.table_id
}

output "view_ref" {
  description = "Full view reference"
  value       = "${var.project_id}.${var.dataset_id}.${google_bigquery_table.view.table_id}"
}