# Module: bq_scheduled_queries
# Creates BigQuery scheduled queries (Data Transfer Service) to materialize tables on a cadence.

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

variable "project_id" { type = string }
variable "dataset_id" { type = string }
variable "location"   { type = string }
variable "timezone"   { type = string }

# Map of scheduled queries to create
# key -> {
#   name        : string
#   schedule    : string (cron)
#   destination : string (table_id)
#   write_mode  : string (WRITE_TRUNCATE|WRITE_APPEND)
#   query       : string (SQL)
# }
variable "queries" {
  type = map(object({
    name        = string
    schedule    = string
    destination = string
    write_mode  = string
    query       = string
  }))
}

variable "service_account_email" {
  type        = string
  description = "Optional service account email to run scheduled query executions"
  default     = null
}

resource "google_bigquery_data_transfer_config" "scheduled_query" {
  for_each = var.queries

  project              = var.project_id
  display_name         = each.value.name
  data_source_id       = "scheduled_query"
  location             = var.location
  schedule             = each.value.schedule
  destination_dataset_id = var.dataset_id

  params = merge(
    { query = each.value.query },
    each.value.destination != "" ? { destination_table_name_template = each.value.destination } : {},
    each.value.write_mode  != "" ? { write_disposition               = each.value.write_mode }  : {}
  )

  # Optional: set a custom service account if needed
  service_account_name = var.service_account_email
}
