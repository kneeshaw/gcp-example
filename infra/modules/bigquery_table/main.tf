variable "project_id" { type = string }
variable "dataset_id" { type = string }
variable "table_id" { type = string }
variable "schema_file" {
  type    = string
  default = null
}
variable "schema_json" {
  type    = string
  default = null
}

# Optional partitioning and clustering. Omit by default.
variable "partition_type" {
  type    = string
  default = null
}
variable "partition_field" {
  type    = string
  default = null
}
variable "clustering" {
  type    = list(string)
  default = []
}
variable "labels" {
  type    = map(string)
  default = {}
}
variable "deletion_protection" {
  type    = bool
  default = true
}

resource "google_bigquery_table" "table" {
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = var.table_id

  deletion_protection = var.deletion_protection

  dynamic "time_partitioning" {
    for_each = var.partition_type == null && var.partition_field == null ? [] : [1]
    content {
      type  = coalesce(var.partition_type, "DAY")
      field = var.partition_field
    }
  }

  clustering = var.clustering

  # Prefer inline JSON when provided (avoids module path restrictions on file())
  schema = var.schema_json != null ? var.schema_json : file(var.schema_file)
  labels = var.labels
}

output "table_id" { value = google_bigquery_table.table.table_id }
output "table_ref" { value = "${var.dataset_id}.${google_bigquery_table.table.table_id}" }
