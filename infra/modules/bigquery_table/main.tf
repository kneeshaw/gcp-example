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

# Optional consolidated table configuration (JSON with keys: description, timePartitioning, clustering, schema.fields)
variable "table_config_file" {
  type    = string
  default = null
}
variable "table_config_json" {
  type    = string
  default = null
}

locals {
  table_cfg = var.table_config_json != null ? jsondecode(var.table_config_json) : (
    var.table_config_file != null ? try(jsondecode(file(var.table_config_file)), null) : null
  )

  schema_fields         = local.table_cfg != null ? try(local.table_cfg.schema.fields, null) : null
  effective_schema      = var.schema_json != null ? try(jsondecode(var.schema_json), null) : (local.schema_fields != null ? local.schema_fields : (var.schema_file != null ? try(jsondecode(file(var.schema_file)), null) : null))
  effective_schema_json = local.effective_schema != null ? jsonencode(local.effective_schema) : null

  effective_time_part   = local.table_cfg != null ? try(local.table_cfg.timePartitioning, null) : null
  effective_clustering  = local.table_cfg != null ? try(local.table_cfg.clustering.fields, null) : null
  effective_description = local.table_cfg != null ? try(local.table_cfg.description, null) : null
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
    for_each = (
      local.effective_time_part != null || var.partition_type != null || var.partition_field != null
    ) ? [1] : []
    content {
      type  = local.effective_time_part != null ? lookup(local.effective_time_part, "type", "DAY") : coalesce(var.partition_type, "DAY")
      field = local.effective_time_part != null ? lookup(local.effective_time_part, "field", null) : var.partition_field
    }
  }

  clustering  = local.effective_clustering != null ? local.effective_clustering : var.clustering
  description = local.effective_description

  # Prefer inline JSON when provided (avoids module path restrictions on file())
  schema = local.effective_schema_json != null ? local.effective_schema_json : (
    var.schema_json != null ? var.schema_json : file(var.schema_file)
  )
  labels = var.labels
}

output "table_id" { value = google_bigquery_table.table.table_id }
output "table_ref" { value = "${var.dataset_id}.${google_bigquery_table.table.table_id}" }
