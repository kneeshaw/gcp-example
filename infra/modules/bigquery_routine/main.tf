variable "dataset_id" {
  description = "BigQuery dataset where the routine will be created"
  type        = string
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "routine_id" {
  description = "Routine identifier (name)"
  type        = string
}

variable "sql_file" {
  description = "Path to SQL file containing the routine definition (TVF)"
  type        = string
}

variable "description" {
  description = "Optional description for the routine"
  type        = string
  default     = null
}

variable "arguments" {
  description = "List of argument definitions for the routine (name/type_kind)"
  type = list(object({
    name      = string
    type_kind = string
  }))
  default = []
}

variable "return_columns" {
  description = "List of return columns (for TABLE_VALUED_FUNCTION) with name/type_kind"
  type = list(object({
    name      = string
    type_kind = string
  }))
  default = []
}

resource "google_bigquery_routine" "tvf" {
  project     = var.project_id
  dataset_id  = var.dataset_id
  routine_id  = var.routine_id
  routine_type = "TABLE_VALUED_FUNCTION"
  language     = "SQL"

  description = var.description

  // Render SQL with project/dataset variables if used in the SQL file
  definition_body = templatefile(var.sql_file, {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })

  dynamic "arguments" {
    for_each = var.arguments
    content {
      name           = arguments.value.name
      argument_kind  = "FIXED_TYPE"
      data_type      = jsonencode({ typeKind = arguments.value.type_kind })
    }
  }

  // Return table schema for TVF encoded as JSON
  return_table_type = jsonencode({
    columns = [for c in var.return_columns : {
      name = c.name
      type = { typeKind = c.type_kind }
    }]
  })
}

output "routine_id" {
  description = "The routine ID"
  value       = google_bigquery_routine.tvf.routine_id
}

output "routine_ref" {
  description = "Full routine reference"
  value       = "${var.project_id}.${var.dataset_id}.${google_bigquery_routine.tvf.routine_id}"
}
