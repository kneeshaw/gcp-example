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
  description = "Path to SQL file containing the TVF definition body"
  type        = string
}

variable "arguments" {
  description = "List of routine argument definitions"
  type = list(object({
    name      = string
    type_kind = string
  }))
  default = []
}

variable "return_columns" {
  description = "List of return table column definitions"
  type = list(object({
    name      = string
    type_kind = string
  }))
}

variable "description" {
  description = "Optional description for the routine"
  type        = string
  default     = null
}

variable "labels" {
  description = "Optional labels for the routine"
  type        = map(string)
  default     = {}
}

resource "google_bigquery_routine" "tvf" {
  project     = var.project_id
  dataset_id  = var.dataset_id
  routine_id  = var.routine_id
  routine_type = "TABLE_VALUED_FUNCTION"
  language    = "SQL"

  description = var.description

  definition_body = templatefile(var.sql_file, {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })

  dynamic "arguments" {
    for_each = var.arguments
    content {
      name = arguments.value.name
      data_type = jsonencode({ typeKind = arguments.value.type_kind })
    }
  }

  return_table_type = jsonencode({
    columns = [for c in var.return_columns : {
      name = c.name
      type = { typeKind = c.type_kind }
    }]
  })

  depends_on = []
}

output "routine_id" {
  value = google_bigquery_routine.tvf.routine_id
}
