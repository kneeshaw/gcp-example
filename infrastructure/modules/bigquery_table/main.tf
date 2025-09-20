variable "project_id" { type = string }
variable "location"   { type = string }
variable "dataset_id" { type = string }
variable "table_id"   { type = string }
variable "schema_file" { type = string }
variable "partition_type" {
	type    = string
	default = "DAY" # Ingestion-time DAY partitioning by default
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
	# Default true for safety; override to false in env module when destructive schema change needed
	default = true
}

resource "google_bigquery_table" "table" {
	project    = var.project_id
	dataset_id = var.dataset_id
	table_id   = var.table_id

	# Allow controlled destructive updates when types must change (e.g. STRING -> TIMESTAMP)
	deletion_protection = var.deletion_protection

	# Time partitioning: if partition_field is provided, use field-based partition; else ingestion-time
	time_partitioning {
		type  = var.partition_type
		field = var.partition_field
	}

	clustering = var.clustering
	schema     = file(var.schema_file) # JSON schema file
	labels     = var.labels
}

output "table_id" { value = google_bigquery_table.table.table_id }
output "table_ref" { value = "${var.dataset_id}.${google_bigquery_table.table.table_id}" }
