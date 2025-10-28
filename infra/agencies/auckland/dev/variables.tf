variable "project_id" { type = string }
variable "gcp_region" { type = string }
variable "environment" { type = string }
variable "agency_prefix" { type = string }
variable "bq_dataset" { type = string }
variable "headers" {
	type    = map(string)
	default = {}
}
variable "datasets" { type = any }

# Optional extras
variable "timezone" {
	type    = string
	default = "UTC"
}
variable "bucket_location" {
	type    = string
	default = null
}

# Optional overrides for bucket names (set to avoid project_id prefix or comply with naming policies)
variable "artifact_bucket_name" {
  type    = string
  default = null
}

variable "data_bucket_name" {
  type    = string
  default = null
}
