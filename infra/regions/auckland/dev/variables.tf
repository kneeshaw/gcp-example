variable "project_id" { type = string }
variable "gcp_region" { type = string }
variable "environment" { type = string }
variable "region_prefix" { type = string }
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
variable "transit_authority" {
  type = string
}
variable "service_boundary_hour" {
  type = number
}
variable "region_name" {
	description = "Optional display name for the region; if unset, transit_authority will be used."
	type        = string
	default     = null
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
