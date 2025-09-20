// moved from agency_env
variable "project_id" { type = string }
variable "region" { type = string }            // label, e.g., "auckland"
variable "gcp_region" { type = string }        // GCP region, e.g., "australia-southeast1"
variable "environment" { type = string }       // deployment environment, e.g., "dev", "prod"
variable "timezone" { type = string }          // IANA timezone, e.g., "Pacific/Auckland"
variable "bucket_location" {
  type    = string
  default = null
}

variable "headers" {
  type    = map(string)
  default = null
}

variable "bq_dataset" {
  description = "Base BigQuery dataset name without environment suffix"
  type        = string
  default     = null
}

// Dataset definitions map. Keys are dataset identifiers, values define fetch & cadence.
variable "datasets" {
  type = map(object({
    url           = string
    response_type = optional(string, "protobuf")
    spec          = optional(string)
    function      = optional(string) // e.g. "realtime" | "schedule" | etc.
    rate = object({
      cron    = optional(string)
      offsets = optional(list(number))
    })
    memory_limit = optional(string)
    cpu_limit    = optional(string)
  }))
  default = {}
}