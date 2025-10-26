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

variable "redis" {
  description = "Optional Memorystore Redis cache configuration."
  type = object({
    enabled                 = optional(bool, true)
    memory_size_gb          = number
    tier                    = optional(string, "STANDARD_HA")
    redis_version           = optional(string, "REDIS_7_0")
    auth_enabled            = optional(bool, true)
    transit_encryption_mode = optional(string, "SERVER_AUTHENTICATION")
    read_replicas_mode      = optional(string, "READ_REPLICAS_DISABLED")
    replica_count           = optional(number, 1)
    resource_prefix         = optional(string)
    redis_name              = optional(string)
    network_name            = optional(string)
    subnet_name             = optional(string)
  subnet_cidr             = optional(string, "10.60.0.0/28")
    connector_name          = optional(string)
    connector_machine_type  = optional(string)
  connector_min_instances = optional(number, 2)
  connector_max_instances = optional(number, 3)
    display_name            = optional(string)
    labels                  = optional(map(string), {})
    maintenance_window = optional(object({
      day = string
      start_time = object({
        hours   = number
        minutes = optional(number, 0)
        seconds = optional(number, 0)
        nanos   = optional(number, 0)
      })
    }))
  })
  default = null
}