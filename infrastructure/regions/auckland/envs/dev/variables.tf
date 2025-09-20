variable "project_id" {
  type    = string
  default = null
}

variable "region" {
  type = string
}

variable "gcp_region" {
  type = string
}

variable "environment" {
  type    = string
  default = "dev"
}

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

variable "datasets" {
  type = map(object({
    url           = string
    response_type = optional(string, "protobuf")
    spec          = optional(string)
    function      = optional(string)
    rate = object({
      cron    = optional(string)
      offsets = optional(list(number))
    })
    memory_limit  = optional(string)
    cpu_limit     = optional(string)
  }))
  default = {}
}

variable "timezone" {
  description = "IANA timezone for schedulers and functions"
  type        = string
  default     = "UTC"
}
