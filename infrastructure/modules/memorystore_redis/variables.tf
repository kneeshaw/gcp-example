variable "project_id" {
  description = "GCP project ID that hosts the Redis cache resources."
  type        = string
}

variable "region" {
  description = "GCP region for the Redis resources."
  type        = string
}

variable "environment" {
  description = "Deployment environment label (e.g., dev, prod)."
  type        = string
}

variable "resource_prefix" {
  description = "Optional prefix used to derive resource names when explicit names are not supplied."
  type        = string
  default     = null
}

variable "network_name" {
  description = "Optional override for the VPC network name that will host Redis and the VPC connector."
  type        = string
  default     = null
}

variable "subnet_name" {
  description = "Optional override for the subnetwork name dedicated to serverless access."
  type        = string
  default     = null
}

variable "subnet_cidr" {
  description = "CIDR range allocated to the serverless access subnetwork."
  type        = string
  default     = "10.60.0.0/28"
}

variable "connector_name" {
  description = "Optional override for the Serverless VPC Access connector name."
  type        = string
  default     = null
}

variable "connector_machine_type" {
  description = "Optional machine type for the Serverless VPC Access connector."
  type        = string
  default     = null
}

variable "connector_min_instances" {
  description = "Optional minimum instance count for the Serverless VPC Access connector."
  type        = number
  default     = 2
}

variable "connector_max_instances" {
  description = "Optional maximum instance count for the Serverless VPC Access connector."
  type        = number
  default     = 3
}

variable "redis_name" {
  description = "Optional override for the Memorystore Redis instance name."
  type        = string
  default     = null
}

variable "memory_size_gb" {
  description = "Memory capacity for the Redis instance in gigabytes."
  type        = number
}

variable "tier" {
  description = "Memorystore service tier (BASIC or STANDARD_HA)."
  type        = string
  default     = "STANDARD_HA"
}

variable "redis_version" {
  description = "Redis engine version for the instance."
  type        = string
  default     = "REDIS_7_0"
}

variable "auth_enabled" {
  description = "Whether AUTH is enabled for the Redis instance."
  type        = bool
  default     = true
}

variable "transit_encryption_mode" {
  description = "Encryption in transit mode for the Redis instance."
  type        = string
  default     = "SERVER_AUTHENTICATION"
}

variable "read_replicas_mode" {
  description = "Read replica configuration for the Redis instance."
  type        = string
  default     = "READ_REPLICAS_DISABLED"
}

variable "replica_count" {
  description = "Replica count when read replicas are enabled."
  type        = number
  default     = 1
}

variable "display_name" {
  description = "Optional display name for the Redis instance."
  type        = string
  default     = null
}

variable "labels" {
  description = "Additional labels to apply to created resources."
  type        = map(string)
  default     = {}
}

variable "maintenance_window" {
  description = "Optional weekly maintenance window definition."
  type = object({
    day = string
    start_time = object({
      hours   = number
      minutes = optional(number, 0)
      seconds = optional(number, 0)
      nanos   = optional(number, 0)
    })
  })
  default = null
}
