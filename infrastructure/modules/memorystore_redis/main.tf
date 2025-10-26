locals {
  base_prefix     = coalesce(var.resource_prefix, "${var.region}-${var.environment}")
  normalized_base = replace(lower(local.base_prefix), "_", "-")
  sanitized_chars = [for ch in split(local.normalized_base, "") : ch if can(regex("[a-z0-9-]", ch))]
  sanitized       = join("", local.sanitized_chars)
  name_seed       = length(local.sanitized) > 0 ? local.sanitized : "redis"

  network_name   = coalesce(var.network_name, "${local.name_seed}-redis-net")
  subnet_name    = coalesce(var.subnet_name, "${local.name_seed}-redis-subnet")
  connector_name = coalesce(var.connector_name, "${local.name_seed}-redis-connector")
  redis_name     = coalesce(var.redis_name, substr("${local.name_seed}-redis", 0, 40))
  display_name   = coalesce(var.display_name, "${title(var.region)} ${upper(var.environment)} Redis Cache")

  labels = merge({
    environment = var.environment
    region      = var.region
  }, var.labels)
}

resource "google_compute_network" "redis" {
  name                    = local.network_name
  project                 = var.project_id
  auto_create_subnetworks = false
  description             = "VPC network dedicated to the Redis cache"
}

resource "google_compute_subnetwork" "redis" {
  name          = local.subnet_name
  project       = var.project_id
  region        = var.region
  ip_cidr_range = var.subnet_cidr
  network       = google_compute_network.redis.id
  stack_type    = "IPV4_ONLY"
  description   = "Subnetwork reserved for Redis access and serverless connectors"
}

resource "google_vpc_access_connector" "redis" {
  name    = local.connector_name
  project = var.project_id
  region  = var.region

  subnet {
    name = google_compute_subnetwork.redis.name
  }

  machine_type  = var.connector_machine_type
  min_instances = coalesce(var.connector_min_instances, 2)
  max_instances = coalesce(var.connector_max_instances, 3)

  depends_on = [google_compute_subnetwork.redis]
}

resource "google_redis_instance" "cache" {
  name               = local.redis_name
  project            = var.project_id
  region             = var.region
  tier               = var.tier
  memory_size_gb     = var.memory_size_gb
  redis_version      = var.redis_version
  display_name       = local.display_name
  labels             = local.labels
  auth_enabled       = var.auth_enabled
  transit_encryption_mode = var.transit_encryption_mode
  read_replicas_mode = var.read_replicas_mode
  replica_count      = var.read_replicas_mode == "READ_REPLICAS_ENABLED" ? var.replica_count : null
  authorized_network = google_compute_network.redis.id

  dynamic "maintenance_policy" {
    for_each = var.maintenance_window == null ? [] : [var.maintenance_window]
    content {
      weekly_maintenance_window {
        day = maintenance_policy.value.day
        start_time {
          hours   = maintenance_policy.value.start_time.hours
          minutes = coalesce(maintenance_policy.value.start_time.minutes, 0)
          seconds = coalesce(maintenance_policy.value.start_time.seconds, 0)
          nanos   = coalesce(maintenance_policy.value.start_time.nanos, 0)
        }
      }
    }
  }

  depends_on = [google_compute_network.redis]
}
