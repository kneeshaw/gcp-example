output "name" {
  description = "Redis instance name."
  value       = google_redis_instance.cache.name
}

output "host" {
  description = "Redis instance hostname."
  value       = google_redis_instance.cache.host
}

output "port" {
  description = "Redis instance port."
  value       = google_redis_instance.cache.port
}

output "auth_string" {
  description = "Redis AUTH string (password)."
  value       = google_redis_instance.cache.auth_string
  sensitive   = true
}

output "connector" {
  description = "Serverless VPC Access connector metadata."
  value = {
    name    = google_vpc_access_connector.redis.name
    project = google_vpc_access_connector.redis.project
    region  = google_vpc_access_connector.redis.region
    id      = google_vpc_access_connector.redis.id
  }
}

output "network" {
  description = "VPC network details for the Redis cache."
  value = {
    name      = google_compute_network.redis.name
    self_link = google_compute_network.redis.self_link
    project   = google_compute_network.redis.project
  }
}

output "subnet" {
  description = "Subnetwork reserved for Redis access."
  value = {
    name        = google_compute_subnetwork.redis.name
    self_link   = google_compute_subnetwork.redis.self_link
    cidr        = google_compute_subnetwork.redis.ip_cidr_range
    region      = google_compute_subnetwork.redis.region
    network     = google_compute_subnetwork.redis.network
  }
}
