output "data_bucket" { value = module.data_bucket.name }
output "worker_urls" { value = { for k, m in module.workers : k => m.uri } }
output "enqueuer_urls" { value = try({ for k, m in module.enqueuers : k => m.enqueuer_uri }, {}) }

output "redis_cache" {
	description = "Details for the optional Redis cache."
	value = try({
		name      = module.redis_cache["primary"].name
		host      = module.redis_cache["primary"].host
		port      = module.redis_cache["primary"].port
		connector = module.redis_cache["primary"].connector
		network   = module.redis_cache["primary"].network
		subnet    = module.redis_cache["primary"].subnet
	}, null)
}

output "redis_auth_string" {
	description = "AUTH string for the optional Redis cache."
	value       = try(module.redis_cache["primary"].auth_string, null)
	sensitive   = true
}