output "data_bucket" { value = module.data_bucket.name }
output "worker_urls" { value = { for k, m in module.workers : k => m.uri } }
output "enqueuer_urls" { value = try({ for k, m in module.enqueuers : k => m.enqueuer_uri }, {}) }