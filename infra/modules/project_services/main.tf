variable "project_id" { type = string }
variable "services" { type = list(string) }

resource "google_project_service" "services" {
  for_each = toset(var.services)
  project  = var.project_id
  service  = each.value
  disable_on_destroy         = false
  disable_dependent_services = false
}

output "enabled" {
  description = "List of enabled services"
  value       = [for s in google_project_service.services : s.service]
}
