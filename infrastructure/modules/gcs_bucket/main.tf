variable "project_id" { type = string }
variable "name" { type = string }
variable "location" { type = string }
variable "uniform" {
  type    = bool
  default = true
}
variable "force_destroy" {
  type    = bool
  default = false
}

resource "google_storage_bucket" "bucket" {
  name                        = var.name
  project                     = var.project_id
  location                    = var.location
  uniform_bucket_level_access = var.uniform
  force_destroy               = var.force_destroy
}

output "name" { value = google_storage_bucket.bucket.name }
