terraform {
  required_providers {
    archive = {
      source = "hashicorp/archive"
    }
  }
}

variable "project_id" { type = string }
variable "location"   { type = string }
variable "bucket_name" { type = string }

variable "create_bucket" {
  type    = bool
  default = true
}

variable "source_dir" { type = string }

variable "object_prefix" {
  type    = string
  default = "functions"
}

variable "excludes" {
  type    = list(string)
  default = [".git", ".github", "infra", "infrastructure", "notebooks", "__pycache__", ".DS_Store"]
}

locals {
  ts              = timestamp()
  object_name     = "${var.object_prefix}/src-${replace(local.ts, ":", "-")}.zip"
  normalized_path = abspath(var.source_dir)
}

# Package local source as a zip using the archive provider
data "archive_file" "src" {
  type        = "zip"
  source_dir  = local.normalized_path
  output_path = "${path.module}/.tmp_src.zip"
  excludes    = var.excludes
}

resource "google_storage_bucket" "artifact" {
  count    = var.create_bucket ? 1 : 0
  project  = var.project_id
  name     = var.bucket_name
  location = var.location
  uniform_bucket_level_access = true
  force_destroy = false
}

resource "google_storage_bucket_object" "source" {
  name   = local.object_name
  bucket = var.bucket_name
  source = data.archive_file.src.output_path
  depends_on = [google_storage_bucket.artifact]
}

output "bucket"      { value = var.bucket_name }
output "object"      { value = google_storage_bucket_object.source.name }
output "source_hash" { value = data.archive_file.src.output_sha256 }
