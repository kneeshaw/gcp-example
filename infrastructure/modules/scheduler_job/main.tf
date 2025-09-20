variable "project_id" { type = string }
variable "region" { type = string }
variable "name" { type = string }
variable "schedule" { type = string }
variable "time_zone" { type = string }
variable "target_uri" { type = string }
variable "invoker_service_account_email" { type = string }
variable "function_name" { type = string }

# Allow the scheduler SA to invoke the function (Cloud Run backend)
resource "google_cloud_run_v2_service_iam_member" "invoker" {
  project  = var.project_id
  location = var.region
  name     = var.function_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.invoker_service_account_email}"
}

resource "google_cloud_scheduler_job" "job" {
  name        = var.name
  description = "Scheduled job managed by module"
  schedule    = var.schedule
  time_zone   = var.time_zone

  http_target {
    http_method = "POST"
    uri         = var.target_uri

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode(jsonencode({ "source" = "scheduler" }))

    oidc_token {
      service_account_email = var.invoker_service_account_email
      audience              = var.target_uri
    }
  }

  depends_on = [google_cloud_run_v2_service_iam_member.invoker]
}

output "name" { value = google_cloud_scheduler_job.job.name }
