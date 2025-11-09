terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
    google-beta = {
      source = "hashicorp/google-beta"
    }
  }
}

# 1. A dedicated service account for Dataform to run with
resource "google_service_account" "dataform_runner" {
  provider     = google-beta
  project      = var.project_id
  account_id   = "${var.dataform_repository_id}-runner"
  display_name = "Dataform Runner for ${var.dataform_repository_id}"
}

# 2. IAM permissions for the service account
resource "google_project_iam_member" "dataform_runner_bigquery_job_user" {
  provider = google-beta
  project  = var.project_id
  role     = "roles/bigquery.jobUser"
  member   = "serviceAccount:${google_service_account.dataform_runner.email}"
}

resource "google_project_iam_member" "dataform_runner_dataform_editor" {
  provider = google-beta
  project  = var.project_id
  role     = "roles/dataform.editor"
  member   = "serviceAccount:${google_service_account.dataform_runner.email}"
}

# Allow the service account to access the Git PAT secret
resource "google_secret_manager_secret_iam_member" "dataform_runner_secret_accessor" {
  provider  = google-beta
  project   = split("/", var.dataform_github_token_secret_version)[1]
  secret_id = split("/", var.dataform_github_token_secret_version)[3]
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dataform_runner.email}"
}

# 3. The Dataform repository, linked to your GitHub repo
resource "google_dataform_repository" "dataform_repo" {
  provider = google-beta
  project  = var.project_id
  region   = var.region
  name     = var.dataform_repository_id

  git_remote_settings {
    url                                = var.github_repo_url
    default_branch                     = "main"
    authentication_token_secret_version = var.dataform_github_token_secret_version
  }

  service_account = google_service_account.dataform_runner.email

  depends_on = [
    google_project_iam_member.dataform_runner_bigquery_job_user,
    google_project_iam_member.dataform_runner_dataform_editor,
    google_secret_manager_secret_iam_member.dataform_runner_secret_accessor
  ]
}

# 4. Release Configuration: Defines compilation settings for a Git commitish (e.g., 'main')
resource "google_dataform_repository_release_config" "release" {
  provider      = google-beta
  project       = var.project_id
  region        = var.region
  repository    = google_dataform_repository.dataform_repo.name
  name          = "${var.dataform_repository_id}-release"
  git_commitish = "main"

  code_compilation_config {
    default_database = var.project_id
    default_schema   = "dataform"
    default_location = var.region
    assertion_schema = "dataform_assertions"
    vars             = var.compilation_vars
  }
}

# 5. Workflow Configurations: Creates multiple scheduled executions based on tags
resource "google_dataform_repository_workflow_config" "workflows" {
  provider   = google-beta
  for_each   = var.workflows
  project    = var.project_id
  region     = var.region
  repository = google_dataform_repository.dataform_repo.name

  name           = each.key
  release_config = google_dataform_repository_release_config.release.id
  cron_schedule  = each.value.cron_schedule
  time_zone      = each.value.time_zone

  invocation_config {
    included_tags                  = each.value.included_tags
    transitive_dependencies_included = true
  }
}