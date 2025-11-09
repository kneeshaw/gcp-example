variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for Dataform resources."
  type        = string
}

variable "dataform_repository_id" {
  description = "The ID for the Dataform repository."
  type        = string
}

variable "github_repo_url" {
  description = "The URL of the GitHub repository for Dataform."
  type        = string
}

variable "dataform_github_token_secret_version" {
  description = "The full resource name of the Secret Manager secret version containing the GitHub PAT."
  type        = string
}

variable "compilation_vars" {
  description = "A map of key-value pairs to be used as variables in Dataform's compilation config."
  type        = map(string)
  default     = {}
}

variable "workflows" {
  description = "A map of workflow configurations to be created for the Dataform repository."
  type = map(object({
    cron_schedule = string
    time_zone     = string
    included_tags = list(string)
  }))
  default = {}
}