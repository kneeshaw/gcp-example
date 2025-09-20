# Configure Google provider (primary) for the project and region
provider "google" {
  project = var.project_id
  region  = var.gcp_region
}

# Configure Google Beta provider (for newer resources if needed)
provider "google-beta" {
  project = var.project_id
  region  = var.gcp_region
}

# Region environment composition (reuses shared modules)
module "region" {
  source = "../../../../modules/region_env"

  project_id       = var.project_id
  region           = var.region
  gcp_region       = var.gcp_region
  environment      = var.environment
  bucket_location  = var.bucket_location
  headers          = var.headers
  datasets         = var.datasets
  timezone         = var.timezone
}

# BigQuery Views (separate module)
module "views" {
  source     = "../../../../modules/bigquery_views"
  project_id = var.project_id
  dataset_id = module.region.bq_dataset_id
  timezone   = var.timezone
  fact_stop_events_enabled = true
  agg_route_hourly_enabled   = true
  agg_network_hourly_enabled = true
  agg_route_daily_enabled    = true
  agg_network_daily_enabled  = true
  baseline_route_hour_dow_enabled   = true
  route_hourly_with_baseline_enabled = true
  fact_trips_enabled = true
  agg_trip_daily_enabled = true
  shapes_geog_enabled = true
  osm_akl_road_links_enabled = true
  trip_edges_enabled = true

  depends_on = [module.region]
}

# BigQuery Scheduled Queries to materialize OSM mapping tables
module "scheduled_queries" {
  source     = "../../../../modules/bq_scheduled_queries"
  project_id = var.project_id
  dataset_id = module.region.bq_dataset_id
  location   = var.gcp_region
  timezone   = var.timezone
  service_account_email = module.region.function_service_account_email

  queries = {
    shapes_geog = {
      name        = "Materialize shapes_geog"
      schedule    = "every day 01:45"
      destination = ""
      write_mode  = ""
      query       = templatefile("${path.module}/../../../../queries/scheduled/shapes_geog.sql", {
        project_id = var.project_id,
        dataset_id = module.region.bq_dataset_id,
      })
    }
    trip_edges = {
      name        = "Materialize trip_edges"
      schedule    = "every day 02:00"
      destination = ""
      write_mode  = ""
      query       = templatefile("${path.module}/../../../../queries/scheduled/trip_edges.sql", {
        project_id = var.project_id,
        dataset_id = module.region.bq_dataset_id,
      })
    }
    route_edges = {
      name        = "Materialize route_edges"
      schedule    = "every day 02:30" # after trip_edges
      destination = ""
      write_mode  = ""
      query       = templatefile("${path.module}/../../../../queries/scheduled/route_edges.sql", {
        project_id = var.project_id,
        dataset_id = module.region.bq_dataset_id,
      })
    }
  }

  depends_on = [module.views]
}

# Output: data bucket name
output "data_bucket" { value = module.region.data_bucket }

# Output: per-dataset worker URLs
output "worker_urls" { value = module.region.worker_urls }

# Output: per-dataset enqueuer URLs (for enqueuer-enabled datasets)
output "enqueuer_urls" { value = module.region.enqueuer_urls }

# Debug: surface unified source artifact details and hash
output "unified_source_object" { value = module.region.unified_source_object }
output "unified_source_bucket" { value = module.region.unified_source_bucket }
output "source_hash" { value = module.region.source_hash }

# Views created by the views module
output "views" { value = concat(module.views.created_views, try(module.views.created_fact_views, []), try(module.views.created_agg_views, []), try(module.views.created_baseline_views, []), try(module.views.created_trip_views, []), try(module.views.created_osm_views, [])) }
