locals {
  planned_vs_actual_query = templatefile("${path.module}/queries/vw_planned_vs_actual.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  fact_stop_events_query = templatefile("${path.module}/queries/vw_fact_stop_events.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
    timezone   = var.timezone
  })
  agg_route_hourly_query = templatefile("${path.module}/queries/vw_agg_route_hourly.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  agg_network_hourly_query = templatefile("${path.module}/queries/vw_agg_network_hourly.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  agg_route_daily_query = templatefile("${path.module}/queries/vw_agg_route_daily.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  agg_network_daily_query = templatefile("${path.module}/queries/vw_agg_network_daily.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  baseline_route_hour_dow_query = templatefile("${path.module}/queries/vw_baseline_route_hour_dow.sql", {
    project_id          = var.project_id
    dataset_id          = var.dataset_id
    timezone            = var.timezone
    baseline_window_days = var.baseline_window_days
  })
  route_hourly_with_baseline_query = templatefile("${path.module}/queries/vw_route_hourly_with_baseline.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  fact_trips_query = templatefile("${path.module}/queries/vw_fact_trips.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  agg_trip_daily_query = templatefile("${path.module}/queries/vw_agg_trip_daily.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })

  shapes_geog_query = templatefile("${path.module}/queries/vw_shapes_geog.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  osm_akl_road_links_query = templatefile("${path.module}/queries/vw_osm_akl_road_links.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
  trip_edges_query = templatefile("${path.module}/queries/vw_trip_edges.sql", {
    project_id = var.project_id
    dataset_id = var.dataset_id
  })
}

resource "google_bigquery_table" "vw_planned_vs_actual" {
  count      = var.planned_vs_actual_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_planned_vs_actual"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = local.planned_vs_actual_query
  }

  labels = {
    type = "view"
  }
}

output "created_views" {
  value = [for v in google_bigquery_table.vw_planned_vs_actual : v.table_id]
}

resource "google_bigquery_table" "vw_fact_stop_events" {
  count      = var.fact_stop_events_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_fact_stop_events"

  depends_on = [google_bigquery_table.vw_planned_vs_actual]

  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = local.fact_stop_events_query
  }

  labels = {
    type = "view"
  }
}

output "created_fact_views" {
  value = [for v in google_bigquery_table.vw_fact_stop_events : v.table_id]
}

resource "google_bigquery_table" "vw_agg_route_hourly" {
  count      = var.agg_route_hourly_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_agg_route_hourly"

  depends_on = [google_bigquery_table.vw_fact_stop_events]

  view {
    use_legacy_sql = false
    query          = local.agg_route_hourly_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_agg_network_hourly" {
  count      = var.agg_network_hourly_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_agg_network_hourly"

  depends_on = [google_bigquery_table.vw_fact_stop_events]

  view {
    use_legacy_sql = false
    query          = local.agg_network_hourly_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_agg_route_daily" {
  count      = var.agg_route_daily_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_agg_route_daily"

  depends_on = [google_bigquery_table.vw_fact_stop_events]

  view {
    use_legacy_sql = false
    query          = local.agg_route_daily_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_agg_network_daily" {
  count      = var.agg_network_daily_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_agg_network_daily"

  depends_on = [google_bigquery_table.vw_fact_stop_events]

  view {
    use_legacy_sql = false
    query          = local.agg_network_daily_query
  }
  labels = { type = "view" }
}

output "created_agg_views" {
  value = concat(
    [for v in google_bigquery_table.vw_agg_route_hourly : v.table_id],
    [for v in google_bigquery_table.vw_agg_network_hourly : v.table_id],
    [for v in google_bigquery_table.vw_agg_route_daily : v.table_id],
    [for v in google_bigquery_table.vw_agg_network_daily : v.table_id]
  )
}

resource "google_bigquery_table" "vw_baseline_route_hour_dow" {
  count      = var.baseline_route_hour_dow_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_baseline_route_hour_dow"

  deletion_protection = false

  depends_on = [google_bigquery_table.vw_fact_stop_events]

  view {
    use_legacy_sql = false
    query          = local.baseline_route_hour_dow_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_route_hourly_with_baseline" {
  count      = var.route_hourly_with_baseline_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_route_hourly_with_baseline"

  depends_on = [
    google_bigquery_table.vw_baseline_route_hour_dow,
    google_bigquery_table.vw_agg_route_hourly
  ]

  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = local.route_hourly_with_baseline_query
  }
  labels = { type = "view" }
}

output "created_baseline_views" {
  value = concat(
    [for v in google_bigquery_table.vw_baseline_route_hour_dow : v.table_id],
    [for v in google_bigquery_table.vw_route_hourly_with_baseline : v.table_id]
  )
}

resource "google_bigquery_table" "vw_fact_trips" {
  count      = var.fact_trips_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_fact_trips"

  depends_on = [google_bigquery_table.vw_fact_stop_events]

  view {
    use_legacy_sql = false
    query          = local.fact_trips_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_agg_trip_daily" {
  count      = var.agg_trip_daily_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_agg_trip_daily"

  depends_on = [google_bigquery_table.vw_fact_trips]

  view {
    use_legacy_sql = false
    query          = local.agg_trip_daily_query
  }
  labels = { type = "view" }
}

output "created_trip_views" {
  value = concat(
    [for v in google_bigquery_table.vw_fact_trips : v.table_id],
    [for v in google_bigquery_table.vw_agg_trip_daily : v.table_id]
  )
}

resource "google_bigquery_table" "vw_shapes_geog" {
  count      = var.shapes_geog_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_shapes_geog"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = local.shapes_geog_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_osm_akl_road_links" {
  count      = var.osm_akl_road_links_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_osm_akl_road_links"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = local.osm_akl_road_links_query
  }
  labels = { type = "view" }
}

resource "google_bigquery_table" "vw_trip_edges" {
  count      = var.trip_edges_enabled ? 1 : 0
  project    = var.project_id
  dataset_id = var.dataset_id
  table_id   = "vw_trip_edges"

  depends_on = [
    google_bigquery_table.vw_shapes_geog,
    google_bigquery_table.vw_osm_akl_road_links
  ]

  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = local.trip_edges_query
  }
  labels = { type = "view" }
}

output "created_osm_views" {
  value = concat(
    [for v in google_bigquery_table.vw_shapes_geog : v.table_id],
    [for v in google_bigquery_table.vw_osm_akl_road_links : v.table_id],
    [for v in google_bigquery_table.vw_trip_edges : v.table_id]
  )
}
