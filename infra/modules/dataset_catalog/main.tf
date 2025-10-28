variable "schemas_root" {
  description = "Absolute or module-relative path to the shared tables directory"
  type        = string
}

locals {
  // Central catalog of datasets and their common tables (shared across agencies/environments)
  // Agencies can override any of these via their var.datasets entries.
  datasets = {
    "vehicle-positions" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/vehicle_positions/rt_vehicle_positions.schema.json" }
      }
      views = {
        latest_positions = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_latest_positions.sql" }
        agg_vehicle_hourly = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_agg_vehicle_hourly.sql" }
      }
    }

    "trip-updates" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/trip_updates/rt_trip_updates.schema.json" }
      }
    }

    "service-alerts" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/service_alerts/rt_service_alerts.schema.json" }
      }
    }

    // Static GTFS schedule dataset
    "schedule" = {
      spec          = "sc"
      response_type = "zip"
      tables = {
        agency          = { schema_file = "${var.schemas_root}/schedule/sc_agency.schema.json" }
        calendar        = { schema_file = "${var.schemas_root}/schedule/sc_calendar.schema.json" }
        calendar_dates  = { schema_file = "${var.schemas_root}/schedule/sc_calendar_dates.schema.json" }
        fare_attributes = { schema_file = "${var.schemas_root}/schedule/sc_fare_attributes.schema.json" }
        fare_rules      = { schema_file = "${var.schemas_root}/schedule/sc_fare_rules.schema.json" }
        feed_info       = { schema_file = "${var.schemas_root}/schedule/sc_feed_info.schema.json" }
        frequencies     = { schema_file = "${var.schemas_root}/schedule/sc_frequencies.schema.json" }
        routes          = { schema_file = "${var.schemas_root}/schedule/sc_routes.schema.json" }
        shapes          = { schema_file = "${var.schemas_root}/schedule/sc_shapes.schema.json" }
        stop_times      = { schema_file = "${var.schemas_root}/schedule/sc_stop_times.schema.json" }
        stops           = { schema_file = "${var.schemas_root}/schedule/sc_stops.schema.json" }
        transfers       = { schema_file = "${var.schemas_root}/schedule/sc_transfers.schema.json" }
        trips           = { schema_file = "${var.schemas_root}/schedule/sc_trips.schema.json" }
      }
      views = {
        schedule_active_routes    = { sql_file = "${var.schemas_root}/../views/schedule/vw_schedule_active_routes.sql" }
        schedule_route_summary    = { sql_file = "${var.schemas_root}/../views/schedule/vw_schedule_route_summary.sql" }
        schedule_stop_frequencies = { sql_file = "${var.schemas_root}/../views/schedule/vw_schedule_stop_frequencies.sql" }
      }
    }

    // Derived daily schedule (materialized table)
    "daily-schedule" = {
      spec          = "ds"
      response_type = ""
      tables = {
        main = { schema_file = "${var.schemas_root}/daily_schedule/ds_daily_schedule.schema.json" }
      }
      functions = {
        generate = {}
      }
    }

    // Analytics and performance views
    "analytics" = {
      spec          = "analytics"
      response_type = ""
      tables = {}
      functions = {}
      views = {
        fact_stop_events           = { sql_file = "${var.schemas_root}/../views/analytics/vw_fact_stop_events.sql" }
        fact_trips                 = { sql_file = "${var.schemas_root}/../views/analytics/vw_fact_trips.sql" }
        live_planned_vs_actual     = { sql_file = "${var.schemas_root}/../views/analytics/vw_live_planned_vs_actual.sql" }
        agg_network_daily          = { sql_file = "${var.schemas_root}/../views/analytics/vw_agg_network_daily.sql" }
        agg_network_hourly         = { sql_file = "${var.schemas_root}/../views/analytics/vw_agg_network_hourly.sql" }
        agg_route_daily            = { sql_file = "${var.schemas_root}/../views/analytics/vw_agg_route_daily.sql" }
        agg_route_hourly           = { sql_file = "${var.schemas_root}/../views/analytics/vw_agg_route_hourly.sql" }
        agg_trip_daily             = { sql_file = "${var.schemas_root}/../views/analytics/vw_agg_trip_daily.sql" }
        baseline_route_hour_dow    = { sql_file = "${var.schemas_root}/../views/analytics/vw_baseline_route_hour_dow.sql" }
        baseline_route_hourly_compare = { sql_file = "${var.schemas_root}/../views/analytics/vw_baseline_route_hourly_compare.sql" }
      }
    }
  }
}

output "datasets" {
  description = "Shared dataset catalog (spec/response_type/tables)"
  value       = local.datasets
}
