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
        vehicle_positions_fact = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_fact.sql" }
        vehicle_positions_agg_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_agg_minute.sql" }
        vehicle_positions_agg_hour = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_agg_hour.sql" }
        vehicle_positions_agg_day = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_agg_day.sql" }
        # Scoped aggregations by dimension
        vehicle_positions_vehicle_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_vehicle_minute.sql" }
        vehicle_positions_vehicle_hourly = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_vehicle_hourly.sql" }
        vehicle_positions_vehicle_daily  = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_vehicle_daily.sql" }

        vehicle_positions_trip_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_trip_minute.sql" }
        vehicle_positions_trip_hourly = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_trip_hourly.sql" }
        vehicle_positions_trip_daily  = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_trip_daily.sql" }

        vehicle_positions_route_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_route_minute.sql" }
        vehicle_positions_route_hourly = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_route_hourly.sql" }
        vehicle_positions_route_daily  = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_route_daily.sql" }
      }
      // Parameterized table-valued functions (TVFs)
      routines = {
        tvf_vehicle_positions_minute = {
          sql_file = "${var.schemas_root}/../views/vehicle_positions/functions/tvf_vehicle_positions_minute.sql"
          args = [
            { name = "vehicle_id", type_kind = "STRING" },
            { name = "start_ts",   type_kind = "TIMESTAMP" },
            { name = "end_ts",     type_kind = "TIMESTAMP" }
          ]
          return_columns = [
            { name = "minute_ts_utc",               type_kind = "TIMESTAMP" },
            { name = "day_of_week_num",             type_kind = "INT64" },
            { name = "day_of_week_name",            type_kind = "STRING" },
            { name = "vehicle_id",                  type_kind = "STRING" },
            { name = "updates_count",               type_kind = "INT64" },
            { name = "active_vehicles",            type_kind = "INT64" },
            { name = "avg_speed_kmh",              type_kind = "FLOAT64" },
            { name = "p50_speed_kmh",              type_kind = "FLOAT64" },
            { name = "p90_speed_kmh",              type_kind = "FLOAT64" },
            { name = "moving_share",               type_kind = "FLOAT64" },
            { name = "avg_update_interval_seconds", type_kind = "FLOAT64" }
          ]
        }
        tvf_trip_positions_minute = {
          sql_file = "${var.schemas_root}/../views/vehicle_positions/functions/tvf_trip_positions_minute.sql"
          args = [
            { name = "trip_id",   type_kind = "STRING" },
            { name = "start_ts",  type_kind = "TIMESTAMP" },
            { name = "end_ts",    type_kind = "TIMESTAMP" }
          ]
          return_columns = [
            { name = "minute_ts_utc",               type_kind = "TIMESTAMP" },
            { name = "day_of_week_num",             type_kind = "INT64" },
            { name = "day_of_week_name",            type_kind = "STRING" },
            { name = "trip_id",                     type_kind = "STRING" },
            { name = "updates_count",               type_kind = "INT64" },
            { name = "active_vehicles",            type_kind = "INT64" },
            { name = "avg_speed_kmh",              type_kind = "FLOAT64" },
            { name = "p50_speed_kmh",              type_kind = "FLOAT64" },
            { name = "p90_speed_kmh",              type_kind = "FLOAT64" },
            { name = "moving_share",               type_kind = "FLOAT64" },
            { name = "avg_update_interval_seconds", type_kind = "FLOAT64" }
          ]
        }
        tvf_route_positions_minute = {
          sql_file = "${var.schemas_root}/../views/vehicle_positions/functions/tvf_route_positions_minute.sql"
          args = [
            { name = "route_id",  type_kind = "STRING" },
            { name = "start_ts",  type_kind = "TIMESTAMP" },
            { name = "end_ts",    type_kind = "TIMESTAMP" }
          ]
          return_columns = [
            { name = "minute_ts_utc",               type_kind = "TIMESTAMP" },
            { name = "day_of_week_num",             type_kind = "INT64" },
            { name = "day_of_week_name",            type_kind = "STRING" },
            { name = "route_id",                    type_kind = "STRING" },
            { name = "updates_count",               type_kind = "INT64" },
            { name = "active_vehicles",            type_kind = "INT64" },
            { name = "avg_speed_kmh",              type_kind = "FLOAT64" },
            { name = "p50_speed_kmh",              type_kind = "FLOAT64" },
            { name = "p90_speed_kmh",              type_kind = "FLOAT64" },
            { name = "moving_share",               type_kind = "FLOAT64" },
            { name = "avg_update_interval_seconds", type_kind = "FLOAT64" }
          ]
        }
      }
    }

    "trip-updates" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/trip_updates/rt_trip_updates.schema.json" }
      }
      views = {
        trip_updates_daily_summary = { sql_file = "${var.schemas_root}/../views/trip_updates/vw_trip_updates_daily_summary.sql" }
      }
    }

    "service-alerts" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/service_alerts/rt_service_alerts.schema.json" }
      }
      views = {
        service_alerts_daily_summary = { sql_file = "${var.schemas_root}/../views/service_alerts/vw_service_alerts_daily_summary.sql" }
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
        schedule_daily_summary    = { sql_file = "${var.schemas_root}/../views/schedule/vw_schedule_daily_summary.sql" }
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
      // views = {
      //   daily_schedule_daily_summary = { sql_file = "${var.schemas_root}/../views/daily_schedule/vw_daily_schedule_daily_summary.sql" }
      // }
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
        agency_daily_summary       = { sql_file = "${var.schemas_root}/../views/analytics/vw_agency_daily_summary.sql" }
      }
    }
  }
}

output "datasets" {
  description = "Shared dataset catalog (spec/response_type/tables)"
  value       = local.datasets
}
