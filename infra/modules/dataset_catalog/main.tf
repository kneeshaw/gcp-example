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
        vehicle_positions_vehicle_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_vehicle_minute.sql" }
        vehicle_positions_vehicle_hour = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_vehicle_hour.sql" }
        vehicle_positions_vehicle_day  = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_vehicle_day.sql" }
        vehicle_positions_trip_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_trip_minute.sql" }
        vehicle_positions_trip_hour = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_trip_hour.sql" }
        vehicle_positions_trip_day  = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_trip_day.sql" }
        vehicle_positions_route_minute = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_route_minute.sql" }
        vehicle_positions_route_hour = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_route_hour.sql" }
        vehicle_positions_route_day  = { sql_file = "${var.schemas_root}/../views/vehicle_positions/vw_vehicle_positions_route_day.sql" }
      }
    }

    "trip-updates" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/trip_updates/rt_trip_updates.schema.json" }
      }
      views = {
        trip_updates_fact = { sql_file = "${var.schemas_root}/../views/trip_updates/vw_trip_updates_fact.sql" }
      }
    }

    "service-alerts" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/service_alerts/rt_service_alerts.schema.json" }
      }
      views = {
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
      views = {
        stop_events_schedule_fact = { sql_file = "${var.schemas_root}/../views/daily_schedule/vw_stop_events_schedule_fact.sql" }
        stop_events_fact          = { sql_file = "${var.schemas_root}/../views/daily_schedule/vw_stop_events_fact.sql" }
          # Dwell and segment facts
          dwell_fact                = { sql_file = "${var.schemas_root}/../views/daily_schedule/vw_dwell_fact.sql" }
          segment_fact              = { sql_file = "${var.schemas_root}/../views/daily_schedule/vw_segment_fact.sql" }
      }
    }

    // Analytics and performance views
    "analytics" = {
      spec          = "analytics"
      response_type = ""
      tables = {}
      functions = {}
      views = {
      }
    }
  }
}

output "datasets" {
  description = "Shared dataset catalog (spec/response_type/tables)"
  value       = local.datasets
}
