variable "schemas_root" {
  description = "Absolute or module-relative path to the shared schemas/files directory"
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
        main = { schema_file = "${var.schemas_root}/rt_vehicle_positions.schema.json" }
      }
    }

    "trip-updates" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/rt_trip_updates.schema.json" }
      }
    }

    "service-alerts" = {
      spec          = "rt"
      response_type = "json"
      tables = {
        main = { schema_file = "${var.schemas_root}/rt_service_alerts.schema.json" }
      }
    }

    // Static GTFS schedule dataset
    "schedule" = {
      spec          = "sc"
      response_type = "zip"
      tables = {
        agency          = { schema_file = "${var.schemas_root}/sc_agency.schema.json" }
        calendar        = { schema_file = "${var.schemas_root}/sc_calendar.schema.json" }
        calendar_dates  = { schema_file = "${var.schemas_root}/sc_calendar_dates.schema.json" }
        fare_attributes = { schema_file = "${var.schemas_root}/sc_fare_attributes.schema.json" }
        fare_rules      = { schema_file = "${var.schemas_root}/sc_fare_rules.schema.json" }
        feed_info       = { schema_file = "${var.schemas_root}/sc_feed_info.schema.json" }
        frequencies     = { schema_file = "${var.schemas_root}/sc_frequencies.schema.json" }
        routes          = { schema_file = "${var.schemas_root}/sc_routes.schema.json" }
        shapes          = { schema_file = "${var.schemas_root}/sc_shapes.schema.json" }
        stop_times      = { schema_file = "${var.schemas_root}/sc_stop_times.schema.json" }
        stops           = { schema_file = "${var.schemas_root}/sc_stops.schema.json" }
        transfers       = { schema_file = "${var.schemas_root}/sc_transfers.schema.json" }
        trips           = { schema_file = "${var.schemas_root}/sc_trips.schema.json" }
      }
    }

    // Derived daily schedule (materialized table)
    "daily-schedule" = {
      spec          = "sc"
      response_type = ""
      tables = {
        main = { schema_file = "${var.schemas_root}/sc_daily_schedule.schema.json" }
      }
    }
  }
}

output "datasets" {
  description = "Shared dataset catalog (spec/response_type/tables)"
  value       = local.datasets
}
