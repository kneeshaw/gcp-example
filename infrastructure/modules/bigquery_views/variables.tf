variable "project_id" { type = string }
variable "dataset_id" { type = string }

variable "planned_vs_actual_enabled" {
  type    = bool
  default = true
}
variable "fact_stop_events_enabled" {
  type    = bool
  default = true
}
variable "timezone" {
  description = "IANA timezone for deriving local time features in views"
  type        = string
  default     = "UTC"
}
variable "agg_route_hourly_enabled" {
  type    = bool
  default = true
}
variable "agg_network_hourly_enabled" {
  type    = bool
  default = true
}
variable "agg_route_daily_enabled" {
  type    = bool
  default = true
}
variable "agg_network_daily_enabled" {
  type    = bool
  default = true
}

variable "baseline_route_hour_dow_enabled" {
  type    = bool
  default = true
}

variable "route_hourly_with_baseline_enabled" {
  type    = bool
  default = true
}

variable "baseline_window_days" {
  description = "Trailing window in days to compute baselines (e.g., 56 for ~8 weeks)"
  type        = number
  default     = 56
}

variable "fact_trips_enabled" {
  type    = bool
  default = true
}

variable "agg_trip_daily_enabled" {
  type    = bool
  default = true
}

variable "shapes_geog_enabled" {
  type    = bool
  default = true
}

variable "osm_akl_road_links_enabled" {
  type    = bool
  default = true
}

variable "trip_edges_enabled" {
  type    = bool
  default = true
}
