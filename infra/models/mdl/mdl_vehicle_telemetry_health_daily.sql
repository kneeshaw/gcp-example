-- infra/models/mdl/mdl_vehicle_telemetry_health_daily.sql
--
-- This model provides a daily summary of telemetry health for each vehicle.
-- It is designed to be a clean, user-friendly semantic layer for BI tools
-- and applications.
--
-- Grain: One row per service_date, vehicle_id, route_mode.
--
SELECT
  -- Identifiers
  service_date,
  vehicle_id,
  route_mode,

  -- Key Health Metrics (Formatted for presentation)
  ROUND(SAFE_DIVIDE(unmonitored_movement_count, position_count) * 100, 2) AS unmonitored_movement_percentage,
  ROUND(unmonitored_movement_seconds / 60, 1) AS unmonitored_movement_minutes,
  ROUND(unmonitored_movement_distance_m / 1000, 2) AS unmonitored_movement_distance_km,
  ROUND(avg_update_interval_seconds, 1) AS avg_update_interval_seconds,

  -- Raw counts for context
  position_count,
  unmonitored_movement_count

FROM
  `${project_id}.${dataset_id}.agg_position_vehicle_day`
ORDER BY
  service_date, vehicle_id;
