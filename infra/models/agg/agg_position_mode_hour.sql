-- infra/models/agg/agg_position_mode_hour.sql
--
-- This model provides a high-level, system-wide hourly view of vehicle
-- performance, aggregated by the mode of transport (e.g., bus, rail, ferry).
--
-- Grain: One row per route_mode, service_date, and hour_local.
--
SELECT
  p.service_date,
  p.hour_local,
  p.route_mode,

  -- Key Performance Indicators
  AVG(p.speed_kmh) AS avg_speed_kmh,
  APPROX_QUANTILES(p.speed_kmh, 100)[OFFSET(95)] AS p95_speed_kmh,
  COUNT(DISTINCT p.vehicle_id) AS distinct_vehicle_count,
  COUNT(p.vehicle_id) AS position_count,
  AVG(p.update_interval_seconds) AS avg_update_interval_seconds,

  -- Unmonitored Movement Statistics
  SUM(IF(p.is_unmonitored_movement, 1, 0)) AS unmonitored_movement_count,
  SUM(IF(p.is_unmonitored_movement, p.update_interval_seconds, 0)) AS unmonitored_movement_seconds,
  SUM(IF(p.is_unmonitored_movement, p.position_delta_m, 0)) AS unmonitored_movement_distance_m

FROM
  `${project_id}.${dataset_id}.fct_vehicle_position` AS p
WHERE
  p.route_mode IS NOT NULL
GROUP BY
  1, 2, 3
ORDER BY
  service_date, hour_local, route_mode;