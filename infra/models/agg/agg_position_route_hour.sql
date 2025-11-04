-- infra/models/agg/agg_position_route_hour.sql
--
-- This model rolls up vehicle position data to provide an hourly summary of
-- performance for each route. It is based on live vehicle positions, not the
-- scheduled plan.
--
-- Grain: One row per route_id, service_date, and hour_local.
--
SELECT
  p.service_date,
  p.hour_local,
  p.route_id,
  r.route_short_name,
  r.route_long_name,
  r.route_mode,

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
LEFT JOIN
  `${project_id}.${dataset_id}.dim_route` AS r
  ON p.route_id = r.route_id
WHERE
  p.route_id IS NOT NULL
GROUP BY
  1, 2, 3, 4, 5, 6
ORDER BY
  service_date, hour_local, route_id;