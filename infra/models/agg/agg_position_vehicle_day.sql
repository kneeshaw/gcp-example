-- infra/models/agg/agg_position_vehicle_day.sql
--
-- This model rolls up the hourly vehicle-level position data to a daily grain.
-- It provides a daily summary of activity for each vehicle.
--
-- Grain: One row per vehicle_id, service_date.
--
SELECT
  service_date,
  vehicle_id,
  route_mode,

  -- Weighted average for speed, using position_count as the weight.
  SAFE_DIVIDE(
    SUM(avg_speed_kmh * position_count),
    SUM(position_count)
  ) AS avg_speed_kmh,

  -- Weighted average for update interval.
  SAFE_DIVIDE(
    SUM(avg_update_interval_seconds * position_count),
    SUM(position_count)
  ) AS avg_update_interval_seconds,

  SUM(position_count) AS position_count,

  -- Sum of unmonitored movement stats
  SUM(unmonitored_movement_count) AS unmonitored_movement_count,
  SUM(unmonitored_movement_seconds) AS unmonitored_movement_seconds,
  SUM(unmonitored_movement_distance_m) AS unmonitored_movement_distance_m

FROM
  `${project_id}.${dataset_id}.agg_position_vehicle_hour`
GROUP BY
  1, 2, 3
ORDER BY
  service_date, vehicle_id;
