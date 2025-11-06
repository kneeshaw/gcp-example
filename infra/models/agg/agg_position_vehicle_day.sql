-- infra/models/agg/agg_position_vehicle_day.sql
--
-- This model aggregates unmonitored vehicle positions by vehicle and day.
-- Unmonitored positions are defined as vehicle positions that are not
-- associated with a scheduled trip.
--
-- Grain: One row per vehicle_id, route_mode, and service_date.
--
WITH
  base AS (
    SELECT
      vp.service_date,
      vp.vehicle_id,
      vp.route_mode,
      vp.update_interval_seconds,
      vp.position_delta_m
    FROM
      `${project_id}.${dataset_id}.fct_vehicle_position` AS vp
    WHERE
      vp.is_unmonitored_movement
  )
SELECT
  b.service_date,
  b.vehicle_id,
  b.route_mode,
  COUNT(*) AS position_count,
  SUM(b.update_interval_seconds) AS unmonitored_duration_seconds,
  SUM(b.position_delta_m) AS unmonitored_distance_m
FROM
  base AS b
GROUP BY
  1, 2, 3
ORDER BY
  1, 2, 3;
