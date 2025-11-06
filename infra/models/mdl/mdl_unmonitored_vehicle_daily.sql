-- infra/models/mdl/mdl_unmonitored_vehicle_daily.sql
--
-- This model enriches the daily vehicle-based unmonitored position aggregations
-- with a rolling 28-day average for comparison. This helps identify vehicles
-- with an unusual increase in unmonitored activity, which could indicate
-- equipment malfunctions.
--
-- Grain: One row per vehicle_id, route_mode, and service_date.
--
WITH
  base AS (
    SELECT
      service_date,
      vehicle_id,
      route_mode,
      position_count,
      unmonitored_distance_m,
   
      AVG(unmonitored_distance_m) OVER (
        PARTITION BY vehicle_id, route_mode
        ORDER BY service_date
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
      ) AS rolling_28_day_avg_distance_m
    FROM
      `${project_id}.${dataset_id}.agg_position_vehicle_day`
  )
SELECT
  b.service_date,
  b.vehicle_id,
  b.route_mode,
  b.position_count,
  b.unmonitored_distance_m,
  b.rolling_28_day_avg_distance_m,
FROM
  base AS b
ORDER BY
  b.service_date,
  b.vehicle_id,
  b.route_mode;
