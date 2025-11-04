-- infra/models/mdl/mdl_vehicle_telemetry_health_detail_daily.sql
--
-- This semantic model provides a detailed daily view of telemetry health for
-- each individual vehicle. It enriches the vehicle-level data with daily and
-- historical averages for the vehicle's own performance, as well as for its
-- route and mode, allowing for multi-layered comparison and anomaly detection.
--
-- Grain: One row per vehicle_id, service_date.
--
WITH
vehicle_daily_metrics AS (
  -- Aggregate individual vehicle metrics to a daily grain.
  SELECT
    service_date,
    vehicle_id,
    -- Use MAX to get the single route_mode for the day.
    MAX(route_mode) AS route_mode,
    -- Find the most frequent route_id for the vehicle on that day.
    -- This is an approximation if a vehicle serves multiple routes.
    APPROX_TOP_COUNT(route_id, 1)[OFFSET(0)].value AS representative_route_id,
    SUM(position_count) AS position_count,
    SUM(unmonitored_movement_count) AS unmonitored_movement_count,
    SAFE_DIVIDE(
      SUM(avg_update_interval_seconds * position_count),
      SUM(position_count)
    ) AS avg_update_interval_seconds
  FROM
    `${project_id}.${dataset_id}.agg_position_vehicle_hour`
  GROUP BY
    1, 2
),

vehicle_metrics_with_historical AS (
  -- Calculate 90-day rolling averages for each vehicle's own performance.
  SELECT
    *,
    -- 90-day rolling average for this vehicle's update interval
    AVG(avg_update_interval_seconds) OVER (
      PARTITION BY vehicle_id
      ORDER BY UNIX_DATE(service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS vehicle_avg_90_day_update_interval_seconds,
    -- 90-day rolling average for this vehicle's unmonitored movement
    AVG(SAFE_DIVIDE(unmonitored_movement_count, position_count)) OVER (
      PARTITION BY vehicle_id
      ORDER BY UNIX_DATE(service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS vehicle_avg_90_day_unmonitored_movement_percentage
  FROM vehicle_daily_metrics
)

SELECT
  -- Vehicle-specific details
  v.service_date,
  v.vehicle_id,
  v.route_mode,
  v.representative_route_id,

  -- Vehicle-specific performance metrics (daily and historical)
  v.avg_update_interval_seconds AS vehicle_avg_update_interval_seconds,
  v.vehicle_avg_90_day_update_interval_seconds,
  SAFE_DIVIDE(v.unmonitored_movement_count, v.position_count) AS vehicle_unmonitored_movement_percentage,
  v.vehicle_avg_90_day_unmonitored_movement_percentage,

  -- Corresponding historical averages for the vehicle's mode
  mode.avg_90_day_update_interval_seconds AS mode_avg_90_day_update_interval_seconds,
  mode.avg_90_day_unmonitored_movement_percentage AS mode_avg_90_day_unmonitored_movement_percentage,

  -- Corresponding historical averages for the vehicle's representative route
  route.avg_90_day_update_interval_seconds AS route_avg_90_day_update_interval_seconds,
  route.avg_90_day_unmonitored_movement_percentage AS route_avg_90_day_unmonitored_movement_percentage

FROM
  vehicle_metrics_with_historical AS v
-- Join mode-level historical aggregates to provide peer-group context
LEFT JOIN
  `${project_id}.${dataset_id}.mdl_vehicle_telemetry_health_mode_daily` AS mode
  ON v.service_date = mode.service_date AND v.route_mode = mode.route_mode
-- Join route-level historical aggregates to provide peer-group context
LEFT JOIN
  `${project_id}.${dataset_id}.mdl_vehicle_telemetry_health_route_daily` AS route
  ON v.service_date = route.service_date AND v.representative_route_id = route.route_id
ORDER BY
  v.service_date DESC, v.vehicle_id;
