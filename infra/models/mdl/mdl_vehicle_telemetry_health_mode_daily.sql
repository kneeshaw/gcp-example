-- infra/models/mdl/mdl_vehicle_telemetry_health_mode_daily.sql
--
-- This semantic model provides a daily overview of vehicle telemetry health,
-- aggregated by transport mode. It is designed to be the primary source for
-- the Telemetry Health dashboard, allowing for easy comparison of current
-- performance against historical averages for each mode.
--
-- Grain: One row per service_date, route_mode.
--
WITH
daily_mode_metrics AS (
  -- First, calculate the core metrics for each mode on a given service_date.
  SELECT
    service_date,
    route_mode,
    SUM(position_count) AS position_count,
    SUM(unmonitored_movement_count) AS unmonitored_movement_count,
    -- Weighted average for the update interval across the day for the mode.
    SAFE_DIVIDE(
      SUM(avg_update_interval_seconds * position_count),
      SUM(position_count)
    ) AS avg_update_interval_seconds
  FROM
    `${project_id}.${dataset_id}.agg_position_mode_hour`
  GROUP BY
    1, 2
),

daily_mode_vehicle_counts AS (
  -- Calculate the distinct vehicle count per mode and day.
  -- We must read from the hourly aggregate and count distinct vehicles
  -- to avoid double-counting vehicles that operate for multiple hours.
  SELECT
    service_date,
    route_mode,
    COUNT(DISTINCT vehicle_id) AS distinct_vehicle_count
  FROM
    `${project_id}.${dataset_id}.agg_position_vehicle_hour`
  GROUP BY
    1, 2
),

metrics_with_historical AS (
  -- Combine the daily metrics and calculate the 90-day rolling average for each key metric,
  -- partitioned by the transport mode. This is crucial for contextualizing performance.
  SELECT
    m.service_date,
    m.route_mode,
    vc.distinct_vehicle_count,
    m.avg_update_interval_seconds,
    SAFE_DIVIDE(m.unmonitored_movement_count, m.position_count) AS unmonitored_movement_percentage,

    -- 90-day rolling average for distinct vehicles, per mode
    AVG(vc.distinct_vehicle_count) OVER (
      PARTITION BY m.route_mode
      ORDER BY UNIX_DATE(m.service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS avg_90_day_distinct_vehicle_count,

    -- 90-day rolling average for update interval, per mode
    AVG(m.avg_update_interval_seconds) OVER (
      PARTITION BY m.route_mode
      ORDER BY UNIX_DATE(m.service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS avg_90_day_update_interval_seconds,

    -- 90-day rolling average for unmonitored movement percentage, per mode
    AVG(SAFE_DIVIDE(m.unmonitored_movement_count, m.position_count)) OVER (
      PARTITION BY m.route_mode
      ORDER BY UNIX_DATE(m.service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS avg_90_day_unmonitored_movement_percentage

  FROM
    daily_mode_metrics AS m
  LEFT JOIN
    daily_mode_vehicle_counts AS vc
    ON m.service_date = vc.service_date AND m.route_mode = vc.route_mode
)

SELECT
  *
FROM
  metrics_with_historical
ORDER BY
  service_date DESC, route_mode;
