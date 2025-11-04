-- infra/models/mdl/mdl_vehicle_telemetry_health_route_daily.sql
--
-- This semantic model provides a daily overview of vehicle telemetry health,
-- aggregated by route. It is designed to be a source for
-- the Telemetry Health dashboard, allowing for easy comparison of current
-- performance against historical averages for each route.
--
-- Grain: One row per service_date, route_id.
--
WITH
daily_route_metrics AS (
  -- First, calculate the core metrics for each route on a given service_date.
  SELECT
    service_date,
    route_id,
    route_short_name,
    route_long_name,
    route_mode,
    SUM(position_count) AS position_count,
    SUM(unmonitored_movement_count) AS unmonitored_movement_count,
    -- Weighted average for the update interval across the day for the route.
    SAFE_DIVIDE(
      SUM(avg_update_interval_seconds * position_count),
      SUM(position_count)
    ) AS avg_update_interval_seconds
  FROM
    `${project_id}.${dataset_id}.agg_position_route_hour`
  GROUP BY
    1, 2, 3, 4, 5
),

daily_route_vehicle_counts AS (
  -- Calculate the distinct vehicle count per route and day from fct_trip.
  -- The service_date in fct_trip is an INT64 (YYYYMMDD), so it must be parsed into a DATE.
  SELECT
    PARSE_DATE('%Y%m%d', CAST(service_date AS STRING)) AS service_date,
    route_id,
    COUNT(DISTINCT vehicle_id) AS distinct_vehicle_count
  FROM
    `${project_id}.${dataset_id}.fct_trip`
  WHERE
    vehicle_id IS NOT NULL
    AND route_id IS NOT NULL
  GROUP BY
    1, 2
),

metrics_with_historical AS (
  -- Combine the daily metrics and calculate the 90-day rolling average for each key metric,
  -- partitioned by the route. This is crucial for contextualizing performance.
  SELECT
    m.service_date,
    m.route_id,
    m.route_short_name,
    m.route_long_name,
    m.route_mode,
    vc.distinct_vehicle_count,
    m.avg_update_interval_seconds,
    SAFE_DIVIDE(m.unmonitored_movement_count, m.position_count) AS unmonitored_movement_percentage,

    -- 90-day rolling average for distinct vehicles, per route
    AVG(vc.distinct_vehicle_count) OVER (
      PARTITION BY m.route_id
      ORDER BY UNIX_DATE(m.service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS avg_90_day_distinct_vehicle_count,

    -- 90-day rolling average for update interval, per route
    AVG(m.avg_update_interval_seconds) OVER (
      PARTITION BY m.route_id
      ORDER BY UNIX_DATE(m.service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS avg_90_day_update_interval_seconds,

    -- 90-day rolling average for unmonitored movement percentage, per route
    AVG(SAFE_DIVIDE(m.unmonitored_movement_count, m.position_count)) OVER (
      PARTITION BY m.route_id
      ORDER BY UNIX_DATE(m.service_date)
      RANGE BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS avg_90_day_unmonitored_movement_percentage

  FROM
    daily_route_metrics AS m
  LEFT JOIN
    daily_route_vehicle_counts AS vc
    ON m.service_date = vc.service_date AND m.route_id = vc.route_id
)

SELECT
  *
FROM
  metrics_with_historical
ORDER BY
  service_date DESC, route_id;
