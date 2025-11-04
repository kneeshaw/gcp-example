-- infra/models/mdl/mdl_vehicle_telemetry_health_daily.sql
--
-- This model provides a daily summary of telemetry health for each vehicle,
-- including a 90-day rolling average for key metrics to provide historical context.
-- It is designed to be a clean, user-friendly semantic layer for BI tools
-- and applications.
--
-- Grain: One row per service_date, vehicle_id, route_mode.
--
WITH daily_metrics AS (
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
),

metrics_with_historical AS (
  SELECT
    *,
    -- Calculate the 90-day rolling average for key health metrics.
    -- This provides a dynamic benchmark for each vehicle's recent performance.
    ROUND(
      AVG(unmonitored_movement_percentage) OVER (
        PARTITION BY vehicle_id, route_mode
        ORDER BY service_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ), 2
    ) AS avg_90day_unmonitored_movement_percentage,

    ROUND(
      AVG(avg_update_interval_seconds) OVER (
        PARTITION BY vehicle_id, route_mode
        ORDER BY service_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ), 1
    ) AS avg_90day_update_interval_seconds

  FROM daily_metrics
)

SELECT
  *
FROM metrics_with_historical
ORDER BY
  service_date, vehicle_id;
