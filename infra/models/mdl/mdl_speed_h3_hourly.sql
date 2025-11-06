-- infra/models/mdl/mdl_speed_h3_hourly.sql
--
-- This model calculates the 28-day rolling average speed for each H3 hexagon
-- at an hourly granularity. It helps in identifying locations with persistent
-- speed anomalies compared to their recent history.
--
-- Grain: One row per H3 index, service_date, and hour.
--
WITH
  speed_data AS (
    -- Source of hourly aggregated speed data per H3 hexagon
    SELECT
      service_date,
      hour,
      h3_index,
      route_mode,
      avg_speed_kmh
    FROM
      `${project_id}.${dataset_id}.agg_speed_h3_hour`
  ),
  rolling_avg AS (
    SELECT
      service_date,
      hour,
      h3_index,
      route_mode,
      avg_speed_kmh,
       -- 28-row window: current + 27 preceding rows
      AVG(avg_speed_kmh) OVER (
        PARTITION BY h3_index, hour, route_mode
        ORDER BY service_date
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
      ) AS avg_speed_kmh_28_day_rolling
    FROM
      speed_data
  )
SELECT
  service_date,
  hour,
  h3_index,
  route_mode,
  avg_speed_kmh,
  avg_speed_kmh_28_day_rolling,
  -- Calculate the percentage change from the 28-day rolling average
  SAFE_DIVIDE(
    avg_speed_kmh - avg_speed_kmh_28_day_rolling,
    avg_speed_kmh_28_day_rolling
  ) AS pct_change_from_28_day_avg
FROM
  rolling_avg
ORDER BY
  service_date,
  hour,
  h3_index;