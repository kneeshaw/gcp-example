-- temp_mdl_speed_h3_hourly.sql --- Model for hourly H3 speed with rolling averages ---
-- This model builds on the aggregated speed data per H3 hexagon and hour,
-- calculating a rolling 28-row average speed for each H3 index, hour, and route mode.
-- This helps identify persistent speed anomalies at specific locations.
-- Grain: One row per service_date, hour, h3_index, route_mode.

WITH base AS (
  SELECT
    service_date,
    day_of_week,
    hour,
    route_id,
    direction_id,
    route_mode,
    h3_8,
    avg_speed_kmh
  FROM `myproject.mydataset.temp_agg_speed_h3_hour`
)
SELECT
  service_date,
  day_of_week,
  hour,
  route_id,
  direction_id,
  route_mode,
  h3_8,
  -- 28-day rolling average per (route,direction,mode,hour,h3_8)
  AVG(avg_speed_kmh) OVER (
    PARTITION BY route_id, direction_id, route_mode, hour, h3_8
    ORDER BY service_date
    RANGE BETWEEN INTERVAL 27 DAY PRECEDING AND CURRENT ROW
  ) AS ra28_avg_speed_kmh,
  avg_speed_kmh AS curr_avg_speed_kmh
FROM base;
