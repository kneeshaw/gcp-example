-- temp_mdl_speed_route_hourly.sql
-- This temporary model computes a 28-day rolling average speed for each route
-- at an hourly granularity. It serves as an intermediary step for further
-- analysis and modeling.
-- Grain: One row per service_date, hour, route_id, direction_id, route_mode.

WITH base AS (
  SELECT
    service_date,
    day_of_week,
    hour,
    route_id,
    direction_id,
    route_mode,
    avg_speed_kmh
  FROM `myproject.mydataset.temp_agg_speed_route_hour`
)
SELECT
  service_date,
  day_of_week,
  hour,
  route_id,
  direction_id,
  route_mode,
  -- 28-day rolling average per (route_id, direction_id, route_mode, hour)
  AVG(avg_speed_kmh) OVER (
    PARTITION BY route_id, direction_id, route_mode, hour
    ORDER BY service_date
    RANGE BETWEEN INTERVAL 27 DAY PRECEDING AND CURRENT ROW
  ) AS ra28_avg_speed_kmh,
  avg_speed_kmh AS curr_avg_speed_kmh
FROM base;
