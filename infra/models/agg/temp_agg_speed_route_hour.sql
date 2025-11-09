-- agg_speed_route_hour.sql
-- This model aggregates vehicle speed data by route, direction, and hour.
-- It provides key metrics such as average speed and speed percentiles to analyze
-- route performance on an hourly basis.
-- Grain: One row per service_date, day_of_week, hour, route_id, direction_id, route_mode

WITH vehicle_hour AS (
  SELECT
    service_date,
    day_of_week,
    hour,
    route_id,
    direction_id,
    route_mode,
    vehicle_id,
    AVG(speed_kmh) AS vehicle_avg_speed
  FROM `${project_id}.${dataset_id}.temp_fct_vehicle_position`
  WHERE speed_kmh IS NOT NULL
  GROUP BY
    service_date, day_of_week, hour,
    route_id, direction_id, route_mode, vehicle_id
)
SELECT
  service_date,
  day_of_week,
  hour,
  route_id,
  direction_id,
  route_mode,
  COUNT(*)                                                AS vehicle_cnt,
  AVG(vehicle_avg_speed)                                  AS avg_speed_kmh,
  APPROX_QUANTILES(vehicle_avg_speed, 100)[OFFSET(50)]    AS p50_speed_kmh,
  APPROX_QUANTILES(vehicle_avg_speed, 100)[OFFSET(85)]    AS p85_speed_kmh,
  APPROX_QUANTILES(vehicle_avg_speed, 100)[OFFSET(95)]    AS p95_speed_kmh,
  MAX(vehicle_avg_speed)                                  AS max_speed_kmh
FROM vehicle_hour
GROUP BY
  service_date, day_of_week, hour,
  route_id, direction_id, route_mode;