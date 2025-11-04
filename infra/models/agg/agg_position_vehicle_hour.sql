-- infra/models/agg/agg_position_vehicle_hour.sql
--
-- This model rolls up vehicle position data to provide an hourly summary of
-- performance for each individual vehicle.
--
-- Grain: One row per vehicle_id, service_date, and hour_local.
--
WITH
vehicle_hour_base AS (
  SELECT
    p.service_date,
    p.hour_local,
    p.vehicle_id,
    p.speed_kmh,
    p.update_interval_seconds,
    p.is_unmonitored_movement,
    p.position_delta_m,
    p.route_id,
    p.timestamp_utc,
    -- Rank positions within the hour to find the last one
    ROW_NUMBER() OVER(PARTITION BY p.vehicle_id, p.service_date, p.hour_local ORDER BY p.timestamp_utc DESC) as rn
  FROM
    `${project_id}.${dataset_id}.fct_vehicle_position` AS p
)
SELECT
  v.service_date,
  v.hour_local,
  v.vehicle_id,
  -- Get the last known route_id for the vehicle in that hour
  MAX(IF(v.rn = 1, v.route_id, NULL)) as last_route_id,

  -- Key Performance Indicators
  AVG(v.speed_kmh) AS avg_speed_kmh,
  COUNT(v.vehicle_id) AS position_count,
  AVG(v.update_interval_seconds) AS avg_update_interval_seconds,

  -- Unmonitored Movement Statistics
  SUM(IF(v.is_unmonitored_movement, 1, 0)) AS unmonitored_movement_count,
  SUM(IF(v.is_unmonitored_movement, v.update_interval_seconds, 0)) AS unmonitored_movement_seconds,
  SUM(IF(v.is_unmonitored_movement, v.position_delta_m, 0)) AS unmonitored_movement_distance_m

FROM
  vehicle_hour_base AS v
GROUP BY
  1, 2, 3
ORDER BY
  service_date, hour_local, vehicle_id;