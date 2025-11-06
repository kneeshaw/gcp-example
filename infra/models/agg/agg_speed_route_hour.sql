-- infra/models/agg/agg_speed_route_hour.sql
--
-- This model aggregates vehicle speed data by route, direction, and hour for in-service vehicles.
-- It serves as the foundation for analyzing route speed performance and identifying potential disruptions.
--
-- Grain: One row per service_date, hour, route_id, direction_id, route_mode.
--
SELECT
  vp.service_date,
  EXTRACT(HOUR FROM vp.timestamp_utc) AS hour,
  FORMAT_DATE('%A', vp.service_date) AS day_of_week,
  vp.route_id,
  vp.direction_id,
  vp.route_mode,
  -- Speed is in metres/second, convert to km/h
  AVG(vp.speed_kmh) AS avg_speed_kmh,
  COUNT(vp.vehicle_id) AS position_count
FROM
  `${project_id}.${dataset_id}.fct_vehicle_position` AS vp
WHERE
  vp.trip_id IS NOT NULL
  AND vp.route_id IS NOT NULL
  AND vp.direction_id IS NOT NULL
GROUP BY
  1, 2, 3, 4, 5, 6
ORDER BY
  1, 2, 4, 5, 6;