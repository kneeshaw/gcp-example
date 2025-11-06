-- infra/models/agg/agg_speed_h3_hour.sql
--
-- This model aggregates vehicle speed data into H3 hexagonal bins at an hourly
-- granularity. It includes all vehicles, regardless of their in-service status,
-- to provide a comprehensive view of vehicle movement across the network.
--
-- Grain: One row per H3 index, service_date, hour, and route_mode.
--
WITH
  base AS (
    SELECT
      vp.service_date,
      vp.timestamp_utc,
      vp.speed_kmh,
      vp.vehicle_id,
      vp.route_mode,
      -- The UDF returns a JSON type. Convert to string and then clean it
      -- to get a scalar value that can be used in the GROUP BY clause.
      REPLACE(REPLACE(TO_JSON_STRING(bigfunctions.australia_southeast1.h3("latLngToCell", JSON_ARRAY(CAST(vp.latitude AS FLOAT64), CAST(vp.longitude AS FLOAT64), 8))), '["', ''), '"]', '') AS h3_index
    FROM
      `regal-dynamo-470908-v9.auckland_data_dev.fct_vehicle_position` AS vp
    WHERE
      vp.latitude IS NOT NULL
      AND vp.longitude IS NOT NULL
      AND vp.route_mode = 'bus'
  )
SELECT
  b.service_date,
  EXTRACT(HOUR FROM b.timestamp_utc) AS hour,
  FORMAT_DATE('%A', b.service_date) AS day_of_week,
  b.h3_index,
  b.route_mode,
  -- Aggregate speed and position counts
  AVG(b.speed_kmh) AS avg_speed_kmh,
  COUNT(b.vehicle_id) AS position_count
FROM
  base AS b
GROUP BY
  1, 2, 3, 4, 5
ORDER BY
  1, 2, 4, 5;