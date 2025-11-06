-- infra/models/agg/agg_position_h3_day.sql
--
-- This model aggregates unmonitored vehicle positions into H3 hexagonal bins
-- at a daily granularity. Unmonitored positions are defined as vehicle
-- positions that are not associated with a scheduled trip.
--
-- Grain: One row per H3 index, route_mode, and service_date.
--
WITH
  base AS (
    SELECT
      vp.service_date,
      vp.route_mode,
      -- The UDF returns a JSON type. Convert to string and then clean it
      -- to get a scalar value that can be used in the GROUP BY clause.
      REPLACE(REPLACE(TO_JSON_STRING(bigfunctions.australia_southeast1.h3("latLngToCell", JSON_ARRAY(CAST(vp.latitude AS FLOAT64), CAST(vp.longitude AS FLOAT64), 10))), '["', ''), '"]', '') AS h3_index,
      vp.update_interval_seconds,
      vp.position_delta_m
    FROM
      `regal-dynamo-470908-v9.auckland_data_dev.fct_vehicle_position` AS vp
    WHERE
      vp.latitude IS NOT NULL
      AND vp.longitude IS NOT NULL
      AND vp.is_unmonitored_movement
  )
SELECT
  b.service_date,
  b.h3_index,
  b.route_mode,
  COUNT(*) AS position_count,
  SUM(b.update_interval_seconds) AS unmonitored_duration_seconds,
  SUM(b.position_delta_m) AS unmonitored_distance_m
FROM
  base AS b
GROUP BY
  1, 2, 3
ORDER BY
  1, 2, 3;
