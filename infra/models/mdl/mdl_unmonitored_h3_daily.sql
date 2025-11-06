-- infra/models/mdl/mdl_unmonitored_h3_daily.sql
--
-- This model enriches the daily H3-based unmonitored position aggregations
-- with a rolling 28-day average for comparison. This helps identify areas
-- with an unusual increase in unmonitored vehicle activity.
--
-- Grain: One row per H3 index, route_mode, and service_date.
--
WITH
  base AS (
    SELECT
      service_date,
      h3_index,
      route_mode,
      position_count,
      unmonitored_distance_m,
      -- Calculate the rolling 28-day average of metrics for each H3 index and mode

      AVG(unmonitored_distance_m) OVER (
        PARTITION BY h3_index, route_mode
        ORDER BY service_date
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
      ) AS rolling_28_day_avg_distance_m
    FROM
      `${project_id}.${dataset_id}.agg_position_h3_day`
  )
SELECT
  b.service_date,
  b.h3_index,
  b.route_mode,
  b.position_count,
  b.unmonitored_distance_m,
  b.rolling_28_day_avg_distance_m,
  -- Provide the geometry for the H3 hexagon for visualization
  bigfunctions.australia_southeast1.h3("cellToBoundary", JSON_ARRAY(b.h3_index)) AS h3_geometry
FROM
  base AS b
ORDER BY
  b.service_date,
  b.h3_index,
  b.route_mode;
