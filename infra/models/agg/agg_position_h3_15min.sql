-- infra/models/agg/agg_position_h3_15min.sql
--
-- This is an advanced geospatial rollup model that aggregates vehicle position
-- data into H3 hexagonal cells for every 15-minute window. It enables powerful
-- heatmap visualizations of network speed, density, and congestion, independent
-- of the underlying route structure.
--
-- Grain: One row per H3 cell ID (resolution 9) and 15-minute time window.
--
WITH
positions_with_h3 AS (
  SELECT
    -- Truncate the timestamp to a 15-minute window for aggregation.
    TIMESTAMP_SECONDS(900 * DIV(UNIX_SECONDS(timestamp_utc), 900)) AS time_window_15min,
    speed_kmh,
    vehicle_id,
    is_unmonitored_movement,
    -- Convert the geographic point to an H3 cell ID using native BQ functions.
    -- Resolution 9 provides a good balance for city-level analysis (~175m edge length).
    h3.ST_H3(geog, 9) AS h3_cell_id
  FROM
    `${project_id}.${dataset_id}.fct_vehicle_position`
  WHERE
    geog IS NOT NULL
)
SELECT
  time_window_15min,
  h3_cell_id,

  -- Key Performance Indicators
  AVG(speed_kmh) AS avg_speed_kmh,
  COUNT(DISTINCT vehicle_id) AS distinct_vehicle_count,
  COUNT(vehicle_id) AS position_count,
  SUM(IF(is_unmonitored_movement, 1, 0)) AS unmonitored_movement_count,
  -- The geometry of the H3 cell for easy visualization in BI tools.
  h3.ST_H3_BOUNDARY(h3_cell_id) AS h3_geometry

FROM
  positions_with_h3
WHERE
  h3_cell_id IS NOT NULL
GROUP BY
  1, 2, 6
ORDER BY
  time_window_15min,
  h3_cell_id;