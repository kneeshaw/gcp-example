-- infra/models/mdl/mdl_unmonitored_movement_segment.sql
--
-- This model identifies periods of unmonitored vehicle movement and represents
-- them as geographic line segments. Each row in this model is a single
-- "jump" where a vehicle has moved a significant distance without providing
-- telemetry updates.
--
-- Grain: One row per unmonitored movement event.
--
WITH
unmonitored_positions AS (
  -- First, select all vehicle positions and retrieve the previous position's
  -- geography and timestamp using the LAG window function. This is needed to construct the line.
  SELECT
    *,
    LAG(geog) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc) AS prev_geog,
    LAG(timestamp_utc) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc) AS prev_timestamp_utc
  FROM
    `${project_id}.${dataset_id}.fct_vehicle_position`
)
SELECT
  -- Identifiers
  p.vehicle_id,
  p.route_id,
  p.trip_id,
  p.route_mode,

  -- Timestamps for the start and end of the gap
  p.prev_timestamp_utc AS gap_start_timestamp_utc,
  p.timestamp_utc AS gap_end_timestamp_utc,

  -- Metrics describing the gap
  p.update_interval_seconds,
  p.position_delta_m,

  -- The geographic representation of the unmonitored movement.
  -- This is a line connecting the last known point to the new point.
  p.prev_geog AS gap_start_geog,
  p.geog AS gap_end_geog,
  ST_MAKELINE(p.prev_geog, p.geog) AS unmonitored_segment_geog

FROM
  unmonitored_positions AS p
WHERE
  -- Filter for only those records that we have flagged as unmonitored movement.
  p.is_unmonitored_movement IS TRUE
  -- Ensure we have a previous geography to create a valid line.
  AND p.prev_geog IS NOT NULL
ORDER BY
  p.service_date, p.vehicle_id, p.timestamp_utc;
