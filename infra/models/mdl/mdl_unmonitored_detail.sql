-- infra/models/mdl/mdl_unmonitored_movements_detail.sql
--
-- This model identifies individual unmonitored movements and captures the
-- start and end coordinates for each event. The grain of this model is
-- one row per unmonitored movement.
--
-- Grain: One row per unmonitored movement event.
--
WITH
  lagged_positions AS (
    -- For each position record, use the LAG window function to get the
    -- coordinates and timestamp of the PREVIOUS position for that same vehicle.
    SELECT
      service_date,
      vehicle_id,
      route_mode,
      timestamp_utc,
      latitude,
      longitude,
      is_unmonitored_movement,
      update_interval_seconds,
      position_delta_m,
      LAG(latitude, 1) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc) AS prev_latitude,
      LAG(longitude, 1) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc) AS prev_longitude,
      LAG(timestamp_utc, 1) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc) AS prev_timestamp_utc
    FROM
      `${project_id}.${dataset_id}.fct_vehicle_position`
  )
SELECT
  -- The current row's timestamp is the 'end' of the gap
  lp.timestamp_utc AS movement_end_utc,
  -- The previous row's timestamp is the 'start' of the gap
  lp.prev_timestamp_utc AS movement_start_utc,
  lp.service_date,
  lp.vehicle_id,
  lp.route_mode,
  -- Start coordinates are from the previous position
  lp.prev_latitude AS start_lat,
  lp.prev_longitude AS start_lon,
  -- End coordinates are from the current position
  lp.latitude AS end_lat,
  lp.longitude AS end_lon,
  -- The pre-calculated interval and distance represent the gap
  lp.update_interval_seconds AS gap_duration_seconds,
  lp.position_delta_m AS gap_distance_m
FROM
  lagged_positions AS lp
WHERE
  -- We only want the rows that represent the END of an unmonitored movement.
  -- These rows contain all the information we need about the gap.
  lp.is_unmonitored_movement
  AND lp.prev_latitude IS NOT NULL -- Ensure we have a valid start point
ORDER BY
  lp.service_date DESC,
  lp.vehicle_id,
  movement_start_utc;