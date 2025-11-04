WITH base AS (
  SELECT
    -- Time
    v.timestamp AS timestamp_utc,

    -- Identifiers
    CAST(v.vehicle_id AS STRING) AS vehicle_id,
    CAST(v.trip_id AS STRING) AS trip_id,
    CAST(v.route_id AS STRING) AS route_id,

    -- Measures and attributes
    SAFE_CAST(v.speed AS FLOAT64) AS speed_kmh,
    SAFE_CAST(v.bearing AS FLOAT64) AS bearing_deg,
    SAFE_CAST(v.occupancy_status AS STRING) AS occupancy_status,
   
    -- Location
    SAFE_CAST(v.latitude AS FLOAT64) AS latitude,
    SAFE_CAST(v.longitude AS FLOAT64) AS longitude,
    IF(v.latitude IS NULL OR v.longitude IS NULL, NULL, ST_GEOGPOINT(v.longitude, v.latitude)) AS geog,

    -- Route mode/type from dimension table
    r.route_type,
    r.route_mode
  FROM `${project_id}.${dataset_id}.stg_vehicle_positions` v
  LEFT JOIN `${project_id}.${dataset_id}.dim_route` r
    ON r.route_id = CAST(v.route_id AS STRING)
)
,
-- Add interval and delta calculations
base_with_deltas AS (
  SELECT
    *,
    -- Time and distance since the previous update for this vehicle
    TIMESTAMP_DIFF(
      timestamp_utc,
      LAG(timestamp_utc) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc),
      SECOND
    ) AS update_interval_seconds,
    -- Straight-line distance traveled since the last position update
    ST_DISTANCE(
      geog,
      LAG(geog) OVER (PARTITION BY vehicle_id ORDER BY timestamp_utc)
    ) AS position_delta_m
  FROM base
)
,
-- Single-row region config (dataset is per-region)
region_cfg AS (
  SELECT region_prefix, region_name, timezone, svc_boundary_hour
  FROM `${project_id}.${dataset_id}.dim_region`
  LIMIT 1
)
,
-- Add localized and derived time fields
localized AS (
  SELECT
    *,
    -- Localized time fields using region timezone and service-day boundary
    DATETIME(timestamp_utc, region_cfg.timezone) AS datetime_local,
    -- Service day: roll back by boundary hours, then take the date
    DATE(DATETIME_SUB(DATETIME(timestamp_utc, region_cfg.timezone), INTERVAL region_cfg.svc_boundary_hour HOUR)) AS service_date
  FROM base_with_deltas
  CROSS JOIN region_cfg
)
,
-- Fill in NULL route_mode values. A vehicle's mode is constant for a given day.
-- We find the mode associated with the vehicle for the day and apply it to all its positions.
mode_filled AS (
  SELECT
    *,
    -- Get the single non-null route_mode for the vehicle/day combination.
    -- This assumes a vehicle operates as a single mode within a service day.
    MAX(route_mode) OVER (PARTITION BY vehicle_id, service_date) AS filled_route_mode
  FROM localized
)
SELECT
  -- Time
  timestamp_utc,
  datetime_local,
  service_date,
  EXTRACT(HOUR FROM datetime_local) AS hour_local,
  FORMAT_DATETIME('%A', datetime_local) AS dow_local,

  -- Identifiers and other measures
  vehicle_id,
  trip_id,
  route_id,
  speed_kmh,
  bearing_deg,
  occupancy_status,
  latitude,
  longitude,
  geog,
  route_type,
  filled_route_mode AS route_mode,

  -- Gap and movement analysis fields
  update_interval_seconds,
  position_delta_m,
  -- Unmonitored movement is detected if there's a significant time gap
  -- AND the vehicle has moved a meaningful distance.
  (
    update_interval_seconds > 120 AND position_delta_m > 50
  ) AS is_unmonitored_movement
FROM mode_filled
