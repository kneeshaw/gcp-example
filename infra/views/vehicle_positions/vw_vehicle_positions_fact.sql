WITH base AS (
  SELECT
    -- Time
    v.timestamp,
    DATE(v.timestamp) AS date_utc,
    TIMESTAMP_TRUNC(v.timestamp, MINUTE, 'UTC') AS minute_ts_utc,
    TIMESTAMP_TRUNC(v.timestamp, HOUR, 'UTC') AS hour_ts_utc,
    -- Day of week (UTC)
    EXTRACT(DAYOFWEEK FROM v.timestamp) AS day_of_week_num, -- 1=Sunday
    FORMAT_TIMESTAMP('%A', v.timestamp, 'UTC') AS day_of_week_name,
    -- Time since the previous update for this vehicle (seconds)
    TIMESTAMP_DIFF(
      v.timestamp,
      LAG(v.timestamp) OVER (PARTITION BY v.vehicle_id ORDER BY v.timestamp),
      SECOND
    ) AS update_interval_seconds,

    -- Identifiers
    CAST(v.vehicle_id AS STRING) AS vehicle_id,
    CAST(v.trip_id AS STRING) AS trip_id,
    CAST(v.route_id AS STRING) AS route_id,

    -- Measures and attributes (source speed is km/h)
    SAFE_CAST(v.speed AS FLOAT64) AS speed_kmh,
    SAFE_CAST(v.bearing AS FLOAT64) AS bearing_deg,
    SAFE_CAST(v.occupancy_status AS STRING) AS occupancy_status,
   
    -- Location
    SAFE_CAST(v.latitude AS FLOAT64) AS latitude,
    SAFE_CAST(v.longitude AS FLOAT64) AS longitude,
    IF(v.latitude IS NULL OR v.longitude IS NULL, NULL, ST_GEOGPOINT(v.longitude, v.latitude)) AS geog,

    -- Simple movement heuristic: > 1.0 km/h
    SAFE_CAST(v.speed AS FLOAT64) > 1.0 AS moving_flag,

    -- Route mode/type from schedule routes (fallback if unavailable)
    SAFE_CAST(r.route_type AS INT64) AS route_type,
    CASE SAFE_CAST(r.route_type AS INT64)
      WHEN 0 THEN 'tram'
      WHEN 1 THEN 'subway'
      WHEN 2 THEN 'rail'
      WHEN 3 THEN 'bus'
      WHEN 4 THEN 'ferry'
      WHEN 5 THEN 'cable_tram'
      WHEN 6 THEN 'aerial_lift'
      WHEN 7 THEN 'funicular'
      WHEN 11 THEN 'trolleybus'
      WHEN 12 THEN 'monorail'
      ELSE NULL
    END AS route_mode
  FROM `${project_id}.${dataset_id}.rt_vehicle_positions` v
  LEFT JOIN `${project_id}.${dataset_id}.sc_routes` r
    ON SAFE_CAST(r.route_id AS STRING) = CAST(v.route_id AS STRING)
)
SELECT
  -- Pass-through all existing fields
  timestamp,
  date_utc,
  minute_ts_utc,
  hour_ts_utc,
  day_of_week_num,
  day_of_week_name,
  update_interval_seconds,
  vehicle_id,
  trip_id,
  route_id,
  speed_kmh,
  bearing_deg,
  occupancy_status,
  latitude,
  longitude,
  geog,
  moving_flag,
  route_type,
  route_mode
FROM base
