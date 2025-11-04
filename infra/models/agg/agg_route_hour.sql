WITH src AS (
  SELECT
    route_id,
    hour_ts_utc,
    vehicle_id,
    speed_kmh
  FROM `${project_id}.${dataset_id}.fct_vehicle_position`
)
SELECT
  route_id,
  hour_ts_utc,
  DATE(hour_ts_utc) AS date_utc,
  EXTRACT(HOUR FROM hour_ts_utc) AS hour_of_day_utc,
  COUNT(*) AS msg_count,
  COUNT(DISTINCT vehicle_id) AS vehicle_count,
  AVG(speed_kmh) AS avg_speed_kmh,
  MIN(speed_kmh) AS min_speed_kmh,
  MAX(speed_kmh) AS max_speed_kmh,
  APPROX_QUANTILES(speed_kmh, 100)[OFFSET(50)] AS p50_speed_kmh,
  APPROX_QUANTILES(speed_kmh, 100)[OFFSET(95)] AS p95_speed_kmh
FROM src
WHERE speed_kmh IS NOT NULL
GROUP BY route_id, hour_ts_utc
