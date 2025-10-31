WITH base AS (
  SELECT
    f.minute_ts_utc,
    EXTRACT(DAYOFWEEK FROM f.minute_ts_utc) AS day_of_week_num,
    FORMAT_TIMESTAMP('%A', f.minute_ts_utc, 'UTC') AS day_of_week_name,
    f.trip_id AS trp_id,
    f.vehicle_id,
    f.speed_kmh,
    f.moving_flag,
    f.update_interval_seconds
  FROM `${project_id}.${dataset_id}.vw_vehicle_positions_fact` AS f
  WHERE f.trip_id = trip_id
    AND f.minute_ts_utc >= TIMESTAMP_TRUNC(start_ts, MINUTE, 'UTC')
    AND f.minute_ts_utc <  TIMESTAMP_TRUNC(end_ts, MINUTE, 'UTC')
), agg AS (
  SELECT
    minute_ts_utc,
    day_of_week_num,
    day_of_week_name,
    trp_id AS trip_id,
    COUNT(*) AS updates_count,
    COUNT(DISTINCT vehicle_id) AS active_vehicles,
    ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
    ROUND(APPROX_QUANTILES(speed_kmh, 100)[SAFE_OFFSET(50)], 2) AS p50_speed_kmh,
    ROUND(APPROX_QUANTILES(speed_kmh, 100)[SAFE_OFFSET(90)], 2) AS p90_speed_kmh,
    ROUND(SAFE_DIVIDE(COUNTIF(moving_flag), NULLIF(COUNT(*), 0)), 3) AS moving_share,
    ROUND(AVG(update_interval_seconds), 1) AS avg_update_interval_seconds
  FROM base
  GROUP BY minute_ts_utc, day_of_week_num, day_of_week_name, trip_id
)
SELECT
  *
FROM agg
ORDER BY minute_ts_utc, trip_id