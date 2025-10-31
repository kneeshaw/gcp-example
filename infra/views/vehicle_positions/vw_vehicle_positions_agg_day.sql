WITH base AS (
  SELECT
    DATE(timestamp) AS date_utc,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_TRUNC(timestamp, DAY)) AS day_of_week_num,
    FORMAT_TIMESTAMP('%A', TIMESTAMP_TRUNC(timestamp, DAY), 'UTC') AS day_of_week_name,
    vehicle_id,
    speed_kmh,
    moving_flag,
    update_interval_seconds,
    TIMESTAMP_TRUNC(timestamp, MINUTE, 'UTC') AS minute_ts_utc
  FROM `${project_id}.${dataset_id}.vw_vehicle_positions_fact`
), daily AS (
  SELECT
    date_utc,
    day_of_week_num,
    day_of_week_name,
    COUNT(*) AS updates_count,
    COUNT(DISTINCT vehicle_id) AS active_vehicles,
    ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
    ROUND(APPROX_QUANTILES(speed_kmh, 100)[SAFE_OFFSET(50)], 2) AS p50_speed_kmh,
    ROUND(APPROX_QUANTILES(speed_kmh, 100)[SAFE_OFFSET(90)], 2) AS p90_speed_kmh,
    ROUND(SAFE_DIVIDE(COUNTIF(moving_flag), NULLIF(COUNT(*), 0)), 3) AS moving_share,
    ROUND(AVG(update_interval_seconds), 1) AS avg_update_interval_seconds,
    MIN(minute_ts_utc) AS first_event_ts,
    MAX(minute_ts_utc) AS last_event_ts
  FROM base
  GROUP BY date_utc, day_of_week_num, day_of_week_name
)
SELECT
  d.*,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), d.last_event_ts, MINUTE) AS minutes_since_last_update,
  CURRENT_TIMESTAMP() AS calculated_at
FROM daily d
ORDER BY d.date_utc DESC
