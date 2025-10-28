-- Planned vs Actual view
WITH rt_latest AS (
  SELECT
    trip_id,
    stop_id,
    start_date,
    stop_sequence,
    ARRAY_AGG(
      STRUCT(
        actual_arrival,
        actual_departure,
        arrival_delay,
        arrival_uncertainty,
        departure_delay,
        departure_uncertainty,
        timestamp AS rt_timestamp,
        vehicle_id,
        entity_id,
        record_id
      )
      ORDER BY timestamp DESC
      LIMIT 1
    )[OFFSET(0)] AS latest
  FROM `${project_id}.${dataset_id}.rt_trip_updates`
  WHERE trip_id IS NOT NULL AND stop_id IS NOT NULL
  GROUP BY trip_id, stop_id, start_date, stop_sequence
)
SELECT
  s.service_date_dt,
  s.service_date,
  s.feed_hash,
  s.route_id,
  s.trip_id,
  s.stop_id,
  s.stop_sequence,
  s.stop_name,
  s.route_short_name,
  s.direction_id,
  s.trip_headsign,
  s.stop_headsign,
  s.shape_id,
  s.shape_dist_traveled,
  s.scheduled_arrival,
  s.scheduled_departure,
  s.scheduled_arrival_s,
  s.scheduled_departure_s,
  -- Actuals (may be NULL if no realtime yet)
  l.latest.actual_arrival   AS actual_arrival,
  l.latest.actual_departure AS actual_departure,
  l.latest.arrival_delay    AS arrival_delay,
  l.latest.arrival_uncertainty    AS arrival_uncertainty,
  l.latest.departure_delay  AS departure_delay,
  l.latest.departure_uncertainty  AS departure_uncertainty,
  l.latest.rt_timestamp     AS rt_timestamp,
  l.latest.vehicle_id       AS vehicle_id,
  l.latest.entity_id        AS entity_id,
  l.latest.record_id        AS record_id,
  l.stop_sequence           AS rt_stop_sequence,
  -- Derived deltas between planned and actual (seconds)
  SAFE_CAST(TIMESTAMP_DIFF(l.latest.actual_arrival, s.scheduled_arrival, SECOND) AS INT64)   AS arrival_delta_s,
  SAFE_CAST(TIMESTAMP_DIFF(l.latest.actual_departure, s.scheduled_departure, SECOND) AS INT64) AS departure_delta_s
FROM `${project_id}.${dataset_id}.ds_daily_schedule` AS s
LEFT JOIN rt_latest AS l
  ON l.trip_id = s.trip_id
 AND l.stop_id = s.stop_id
 AND l.start_date = CAST(s.service_date AS STRING)
 AND l.stop_sequence = s.stop_sequence
