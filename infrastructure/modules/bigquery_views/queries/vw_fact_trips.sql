-- Trip-level fact view
-- Derives per-trip KPIs: first/last stop performance, ran/started/finished, cancellation
WITH events AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_fact_stop_events`
),
rt_status AS (
  SELECT
    trip_id,
    start_date,
    ARRAY_AGG(STRUCT(schedule_relationship, timestamp) ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)].schedule_relationship AS schedule_relationship
  FROM `${project_id}.${dataset_id}.rt_trip_updates`
  GROUP BY trip_id, start_date
)
SELECT
  e.service_date_dt,
  e.service_date,
  e.feed_hash,
  e.route_id,
  e.direction_id,
  e.trip_id,
  -- counts
  COUNT(*) AS scheduled_stops,
  COUNTIF(e.is_observed) AS observed_stops,
  COUNTIF(e.is_predicted) AS predicted_stops,
  -- first stop info (order by stop_sequence asc)
  ARRAY_AGG(e.stop_id ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_id,
  ARRAY_AGG(e.stop_name ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_name,
  ARRAY_AGG(e.scheduled_departure ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_scheduled_departure,
  ARRAY_AGG(e.actual_departure ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_actual_departure,
  ARRAY_AGG(e.is_observed ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_is_observed,
  ARRAY_AGG(e.otp_flag ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_otp_flag,
  ARRAY_AGG(e.early_late_s ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stop_delta_s,
  -- last stop info (order by stop_sequence desc)
  ARRAY_AGG(e.stop_id ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_id,
  ARRAY_AGG(e.stop_name ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_name,
  ARRAY_AGG(e.scheduled_arrival ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_scheduled_arrival,
  ARRAY_AGG(e.actual_arrival ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_actual_arrival,
  ARRAY_AGG(e.is_observed ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_is_observed,
  ARRAY_AGG(e.otp_flag ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_otp_flag,
  ARRAY_AGG(e.early_late_s ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_stop_delta_s,
  -- trip-level flags
  COUNTIF(e.is_observed) > 0 AS trip_ran,
  ARRAY_AGG(e.is_observed ORDER BY e.stop_sequence ASC LIMIT 1)[OFFSET(0)] AS trip_started,
  ARRAY_AGG(e.is_observed ORDER BY e.stop_sequence DESC LIMIT 1)[OFFSET(0)] AS trip_finished,
  -- cancellation from realtime status
  IFNULL(
    ARRAY_AGG(
      IF(rs.schedule_relationship = 'CANCELED', TRUE, FALSE)
      ORDER BY e.rt_timestamp DESC LIMIT 1
    )[OFFSET(0)],
    FALSE
  ) AS trip_canceled,
  -- capture latest schedule_relationship for reference
  ARRAY_AGG(rs.schedule_relationship ORDER BY e.rt_timestamp DESC LIMIT 1)[OFFSET(0)] AS latest_schedule_relationship
FROM events e
LEFT JOIN rt_status rs
  ON rs.trip_id = e.trip_id
 AND rs.start_date = CAST(e.service_date AS STRING)
GROUP BY e.service_date_dt, e.service_date, e.feed_hash, e.route_id, e.direction_id, e.trip_id
;
