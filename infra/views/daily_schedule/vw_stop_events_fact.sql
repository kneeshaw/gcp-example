-- Combined Stop Events Fact
-- Join scheduled stop events with realtime TripUpdates facts
-- Inputs:
--   - vw_stop_events_schedule_fact (schedule-side stop events)
--   - vw_trip_updates_fact (realtime stop events)

WITH sched AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_stop_events_schedule_fact`
), rt_raw AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_trip_updates_fact`
), rt_grouped AS (
  SELECT
    trip_id,
    start_date,
    stop_id,
    stop_sequence,

    -- Latest feed timestamp seen for this stop-event
    MAX(rt_timestamp) AS rt_timestamp,

    -- Last non-null per field by most recent rt_timestamp
    (ARRAY_AGG(vehicle_id IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS vehicle_id,
    (ARRAY_AGG(entity_id IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS entity_id,
    (ARRAY_AGG(record_id IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS record_id,

    (ARRAY_AGG(actual_arrival IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS actual_arrival,
    (ARRAY_AGG(actual_departure IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS actual_departure,

    (ARRAY_AGG(arrival_delay_s IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS arrival_delay_s,
    (ARRAY_AGG(arrival_uncertainty IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS arrival_uncertainty,
    (ARRAY_AGG(departure_delay_s IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS departure_delay_s,
    (ARRAY_AGG(departure_uncertainty IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS departure_uncertainty,
    (ARRAY_AGG(schedule_relationship IGNORE NULLS ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS schedule_relationship,

    (ARRAY_AGG(arrival_is_observed ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS arrival_is_observed,
    (ARRAY_AGG(departure_is_observed ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS departure_is_observed,
    (ARRAY_AGG(arrival_is_predicted ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS arrival_is_predicted,
    (ARRAY_AGG(departure_is_predicted ORDER BY rt_timestamp DESC LIMIT 1))[SAFE_OFFSET(0)] AS departure_is_predicted
  FROM rt_raw
  GROUP BY trip_id, start_date, stop_id, stop_sequence
), rt AS (
  SELECT
    g.*,
    -- Event timestamp and time features recomputed from consolidated actuals/latest rt_timestamp
    COALESCE(g.actual_arrival, g.actual_departure, g.rt_timestamp) AS event_ts_utc,
    EXTRACT(HOUR FROM COALESCE(g.actual_arrival, g.actual_departure, g.rt_timestamp)) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM COALESCE(g.actual_arrival, g.actual_departure, g.rt_timestamp)) AS day_of_week,
    -- Early/late and collapsed flags recomputed from consolidated fields
    COALESCE(g.arrival_delay_s, g.departure_delay_s) AS early_late_s,
    IF(COALESCE(g.arrival_delay_s, g.departure_delay_s) BETWEEN -60 AND 300, TRUE, FALSE) AS otp_flag,
    COALESCE(g.arrival_is_observed, g.departure_is_observed, FALSE) AS is_observed,
    COALESCE(g.arrival_is_predicted, g.departure_is_predicted, FALSE) AS is_predicted
  FROM rt_grouped g
)
SELECT
  -- Service context
  s.service_date_dt,
  s.service_date,
  s.feed_hash,

  -- Trip/route/stop keys and attributes (prefer schedule for canonical keys)
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

  -- Scheduled times
  s.scheduled_arrival,
  s.scheduled_departure,
  s.scheduled_arrival_s,
  s.scheduled_departure_s,
  s.event_ts_utc        AS scheduled_event_ts_utc,
  s.hour_of_day         AS scheduled_hour_of_day,
  s.day_of_week         AS scheduled_day_of_week,

  -- Realtime fields (nullable if no RT)
  r.rt_timestamp,
  r.vehicle_id,
  r.entity_id,
  r.record_id,
  r.actual_arrival,
  r.actual_departure,
  r.arrival_delay_s,
  r.arrival_uncertainty,
  r.departure_delay_s,
  r.departure_uncertainty,
  r.schedule_relationship,
  r.event_ts_utc        AS rt_event_ts_utc,
  r.hour_of_day         AS rt_hour_of_day,
  r.day_of_week         AS rt_day_of_week,

  -- Observed/predicted flags, OTP, and combined early/late
  r.arrival_is_observed,
  r.departure_is_observed,
  r.arrival_is_predicted,
  r.departure_is_predicted,
  r.is_observed,
  r.is_predicted,
  r.early_late_s,
  r.otp_flag
FROM sched s
LEFT JOIN rt r
  ON r.trip_id = s.trip_id
 AND r.stop_id = s.stop_id
 AND r.stop_sequence = s.stop_sequence
 AND r.start_date = CAST(s.service_date AS STRING)
;
