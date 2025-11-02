-- Dwell Fact: time spent at each stop
-- Source: vw_stop_events_fact (combined schedule + realtime stop events)

WITH events AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_stop_events_fact`
)
SELECT
  -- Keys and context
  e.service_date_dt,
  e.service_date,
  e.feed_hash,
  e.route_id,
  e.direction_id,
  e.trip_id,
  e.stop_id,
  e.stop_sequence,
  e.stop_name,

  -- Schedule times
  e.scheduled_arrival,
  e.scheduled_departure,
  SAFE_CAST(TIMESTAMP_DIFF(e.scheduled_departure, e.scheduled_arrival, SECOND) AS INT64) AS planned_dwell_s,

  -- Realtime times
  e.actual_arrival,
  e.actual_departure,
  SAFE_CAST(TIMESTAMP_DIFF(e.actual_departure, e.actual_arrival, SECOND) AS INT64) AS rt_dwell_s,

  -- Chosen dwell (prefer realtime when available)
  COALESCE(
    SAFE_CAST(TIMESTAMP_DIFF(e.actual_departure, e.actual_arrival, SECOND) AS INT64),
    SAFE_CAST(TIMESTAMP_DIFF(e.scheduled_departure, e.scheduled_arrival, SECOND) AS INT64)
  ) AS dwell_s,

  -- Observation/prediction flags at the stop level
  (e.arrival_is_observed AND e.departure_is_observed) AS dwell_is_observed,
  ((e.arrival_is_predicted OR e.departure_is_predicted) AND NOT (e.arrival_is_observed AND e.departure_is_observed)) AS dwell_is_predicted,

  -- Vehicle context (nullable)
  e.vehicle_id,
  e.entity_id,
  e.record_id
FROM events e
;
