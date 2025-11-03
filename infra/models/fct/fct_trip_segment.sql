-- Segment Fact: time and speed between consecutive stops on a trip
-- Source: trip_events_fact (combined schedule + realtime stop events)

WITH events AS (
  SELECT * FROM `${project_id}.${dataset_id}.fct_trip_event`
), ordered AS (
  SELECT
    e.*,
    LEAD(e.stop_id)            OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS to_stop_id,
    LEAD(e.stop_sequence)      OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS to_stop_sequence,
    LEAD(e.stop_name)          OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS to_stop_name,
    LEAD(e.scheduled_arrival)  OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS to_scheduled_arrival,
    LEAD(e.actual_arrival)     OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS to_actual_arrival,
    LEAD(e.shape_dist_traveled)OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS to_shape_dist_traveled,
    LEAD(e.arrival_is_observed)OVER(PARTITION BY e.trip_id, e.service_date ORDER BY e.stop_sequence) AS arrival_is_observed_next
  FROM events e
), segments AS (
  SELECT
    o.*,
    COALESCE(o.actual_departure, o.scheduled_departure) AS from_time,
    COALESCE(o.to_actual_arrival, o.to_scheduled_arrival) AS to_time,
    (o.departure_is_observed AND o.arrival_is_observed_next) AS travel_is_observed
  FROM ordered o
)
SELECT
  -- Service/trip context
  o.service_date_dt,
  o.service_date,
  o.feed_hash,
  o.route_id,
  o.direction_id,
  o.trip_id,

  -- From/To stop info
  o.stop_id          AS from_stop_id,
  o.stop_sequence    AS from_stop_sequence,
  o.stop_name        AS from_stop_name,
  o.to_stop_id       AS to_stop_id,
  o.to_stop_sequence AS to_stop_sequence,
  o.to_stop_name     AS to_stop_name,

  -- Planned segment timing
  o.scheduled_departure AS from_scheduled_departure,
  o.to_scheduled_arrival AS to_scheduled_arrival,
  SAFE_CAST(TIMESTAMP_DIFF(o.to_scheduled_arrival, o.scheduled_departure, SECOND) AS INT64) AS planned_travel_s,

  -- Realtime segment timing
  o.actual_departure AS from_actual_departure,
  o.to_actual_arrival AS to_actual_arrival,
  SAFE_CAST(TIMESTAMP_DIFF(o.to_actual_arrival, o.actual_departure, SECOND) AS INT64) AS rt_travel_s,

  -- Chosen segment travel time (prefer realtime)
  COALESCE(
    SAFE_CAST(TIMESTAMP_DIFF(o.to_actual_arrival, o.actual_departure, SECOND) AS INT64),
    SAFE_CAST(TIMESTAMP_DIFF(o.to_scheduled_arrival, o.scheduled_departure, SECOND) AS INT64)
  ) AS travel_s,

  -- Approx distance between stops from shape distances (may be NULL or non-monotonic; guard negatives)
  SAFE_CAST(GREATEST(o.to_shape_dist_traveled - o.shape_dist_traveled, 0) AS FLOAT64) AS segment_length_m,

  -- Speeds (km/h), planned vs realtime
  SAFE_CAST(3.6 * SAFE_DIVIDE(GREATEST(o.to_shape_dist_traveled - o.shape_dist_traveled, 0), NULLIF(SAFE_CAST(TIMESTAMP_DIFF(o.to_scheduled_arrival, o.scheduled_departure, SECOND) AS FLOAT64), 0)) AS FLOAT64) AS planned_speed_kmh,
  SAFE_CAST(3.6 * SAFE_DIVIDE(GREATEST(o.to_shape_dist_traveled - o.shape_dist_traveled, 0), NULLIF(SAFE_CAST(TIMESTAMP_DIFF(o.to_actual_arrival, o.actual_departure, SECOND) AS FLOAT64), 0)) AS FLOAT64) AS rt_speed_kmh,
  -- Vehicle positions enrichments within segment window [from_time, to_time]
  AVG(v.speed_kmh) AS vp_avg_speed_kmh,
  -- Approximate mode of occupancy_status during the segment
  ((APPROX_TOP_COUNT(v.occupancy_status, 1))[SAFE_OFFSET(0)].value) AS vp_occupancy_status,

  -- Observation flag precomputed in segments
  o.travel_is_observed
FROM segments o
LEFT JOIN `${project_id}.${dataset_id}.fct_vehicle_position` v
  ON v.trip_id = o.trip_id
 AND o.from_time IS NOT NULL AND o.to_time IS NOT NULL
 AND v.timestamp >= o.from_time AND v.timestamp <= o.to_time
WHERE o.to_stop_id IS NOT NULL
GROUP BY
  o.service_date_dt,
  o.service_date,
  o.feed_hash,
  o.route_id,
  o.direction_id,
  o.trip_id,
  o.stop_id,
  o.stop_sequence,
  o.stop_name,
  o.to_stop_id,
  o.to_stop_sequence,
  o.to_stop_name,
  o.scheduled_departure,
  o.to_scheduled_arrival,
  planned_travel_s,
  o.actual_departure,
  o.to_actual_arrival,
  rt_travel_s,
  travel_s,
  segment_length_m,
  planned_speed_kmh,
  rt_speed_kmh,
  o.travel_is_observed
;
