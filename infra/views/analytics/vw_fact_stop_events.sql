-- Fact Stop Events view: derives time features and flags from planned vs actual
-- Inputs: vw_live_planned_vs_actual
-- Note: Using Pacific/Auckland as example; parameterize if needed via Terraform var later

WITH base AS (
  SELECT
    service_date_dt,
    service_date,
    feed_hash,
    route_id,
    trip_id,
    direction_id,
    stop_id,
    stop_sequence,
    stop_name,
    route_short_name,
    trip_headsign,
    stop_headsign,
    shape_id,
    shape_dist_traveled,
    scheduled_arrival,
    scheduled_departure,
    scheduled_arrival_s,
    scheduled_departure_s,
    actual_arrival,
    actual_departure,
    arrival_delay,
    departure_delay,
    arrival_uncertainty,
    departure_uncertainty,
    rt_timestamp,
    vehicle_id,
    entity_id,
    record_id,
    arrival_delta_s,
    departure_delta_s
  FROM `${project_id}.${dataset_id}.vw_live_planned_vs_actual`
)
SELECT
  b.*,
  -- Choose an event timestamp for bucketing: prefer actual_arrival/departure else scheduled
  COALESCE(b.actual_arrival, b.actual_departure, b.scheduled_arrival, b.scheduled_departure) AS event_ts,
  -- Local time features
  EXTRACT(HOUR FROM DATETIME(COALESCE(b.actual_arrival, b.scheduled_arrival), "${timezone}")) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM DATE(b.service_date_dt)) AS day_of_week,
  -- Flags
  IF(b.actual_arrival IS NOT NULL OR b.actual_departure IS NOT NULL, TRUE, FALSE) AS has_actual,
  IF(b.actual_arrival IS NOT NULL, TRUE, FALSE) AS arrived,
  IF(b.actual_departure IS NOT NULL, TRUE, FALSE) AS departed,
  -- Observation vs prediction classification
  -- Observed when the realtime event time is at or before the feed timestamp and uncertainty == 0
  IF(b.actual_arrival IS NOT NULL
     AND b.actual_arrival <= b.rt_timestamp
     AND COALESCE(b.arrival_uncertainty, 1) = 0, TRUE, FALSE) AS arrival_is_observed,
  IF(b.actual_departure IS NOT NULL
     AND b.actual_departure <= b.rt_timestamp
     AND COALESCE(b.departure_uncertainty, 1) = 0, TRUE, FALSE) AS departure_is_observed,
  -- Predicted when we have a realtime time but it's in the future vs feed timestamp or uncertainty > 0
  IF(b.actual_arrival IS NOT NULL
     AND (b.actual_arrival > b.rt_timestamp OR COALESCE(b.arrival_uncertainty, 0) > 0), TRUE, FALSE) AS arrival_is_predicted,
  IF(b.actual_departure IS NOT NULL
     AND (b.actual_departure > b.rt_timestamp OR COALESCE(b.departure_uncertainty, 0) > 0), TRUE, FALSE) AS departure_is_predicted,
  -- Event-level collapsed flags (prefer arrival else departure)
  COALESCE(
    IF(b.actual_arrival IS NOT NULL, IF(b.actual_arrival <= b.rt_timestamp AND COALESCE(b.arrival_uncertainty, 1) = 0, TRUE, FALSE), NULL),
    IF(b.actual_departure IS NOT NULL, IF(b.actual_departure <= b.rt_timestamp AND COALESCE(b.departure_uncertainty, 1) = 0, TRUE, FALSE), NULL),
    FALSE
  ) AS is_observed,
  COALESCE(
    IF(b.actual_arrival IS NOT NULL, IF(b.actual_arrival > b.rt_timestamp OR COALESCE(b.arrival_uncertainty, 0) > 0, TRUE, FALSE), NULL),
    IF(b.actual_departure IS NOT NULL, IF(b.actual_departure > b.rt_timestamp OR COALESCE(b.departure_uncertainty, 0) > 0, TRUE, FALSE), NULL),
    FALSE
  ) AS is_predicted,
  -- Early/late seconds: prefer arrival; fallback to departure
  COALESCE(b.arrival_delta_s, b.departure_delta_s) AS early_late_s,
  -- OTP window: [-60s, +300s] inclusive; adjust later if needed
  IF(COALESCE(b.arrival_delta_s, b.departure_delta_s) BETWEEN -60 AND 300, TRUE, FALSE) AS otp_flag
FROM base b
;