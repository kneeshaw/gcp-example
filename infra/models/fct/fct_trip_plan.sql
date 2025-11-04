-- Stop Event Fact View (Daily Schedule)
-- One row per scheduled stop event with normalized keys and time features
-- Source: ds_daily_schedule (materialized daily schedule table)

SELECT
  -- Service context
  s.service_date_dt,
  s.service_date,
  s.feed_hash,

  -- Trip/route/stop keys and attributes
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

  -- Scheduled times (UTC)
  s.scheduled_arrival,
  s.scheduled_departure,
  s.scheduled_arrival_s,
  s.scheduled_departure_s,

  -- Chosen event timestamp (UTC) for bucketing and joins
  COALESCE(s.scheduled_arrival, s.scheduled_departure) AS event_ts_utc,

  -- Local time features (parameterized timezone)
  EXTRACT(HOUR FROM DATETIME(COALESCE(s.scheduled_arrival, s.scheduled_departure), "${timezone}")) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM DATE(s.service_date_dt)) AS day_of_week
FROM `${project_id}.${dataset_id}.stg_daily_schedule` s
;
