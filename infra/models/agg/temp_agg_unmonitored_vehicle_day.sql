-- agg_unmonitored_vehicle_day.sql
-- Daily aggregates of unmonitored movements per vehicle, from fct_vehicle_segment

SELECT
  service_date,
  vehicle_id,
  route_mode,

  -- Coverage/context
  COUNT(*)                                                   AS seg_cnt,
  COUNTIF(is_gap)                                            AS gap_seg_cnt,
  SAFE_DIVIDE(COUNTIF(is_gap), COUNT(*))                     AS gap_seg_ratio,
  SUM(IF(is_gap, delta_seconds, 0))                          AS gap_seconds_total,
  AVG(IF(is_gap, delta_seconds, NULL))                       AS gap_seconds_avg,

  -- Unmonitored movement signal (movement during a gap)
  COUNTIF(is_unmonitored_movement)                           AS unmonitored_cnt,
  SUM(IF(is_unmonitored_movement, delta_distance_m, 0))      AS unmonitored_total_m,
  AVG(IF(is_unmonitored_movement, delta_distance_m, NULL))   AS unmonitored_avg_m,
  MAX(IF(is_unmonitored_movement, delta_distance_m, NULL))   AS unmonitored_max_m,

  -- Useful context
  COUNT(DISTINCT route_id)                                   AS route_cnt

  -- If you want timestamps for QA, uncomment these (they exist in the temp build):
  -- , MIN(IF(is_gap, start_ts_utc, NULL))                   AS first_gap_start_utc
  -- , MAX(IF(is_gap, end_ts_utc, NULL))                     AS last_gap_end_utc

FROM `${project_id}.${dataset_id}.temp_fct_vehicle_segment`
GROUP BY service_date, vehicle_id, route_mode;
