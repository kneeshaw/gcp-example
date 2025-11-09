-- agg_unmonitored_h3_day.sql
-- Daily unmonitored-movement hotspots by H3-10, split by route & mode
-- Source: fct_vehicle_segment

SELECT
  service_date,
  start_h3_10 AS h3_10,        -- keep H3 at resolution 10
  route_mode,

  -- Coverage / gap context
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
  COUNT(DISTINCT vehicle_id)                                 AS vehicle_cnt,
  COUNT(DISTINCT route_id)                                   AS route_cnt,


FROM `${project_id}.${dataset_id}.temp_fct_vehicle_segment`
WHERE start_h3_10 IS NOT NULL
GROUP BY
  service_date, h3_10, route_mode;
