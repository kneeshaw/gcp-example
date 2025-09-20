-- Compare route-hourly aggregates with trailing baselines
WITH cur AS (
  SELECT
    service_date_dt,
    route_id,
    direction_id,
    day_of_week,
    hour_of_day,
    events_with_actual,
    events_total,
    otp_rate,
    avg_early_late_s,
    p50_early_late_s,
    p90_early_late_s
  FROM `${project_id}.${dataset_id}.vw_agg_route_hourly`
)
SELECT
  cur.service_date_dt,
  cur.route_id,
  cur.direction_id,
  cur.day_of_week,
  cur.hour_of_day,
  cur.events_with_actual,
  cur.events_total,
  cur.otp_rate,
  cur.avg_early_late_s,
  cur.p50_early_late_s,
  cur.p90_early_late_s,
  base.baseline_otp_rate,
  base.baseline_avg_early_late_s,
  base.baseline_p50_early_late_s,
  base.baseline_p90_early_late_s,
  -- deltas
  cur.otp_rate - base.baseline_otp_rate                          AS delta_otp_rate,
  cur.avg_early_late_s - base.baseline_avg_early_late_s          AS delta_avg_early_late_s,
  cur.p50_early_late_s - base.baseline_p50_early_late_s          AS delta_p50_early_late_s,
  cur.p90_early_late_s - base.baseline_p90_early_late_s          AS delta_p90_early_late_s
FROM cur
LEFT JOIN `${project_id}.${dataset_id}.vw_baseline_route_hour_dow` base
  ON base.route_id = cur.route_id
 AND base.direction_id = cur.direction_id
 AND base.day_of_week = cur.day_of_week
 AND base.hour_of_day = cur.hour_of_day;