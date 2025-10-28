-- Baseline metrics by (route, direction, hour_of_day, day_of_week) over trailing window
WITH hist AS (
  SELECT
    route_id,
    direction_id,
    hour_of_day,
    day_of_week,
    early_late_s,
    otp_flag,
    has_actual
  FROM `${project_id}.${dataset_id}.vw_fact_stop_events`
  WHERE service_date_dt BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 30 DAY)
                            AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 DAY)
)
SELECT
  route_id,
  direction_id,
  day_of_week,
  hour_of_day,
  SAFE_DIVIDE(COUNTIF(otp_flag), COUNTIF(has_actual))           AS baseline_otp_rate,
  AVG(early_late_s)                                             AS baseline_avg_early_late_s,
  APPROX_QUANTILES(early_late_s, 101)[OFFSET(50)]               AS baseline_p50_early_late_s,
  APPROX_QUANTILES(early_late_s, 101)[OFFSET(90)]               AS baseline_p90_early_late_s,
  COUNT(*)                                                      AS baseline_events
FROM hist
GROUP BY route_id, direction_id, day_of_week, hour_of_day
