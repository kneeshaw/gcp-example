-- Route-daily aggregates from fact stop events
SELECT
  service_date_dt,
  route_id,
  direction_id,
  COUNTIF(has_actual)                           AS events_with_actual,
  COUNT(*)                                      AS events_total,
  SAFE_DIVIDE(COUNTIF(otp_flag AND is_observed), COUNTIF(has_actual AND is_observed)) AS otp_rate,
  AVG(IF(is_observed, early_late_s, NULL))      AS avg_early_late_s,
  APPROX_QUANTILES(IF(is_observed, early_late_s, NULL), 101)[OFFSET(50)] AS p50_early_late_s,
  APPROX_QUANTILES(IF(is_observed, early_late_s, NULL), 101)[OFFSET(90)] AS p90_early_late_s,
  COUNTIF(is_observed AND early_late_s > 600)   AS late_over_10m,
  COUNTIF(is_observed AND early_late_s < -180)  AS early_over_3m
FROM `${project_id}.${dataset_id}.vw_fact_stop_events`
GROUP BY service_date_dt, route_id, direction_id
;
