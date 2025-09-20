-- Trip-daily aggregates by route/direction
WITH trips AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_fact_trips`
)
SELECT
  service_date_dt,
  route_id,
  direction_id,
  COUNT(*) AS trips_scheduled,
  COUNTIF(trip_ran) AS trips_ran,
  COUNTIF(trip_canceled) AS trips_canceled,
  SAFE_DIVIDE(COUNTIF(first_stop_otp_flag), COUNT(*)) AS first_stop_otp_rate,
  AVG(first_stop_delta_s) AS avg_first_stop_delta_s
FROM trips
GROUP BY service_date_dt, route_id, direction_id
;
