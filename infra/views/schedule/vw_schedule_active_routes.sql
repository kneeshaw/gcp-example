-- Active routes with current service information
-- Combines route metadata with calendar data to show active services
SELECT 
  r.route_id,
  r.route_short_name,
  r.route_long_name,
  r.route_type,
  r.route_color,
  COUNT(DISTINCT t.trip_id) as total_trips,
  COUNT(DISTINCT t.service_id) as service_count,
  MIN(c.start_date) as service_start_date,
  MAX(c.end_date) as service_end_date
FROM `${project_id}.${dataset_id}.sc_routes` r
LEFT JOIN `${project_id}.${dataset_id}.sc_trips` t ON r.route_id = t.route_id
LEFT JOIN `${project_id}.${dataset_id}.sc_calendar` c ON t.service_id = c.service_id
WHERE PARSE_DATE('%Y%m%d', CAST(c.end_date AS STRING)) >= CURRENT_DATE()
GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type, r.route_color
ORDER BY r.route_short_name