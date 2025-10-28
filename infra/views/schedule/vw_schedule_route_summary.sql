-- Route summary with trip and stop statistics
-- Provides overview metrics for each route
SELECT 
  r.route_id,
  r.route_short_name,
  r.route_long_name,
  r.route_type,
  COUNT(DISTINCT t.trip_id) as total_trips,
  COUNT(DISTINCT st.stop_id) as unique_stops,
  COUNT(st.stop_sequence) as total_stop_times,
  AVG(
    CASE 
      WHEN st.departure_time IS NOT NULL AND st.arrival_time IS NOT NULL
      THEN TIME_DIFF(
        PARSE_TIME('%H:%M:%S', st.departure_time),
        PARSE_TIME('%H:%M:%S', st.arrival_time),
        SECOND
      )
    END
  ) as avg_stop_duration_seconds
FROM `${project_id}.${dataset_id}.sc_routes` r
LEFT JOIN `${project_id}.${dataset_id}.sc_trips` t ON r.route_id = t.route_id
LEFT JOIN `${project_id}.${dataset_id}.sc_stop_times` st ON t.trip_id = st.trip_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
ORDER BY total_trips DESC, r.route_short_name