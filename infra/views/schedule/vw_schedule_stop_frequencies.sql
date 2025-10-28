-- Stop frequencies by time of day
-- Shows service frequency at each stop broken down by hour
SELECT 
  s.stop_id,
  s.stop_name,
  s.stop_lat,
  s.stop_lon,
  EXTRACT(HOUR FROM PARSE_TIME('%H:%M:%S', st.departure_time)) as departure_hour,
  COUNT(*) as departures_count,
  COUNT(DISTINCT st.trip_id) as unique_trips,
  COUNT(DISTINCT t.route_id) as routes_served
FROM `${project_id}.${dataset_id}.sc_stops` s
JOIN `${project_id}.${dataset_id}.sc_stop_times` st ON s.stop_id = st.stop_id
JOIN `${project_id}.${dataset_id}.sc_trips` t ON st.trip_id = t.trip_id
WHERE st.departure_time IS NOT NULL
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, departure_hour
ORDER BY s.stop_name, departure_hour