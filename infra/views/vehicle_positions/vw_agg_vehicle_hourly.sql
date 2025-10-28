-- Hourly summary of vehicle position activity
-- Aggregates vehicle counts and coverage by hour and route
SELECT 
  EXTRACT(DATE FROM timestamp) as position_date,
  EXTRACT(HOUR FROM timestamp) as position_hour,
  route_id,
  COUNT(DISTINCT vehicle_id) as unique_vehicles,
  COUNT(*) as total_positions,
  AVG(speed) as avg_speed,
  MIN(timestamp) as first_position_time,
  MAX(timestamp) as last_position_time
FROM `${project_id}.${dataset_id}.rt_vehicle_positions`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1, 2, 3
ORDER BY position_date DESC, position_hour DESC, route_id