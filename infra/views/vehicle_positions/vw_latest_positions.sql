-- Latest vehicle position for each vehicle
-- Shows the most recent position record per vehicle_id
SELECT 
  vehicle_id,
  timestamp,
  latitude,
  longitude,
  bearing,
  speed,
  trip_id,
  route_id,
  EXTRACT(DATE FROM timestamp) as position_date,
  ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY timestamp DESC) as rn
FROM `${project_id}.${dataset_id}.rt_vehicle_positions`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
QUALIFY rn = 1