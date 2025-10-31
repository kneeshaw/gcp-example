-- Daily summary of materialized daily schedule data quality and operational metrics
-- Provides executive overview of daily schedule processing system health
SELECT 
  schedule_date as summary_date,
  'daily_schedule' as dataset_name,
  
  -- Data Volume Metrics
  COUNT(*) as total_scheduled_trips,
  COUNT(DISTINCT route_id) as unique_routes_scheduled,
  COUNT(DISTINCT trip_id) as unique_trips_scheduled,
  COUNT(DISTINCT stop_id) as unique_stops_served,
  COUNT(DISTINCT service_id) as unique_services,
  
  -- Service Type Distribution
  COUNT(DISTINCT CASE WHEN route_type = 0 THEN route_id END) as tram_routes,
  COUNT(DISTINCT CASE WHEN route_type = 1 THEN route_id END) as subway_routes,
  COUNT(DISTINCT CASE WHEN route_type = 2 THEN route_id END) as rail_routes,
  COUNT(DISTINCT CASE WHEN route_type = 3 THEN route_id END) as bus_routes,
  COUNT(DISTINCT CASE WHEN route_type = 4 THEN route_id END) as ferry_routes,
  
  -- Temporal Distribution
  COUNTIF(EXTRACT(HOUR FROM departure_time) BETWEEN 6 AND 9) as morning_peak_trips,
  COUNTIF(EXTRACT(HOUR FROM departure_time) BETWEEN 16 AND 19) as evening_peak_trips,
  COUNTIF(EXTRACT(HOUR FROM departure_time) BETWEEN 9 AND 16) as midday_trips,
  COUNTIF(EXTRACT(HOUR FROM departure_time) BETWEEN 19 AND 23) as evening_trips,
  COUNTIF(EXTRACT(HOUR FROM departure_time) BETWEEN 0 AND 6) as overnight_trips,
  
  -- Service Frequency Analysis
  COUNT(*) / NULLIF(COUNT(DISTINCT route_id), 0) as avg_trips_per_route,
  COUNT(*) / NULLIF(COUNT(DISTINCT stop_id), 0) as avg_trips_per_stop,
  COUNT(DISTINCT stop_id) / NULLIF(COUNT(DISTINCT trip_id), 0) as avg_stops_per_trip,
  
  -- Day Type Classification
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM schedule_date) IN (1, 7) THEN 'WEEKEND'
    WHEN EXTRACT(DAYOFWEEK FROM schedule_date) IN (2, 3, 4, 5, 6) THEN 'WEEKDAY'
  END as day_type,
  
  -- Data Quality Metrics
  COUNTIF(departure_time IS NOT NULL) / COUNT(*) as departure_time_completeness,
  COUNTIF(arrival_time IS NOT NULL) / COUNT(*) as arrival_time_completeness,
  COUNTIF(stop_sequence IS NOT NULL) / COUNT(*) as stop_sequence_completeness,
  COUNTIF(route_id IS NOT NULL) / COUNT(*) as route_assignment_completeness,
  
  -- Schedule Integrity Checks
  COUNT(DISTINCT CASE WHEN stop_sequence = 1 THEN trip_id END) as trips_with_first_stop,
  COUNT(DISTINCT trip_id) - COUNT(DISTINCT CASE WHEN stop_sequence = 1 THEN trip_id END) as orphaned_trips,
  
  -- Geographic Coverage
  COUNT(DISTINCT stop_id) / (
    SELECT COUNT(DISTINCT stop_id) 
    FROM `${project_id}.${dataset_id}.sc_stops`
  ) as stop_utilization_ratio,
  
  -- Service Span Analysis
  MIN(departure_time) as earliest_departure,
  MAX(departure_time) as latest_departure,
  TIME_DIFF(MAX(departure_time), MIN(departure_time), HOUR) as service_span_hours,
  
  -- Trip Duration Analysis
  AVG(
    CASE 
      WHEN trip_last_departure IS NOT NULL AND trip_first_departure IS NOT NULL
      THEN TIME_DIFF(trip_last_departure, trip_first_departure, MINUTE)
    END
  ) as avg_trip_duration_minutes,
  
  -- Data Processing Health
  MAX(created_at) as latest_processing_time,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR) as hours_since_last_processing,
  CASE 
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR) <= 2 THEN 'FRESH'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR) <= 24 THEN 'STALE'
    ELSE 'VERY_STALE'
  END as processing_freshness_status,
  
  -- Schedule Density
  COUNT(*) / NULLIF(TIME_DIFF(MAX(departure_time), MIN(departure_time), HOUR), 0) as avg_departures_per_hour,
  
  CURRENT_TIMESTAMP() as calculated_at

FROM `${project_id}.${dataset_id}.daily_schedule`
WHERE schedule_date = CURRENT_DATE('UTC')
GROUP BY summary_date, dataset_name, day_type