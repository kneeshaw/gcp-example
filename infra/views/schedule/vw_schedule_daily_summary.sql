-- Daily summary of GTFS schedule data quality and operational metrics
-- Provides executive overview of static schedule data health
SELECT 
  CURRENT_DATE('UTC') as summary_date,
  'schedule' as dataset_name,
  
  -- Data Quality Metrics
  COUNT(DISTINCT r.route_id) as total_routes,
  COUNT(DISTINCT t.trip_id) as total_trips,
  COUNT(DISTINCT s.stop_id) as total_stops,
  COUNT(DISTINCT st.stop_id) as active_stops,
  COUNT(st.stop_sequence) as total_stop_times,
  
  -- Service Coverage Metrics
  COUNT(DISTINCT CASE WHEN r.route_type = 0 THEN r.route_id END) as tram_routes,
  COUNT(DISTINCT CASE WHEN r.route_type = 1 THEN r.route_id END) as subway_routes,
  COUNT(DISTINCT CASE WHEN r.route_type = 2 THEN r.route_id END) as rail_routes,
  COUNT(DISTINCT CASE WHEN r.route_type = 3 THEN r.route_id END) as bus_routes,
  COUNT(DISTINCT CASE WHEN r.route_type = 4 THEN r.route_id END) as ferry_routes,
  
  -- Service Frequency Analysis
  COUNT(DISTINCT t.trip_id) / NULLIF(COUNT(DISTINCT r.route_id), 0) as avg_trips_per_route,
  COUNT(st.stop_sequence) / NULLIF(COUNT(DISTINCT t.trip_id), 0) as avg_stops_per_trip,
  
  -- Data Completeness Checks
  COUNTIF(r.route_short_name IS NOT NULL) / NULLIF(COUNT(r.route_id), 0) as route_name_completeness,
  COUNTIF(s.stop_name IS NOT NULL) / NULLIF(COUNT(s.stop_id), 0) as stop_name_completeness,
  COUNTIF(st.arrival_time IS NOT NULL) / NULLIF(COUNT(st.stop_sequence), 0) as arrival_time_completeness,
  COUNTIF(st.departure_time IS NOT NULL) / NULLIF(COUNT(st.stop_sequence), 0) as departure_time_completeness,
  
  -- Geographic Coverage
  COUNT(DISTINCT CASE WHEN s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL THEN s.stop_id END) as geo_located_stops,
  COUNTIF(s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL) / NULLIF(COUNT(s.stop_id), 0) as geo_completeness,
  
  -- Operational Complexity
  MAX(st.stop_sequence) as max_stops_on_trip,
  AVG(
    CASE 
      WHEN st.departure_time IS NOT NULL AND st.arrival_time IS NOT NULL
      THEN TIME_DIFF(
        PARSE_TIME('%H:%M:%S', st.departure_time),
        PARSE_TIME('%H:%M:%S', st.arrival_time),
        SECOND
      )
    END
  ) as avg_stop_duration_seconds,
  
  -- Data Freshness
  MAX(COALESCE(fi.feed_version, 'unknown')) as feed_version,
  CURRENT_TIMESTAMP() as calculated_at

FROM `${project_id}.${dataset_id}.sc_routes` r
LEFT JOIN `${project_id}.${dataset_id}.sc_trips` t ON r.route_id = t.route_id
LEFT JOIN `${project_id}.${dataset_id}.sc_stop_times` st ON t.trip_id = st.trip_id
LEFT JOIN `${project_id}.${dataset_id}.sc_stops` s ON st.stop_id = s.stop_id
LEFT JOIN `${project_id}.${dataset_id}.sc_feed_info` fi ON TRUE

-- Group all metrics into a single summary row
GROUP BY summary_date, dataset_name