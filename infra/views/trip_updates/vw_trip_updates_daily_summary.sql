-- Daily summary of real-time trip update data quality and operational metrics
-- Provides executive overview of trip delay/cancellation tracking system health
SELECT 
  DATE(timestamp) as summary_date,
  'trip_updates' as dataset_name,
  
  -- Data Volume Metrics
  COUNT(*) as total_trip_update_records,
  COUNT(DISTINCT trip_id) as unique_trips_updated,
  COUNT(DISTINCT route_id) as unique_routes_with_updates,
  COUNT(DISTINCT vehicle_id) as unique_vehicles_with_updates,
  
  -- Trip Status Distribution
  COUNT(DISTINCT trip_id) as total_active_trips,
  COUNTIF(schedule_relationship = '0') as scheduled_trips,
  COUNTIF(schedule_relationship = '1') as added_trips,
  COUNTIF(schedule_relationship = '2') as unscheduled_trips,
  COUNTIF(schedule_relationship = '3') as canceled_trips,
  
  -- Delay Analysis
  AVG(delay) as avg_delay_seconds,
  STDDEV(delay) as delay_std_dev,
  MIN(delay) as min_delay_seconds,
  MAX(delay) as max_delay_seconds,
  COUNTIF(delay > 0) as delayed_trips,
  COUNTIF(delay < 0) as early_trips,
  COUNTIF(ABS(delay) <= 300) as on_time_trips,  -- Within 5 minutes
  
  -- On-Time Performance
  COUNTIF(ABS(delay) <= 300) / NULLIF(COUNTIF(delay IS NOT NULL), 0) as on_time_performance,
  COUNTIF(delay > 300) / NULLIF(COUNTIF(delay IS NOT NULL), 0) as late_performance,
  COUNTIF(delay < -300) / NULLIF(COUNTIF(delay IS NOT NULL), 0) as very_early_performance,
  
  -- Severe Delay Analysis
  COUNTIF(delay > 900) as severely_delayed_trips,  -- > 15 minutes
  COUNTIF(delay > 1800) as critically_delayed_trips,  -- > 30 minutes
  
  -- Data Quality Metrics
  COUNTIF(delay IS NOT NULL) / COUNT(*) as delay_data_completeness,
  COUNTIF(trip_id IS NOT NULL) / COUNT(*) as trip_id_completeness,
  COUNTIF(route_id IS NOT NULL) / COUNT(*) as route_id_completeness,
  COUNTIF(vehicle_id IS NOT NULL) / COUNT(*) as vehicle_assignment_rate,
  
  -- Stop-Level Updates
  COUNT(DISTINCT stop_sequence) as unique_stop_sequences,
  AVG(stop_sequence) as avg_stop_sequence,
  MAX(stop_sequence) as max_stop_sequence,
  
  -- Temporal Coverage
  MIN(timestamp) as earliest_update,
  MAX(timestamp) as latest_update,
  TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), HOUR) as coverage_hours,
  COUNT(*) / NULLIF(TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), HOUR), 0) as avg_updates_per_hour,
  
  -- Update Frequency Analysis
  COUNT(*) / NULLIF(COUNT(DISTINCT trip_id), 0) as avg_updates_per_trip,
  
  -- Data Freshness
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(timestamp), MINUTE) as minutes_since_last_update,
  CASE 
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(timestamp), MINUTE) <= 10 THEN 'FRESH'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(timestamp), MINUTE) <= 30 THEN 'STALE'
    ELSE 'VERY_STALE'
  END as data_freshness_status,
  
  -- Service Impact
  COUNTIF(schedule_relationship = '3') / NULLIF(COUNT(DISTINCT trip_id), 0) as cancellation_rate,
  
  -- Anomaly Detection
  COUNTIF(ABS(delay) > 3600) as extreme_delay_anomalies,  -- > 1 hour early/late
  COUNTIF(stop_sequence < 0) as invalid_stop_sequences,
  
  CURRENT_TIMESTAMP() as calculated_at

FROM `${project_id}.${dataset_id}.rt_trip_updates`
WHERE DATE(timestamp) = CURRENT_DATE('UTC')
GROUP BY summary_date, dataset_name