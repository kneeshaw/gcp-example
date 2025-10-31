-- Agency-wide daily summary combining all datasets for executive dashboard
-- Provides comprehensive operational overview across schedule, real-time, and alerts
WITH schedule_metrics AS (
  SELECT 
    CURRENT_DATE('UTC') as summary_date,
    COUNT(DISTINCT r.route_id) as total_routes,
    COUNT(DISTINCT t.trip_id) as total_scheduled_trips,
    COUNT(DISTINCT s.stop_id) as total_stops,
    COUNT(DISTINCT CASE WHEN r.route_type = 3 THEN r.route_id END) as bus_routes,
    COUNT(DISTINCT CASE WHEN r.route_type = 2 THEN r.route_id END) as rail_routes,
    COUNT(DISTINCT CASE WHEN r.route_type = 4 THEN r.route_id END) as ferry_routes
  FROM `${project_id}.${dataset_id}.sc_routes` r
  LEFT JOIN `${project_id}.${dataset_id}.sc_trips` t ON r.route_id = t.route_id
  LEFT JOIN `${project_id}.${dataset_id}.sc_stop_times` st ON t.trip_id = st.trip_id
  LEFT JOIN `${project_id}.${dataset_id}.sc_stops` s ON st.stop_id = s.stop_id
),

realtime_metrics AS (
  SELECT 
    DATE(timestamp) as summary_date,
    COUNT(DISTINCT vehicle_id) as active_vehicles,
    COUNT(DISTINCT trip_id) as trips_with_vehicles,
    COUNT(*) as total_position_updates,
    AVG(speed) as avg_fleet_speed,
    0 as vehicles_at_stops,  -- Field not available in current schema
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(timestamp), MINUTE) as minutes_since_last_position
  FROM `${project_id}.${dataset_id}.rt_vehicle_positions`
  WHERE DATE(timestamp) = CURRENT_DATE('UTC')
  GROUP BY DATE(timestamp)
),

trip_performance AS (
  SELECT 
    DATE(timestamp) as summary_date,
    COUNT(DISTINCT trip_id) as monitored_trips,
    AVG(delay) as avg_delay_seconds,
    COUNTIF(ABS(delay) <= 300) / NULLIF(COUNTIF(delay IS NOT NULL), 0) as on_time_performance,
    COUNTIF(delay > 300) / NULLIF(COUNTIF(delay IS NOT NULL), 0) as late_performance,
    COUNTIF(schedule_relationship = '3') as cancelled_trips,
    COUNTIF(schedule_relationship = '3') / NULLIF(COUNT(DISTINCT trip_id), 0) as cancellation_rate,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(timestamp), MINUTE) as minutes_since_last_update
  FROM `${project_id}.${dataset_id}.rt_trip_updates`
  WHERE DATE(timestamp) = CURRENT_DATE('UTC')
  GROUP BY DATE(timestamp)
),

service_disruptions AS (
  SELECT 
    DATE(period_start) as summary_date,
    COUNT(DISTINCT alert_id) as total_alerts_today,
    COUNT(DISTINCT CASE 
      WHEN period_start <= CURRENT_TIMESTAMP() 
      AND (period_end IS NULL OR period_end >= CURRENT_TIMESTAMP())
      THEN alert_id 
    END) as active_alerts,
    COUNTIF(severity_level = '3') as severe_alerts,
    COUNTIF(effect IN ('1', '2', '3')) as service_disrupting_alerts,
    COUNT(DISTINCT route_id) as routes_with_alerts
  FROM `${project_id}.${dataset_id}.rt_service_alerts`
  WHERE DATE(period_start) = CURRENT_DATE('UTC')
  GROUP BY DATE(period_start)
)

-- daily_schedule_health AS (
--   SELECT 
--     schedule_date as summary_date,
--     COUNT(*) as processed_trips_today,
--     COUNT(DISTINCT route_id) as routes_in_service,
--     COUNT(DISTINCT stop_id) as stops_in_service,
--     MAX(created_at) as latest_schedule_processing,
--     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR) as hours_since_processing
--   FROM `${project_id}.${dataset_id}.daily_schedule`
--   WHERE schedule_date = CURRENT_DATE('UTC')
-- )

SELECT 
  CURRENT_DATE('UTC') as summary_date,
  'agency_operations' as summary_type,
  
  -- === SERVICE DELIVERY METRICS ===
  -- On-Time Performance
  COALESCE(tp.on_time_performance, 0) as on_time_performance_pct,
  COALESCE(tp.late_performance, 0) as late_performance_pct,
  COALESCE(tp.avg_delay_seconds, 0) as avg_delay_seconds,
  
  -- Service Completion
  COALESCE(tp.cancellation_rate, 0) as cancellation_rate_pct,
  COALESCE(tp.cancelled_trips, 0) as cancelled_trips_count,
  COALESCE(tp.monitored_trips, 0) as monitored_trips_count,
  
  -- Service Reliability Score (composite metric)
  CASE 
    WHEN tp.on_time_performance IS NULL THEN NULL
    ELSE (tp.on_time_performance * 0.6) + ((1 - COALESCE(tp.cancellation_rate, 0)) * 0.4)
  END as service_reliability_score,
  
  -- === FLEET UTILIZATION METRICS ===
  -- Vehicle Deployment
  COALESCE(rm.active_vehicles, 0) as active_vehicles_count,
  COALESCE(rm.trips_with_vehicles, 0) as trips_with_vehicles_count,
  COALESCE(rm.trips_with_vehicles, 0) / NULLIF(COALESCE(sm.total_scheduled_trips, 1), 0) as vehicle_assignment_rate,
  
  -- Fleet Activity
  COALESCE(rm.avg_fleet_speed, 0) as avg_fleet_speed_mps,
  COALESCE(rm.vehicles_at_stops, 0) as vehicles_currently_at_stops,
  COALESCE(rm.total_position_updates, 0) as position_updates_today,
  
  -- === NETWORK COVERAGE METRICS ===
  -- Infrastructure
  COALESCE(sm.total_routes, 0) as total_routes_count,
  COALESCE(sm.total_stops, 0) as total_stops_count,
  COALESCE(sm.total_routes, 0) as routes_in_service_today,
  0 as stops_in_service_today,  -- Will be available when daily_schedule table exists
  
  -- Service Mode Distribution
  COALESCE(sm.bus_routes, 0) as bus_routes_count,
  COALESCE(sm.rail_routes, 0) as rail_routes_count,
  COALESCE(sm.ferry_routes, 0) as ferry_routes_count,
  
  -- === SERVICE DISRUPTION METRICS ===
  -- Alert Volume
  COALESCE(sd.total_alerts_today, 0) as alerts_issued_today,
  COALESCE(sd.active_alerts, 0) as alerts_currently_active,
  COALESCE(sd.severe_alerts, 0) as severe_alerts_today,
  COALESCE(sd.service_disrupting_alerts, 0) as disrupting_alerts_today,
  
  -- Impact Scope
  COALESCE(sd.routes_with_alerts, 0) as routes_affected_by_alerts,
  COALESCE(sd.routes_with_alerts, 0) / NULLIF(COALESCE(sm.total_routes, 1), 0) as network_disruption_ratio,
  
  -- === DATA HEALTH METRICS ===
  -- Real-time Data Freshness
  COALESCE(rm.minutes_since_last_position, 999) as minutes_since_last_vehicle_position,
  COALESCE(tp.minutes_since_last_update, 999) as minutes_since_last_trip_update,
  
  -- Data Quality Status
  CASE 
    WHEN rm.minutes_since_last_position <= 15 AND tp.minutes_since_last_update <= 10 THEN 'EXCELLENT'
    WHEN rm.minutes_since_last_position <= 30 AND tp.minutes_since_last_update <= 30 THEN 'GOOD'
    WHEN rm.minutes_since_last_position <= 60 AND tp.minutes_since_last_update <= 60 THEN 'FAIR'
    ELSE 'POOR'
  END as realtime_data_health_status,
  
  -- Schedule Processing Health
  999 as hours_since_schedule_processing,  -- Will be available when daily_schedule table exists
  CASE 
    WHEN 999 <= 2 THEN 'FRESH'
    WHEN 999 <= 24 THEN 'STALE'
    ELSE 'VERY_STALE'
  END as schedule_processing_status,
  
  -- === OPERATIONAL EFFICIENCY METRICS ===
  -- Service Density
  COALESCE(sm.total_scheduled_trips, 0) as planned_trips_today,
  COALESCE(sm.total_scheduled_trips, 0) / NULLIF(COALESCE(sm.total_routes, 1), 0) as avg_trips_per_route,
  
  -- System Utilization
  COALESCE(rm.active_vehicles, 0) / NULLIF(COALESCE(sm.total_scheduled_trips, 1), 0) as vehicle_to_trip_ratio,
  
  -- === PASSENGER EXPERIENCE INDICATORS ===
  -- Service Predictability
  CASE 
    WHEN tp.on_time_performance >= 0.85 AND tp.cancellation_rate <= 0.02 THEN 'EXCELLENT'
    WHEN tp.on_time_performance >= 0.75 AND tp.cancellation_rate <= 0.05 THEN 'GOOD'
    WHEN tp.on_time_performance >= 0.65 AND tp.cancellation_rate <= 0.10 THEN 'FAIR'
    ELSE 'POOR'
  END as passenger_experience_rating,
  
  -- Information Quality
  CASE 
    WHEN sd.active_alerts = 0 THEN 'NO_DISRUPTIONS'
    WHEN sd.service_disrupting_alerts = 0 THEN 'MINOR_DISRUPTIONS'
    WHEN sd.service_disrupting_alerts <= 3 THEN 'MODERATE_DISRUPTIONS'
    ELSE 'MAJOR_DISRUPTIONS'
  END as service_disruption_level,
  
  -- === METADATA ===
  CURRENT_TIMESTAMP() as calculated_at,
  'v1.0' as summary_version

FROM schedule_metrics sm
FULL OUTER JOIN realtime_metrics rm ON sm.summary_date = rm.summary_date
FULL OUTER JOIN trip_performance tp ON COALESCE(sm.summary_date, rm.summary_date) = tp.summary_date
FULL OUTER JOIN service_disruptions sd ON COALESCE(sm.summary_date, rm.summary_date, tp.summary_date) = sd.summary_date
-- FULL OUTER JOIN daily_schedule_health dsh ON COALESCE(sm.summary_date, rm.summary_date, tp.summary_date, sd.summary_date) = dsh.summary_date