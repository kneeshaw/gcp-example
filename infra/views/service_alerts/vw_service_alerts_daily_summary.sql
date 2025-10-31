-- Daily summary of service alert data quality and operational metrics
-- Provides executive overview of service disruption communication system health
SELECT 
  DATE(period_start) as summary_date,
  'service_alerts' as dataset_name,
  
  -- Data Volume Metrics
  COUNT(*) as total_alert_records,
  COUNT(DISTINCT alert_id) as unique_alerts,
  COUNT(DISTINCT route_id) as routes_with_alerts,
  COUNT(DISTINCT stop_id) as stops_with_alerts,
  
  -- Alert Severity Distribution
  COUNTIF(severity_level = '1') as info_alerts,
  COUNTIF(severity_level = '2') as warning_alerts,
  COUNTIF(severity_level = '3') as severe_alerts,
  COUNTIF(severity_level IS NULL) as unclassified_alerts,
  
  -- Alert Cause Analysis
  COUNTIF(cause = '1') as unknown_cause,
  COUNTIF(cause = '2') as other_cause,
  COUNTIF(cause = '3') as technical_problem,
  COUNTIF(cause = '4') as strike,
  COUNTIF(cause = '5') as demonstration,
  COUNTIF(cause = '6') as accident,
  COUNTIF(cause = '7') as holiday,
  COUNTIF(cause = '8') as weather,
  COUNTIF(cause = '9') as maintenance,
  COUNTIF(cause = '10') as construction,
  COUNTIF(cause = '11') as police_activity,
  COUNTIF(cause = '12') as medical_emergency,
  
  -- Alert Effect Analysis
  COUNTIF(effect = '1') as no_service,
  COUNTIF(effect = '2') as reduced_service,
  COUNTIF(effect = '3') as significant_delays,
  COUNTIF(effect = '4') as detour,
  COUNTIF(effect = '5') as additional_service,
  COUNTIF(effect = '6') as modified_service,
  COUNTIF(effect = '7') as other_effect,
  COUNTIF(effect = '8') as unknown_effect,
  COUNTIF(effect = '9') as stop_moved,
  
  -- Temporal Analysis
  COUNT(DISTINCT CASE 
    WHEN period_start <= CURRENT_TIMESTAMP() 
    AND (period_end IS NULL OR period_end >= CURRENT_TIMESTAMP())
    THEN alert_id 
  END) as currently_active_alerts,
  
  AVG(
    CASE 
      WHEN period_end IS NOT NULL 
      THEN TIMESTAMP_DIFF(period_end, period_start, HOUR)
    END
  ) as avg_alert_duration_hours,
  
  -- Data Quality Metrics
  COUNTIF(header IS NOT NULL AND header != '') / COUNT(*) as header_completeness,
  COUNTIF(description IS NOT NULL AND description != '') / COUNT(*) as description_completeness,
  COUNTIF(url IS NOT NULL AND url != '') / COUNT(*) as url_completeness,
  COUNTIF(cause IS NOT NULL) / COUNT(*) as cause_classification_rate,
  COUNTIF(effect IS NOT NULL) / COUNT(*) as effect_classification_rate,
  
  -- Geographic Coverage
  COUNT(DISTINCT route_id) / (
    SELECT COUNT(DISTINCT route_id) 
    FROM `${project_id}.${dataset_id}.sc_routes`
  ) as route_coverage_ratio,
  
  -- Alert Frequency
  COUNT(DISTINCT alert_id) / 24 as avg_alerts_per_hour,
  
  -- Service Impact Assessment
  COUNTIF(effect IN ('1', '2', '3')) as service_disrupting_alerts,  -- No service, reduced service, significant delays
  COUNTIF(effect IN ('1', '2', '3')) / NULLIF(COUNT(DISTINCT alert_id), 0) as disruption_alert_ratio,
  
  -- Communication Quality
  AVG(LENGTH(header)) as avg_header_length,
  AVG(LENGTH(description)) as avg_description_length,
  
  -- Data Freshness
  MAX(period_start) as latest_alert_start,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(period_start), MINUTE) as minutes_since_last_alert,
  
  -- Critical Alerts (High severity + Service disrupting)
  COUNTIF(severity_level = '3' AND effect IN ('1', '2', '3')) as critical_service_alerts,
  
  CURRENT_TIMESTAMP() as calculated_at

FROM `${project_id}.${dataset_id}.rt_service_alerts`
WHERE DATE(period_start) = CURRENT_DATE('UTC')
GROUP BY summary_date, dataset_name