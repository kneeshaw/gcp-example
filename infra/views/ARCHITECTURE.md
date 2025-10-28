# BigQuery Views Architecture & Development Plan

## Overview

This document outlines our systematic approach to BigQuery view development for transit analytics, based on a clear distinction between single-dataset and multi-dataset analytical views.

## Architecture Principles

### Dataset-Source Distinction

We've identified two fundamental types of analytical views:

1. **Single-Dataset Views**: Focus on one data source (e.g., schedule, vehicle_positions, trip_updates)
2. **Multi-Dataset Views**: Combine multiple data sources for cross-functional analysis

### Naming Conventions

- **Single-Dataset**: `vw_{dataset}_{type}_{grain}_{timeframe}`
- **Multi-Dataset**: `vw_{type}_{grain}_{timeframe}`
- **Summary Reports**: `vw_{scope}_daily_summary`

### Baseline Analytics Pattern

Our baseline concept uses 30-day trailing windows with context dimensions:
- Route, direction, hour, day_of_week as analytical dimensions
- APPROX_QUANTILES for statistical baselines (median, p25, p75)
- Comparison views showing current vs baseline performance

## Current View Inventory

### Analytics (Multi-Dataset)
- `vw_fact_stop_events.sql` - Core fact table combining schedule + real-time
- `vw_fact_trips.sql` - Trip-level facts
- `vw_baseline_route_hour_dow.sql` - Statistical baselines by route/hour/day
- `vw_baseline_route_hourly_compare.sql` - Current vs baseline comparison
- `vw_agg_network_daily.sql` - Network-wide daily metrics
- `vw_agg_network_hourly.sql` - Network-wide hourly metrics
- `vw_agg_route_daily.sql` - Route-level daily aggregations
- `vw_agg_route_hourly.sql` - Route-level hourly aggregations
- `vw_agg_trip_daily.sql` - Trip-level daily aggregations
- `vw_live_planned_vs_actual.sql` - Real-time vs schedule comparison

### Schedule (Single-Dataset)
- `vw_schedule_active_routes.sql` - Currently active routes
- `vw_schedule_route_summary.sql` - Route-level schedule statistics
- `vw_schedule_stop_frequencies.sql` - Stop service frequency analysis

### Vehicle Positions (Single-Dataset)
- `vw_agg_vehicle_hourly.sql` - Vehicle-level hourly aggregations
- `vw_latest_positions.sql` - Most recent vehicle positions

## Planned View Development

### Phase 1: Complete Single-Dataset Coverage

| Dataset | View Name | Purpose | Priority | Status |
|---------|-----------|---------|----------|--------|
| schedule | `vw_schedule_daily_summary` | Daily schedule KPIs | High | Planned |
| schedule | `vw_schedule_route_patterns` | Route pattern analysis | Medium | Planned |
| schedule | `vw_schedule_stop_coverage` | Stop coverage analysis | Low | Planned |
| vehicle_positions | `vw_vehicle_positions_daily_summary` | Daily vehicle KPIs | High | Planned |
| vehicle_positions | `vw_vehicle_utilization` | Vehicle utilization metrics | Medium | Planned |
| vehicle_positions | `vw_vehicle_dwell_analysis` | Dwell time patterns | Low | Planned |
| trip_updates | `vw_trip_updates_daily_summary` | Daily trip update KPIs | High | Planned |
| trip_updates | `vw_trip_delay_patterns` | Delay pattern analysis | Medium | Planned |
| trip_updates | `vw_trip_cancellation_analysis` | Cancellation trends | Medium | Planned |
| service_alerts | `vw_service_alerts_daily_summary` | Daily alert KPIs | High | Planned |
| service_alerts | `vw_alert_impact_analysis` | Alert impact on service | Medium | Planned |

### Phase 2: Enhanced Multi-Dataset Analytics

| Type | View Name | Purpose | Priority | Status |
|------|-----------|---------|----------|--------|
| Agency Summary | `vw_agency_daily_summary` | Executive dashboard metrics | High | Planned |
| Baseline | `vw_baseline_stop_dwell` | Stop dwell time baselines | Medium | Planned |
| Baseline | `vw_baseline_route_speed` | Route speed baselines | Medium | Planned |
| Performance | `vw_punctuality_analysis` | On-time performance trends | High | Planned |
| Performance | `vw_service_reliability` | Service reliability metrics | High | Planned |
| Operational | `vw_peak_hour_analysis` | Peak period performance | Medium | Planned |
| Operational | `vw_corridor_analysis` | Major corridor performance | Medium | Planned |
| Passenger | `vw_passenger_impact` | Passenger experience metrics | Low | Planned |

### Phase 3: Advanced Analytics

| Type | View Name | Purpose | Priority | Status |
|------|-----------|---------|----------|--------|
| Predictive | `vw_delay_prediction_features` | ML feature engineering | Low | Future |
| Network | `vw_network_resilience` | System resilience metrics | Low | Future |
| Optimization | `vw_schedule_optimization_candidates` | Schedule improvement opportunities | Low | Future |
| Benchmarking | `vw_peer_comparison` | Cross-agency benchmarking | Low | Future |

## Summary Report Structure

### Dataset-Level Summaries
Each single-dataset folder will contain a daily summary view with standardized metrics:

- **Data Quality**: Record counts, completeness, freshness
- **Operational KPIs**: Dataset-specific performance indicators
- **Trend Analysis**: Day-over-day, week-over-week comparisons
- **Exception Alerts**: Anomaly detection and threshold violations

### Agency-Level Summary
The multi-dataset analytics folder will contain an agency summary combining:

- **Service Delivery**: On-time performance, service completion
- **Fleet Utilization**: Vehicle productivity, maintenance efficiency
- **Passenger Experience**: Journey time reliability, service frequency
- **Operational Efficiency**: Cost per service hour, resource optimization

## Development Guidelines

### View Dependencies
1. Fact tables first (foundational data models)
2. Baseline views second (statistical references)
3. Comparison views third (performance analysis)
4. Summary views last (reporting layer)

### Performance Considerations
- Use appropriate partitioning (date-based for time series)
- Implement incremental refresh where possible
- Consider materialized views for frequently accessed aggregations
- Monitor query costs and optimize for BigQuery pricing

### Data Quality Standards
- Include data freshness checks in all views
- Implement null handling strategies
- Add data validation assertions
- Document assumptions and limitations

## Implementation Strategy

### Immediate Next Steps
1. Complete Phase 1 dataset summaries for immediate operational visibility
2. Implement agency daily summary for executive reporting
3. Establish baseline views for performance benchmarking
4. Create standardized view templates for consistency

### Long-term Vision
- Automated view deployment through Terraform
- Integration with monitoring and alerting systems
- Real-time dashboard integration
- Machine learning feature stores for predictive analytics

## Success Metrics

### Technical Success
- View query performance < 10 seconds for dashboard views
- Data freshness within 15 minutes for real-time views
- 99.9% view availability during business hours

### Business Success
- Improved operational decision-making speed
- Reduced manual reporting effort by 80%
- Enhanced service quality through data-driven insights
- Proactive issue identification and resolution

---

*This document serves as the living architecture guide for our BigQuery analytics platform. It will be updated as we implement and refine our view development approach.*