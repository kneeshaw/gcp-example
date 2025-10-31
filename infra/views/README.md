# BigQuery Views

This directory contains SQL definitions for BigQuery views organized by dataset. Views are automatically created by Terraform using logical function-based naming for better UI grouping.

## View Naming Convention

Views use the `vw_` prefix with logical function names for optimal BigQuery UI organization:

- **Aggregation views**: `vw_agg_*` - All aggregation views group together
- **Baseline views**: `vw_baseline_*` - Historical baseline and comparison views  
- **Fact views**: `vw_fact_*` - Core analytical fact tables
- **Latest/Live views**: `vw_latest_*`, `vw_live_*` - Real-time and current state views
- **Schedule views**: `vw_schedule_*` - GTFS schedule-related views

## Generated View Names

The following views will be created in BigQuery (grouped by function):

### 📊 Aggregation Views

- `vw_agg_network_daily` - Daily network-wide performance aggregates
- `vw_agg_network_hourly` - Hourly network-wide performance aggregates  
- `vw_agg_route_daily` - Daily route-level performance aggregates
- `vw_agg_route_hourly` - Hourly route-level performance aggregates
- `vw_agg_trip_daily` - Daily trip-level performance aggregates
- `vw_agg_vehicle_hourly` - Hourly vehicle position aggregates

### 📈 Baseline Views

- `vw_baseline_route_hour_dow` - Historical baselines for route performance
- `vw_baseline_route_hourly_compare` - Current vs baseline performance comparison

### 🔢 Fact Views

- `vw_fact_stop_events` - Stop-level performance metrics with time features
- `vw_fact_trips` - Trip-level KPIs and performance summary

### ⚡ Latest/Live Views

- `vw_latest_positions` - Most recent position per vehicle
- `vw_live_planned_vs_actual` - Real-time comparison of scheduled vs actual times

### 📅 Schedule Views

- `vw_schedule_active_routes` - Routes with current service
- `vw_schedule_route_summary` - Trip/stop statistics per route  
- `vw_schedule_stop_frequencies` - Service frequency by stop/hour

## Directory Structure

```text
views/
├── vehicle_positions/           # Real-time vehicle position analytics
│   ├── vw_latest_positions.sql     # Most recent position per vehicle
│   └── vw_agg_vehicle_hourly.sql   # Hourly vehicle position aggregates
├── schedule/                    # GTFS schedule data analytics
│   ├── vw_schedule_active_routes.sql    # Routes with current service
│   ├── vw_schedule_route_summary.sql    # Trip/stop statistics per route
│   └── vw_schedule_stop_frequencies.sql # Service frequency by stop/hour
└── analytics/                   # Advanced performance analytics
    ├── vw_fact_stop_events.sql     # Stop-level performance metrics with time features
    ├── vw_fact_trips.sql           # Trip-level KPIs and performance summary
    ├── vw_live_planned_vs_actual.sql    # Real-time comparison of scheduled vs actual times
    ├── vw_agg_network_daily.sql    # Daily network-wide performance aggregates
    ├── vw_agg_network_hourly.sql   # Hourly network-wide performance aggregates
    ├── vw_agg_route_daily.sql      # Daily route-level performance aggregates
    ├── vw_agg_route_hourly.sql     # Hourly route-level performance aggregates
    ├── vw_agg_trip_daily.sql       # Daily trip-level performance aggregates
    ├── vw_baseline_route_hour_dow.sql       # Historical baselines for route performance
    └── vw_baseline_route_hourly_compare.sql # Current vs baseline performance comparison
```

## Adding a New View

### 1. Create the SQL File

Create a new `.sql` file in the appropriate dataset folder with the `vw_` prefix:

```bash
# For vehicle positions dataset
touch infra/views/vehicle_positions/vw_my_new_view.sql

# For schedule dataset  
touch infra/views/schedule/vw_my_new_view.sql

# For a new dataset
mkdir infra/views/my_dataset/
touch infra/views/my_dataset/my_view.sql
```

### 2. Write the SQL Query

Write your BigQuery SQL in the file. Use the fully qualified table names:

```sql
-- Example: infra/views/vehicle_positions/delay_analysis.sql
-- Vehicle delay analysis comparing scheduled vs actual
SELECT 
  route_id,
  vehicle_id,
  AVG(delay_minutes) as avg_delay,
  COUNT(*) as position_count
FROM `regal-dynamo-470908-v9.auckland_data_dev.rt_vehicle_positions` vp
JOIN `regal-dynamo-470908-v9.auckland_data_dev.sc_trips` t 
  ON vp.trip_id = t.trip_id
WHERE DATE(vp.timestamp) = CURRENT_DATE()
GROUP BY route_id, vehicle_id
ORDER BY avg_delay DESC
```

### 3. Register in Dataset Catalog

Add the view to the appropriate dataset in `infra/modules/dataset_catalog/main.tf`:

```hcl
"vehicle-positions" = {
  spec          = "rt"
  response_type = "json"
  tables = {
    main = { schema_file = "${var.schemas_root}/vehicle_positions/rt_vehicle_positions.schema.json" }
  }
  views = {
    latest_positions = { sql_file = "${var.schemas_root}/../views/vehicle_positions/latest_positions.sql" }
    hourly_summary   = { sql_file = "${var.schemas_root}/../views/vehicle_positions/hourly_summary.sql" }
    delay_analysis   = { sql_file = "${var.schemas_root}/../views/vehicle_positions/delay_analysis.sql" }  # ← ADD THIS
  }
}
```

### 4. Deploy with Terraform

Run terraform to create the view:

```bash
cd infra/agencies/auckland/dev
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

### 5. Query the View

The view will be created with the naming pattern `v_{dataset}_{view_name}`:

```sql
-- Query your new view
SELECT * FROM `regal-dynamo-470908-v9.auckland_data_dev.v_vehicle_positions_delay_analysis`
LIMIT 10;
```

## View Naming Conventions

- **SQL Files**: Use descriptive names with underscores: `latest_positions.sql`, `hourly_summary.sql`
- **View Names**: Auto-generated as `v_{dataset}_{view_name}` (e.g., `v_vehicle_positions_latest_positions`)
- **Folder Names**: Use underscores to match dataset folders in `tables/`: `vehicle_positions/`, `schedule/`

## Best Practices

### SQL Guidelines

1. **Use Fully Qualified Names**: Always include project and dataset in table references
2. **Add Comments**: Document complex logic and business rules
3. **Optimize Performance**: Use appropriate WHERE clauses and avoid SELECT *
4. **Handle Nulls**: Consider NULL values in joins and aggregations

### View Design

1. **Single Purpose**: Each view should serve a specific analytical need
2. **Reusable**: Design views that can be used by multiple consumers
3. **Documented**: Include description comments at the top of SQL files
4. **Tested**: Verify view logic with sample queries before deployment

### Example Template

```sql
-- View Name: Route Performance Summary
-- Purpose: Daily performance metrics by route including on-time rates
-- Dependencies: sc_routes, rt_vehicle_positions, sc_trips
-- Last Updated: 2025-10-28

SELECT 
  r.route_id,
  r.route_short_name,
  DATE(vp.timestamp) as service_date,
  COUNT(DISTINCT vp.vehicle_id) as vehicles_operated,
  COUNT(*) as total_positions,
  -- source speed is already km/h
  AVG(vp.speed) as avg_speed_kmh,
  -- Add your metrics here
FROM `regal-dynamo-470908-v9.auckland_data_dev.sc_routes` r
JOIN `regal-dynamo-470908-v9.auckland_data_dev.sc_trips` t ON r.route_id = t.route_id  
JOIN `regal-dynamo-470908-v9.auckland_data_dev.rt_vehicle_positions` vp ON t.trip_id = vp.trip_id
WHERE DATE(vp.timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAYS)
GROUP BY r.route_id, r.route_short_name, service_date
ORDER BY service_date DESC, r.route_short_name
```

## Troubleshooting

- **File Not Found**: Ensure the SQL file path in the catalog matches the actual file location
- **SQL Errors**: Test your SQL directly in BigQuery Console before adding to catalog
- **Permission Errors**: Views inherit the same permissions as the underlying tables
- **Performance Issues**: Consider materializing frequently-used complex views as tables instead

## Dataset Descriptions

### vehicle_positions/

Real-time vehicle tracking data analytics. Views provide insights into vehicle activity patterns, fleet utilization, and operational coverage.

### schedule/

GTFS static schedule data analytics. Views analyze route structures, service patterns, and scheduled operations.

### analytics/

Advanced transit performance analytics combining real-time and scheduled data. These views derive key performance indicators (KPIs), on-time performance metrics, and operational insights by comparing planned vs actual service delivery.

## Agency-Specific Views

To add views specific to an agency (not shared), add them directly in the agency's `dev.tfvars`:

```hcl
datasets = {
  "vehicle-positions" = {
    views = {
      auckland_specific_view = { sql_file = "path/to/auckland/specific/view.sql" }
    }
  }
}
```

These will be merged with the shared catalog views automatically.
