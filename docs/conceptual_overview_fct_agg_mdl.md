# Conceptual Overview: Facts, Aggregates, and MDL Layer (with Reusable Metrics)
Author: MAK Group NZ  
Status: Draft v1.0

---

## 0) Purpose
This document explains **how our vehicle telemetry warehouse is organised** across **facts (FCT)**, **aggregates (AGG)**, and **model layer (MDL)**. It also shows **how to build reusable metrics** (e.g., rolling averages) so we can apply the same logic to speed, untracked movements, dwell, headway, etc.

---

## 1) Layered Architecture (at a glance)

```
          ┌────────────────────────────────────────────────────────────┐
          │                  STAGING (raw → typed)                     │
          │  stg_vehicle_position                                      │
          └───────────────┬────────────────────────────────────────────┘
                          │  (enrich / attribute at point grain)
                          ▼
          ┌────────────────────────────────────────────────────────────┐
          │                       FACTS (FCT)                          │
          │  fct_vehicle_position   → point-level fact (attributed)    │
          │  fct_vehicle_segment    → segment-level fact (point→point) │
          └───────────────┬────────────────────────────────────────────┘
                          │  (vehicle-first rollups within an entity)
                          ▼
          ┌────────────────────────────────────────────────────────────┐
          │            VEHICLE×ENTITY×TIME (intermediate)              │
          │  veh_loc_hour    → (vehicle,h3,hour)                       │
          │  veh_route_hour  → (vehicle,route,dir,hour)                │
          │  veh_only_hour   → (vehicle,hour) [optional]               │
          └───────────────┬────────────────────────────────────────────┘
                          │  (cross-vehicle aggregation / serving)
                          ▼
          ┌────────────────────────────────────────────────────────────┐
          │                   AGGREGATES (AGG)                         │
          │  agg_location_hour / day                                   │
          │  agg_route_hour    / day                                   │
          └───────────────┬────────────────────────────────────────────┘
                          │  (analytics indicators, trends, alerts)
                          ▼
          ┌────────────────────────────────────────────────────────────┐
          │                      MODEL LAYER (MDL)                     │
          │  mdl_*  (rolling windows, z-scores, seasonality, alerts)   │
          └────────────────────────────────────────────────────────────┘
```

**Key principle:** Attribute entities at the **finest grain** (point), create **segments** for physics-based metrics, then **roll vehicle-first within each entity** (H3 / route), and only then aggregate across vehicles. MDL adds **derived indicators** without changing grains.

---

## 2) Facts (FCT)

### 2.1 `fct_vehicle_position` (point-level, attributed)
**Grain:** one row per update.  
**What it adds (from staging):** `service_date`, `hour`, `geog`, `h3_9`, `trip_id`, `route_id`, `direction_id`, basic quality flags.

**Use cases:** fast filters, map dots, simple counts, input to segments.

**Partition / cluster:** `PARTITION BY DATE(ts)`; cluster `vehicle_id, ts`.

---

### 2.2 `fct_vehicle_segment` (segment-level, physics-based)
**Grain:** one row per `point → next point` per vehicle (using `LEAD()`).  
**Fields:** `ts_start`, `ts_end`, `time_s`, `distance_m`, `speed_kmh_segment`, `h3_start`, `route_id`, `direction_id`, `service_date_start`, `hour_start`, outlier flags (kept, not filtered).

**Why:** correct speed = distance / time (weighted), avoids message-rate bias.

**Partition / cluster:** `PARTITION BY service_date_start`; cluster `vehicle_id, route_id, h3_start`.

---

## 3) Vehicle×Entity×Time (Intermediate Rollups)

These remove message-rate bias by producing **one row per vehicle per entity per hour**.

### 3.1 `veh_loc_hour` (vehicle × H3 × hour)
- `distance_m`, `time_s`, `avg_speed_kmh_vehicle_hour`, `segment_count`, `outlier_segment_count`.

### 3.2 `veh_route_hour` (vehicle × route × direction × hour)
- Same structure; restricted to rows with valid `route_id`.

### 3.3 `veh_only_hour` (vehicle × hour) [optional]
- Useful for QA, reliability, availability metrics.

All three support accurate weighting in later aggregates:
```
avg_speed_kmh = 3.6 * SUM(distance_m) / NULLIF(SUM(time_s),0)
```

---

## 4) Serving Aggregates (AGG)

### 4.1 `agg_location_hour` / `agg_location_day`
**Keys:** `(h3, service_date, hour)` → hour; `(h3, service_date)` → day.  
**Metrics:** vehicle_count, total_distance_m, total_time_s, avg_speed_kmh.  
**Rule:** Always recompute day from hour by summing distance/time, then speed.

### 4.2 `agg_route_hour` / `agg_route_day`
**Keys:** `(route_id, direction_id, service_date, hour)`; day version similar.  
**Metrics:** as above, plus route reliability KPIs as needed.

**Inclusive vs Clean views:** maintain `_inc` (all segments) and `_clean` (thresholded by `outlier_ratio`) as **views** so consumers can choose.

---

## 5) MDL Layer (Indicators, Trends, Alerts)

The **mdl_** models **do not change grain**; they add **derived metrics** on top of AGG (or VEH×ENTITY×TIME) tables.

### 5.1 Typical MDL files
- `mdl_speed_h3_hourly.sql` → rolling averages / pct diffs on `agg_location_hour`
- `mdl_speed_route_hourly.sql` → same for routes
- `mdl_untracked_route_hourly.sql` → indicators for untracked movements
- `mdl_congestion_h3_hourly.sql` → z-score, Bollinger-like bands on speed

### 5.2 Reusable indicators via dbt macros (recommended)
`macros/indicators/rolling_avg.sql`
```jinja
{% macro rolling_avg(measure, partition_cols, order_col,
                     window_type='RANGE', window_size=27, window_unit='DAY') %}
AVG({{ measure }}) OVER (
  PARTITION BY {{ partition_cols | join(', ') }}
  ORDER BY {{ order_col }}
  {%- if window_type | upper == 'ROWS' -%}
    ROWS BETWEEN {{ window_size }} PRECEDING AND CURRENT ROW
  {%- else -%}
    RANGE BETWEEN INTERVAL {{ window_size }} {{ window_unit }} PRECEDING AND CURRENT ROW
  {%- endif -%}
)
{% endmacro %}
```

`macros/indicators/pct_change.sql`
```jinja
{% macro pct_change(current_expr, baseline_expr) %}
SAFE_DIVIDE( ({{ current_expr }}) - ({{ baseline_expr }}),
             ({{ baseline_expr }}) )
{% endmacro %}
```

`macros/indicators/rolling_stddev.sql`
```jinja
{% macro rolling_stddev(measure, partition_cols, order_col,
                        window_type='RANGE', window_size=27, window_unit='DAY') %}
STDDEV_SAMP({{ measure }}) OVER (
  PARTITION BY {{ partition_cols | join(', ') }}
  ORDER BY {{ order_col }}
  {%- if window_type | upper == 'ROWS' -%}
    ROWS BETWEEN {{ window_size }} PRECEDING AND CURRENT ROW
  {%- else -%}
    RANGE BETWEEN INTERVAL {{ window_size }} {{ window_unit }} PRECEDING AND CURRENT ROW
  {%- endif -%}
)
{% endmacro %}
```

**Example use in `mdl_speed_h3_hourly.sql`:**
```sql
SELECT
  service_date,
  hour,
  h3,
  avg_speed_kmh,
  {{ rolling_avg('avg_speed_kmh', ['h3','hour'], 'service_date') }} AS ra_28d,
  {{ pct_change('avg_speed_kmh', 'ra_28d') }} AS pct_vs_28d,
  {{ rolling_stddev('avg_speed_kmh', ['h3','hour'], 'service_date') }} AS sd_28d
FROM {{ ref('agg_location_hour') }};
```

> Swap the `measure` to reuse for **untracked movements**, **dwell**, **headway gaps**, etc. The grain stays the same; only the metric differs.

### 5.3 Optional: BigQuery Table Functions (TVFs)
Offer pure-SQL reuse (fixed column names) for analysts and APIs:
- `analytics.ra_28d_entity_hour(t ANY TYPE)` where `t` has `(entity_id, hour_key, metric)`.

---

## 6) Naming, Grain, and Contracts

- `fct_*` → physical facts (point or segment). **Define the grain in the name/comment.**
- `veh_*_hour` → vehicle-first rollups within an entity; **must** have `distance_m`, `time_s`.
- `agg_*` → cross-vehicle serving tables; day derived from hour.
- `mdl_*` → derived indicators at the same grain as input aggregates.
- **Never average across raw messages for speeds.** Always weight by physics (distance/time).

---

## 7) Partitioning & Clustering (summary)
- Facts: partition by time (`DATE(ts)` / `service_date_start`); cluster by access keys.  
- VEH×ENTITY×TIME: partition by `service_date`; cluster by entity keys.  
- AGG: partition by `service_date`; cluster by entity keys.  
- MDL: often **views**; materialize if heavy.

---

## 8) Data Quality & Outliers
- Keep **all segments**; add flags (`flag_gap_outlier`, `flag_speed_outlier`, etc.).  
- Provide **_inc** (inclusive) and **_clean** (thresholded) views at AGG level.  
- Expose **outlier ratios** so dashboards can annotate or filter.

---

## 9) Example dbt Structure

```
dbt/
  models/
    stg/
      stg_vehicle_position.sql
    fact/
      fct_vehicle_position.sql
      fct_vehicle_segment.sql
    veh/
      veh_loc_hour.sql
      veh_route_hour.sql
    agg/
      agg_location_hour.sql
      agg_location_day.sql
      agg_route_hour.sql
      agg_route_day.sql
    mdl/
      mdl_speed_h3_hourly.sql
      mdl_speed_route_hourly.sql
      mdl_untracked_route_hourly.sql
  macros/
    indicators/
      rolling_avg.sql
      pct_change.sql
      rolling_stddev.sql
    windows/
      last_n_rows.sql
      last_n_days.sql
```

---

## 10) Rollout Plan (pragmatic)

1. **Stabilise FCTs** (`fct_vehicle_position`, `fct_vehicle_segment`).  
2. Build **veh_*_hour** tables (vehicle-first within entity).  
3. Build **agg_* (hour)**, then derive **day**.  
4. Add **mdl_* indicators** using macros.  
5. Provide both **inclusive** and **clean** views.  
6. Wire **Plotly/deck.gl** to AGG / MDL outputs; keep SQL server-side.

---

## 11) Takeaways
- Attribute early, segment once, **vehicle-first** rollups, then aggregate.  
- MDL adds trading-style indicators **without changing grain**.  
- Reuse comes from **dbt macros** (and optional TVFs), not from duplicating models.  
- Outliers are **kept** with flags; consumers choose filters.

