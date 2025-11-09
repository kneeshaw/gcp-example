# Specification: `stg_vehicle_position`  

### GTFS Analytics – Staging Layer Design  

Author: MAK Group NZ  
Status: Draft  
Version: 1.0  

---

## 1. Purpose

`stg_vehicle_position` is the **clean ingest staging table** for raw AVL/RT vehicle telemetry.  
Its role is to take raw feed updates (JSON, protobuf, CSV, etc.) and normalise them into a stable, typed, immutable structure ready for downstream transformation.

It provides:

- A **single, reliable source of truth** for all raw positional updates.  
- Light normalisation and validation (timestamps, coordinates, uniqueness).  
- No attribution or enrichment (H3, route, trip, segments).  
- No filtering (except malformed updates).  
- A stable platform for downstream facts:  
  - `fct_vehicle_position` (attributed point-level fact)  
  - `fct_vehicle_segment` (derived segment fact)

---

## 2. Design Principles

1. **Immutability**  
2. **Minimal transformation**  
3. **Schema stability**  
4. **Strict typing**  
5. **No attribution**  

---

## 3. Schema Specification

| Field | Type | Description |
|-------|------|-------------|
| position_id | STRING | Unique identifier. |
| vehicle_id | STRING | Vehicle identifier. |
| ts | TIMESTAMP | UTC timestamp. |
| latitude | FLOAT64 | Raw latitude. |
| longitude | FLOAT64 | Raw longitude. |
| speed_kmh | FLOAT64 | Raw speed if provided. |
| heading | FLOAT64 | Bearing. |
| gps_valid | BOOL | True if coordinates are within bounds. |
| dup_flag | BOOL | Duplicate record marker. |
| ingest_ts | TIMESTAMP | When record was ingested. |
| raw_payload | STRING | Original data. |

---

## 4. Transformations

### Timestamp Normalisation  

### Coordinate Validation  

### Duplicate Detection  

### Raw Payload Preservation  

---

## 5. Partitioning / Clustering

```
PARTITION BY DATE(ts)
CLUSTER BY vehicle_id, ts
```

---

## 6. Downstream Relationships

- `fct_vehicle_position` → point fact with attribution  
- `fct_vehicle_segment` → segment fact built using `LEAD()`  
- Aggregates (hourly, daily)  
- Indicator models (rolling avg, z-score, anomaly scoring)

---

## 7. Example DDL

```sql
CREATE TABLE `project.dataset.stg_vehicle_position` (
  position_id STRING,
  vehicle_id STRING,
  ts TIMESTAMP,
  latitude FLOAT64,
  longitude FLOAT64,
  speed_kmh FLOAT64,
  heading FLOAT64,
  gps_valid BOOL,
  dup_flag BOOL,
  ingest_ts TIMESTAMP,
  raw_payload STRING
)
PARTITION BY DATE(ts)
CLUSTER BY vehicle_id, ts;
```
