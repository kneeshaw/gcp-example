# H3 Indexing Instruction for AI Agents (BigQuery)

## 🧭 Purpose
This instruction defines how AI-driven queries and pipelines should generate and use **H3 spatial indexes** within **BigQuery** for Australian-region datasets.  
It ensures consistent spatial aggregation, easy provider swapping, and compatibility with existing GTFS Analytics tables.

---

## ⚙️ Preferred Provider

**Provider:** [BigFunctions H3](https://unytics.io/bigfunctions/h3/)  
**Region:** `australia_southeast1` (or `_2` if using AU-SE2)  
**Reason:** Publicly available, fast, no external permissions, fully compatible with BigQuery.

---

## 🧩 Implementation Steps

### 1. Generate H3 Index (Direct Call)
Use BigFunctions to compute an H3 cell from a latitude/longitude pair:

```sql
SELECT bigfunctions.australia_southeast1.h3(
  "latLngToCell",
  JSON_ARRAY(<latitude>, <longitude>, <resolution>)
) AS h3_index;
```

Example:
```sql
SELECT bigfunctions.australia_southeast1.h3(
  "latLngToCell",
  JSON_ARRAY(-36.8485, 174.7633, 9)
) AS h3_index;
```

---

### 2. Wrap in a Project-level SQL UDF
To simplify switching providers later, create a reusable function:

```sql
CREATE OR REPLACE FUNCTION `yourproj.analytics.H3_FROMGEOGPOINT`(
  geog GEOGRAPHY,
  res INT64
) AS (
  bigfunctions.australia_southeast1.h3(
    "latLngToCell",
    JSON_ARRAY(ST_Y(geog), ST_X(geog), res)
  )
);
```

---

### 3. Use the Wrapper in Queries

```sql
SELECT
  *,
  `yourproj.analytics`.H3_FROMGEOGPOINT(
    ST_GEOGPOINT(longitude, latitude), 9
  ) AS h3_9
FROM `yourproj.mart.fct_vehicle_positions`;
```

This approach ensures all downstream logic (aggregates, joins, etc.) can rely on a consistent H3 field.

---

## 🔄 Optional Alternative (CARTO Toolbox)

If CARTO access is approved and advanced geospatial functions are required, update the UDF body:

```sql
CREATE OR REPLACE FUNCTION `yourproj.analytics.H3_FROMGEOGPOINT`(
  geog GEOGRAPHY,
  res INT64
) AS (
  `carto-un-au-se1`.carto.H3_FROMGEOGPOINT(geog, res)
);
```

**Note:** This requires user/service account access to CARTO’s shared project or a local manual install.

---

## 🧠 AI Instruction Summary

| Field | Description |
|-------|--------------|
| **instruction_id** | `bigquery_h3_usage` |
| **context** | For all spatial indexing tasks in BigQuery, use the BigFunctions H3 library to maintain compatibility in Australia. Wrap all provider calls behind a custom UDF (`H3_FROMGEOGPOINT`) to allow easy provider switching. |
| **steps** | 1. Use BigFunctions call<br>2. Create wrapper UDF<br>3. Use wrapper in all queries<br>4. Replace provider later if needed |
| **preferred_resolution** | `9` (city/route-level granularity) |
| **notes** | - The H3 index supports spatial aggregation and coverage analysis.<br>- Store output as `STRING` unless INT64 optimisation is needed.<br>- Avoid direct calls to external H3 projects unless access has been verified. |

---

## ✅ Summary
- Default to **BigFunctions** for all H3-related operations in `australia_southeast1`.  
- Always go through the **`H3_FROMGEOGPOINT`** wrapper.  
- The wrapper makes it easy to switch to **CARTO** or another provider later without refactoring code.  
- Use H3 resolution **9** for route-level, **10** for stop-level analysis.

---

**File:** `/docs/ai_instructions/h3_indexing.md`  
**Maintainer:** MAK Group NZ  
**Last Updated:** 2025-11-05