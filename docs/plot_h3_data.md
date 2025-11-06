# Plotly Visualisation of H3 Data (BigQuery + Python)

## 🧭 Purpose
This instruction defines how AI-driven analytics or notebooks should **visualise H3-indexed spatial data** extracted from BigQuery using **Plotly**.  
It ensures consistent rendering of vehicle movement, coverage, or congestion data based on H3 cell aggregations.

---

## ⚙️ Input Requirements

- **BigQuery table** containing:
  - `h3` (H3 index as `STRING`)
  - A numeric metric (e.g. `vehicle_count`, `speed_avg`, `incident_count`)
  - Optional: `latitude`, `longitude` for centroids  
- The AI must know the BigQuery project and dataset context (e.g. `mak-group-gtfs.mart.fct_vehicle_positions`).

---

## 🧩 Workflow Steps

### 1. Query Data from BigQuery
Use BigQuery to aggregate metrics by H3 cell.

```sql
SELECT
  h3_9 AS h3,
  COUNT(*) AS vehicle_count
FROM `yourproj.mart.fct_vehicle_positions`
WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) AND CURRENT_TIMESTAMP()
GROUP BY h3_9;
```

Fetch this with Python:

```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="yourproj")
df = client.query("<SQL above>").to_dataframe()
```

---

### 2. Convert H3 Cells to Polygons
Use the [`h3`](https://pypi.org/project/h3/) library to generate GeoJSON-compatible polygons.

```python
import h3
import shapely.geometry as geom

def h3_to_polygon(h):
    boundary = h3.h3_to_geo_boundary(h, geo_json=True)
    return geom.Polygon(boundary)

df["geometry"] = df["h3"].apply(h3_to_polygon)
```

For mapping libraries, also compute centroid coordinates:

```python
df["lat"] = df["h3"].apply(lambda x: h3.h3_to_geo(x)[0])
df["lon"] = df["h3"].apply(lambda x: h3.h3_to_geo(x)[1])
```

---

### 3. Plot with Plotly

#### **Option A – Centroid Scatter Plot**
Use `plotly.express` for simple density maps.

```python
import plotly.express as px

fig = px.scatter_mapbox(
    df,
    lat="lat",
    lon="lon",
    color="vehicle_count",
    size="vehicle_count",
    color_continuous_scale="Viridis",
    zoom=9,
    mapbox_style="carto-positron",
    title="Vehicle Count by H3 Cell"
)
fig.show()
```

---

#### **Option B – Hexagon Polygons**
Render true H3 hexagons as filled shapes.

```python
import plotly.graph_objects as go

features = []
for _, row in df.iterrows():
    boundary = h3.h3_to_geo_boundary(row["h3"], geo_json=True)
    lats, lons = zip(*boundary)
    features.append(
        go.Scattermapbox(
            fill="toself",
            lat=lats,
            lon=lons,
            line=dict(width=1, color="black"),
            fillcolor="rgba(0, 200, 255, 0.4)",
            text=f"{row['vehicle_count']} vehicles",
            hoverinfo="text",
        )
    )

fig = go.Figure(features)
fig.update_layout(
    mapbox_style="carto-positron",
    mapbox_zoom=9,
    mapbox_center={"lat": -36.85, "lon": 174.76},
    title="H3 Vehicle Coverage"
)
fig.show()
```

---

## 🧠 AI Instruction Summary

| Field | Description |
|-------|--------------|
| **instruction_id** | `plot_h3_data` |
| **context** | When visualising data indexed by H3 in BigQuery, fetch aggregated metrics, convert to polygons using the Python `h3` package, and render on a Plotly Mapbox plot. |
| **steps** | 1. Query data from BigQuery<br>2. Convert H3 cells to polygons or centroids<br>3. Render using Plotly (`scatter_mapbox` or `graph_objects`) |
| **mapbox_style** | `"carto-positron"` |
| **preferred_resolution** | `9` for city-level, `10` for stop-level |
| **output_type** | Interactive map in HTML or Jupyter display |
| **notes** | - Keep under ~500 polygons for good performance.<br>- Use centroid scatter for large datasets.<br>- Use polygons when shape accuracy matters. |

---

## ✅ Summary

- Use **BigQuery → pandas → Plotly** workflow.  
- The AI should always use **H3 polygons or centroids** for spatial plotting.  
- Store output as `.html` or inline display for notebooks.  
- Default zoom and centre for Auckland:  
  - `zoom=9`, `center={"lat": -36.85, "lon": 174.76}`.

---

**File:** `/docs/ai_instructions/plot_h3_data.md`  
**Maintainer:** MAK Group NZ  
**Last Updated:** 2025-11-06