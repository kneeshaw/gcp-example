-- Materialize mapping of GTFS shape_id to OSM road links once
-- Inputs: `${project_id}.${dataset_id}.shapes_geog`, `${project_id}.${dataset_id}.vw_osm_akl_road_links`
-- Output table: `${project_id}.${dataset_id}.shape_edges`

CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.shape_edges`
CLUSTER BY shape_id, edge_id AS
WITH shapes AS (
  SELECT
    shape_id,
    geom,
    ST_BUFFER(geom, 8) AS buf_geom,
    ST_BOUNDINGBOX(ST_BUFFER(geom, 8)) AS env_box
  FROM `${project_id}.${dataset_id}.shapes_geog`
), roads AS (
  SELECT
    edge_id,
    name AS road_name,
    highway,
    oneway,
    maxspeed,
    length_m AS edge_length_m,
    geom AS edge_geom,
    ST_BOUNDINGBOX(geom) AS env_box,
    ST_SIMPLIFY(geom, 5) AS simple_geom
  FROM `${project_id}.${dataset_id}.vw_osm_akl_road_links`
)
SELECT
  s.shape_id,
  r.edge_id,
  ANY_VALUE(r.road_name) AS road_name,
  ANY_VALUE(r.highway) AS highway,
  ANY_VALUE(r.oneway) AS oneway,
  ANY_VALUE(r.maxspeed) AS maxspeed,
  ANY_VALUE(r.edge_length_m) AS edge_length_m,
  -- Use the maximum measured overlap per (shape, edge) to be robust to any duplicate matches
  MAX(ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5)))) AS overlap_m,
  ANY_VALUE(r.edge_geom) AS edge_geom
FROM shapes s
JOIN roads r
  ON ST_INTERSECTSBOX(r.edge_geom, s.env_box.xmin, s.env_box.ymin, s.env_box.xmax, s.env_box.ymax)
 AND ST_DWithin(r.edge_geom, s.geom, 8)
WHERE r.highway NOT IN ("footway", "path", "cycleway", "steps", "track", "pedestrian", "bridleway", "corridor")
GROUP BY s.shape_id, r.edge_id
HAVING MAX(ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5)))) > 30.0
  AND MAX(ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5)))) / ANY_VALUE(r.edge_length_m) >= 0.10;
