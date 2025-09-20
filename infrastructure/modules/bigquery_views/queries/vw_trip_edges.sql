-- Map GTFS trips to OSM road links via spatial intersection of shapes and links
-- Inputs:
--   - `${project_id}.${dataset_id}.sc_trips` (trip_id, shape_id, route_id, direction_id, service_id)
--   - `${project_id}.${dataset_id}.vw_shapes_geog` (shape_id, geom)
--   - `${project_id}.${dataset_id}.vw_osm_akl_road_links` (edge_id, geom, attrs)
-- Output:
--   - trip_id-edge_id exploded rows with overlap meters

WITH shapes AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_shapes_geog`
), roads AS (
  SELECT * FROM `${project_id}.${dataset_id}.vw_osm_akl_road_links`
), trips AS (
  SELECT trip_id, route_id, direction_id, shape_id
  FROM `${project_id}.${dataset_id}.sc_trips`
)
SELECT
  t.trip_id,
  t.route_id,
  t.direction_id,
  t.shape_id,
  r.edge_id,
  r.name AS road_name,
  r.highway,
  r.oneway,
  r.maxspeed,
  r.length_m AS edge_length_m,
  -- compute overlap length using a small buffer around the shape to allow for snap tolerance
  ST_LENGTH(ST_INTERSECTION(r.geom, ST_BUFFER(s.geom, 15))) AS overlap_m,
  r.geom AS edge_geom
FROM trips t
JOIN shapes s USING (shape_id)
JOIN roads r
  ON ST_INTERSECTS(r.geom, ST_BUFFER(s.geom, 15))
WHERE ST_LENGTH(ST_INTERSECTION(r.geom, ST_BUFFER(s.geom, 15))) > 20.0;
