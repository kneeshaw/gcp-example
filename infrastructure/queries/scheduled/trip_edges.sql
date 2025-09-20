-- Create clustered trip_edges table from vw_trip_edges
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.trip_edges`
CLUSTER BY route_id, edge_id AS
WITH shapes AS (
  SELECT shape_id, geom FROM `${project_id}.${dataset_id}.shapes_geog`
), roads AS (
  SELECT edge_id, name AS road_name, highway, oneway, maxspeed, length_m AS edge_length_m, geom AS edge_geom
  FROM `${project_id}.${dataset_id}.vw_osm_akl_road_links`
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
  r.road_name,
  r.highway,
  r.oneway,
  r.maxspeed,
  r.edge_length_m,
  ST_LENGTH(ST_INTERSECTION(r.edge_geom, ST_BUFFER(s.geom, 15))) AS overlap_m,
  r.edge_geom
FROM trips t
JOIN shapes s USING (shape_id)
JOIN roads r
  -- cheaper prefilter before INTERSECTION
  ON ST_DWithin(r.edge_geom, s.geom, 15)
WHERE ST_LENGTH(ST_INTERSECTION(r.edge_geom, ST_BUFFER(s.geom, 15))) > 20.0;