-- Insert trip_edges for a single route_id shard
-- Variables: project_id, dataset_id, route_id

INSERT INTO `${project_id}.${dataset_id}.trip_edges`
WITH shapes AS (
  SELECT
    shape_id,
    geom,
    ST_BUFFER(geom, 15) AS buf_geom,
    ST_ENVELOPE(ST_BUFFER(geom, 15)) AS env_geom
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
    ST_ENVELOPE(geom) AS env_geom,
    ST_SIMPLIFY(geom, 5) AS simple_geom
  FROM `${project_id}.${dataset_id}.vw_osm_akl_road_links`
), trips AS (
  SELECT trip_id, route_id, direction_id, shape_id
  FROM `${project_id}.${dataset_id}.sc_trips`
  WHERE route_id = "${route_id}"
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
  ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5))) AS overlap_m,
  r.edge_geom
FROM trips t
JOIN shapes s USING (shape_id)
JOIN roads r
  ON ST_INTERSECTS(r.env_geom, s.env_geom)
 AND ST_DWithin(r.edge_geom, s.geom, 15)
WHERE ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5))) > 20.0;
