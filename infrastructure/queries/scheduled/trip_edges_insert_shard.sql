-- Insert shard of trip_edges partitioned by route_id hash
-- Variables: project_id, dataset_id, shard (int), shard_mod (int)

INSERT INTO `${project_id}.${dataset_id}.trip_edges`
WITH shapes AS (
  SELECT
    shape_id,
    geom,
  ST_BUFFER(geom, 15) AS buf_geom,
  ST_BOUNDINGBOX(ST_BUFFER(geom, 15)) AS env_box
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
), trips AS (
  SELECT trip_id, route_id, direction_id, shape_id
  FROM `${project_id}.${dataset_id}.sc_trips`
  WHERE MOD(ABS(FARM_FINGERPRINT(route_id)), ${shard_mod}) = ${shard}
)
SELECT
  t.trip_id,
  t.route_id,
  t.direction_id,
  t.shape_id,
  r.edge_id,
  ANY_VALUE(r.road_name) AS road_name,
  ANY_VALUE(r.highway) AS highway,
  ANY_VALUE(r.oneway) AS oneway,
  ANY_VALUE(r.maxspeed) AS maxspeed,
  ANY_VALUE(r.edge_length_m) AS edge_length_m,
  MAX(ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5)))) AS overlap_m,
  ANY_VALUE(r.edge_geom) AS edge_geom
FROM trips t
JOIN shapes s USING (shape_id)
JOIN roads r
  ON ST_INTERSECTSBOX(r.edge_geom, s.env_box.xmin, s.env_box.ymin, s.env_box.xmax, s.env_box.ymax)
 AND ST_DWithin(r.edge_geom, s.geom, 15)
LEFT JOIN `${project_id}.${dataset_id}.trip_edges` te
  ON te.trip_id = t.trip_id AND te.edge_id = r.edge_id
WHERE te.trip_id IS NULL
GROUP BY t.trip_id, t.route_id, t.direction_id, t.shape_id, r.edge_id
HAVING MAX(ST_LENGTH(ST_INTERSECTION(r.simple_geom, ST_SIMPLIFY(s.buf_geom, 5)))) > 20.0;
