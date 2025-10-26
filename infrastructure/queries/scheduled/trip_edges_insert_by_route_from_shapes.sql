-- Insert trip_edges for a single route_id by expanding from shape_edges
-- Variables: project_id, dataset_id, route_id

INSERT INTO `${project_id}.${dataset_id}.trip_edges`
SELECT
  t.trip_id,
  t.route_id,
  t.direction_id,
  t.shape_id,
  se.edge_id,
  se.road_name,
  se.highway,
  se.oneway,
  se.maxspeed,
  se.edge_length_m,
  se.overlap_m,
  se.edge_geom
FROM `${project_id}.${dataset_id}.sc_trips` t
JOIN `${project_id}.${dataset_id}.shape_edges` se
  ON se.shape_id = t.shape_id
LEFT JOIN `${project_id}.${dataset_id}.trip_edges` te
  ON te.trip_id = t.trip_id AND te.edge_id = se.edge_id
WHERE t.route_id = "${route_id}"
  AND te.trip_id IS NULL; -- anti-join to prevent duplicates
