-- Create clustered route_edges table from trip_edges aggregation
CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.route_edges`
CLUSTER BY route_id, edge_id AS
SELECT
  route_id,
  edge_id,
  ANY_VALUE(road_name) AS road_name,
  ANY_VALUE(highway) AS highway,
  ANY_VALUE(oneway) AS oneway,
  ANY_VALUE(maxspeed) AS maxspeed,
  ANY_VALUE(edge_length_m) AS edge_length_m,
  ANY_VALUE(edge_geom) AS edge_geom,
  COUNT(DISTINCT trip_id) AS trips_using_edge,
  SUM(overlap_m) AS total_overlap_m
FROM `${project_id}.${dataset_id}.trip_edges`
GROUP BY route_id, edge_id;