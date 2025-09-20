-- Materialize a LINESTRING per GTFS shape_id from points
-- Inputs: `${project_id}.${dataset_id}.sc_shapes`
-- Output table: `${project_id}.${dataset_id}.shapes_geog`

CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.shapes_geog`
CLUSTER BY shape_id AS
SELECT
  shape_id,
  ST_MAKELINE(
    ARRAY_AGG(ST_GEOGPOINT(shape_pt_lon, shape_pt_lat)
      ORDER BY shape_pt_sequence)
  ) AS geom
FROM `${project_id}.${dataset_id}.sc_shapes`
GROUP BY shape_id;
