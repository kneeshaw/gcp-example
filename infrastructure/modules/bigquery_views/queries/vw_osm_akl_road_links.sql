-- OSM road links for Auckland region from a locally materialized table
-- Source table is created by src/osm/ingest_osm_roads.py into `${project_id}.${dataset_id}.osm_akl_road_links`
-- Filter retained for clarity; geometry already limited to Auckland in the ingest

SELECT
  edge_id,
  name,
  highway,
  oneway,
  maxspeed,
  length_m,
  geom
FROM `${project_id}.${dataset_id}.osm_akl_road_links`;
