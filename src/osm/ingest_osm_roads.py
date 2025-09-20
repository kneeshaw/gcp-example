#!/usr/bin/env python3
"""
Ingest Auckland OSM road links into a local BigQuery table (AU dataset) by:
1) Querying the US public dataset bigquery-public-data.geo_openstreetmap.linestrings
   (location='US') with an Auckland bounding box and common highway classes.
2) Writing results into an AU staging table with WKT geometry.
3) Creating the final AU table with GEOGRAPHY by converting WKT -> GEOGRAPHY.

This avoids cross-location query restrictions by moving data through the client.

Usage:
  python src/osm/ingest_osm_roads.py --project regal-dynamo-470908-v9 \
         --dataset auckland_data_dev --temp_table osm_akl_road_links_stage \
         --final_table osm_akl_road_links

Auth:
  Ensure Application Default Credentials are set (e.g., `gcloud auth application-default login`)
  and the account has BigQuery read access to public datasets and write access to the target dataset.
"""
from __future__ import annotations

import argparse
from typing import List, Dict

from google.cloud import bigquery


AKL_BBOX_WKT = "POLYGON((174.4 -37.4, 175.0 -37.4, 175.0 -36.5, 174.4 -36.5, 174.4 -37.4))"

HIGHWAY_CLASSES = (
    "motorway","trunk","primary","secondary","tertiary",
    "unclassified","residential","service","motorway_link","trunk_link",
    "primary_link","secondary_link","tertiary_link","living_street","busway"
)


def build_public_query(source_table: str, tags_field: str = "all_tags") -> str:
    highway_list = ",".join([f"'{h}'" for h in HIGHWAY_CLASSES])
    return f"""
WITH akl_bbox AS (
  SELECT ST_GEOGFROMTEXT('{AKL_BBOX_WKT}') AS geom
)
SELECT
  id AS edge_id,
  (SELECT value FROM UNNEST({tags_field}) WHERE key = 'name') AS name,
  (SELECT value FROM UNNEST({tags_field}) WHERE key = 'highway') AS highway,
  (SELECT value FROM UNNEST({tags_field}) WHERE key = 'oneway') AS oneway,
  (SELECT value FROM UNNEST({tags_field}) WHERE key = 'maxspeed') AS maxspeed,
  ST_LENGTH(geometry) AS length_m,
  ST_ASTEXT(geometry) AS wkt
FROM `{source_table}`
WHERE EXISTS (SELECT 1 FROM UNNEST({tags_field}) t WHERE t.key = 'highway')
  AND (SELECT value FROM UNNEST({tags_field}) WHERE key = 'highway') IN ({highway_list})
  AND ST_INTERSECTS(geometry, (SELECT geom FROM akl_bbox))
"""


def run_ingest(project: str, dataset: str, temp_table: str, final_table: str) -> None:
  client = bigquery.Client(project=project)

  # Discover dataset location to run AU-side jobs correctly
  dataset_ref = bigquery.DatasetReference(project, dataset)
  dataset_obj = client.get_dataset(dataset_ref)
  dataset_location = dataset_obj.location

  # Discover source (public) dataset location to run the query in the correct region
  source_dataset_ref = bigquery.DatasetReference("bigquery-public-data", "geo_openstreetmap")
  source_dataset_obj = client.get_dataset(source_dataset_ref)
  source_location = source_dataset_obj.location

  # Step 1: Run query to fetch Auckland subset from the public dataset
  # Use planet_ways (or lines) table depending on availability; planet_ways has all_tags
  source_table = "bigquery-public-data.geo_openstreetmap.planet_ways"
  query = build_public_query(source_table=source_table, tags_field="all_tags")
  job_config = bigquery.QueryJobConfig()
  job_config.use_legacy_sql = False

  print(f"Running public dataset query in location {source_location} to fetch OSM roads subset…")
  query_job = client.query(query, job_config=job_config, location=source_location)
  rows_iter = query_job.result()
  # Materialize results in client and convert Row objects to plain dicts for JSON load
  rows = [
    {
      "edge_id": r["edge_id"],
      "name": r["name"],
      "highway": r["highway"],
      "oneway": r["oneway"],
      "maxspeed": r["maxspeed"],
      "length_m": r["length_m"],
      "wkt": r["wkt"],
    }
    for r in rows_iter
  ]
  print(f"Fetched {len(rows)} rows from public OSM dataset.")

  # Step 2: Load into AU staging table with WKT geometry
  temp_table_id = f"{project}.{dataset}.{temp_table}"
  schema = [
    bigquery.SchemaField("edge_id", "INT64"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("highway", "STRING"),
    bigquery.SchemaField("oneway", "STRING"),
    bigquery.SchemaField("maxspeed", "STRING"),
    bigquery.SchemaField("length_m", "FLOAT64"),
    bigquery.SchemaField("wkt", "STRING"),
  ]

  print(f"Loading {len(rows)} rows into staging table {temp_table_id}…")
  load_job = client.load_table_from_json(
    rows,
    destination=temp_table_id,
    job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
  )
  load_job.result()
  print("Staging load complete.")

  # Step 3: Create/replace final table with GEOGRAPHY
  final_table_id = f"{project}.{dataset}.{final_table}"
  convert_sql = f"""
  CREATE OR REPLACE TABLE `{final_table_id}` AS
  SELECT
    edge_id, name, highway, oneway, maxspeed, length_m,
    ST_GEOGFROMTEXT(wkt) AS geom
  FROM `{temp_table_id}`
  """
  print(f"Creating final table {final_table_id} with GEOGRAPHY geometry…")
  conv_job = client.query(convert_sql, location=dataset_location)
  conv_job.result()
  print("Final table created.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True, help="GCP project ID")
    ap.add_argument("--dataset", required=True, help="BigQuery dataset ID (AU)")
    ap.add_argument("--temp_table", default="osm_akl_road_links_stage")
    ap.add_argument("--final_table", default="osm_akl_road_links")
    args = ap.parse_args()

    run_ingest(args.project, args.dataset, args.temp_table, args.final_table)


if __name__ == "__main__":
    main()
