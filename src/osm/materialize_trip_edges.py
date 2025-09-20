#!/usr/bin/env python3
"""
Materialize vw_trip_edges into clustered tables for faster queries.

Usage:
  python src/osm/materialize_trip_edges.py --project regal-dynamo-470908-v9 \
         --dataset auckland_data_dev --trip-table trip_edges --route-table route_edges

Creates:
  - {project}.{dataset}.{trip_table} clustered by (route_id, edge_id)
  - {project}.{dataset}.{route_table} clustered by (route_id, edge_id)

The route-level table is a deduplicated aggregation per (route_id, edge_id)
with summary attributes for fast lookups like "edges for route INN-202".
"""
from __future__ import annotations

import argparse
from google.cloud import bigquery


def run(project: str, dataset: str, trip_table: str, route_table: str) -> None:
    client = bigquery.Client(project=project)

    dataset_ref = bigquery.DatasetReference(project, dataset)
    ds = client.get_dataset(dataset_ref)
    location = ds.location

    trip_dest = f"{project}.{dataset}.{trip_table}"
    trip_sql = f"""
    CREATE OR REPLACE TABLE `{trip_dest}`
    CLUSTER BY route_id, edge_id AS
    SELECT
      trip_id,
      route_id,
      direction_id,
      shape_id,
      edge_id,
      road_name,
      highway,
      oneway,
      maxspeed,
      edge_length_m,
      overlap_m,
      edge_geom
    FROM `{project}.{dataset}.vw_trip_edges`
    """
    print(f"Materializing {trip_dest} (this may take several minutes)…")
    job = client.query(trip_sql, location=location)
    job.result()
    print("Trip-edge table done.")

    route_dest = f"{project}.{dataset}.{route_table}"
    route_sql = f"""
    CREATE OR REPLACE TABLE `{route_dest}`
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
    FROM `{trip_dest}`
    GROUP BY route_id, edge_id
    """
    print(f"Materializing {route_dest}…")
    job = client.query(route_sql, location=location)
    job.result()
    print("Route-edge table done.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--trip-table", default="trip_edges")
    ap.add_argument("--route-table", default="route_edges")
    args = ap.parse_args()
    run(args.project, args.dataset, args.trip_table, args.route_table)
