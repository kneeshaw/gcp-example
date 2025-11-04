-- infra/models/dim/dim_route.sql
--
-- This model creates a dimension table for GTFS routes. It deduplicates routes
-- to ensure that only the most recent version of each route is included, based
-- on the ingestion timestamp from the associated feed.
--
-- Grain: One row per unique route_id, representing the latest known version.
--
WITH
-- Source routes with feed ingestion timestamp
routes_with_feed_timestamp AS (
  SELECT
    r.route_id,
    r.agency_id,
    r.route_short_name,
    r.route_long_name,
    r.route_desc,
    r.route_type,
    r.route_url,
    r.route_color,
    r.route_text_color,
    f.ingested_at_utc,
    -- Add a row number to rank routes by ingestion time, descending.
    ROW_NUMBER() OVER (PARTITION BY r.route_id ORDER BY f.ingested_at_utc DESC) as rn
  FROM
    `${project_id}.${dataset_id}.stg_routes` AS r
  INNER JOIN
    -- Join to dim_feed to get the ingestion timestamp for each feed version.
    `${project_id}.${dataset_id}.dim_feed` AS f
    ON r.feed_hash = f.feed_hash
)
-- Final selection: take only the most recent version of each route.
SELECT
  route_id,
  agency_id,
  route_short_name,
  route_long_name,
  route_desc,
  route_type,
  route_url,
  route_color,
  route_text_color,
  -- Add route mode for easier analysis, consistent with other models.
  CASE route_type
    WHEN 0 THEN 'tram'
    WHEN 1 THEN 'subway'
    WHEN 2 THEN 'rail'
    WHEN 3 THEN 'bus'
    WHEN 4 THEN 'ferry'
    WHEN 5 THEN 'cable_tram'
    WHEN 6 THEN 'aerial_lift'
    WHEN 7 THEN 'funicular'
    WHEN 11 THEN 'trolleybus'
    WHEN 12 THEN 'monorail'
    ELSE NULL
  END AS route_mode
FROM
  routes_with_feed_timestamp
WHERE
  -- Keep only the #1 ranked row per route_id, which is the latest version.
  rn = 1;
