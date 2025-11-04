-- infra/models/dim/dim_feed.sql
--
-- This model normalises the GTFS feed_info.txt file and adds effective dating
-- to produce a dimension table of schedule versions.
--
-- Grain: One row per unique feed_hash, representing a distinct version of the
--        GTFS schedule.
--
-- Logic:
-- 1. Deduplicate feed_info entries to ensure one row per feed_hash, taking the
--    earliest ingestion timestamp.
-- 2. Handle the rare case where two different feeds have the same start date by
--    selecting the one ingested later.
-- 3. Calculate the effective date range for each feed version. The 'effective_to_date'
--    is derived from the start date of the *next* feed, which correctly handles
--    cases where feeds are superseded before their declared end date.
--
WITH
-- Step 1: Read and deduplicate feed info, parsing integer dates.
-- This ensures we have one record per feed_hash, using its first ingestion time.
feed_info_cleaned AS (
  SELECT
    feed_hash,
    feed_publisher_name,
    feed_version,
    PARSE_DATE('%Y%m%d', CAST(feed_start_date AS STRING)) AS feed_start_date,
    PARSE_DATE('%Y%m%d', CAST(feed_end_date AS STRING)) AS feed_end_date_declared,
    MIN(created_at) AS ingested_at_utc
  FROM
    `${project_id}.${dataset_id}.stg_feed_info`
  WHERE
    feed_start_date IS NOT NULL
  GROUP BY
    1, 2, 3, 4, 5
),

-- Step 2: Rank feeds to handle overlaps. If two feeds have the same start date,
-- the one ingested more recently is considered authoritative.
feeds_ranked AS (
  SELECT
    *,
    -- Assign a row number to each feed, partitioned by start date.
    -- We prioritize the feed that was loaded most recently in case of a tie.
    ROW_NUMBER() OVER (PARTITION BY feed_start_date ORDER BY ingested_at_utc DESC) AS rn
  FROM
    feed_info_cleaned
),

-- Step 3: Calculate the effective date ranges for each feed version.
feeds_with_effective_dates AS (
  SELECT
    feed_hash,
    feed_publisher_name,
    feed_version,
    ingested_at_utc,
    feed_start_date AS feed_start_date_declared,
    feed_end_date_declared,
    feed_start_date AS effective_from_date,
    -- The feed is effective until the day before the next feed starts.
    -- If there is no next feed, it is effective indefinitely (NULL).
    LEAD(feed_start_date) OVER (ORDER BY feed_start_date) - 1 AS effective_to_date
  FROM
    feeds_ranked
  -- Filter to keep only the highest-ranked feed for each start date.
  WHERE rn = 1
)

-- Final Step: Select all fields and add the is_current flag.
SELECT
  feed_hash,
  feed_publisher_name,
  feed_version,
  ingested_at_utc,
  feed_start_date_declared,
  feed_end_date_declared,
  effective_from_date,
  effective_to_date,
  -- A feed is 'current' if its effective_to_date is NULL, meaning it's the latest version.
  (effective_to_date IS NULL) AS is_current
FROM
  feeds_with_effective_dates
ORDER BY
  effective_from_date;