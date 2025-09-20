Auckland region - dev environment

This environment composes four products for the Auckland region using existing modules:
- vehicle-positions: 5s cadence via Cloud Tasks enqueuer (offsets from vehicle_positions_offsets)
- trip-updates: cron every minute (default)
- service-alerts: cron every minute (default)
- schedule (GTFS static): cron every 15 minutes

Inputs (via tfvars):
- project_id, region (label like "auckland"), gcp_region (GCP region like "australia-southeast1"), bucket_location (optional)
- headers: map(string) of HTTP headers for all feeds (optional)
- vehicle_positions_url, trip_updates_url, service_alerts_url, schedule_url
- vehicle_positions_offsets (default 0..55 step 5)
- service_alerts_cron (default */1), trip_updates_cron (optional; defaults to service_alerts_cron), schedule_cron (default */15), time_zone

Outputs:
- data_bucket, worker_urls, enqueuer_urls

Notes:
- All workers share one SA with storage.objectAdmin on the data bucket and logWriter at project.
- Source zips are packaged locally from src/gtfs_ingest and src/tasks_enqueuer.
- Headers are base64 JSON via env GTFS_HEADERS to the worker.
