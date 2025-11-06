-- infra/models/mdl/mdl_speed_route_hourly.sql
--
-- Calculates a rolling average speed for each route at an hourly granularity,
-- comparing the current hour's speed against its recent history for the same
-- hour AND day_of_week (seasonality control).
-- Grain: One row per service_date, hour, route_id, direction_id, route_mode.
--
WITH
  speed_data AS (
    SELECT
      DATE(service_date) AS service_date,
      hour,
      day_of_week,
      route_id,
      direction_id,
      route_mode,
      avg_speed_kmh,
      position_count
    FROM
      `${project_id}.${dataset_id}.agg_speed_route_hour`
  ),
  rolling_avg AS (
    SELECT
      service_date,
      hour,
      day_of_week,
      route_id,
      direction_id,
      route_mode,
      avg_speed_kmh,
      position_count,
      -- 28-row window: current row + 27 preceding rows within the same
      -- route_id, direction_id, route_mode, hour, and day_of_week.
      -- (Counts rows, not calendar days.)
      AVG(avg_speed_kmh) OVER (
        PARTITION BY
          route_id,
          direction_id,
          route_mode,
          hour,
          day_of_week
        ORDER BY
          service_date
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
      ) AS avg_speed_kmh_28_row_rolling
    FROM
      speed_data
  )
SELECT
  service_date,
  hour,
  day_of_week,
  route_id,
  direction_id,
  route_mode,
  avg_speed_kmh,
  avg_speed_kmh_28_row_rolling AS avg_speed_kmh_rolling,
  position_count,
  SAFE_DIVIDE(
    avg_speed_kmh - avg_speed_kmh_28_row_rolling,
    avg_speed_kmh_28_row_rolling
  ) AS pct_change_from_rolling
FROM
  rolling_avg
ORDER BY
  service_date DESC,
  hour DESC,
  route_id,
  direction_id;
