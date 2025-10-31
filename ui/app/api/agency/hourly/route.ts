import { NextResponse } from "next/server";
import { BigQuery } from "@google-cloud/bigquery";

export const revalidate = 300; // cache for 5 minutes

function normalizeValue(v: any): any {
  if (v == null) return v;
  if (typeof v === "object" && "value" in v && Object.keys(v).length === 1) {
    const raw = (v as any).value;
    const num = Number(raw);
    return Number.isNaN(num) ? raw : num;
  }
  if (v instanceof Date) return v.toISOString();
  return v;
}

function normalizeRow(row: Record<string, any> | undefined): Record<string, any> | undefined {
  if (!row) return row;
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(row)) {
    if (Array.isArray(v)) out[k] = v.map((x) => normalizeValue(x));
    else if (v && typeof v === "object" && !("value" in v)) {
      const nested: Record<string, any> = {};
      for (const [nk, nv] of Object.entries(v)) nested[nk] = normalizeValue(nv);
      out[k] = nested;
    } else out[k] = normalizeValue(v);
  }
  return out;
}

const bq = new BigQuery({ projectId: process.env.GCP_PROJECT });

export async function GET(request: Request) {
  try {
    const project = process.env.GCP_PROJECT as string;
    const dataset = process.env.BQ_DATASET as string;
    const location = process.env.BQ_LOCATION as string | undefined;
    if (!project || !dataset) {
      return NextResponse.json({ error: "Missing GCP_PROJECT or BQ_DATASET" }, { status: 500 });
    }

    const url = new URL(request.url);
    const dateParam = url.searchParams.get("date");
    const isValidDate = (s: string) => /^\d{4}-\d{2}-\d{2}$/.test(s);
    const todayUtc = new Date().toISOString().slice(0, 10);
    const targetDate = dateParam && isValidDate(dateParam) ? dateParam : todayUtc;

    const query = `
      WITH hours AS (
        SELECT ts AS hour_utc
        FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
          TIMESTAMP(@date), TIMESTAMP_ADD(TIMESTAMP(@date), INTERVAL 23 HOUR), INTERVAL 1 HOUR
        )) AS ts
      ),
      vp AS (
        SELECT
          TIMESTAMP_TRUNC(timestamp, HOUR, 'UTC') AS hour_utc,
          COUNT(DISTINCT vehicle_id) AS active_vehicles
        FROM \`${project}.${dataset}.rt_vehicle_positions\`
        WHERE DATE(timestamp) = @date
        GROUP BY hour_utc
      ),
      tu AS (
        SELECT
          TIMESTAMP_TRUNC(timestamp, HOUR, 'UTC') AS hour_utc,
          SAFE_DIVIDE(COUNTIF(ABS(delay) <= 300), NULLIF(COUNTIF(delay IS NOT NULL), 0)) AS on_time_pct,
          AVG(delay) AS avg_delay_seconds,
          COUNT(*) AS updates_count
        FROM \`${project}.${dataset}.rt_trip_updates\`
        WHERE DATE(timestamp) = @date
        GROUP BY hour_utc
      ),
      sa AS (
        SELECT
          h.hour_utc,
          COUNTIF(
            period_start <= TIMESTAMP_ADD(h.hour_utc, INTERVAL 59 MINUTE)
            AND (period_end IS NULL OR period_end >= h.hour_utc)
          ) AS active_alerts
        FROM hours h
        LEFT JOIN \`${project}.${dataset}.rt_service_alerts\` a
          ON a.period_start <= TIMESTAMP_ADD(h.hour_utc, INTERVAL 59 MINUTE)
         AND (a.period_end IS NULL OR a.period_end >= h.hour_utc)
        GROUP BY h.hour_utc
      )
      SELECT
        h.hour_utc,
        COALESCE(vp.active_vehicles, 0) AS active_vehicles,
        tu.on_time_pct,
        tu.avg_delay_seconds,
        COALESCE(tu.updates_count, 0) AS updates_count,
        COALESCE(sa.active_alerts, 0) AS active_alerts
      FROM hours h
      LEFT JOIN vp ON vp.hour_utc = h.hour_utc
      LEFT JOIN tu ON tu.hour_utc = h.hour_utc
      LEFT JOIN sa ON sa.hour_utc = h.hour_utc
      ORDER BY h.hour_utc
    `;

    const [rows] = await bq.query({ query, params: { date: targetDate }, location });
    const normalized = (rows || []).map((r: any) => normalizeRow(r));
    return NextResponse.json({ date: targetDate, rows: normalized });
  } catch (err: any) {
    console.error("/api/agency/hourly error:", err);
    return NextResponse.json({ error: err?.message ?? "Unknown error" }, { status: 500 });
  }
}
