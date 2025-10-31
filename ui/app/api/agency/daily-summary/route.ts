import { NextResponse } from "next/server";
import { BigQuery } from "@google-cloud/bigquery";

function normalizeValue(v: any): any {
  if (v == null) return v;
  // BigQuery numeric wrappers sometimes come back as objects like { value: "123" }
  if (typeof v === "object" && "value" in v && Object.keys(v).length === 1) {
    const raw = (v as any).value;
    const num = Number(raw);
    return Number.isNaN(num) ? raw : num;
  }
  // Convert Date objects to ISO strings for JSON/React safety
  if (v instanceof Date) return v.toISOString();
  return v;
}

function normalizeRow(row: Record<string, any> | undefined): Record<string, any> | undefined {
  if (!row) return row;
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(row)) {
    if (Array.isArray(v)) {
      out[k] = v.map((x) => normalizeValue(x));
    } else if (v && typeof v === "object" && !("value" in v)) {
      // For nested structs, shallow-normalize values
      const nested: Record<string, any> = {};
      for (const [nk, nv] of Object.entries(v)) {
        nested[nk] = normalizeValue(nv);
      }
      out[k] = nested;
    } else {
      out[k] = normalizeValue(v);
    }
  }
  return out;
}
export const revalidate = 300; // cache for 5 minutes

const bq = new BigQuery({ projectId: process.env.GCP_PROJECT });

export async function GET(request: Request) {
  try {
    const project = process.env.GCP_PROJECT as string;
    const dataset = process.env.BQ_DATASET as string;
    const location = process.env.BQ_LOCATION as string | undefined; // e.g., 'australia-southeast1', 'US', 'EU'

    if (!project || !dataset) {
      return NextResponse.json(
        { error: "Missing GCP_PROJECT or BQ_DATASET environment variables" },
        { status: 500 }
      );
    }

    const url = new URL(request.url);
    const dateParam = url.searchParams.get("date");
    const isValidDate = (s: string) => /^\d{4}-\d{2}-\d{2}$/.test(s);
    const todayUtc = new Date().toISOString().slice(0, 10);
    const targetDate = dateParam && isValidDate(dateParam) ? dateParam : todayUtc;

    // Try to fetch for requested/today's date first
    let [rows] = await bq.query({
      query: `
        SELECT *
        FROM \`${project}.${dataset}.vw_agency_daily_summary\`
        WHERE summary_date = @targetDate
        ORDER BY calculated_at DESC
        LIMIT 1
      `,
      params: { targetDate },
      location,
    });

    let fallbackUsed = false;

    // If no row for that date, fall back to the latest available row
    if (!rows || rows.length === 0) {
      fallbackUsed = true;
      [rows] = await bq.query({
        query: `
          SELECT *
          FROM \`${project}.${dataset}.vw_agency_daily_summary\`
          ORDER BY summary_date DESC, calculated_at DESC
          LIMIT 1
        `,
        location,
      });
    }

    let payload: any = normalizeRow(rows?.[0]) ?? {};

    // If realtime looks stale and KPIs are zero, try a lightweight fallback to the most recent RT day
    const onTimeZero = (payload.on_time_performance_pct ?? 0) === 0;
    const minutesSinceUpdate = payload.minutes_since_last_trip_update ?? 999;
    if (onTimeZero && minutesSinceUpdate >= 999) {
      // Find latest date with trip updates in the last 14 days
      const [dateRows] = await bq.query({
        query: `
          SELECT DATE(MAX(timestamp)) AS d
          FROM \`${project}.${dataset}.rt_trip_updates\`
          WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
        `,
        location,
      });

      const lastRtDate = dateRows?.[0]?.d as string | undefined;
      if (lastRtDate) {
        try {
          // Compute the three KPIs for that date
          const [[rtKpis], [vpKpis]] = await Promise.all([
            bq.query({
              query: `
                SELECT 
                  AVG(delay) as avg_delay_seconds,
                  SAFE_DIVIDE(COUNTIF(ABS(delay) <= 300), NULLIF(COUNTIF(delay IS NOT NULL), 0)) as on_time_performance,
                  SAFE_DIVIDE(COUNTIF(schedule_relationship = '3'), NULLIF(COUNT(DISTINCT trip_id), 0)) as cancellation_rate
                FROM \`${project}.${dataset}.rt_trip_updates\`
                WHERE DATE(timestamp) = @d
              `,
              params: { d: lastRtDate },
              location,
            }),
            bq.query({
              query: `
                SELECT COUNT(DISTINCT vehicle_id) AS active_vehicles
                FROM \`${project}.${dataset}.rt_vehicle_positions\`
                WHERE DATE(timestamp) = @d
              `,
              params: { d: lastRtDate },
              location,
            }),
          ]);

          const rt = normalizeRow(rtKpis?.[0]) ?? {};
          const vp = normalizeRow(vpKpis?.[0]) ?? {};
          payload = {
            ...payload,
            on_time_performance_pct: rt.on_time_performance ?? payload.on_time_performance_pct ?? 0,
            cancellation_rate_pct: rt.cancellation_rate ?? payload.cancellation_rate_pct ?? 0,
            avg_delay_seconds: rt.avg_delay_seconds ?? payload.avg_delay_seconds ?? 0,
            active_vehicles_count: vp.active_vehicles ?? payload.active_vehicles_count ?? 0,
            _fallback: true,
            _fallback_reason: "no_rt_today_using_latest",
            _fallback_rt_date: lastRtDate,
          };
        } catch (e) {
          // Ignore fallback errors and return original payload
          console.warn("Fallback KPI computation failed:", e);
        }
      }
    }

    return NextResponse.json({ ...payload, _source_date: targetDate, _fallback: fallbackUsed || payload._fallback === true });
  } catch (err: any) {
    // Ensure we always return valid JSON on error for the client to handle gracefully
    console.error("/api/agency/daily-summary error:", err);
    return NextResponse.json(
      { error: err?.message ?? "Unknown server error" },
      { status: 500 }
    );
  }
}
