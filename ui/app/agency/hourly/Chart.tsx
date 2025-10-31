"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type HourRow = {
  hour_utc: string;
  active_vehicles: number;
  on_time_pct: number | null;
  updates_count: number;
  active_alerts?: number;
};

export default function HourlyChart({ date }: { date: string }) {
  const [data, setData] = useState<HourRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    const run = async () => {
      try {
        setError(null);
        const res = await fetch(`/api/agency/hourly?date=${date}`, { signal: ctrl.signal });
        const json = await res.json();
        if (!res.ok) throw new Error(json?.error || `Request failed (${res.status})`);
        setData(json.rows as HourRow[]);
      } catch (e: any) {
        if (e.name !== "AbortError") setError(e.message || String(e));
      }
    };
    run();
    return () => ctrl.abort();
  }, [date]);

  const traces = useMemo(() => {
    if (!data) return [] as any[];
    const x = data.map((r) => r.hour_utc);
    return [
      {
        x,
        y: data.map((r) => r.updates_count ?? 0),
        type: "bar" as const,
        name: "Trip updates",
        marker: { color: "#94a3b8" },
        opacity: 0.3,
        yaxis: "y",
      },
      {
        x,
        y: data.map((r) => r.active_vehicles ?? 0),
        type: "scatter" as const,
        mode: "lines+markers" as const,
        name: "Active vehicles",
        line: { color: "#2563eb" },
        yaxis: "y",
      },
      {
        x,
        y: data.map((r) => (r.on_time_pct ?? 0)),
        type: "scatter" as const,
        mode: "lines+markers" as const,
        name: "On-time %",
        line: { color: "#16a34a" },
        yaxis: "y2",
      },
    ];
  }, [data]);

  const layout = useMemo(() => ({
    margin: { l: 50, r: 50, t: 20, b: 40 },
    barmode: "overlay" as const,
    xaxis: { title: "Hour (UTC)", type: "date" as const, tickformat: "%H:00" },
    yaxis: { title: "Vehicles / Updates", rangemode: "tozero" as const },
    yaxis2: { title: "On-time %", overlaying: "y" as const, side: "right" as const, range: [0, 1], tickformat: ".0%" },
    legend: { orientation: "h" as const },
    height: 420,
    showlegend: true,
  }), []);

  if (error) {
    return <div className="p-3 rounded border border-red-200 bg-red-50 text-red-700">{error}</div>;
  }
  if (!data) {
    return <div className="text-slate-500">Loading hourly metrics…</div>;
  }

  return <Plot data={traces as any} layout={layout as any} style={{ width: "100%" }} config={{ displayModeBar: false }} />;
}
