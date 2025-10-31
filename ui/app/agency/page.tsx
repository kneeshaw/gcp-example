async function getData() {
  const base = process.env.NEXT_PUBLIC_BASE_URL ?? "http://localhost:3000";
  const url = `${base}/api/agency/daily-summary`;

  try {
    const res = await fetch(url, { next: { revalidate: 300 } });
    // Be defensive: parse as text first to avoid JSON parse errors
    const text = await res.text();
    if (!res.ok) {
      try {
        const j = text ? JSON.parse(text) : null;
        return { __error: j?.error ?? `Request failed (${res.status})` } as any;
      } catch {
        return { __error: `Request failed (${res.status})` } as any;
      }
    }

    if (!text) return {} as any;
    try {
      return JSON.parse(text);
    } catch (_e) {
      // Non-JSON response body; return defaults with error marker
      return { __error: "API returned non-JSON response" } as any;
    }
  } catch (_e) {
    // Network or other unexpected error
    return { __error: "Network error contacting API" } as any;
  }
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="p-4 rounded-lg border bg-white">
      <div className="text-sm text-slate-600">{label}</div>
      <div className="text-xl font-semibold">{typeof value === 'object' ? String(value) : value}</div>
    </div>
  );
}

export default async function Page() {
  const d = await getData();
  return (
    <main className="space-y-4">
      <h1 className="text-2xl font-bold">Agency daily summary</h1>
      {d.__error ? (
        <div className="p-3 rounded border border-red-200 bg-red-50 text-red-700">
          {String(d.__error)}
        </div>
      ) : null}
      {d._fallback ? (
        <div className="p-3 rounded border border-amber-200 bg-amber-50 text-amber-700">
          Showing latest available realtime metrics{d._fallback_rt_date ? ` from ${d._fallback_rt_date}` : ""}.
        </div>
      ) : null}
      <div className="grid md:grid-cols-3 gap-4">
        <Stat label="On-time %" value={`${Math.round((d.on_time_performance_pct ?? 0) * 100)}%`} />
        <Stat label="Cancellation %" value={`${Math.round((d.cancellation_rate_pct ?? 0) * 100)}%`} />
  <Stat label="Active vehicles" value={String(d.active_vehicles_count ?? 0)} />
      </div>
      {d.summary_date ? (
        <div className="text-sm text-slate-500">Summary date: {d.summary_date} {d._fallback ? '(latest available)' : ''}</div>
      ) : null}
    </main>
  );
}
