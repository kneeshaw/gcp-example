import HourlyChart from "./Chart";

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

export default function Page() {
  const d = todayUtc();
  return (
    <main className="space-y-6">
      <h1 className="text-2xl font-bold">Hourly fleet load & reliability</h1>
      <div className="text-slate-600 text-sm">Date: {d} (UTC)</div>
      <HourlyChart date={d} />
    </main>
  );
}
