import Link from "next/link";

export default function Home() {
  return (
    <main className="space-y-6">
      <h1 className="text-2xl font-bold">Transit Analytics</h1>
      <p className="text-slate-600">Explore public KPIs from BigQuery views.</p>
      <ul className="list-disc pl-6">
        <li>
          <Link className="text-blue-600 underline" href="/agency">
            Agency daily summary
          </Link>
        </li>
        <li>
          <Link className="text-blue-600 underline" href="/agency/hourly">
            Hourly fleet load & reliability
          </Link>
        </li>
      </ul>
    </main>
  );
}
