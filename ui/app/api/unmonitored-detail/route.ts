import { NextResponse } from 'next/server';
import { BigQuery, BigQueryTimestamp } from '@google-cloud/bigquery';

const bigquery = new BigQuery();

// Define an interface for the expected row structure from BigQuery
interface UnmonitoredMovementRow {
  movement_start_utc: BigQueryTimestamp;
  movement_end_utc: BigQueryTimestamp;
  vehicle_id: string;
  route_mode: string;
  gap_duration_seconds: number;
  gap_distance_m: number;
}

export async function GET(_request: Request) { // Add type and underscore for unused param
  const project = process.env.GCP_PROJECT;
  const dataset = process.env.BQ_DATASET;
  const table = 'mdl_unmonitored_detail';

  const query = `
    SELECT
      movement_start_utc,
      movement_end_utc,
      vehicle_id,
      route_mode,
      gap_duration_seconds,
      gap_distance_m
    FROM \`${project}.${dataset}.${table}\`
    ORDER BY movement_end_utc DESC
    LIMIT 200;
  `;

  try {
    const [rows] = await bigquery.query({
      query: query,
      location: process.env.BQ_LOCATION,
    });

    // Convert BigQuery timestamp objects to ISO strings for JSON serialization
    const results = (rows as UnmonitoredMovementRow[]).map(row => ({
      movement_start_utc: row.movement_start_utc.value,
      movement_end_utc: row.movement_end_utc.value,
      vehicle_id: row.vehicle_id,
      route_mode: row.route_mode,
      gap_duration_seconds: row.gap_duration_seconds,
      gap_distance_m: row.gap_distance_m,
    }));

    return NextResponse.json(results);
  } catch (error) {
    console.error('Error querying BigQuery:', error);
    return NextResponse.json(
      { error: 'Failed to fetch data from BigQuery.' },
      { status: 500 }
    );
  }
}