'use client'; // This marks the component as a Client Component

import { useState, useEffect } from 'react';

// Define an interface for the data structure of each row
interface UnmonitoredMovement {
  movement_start_utc: string;
  movement_end_utc: string;
  vehicle_id: string;
  route_mode: string;
  gap_duration_seconds: number;
  gap_distance_m: number;
}

// Helper function to format seconds into a readable string
const formatDuration = (seconds: number): string => {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return `${minutes}m ${remainingSeconds}s`;
};

// Helper function to format a UTC date string into a local time string
const formatTime = (utcString: string): string => {
  if (!utcString) return 'N/A';
  return new Date(utcString).toLocaleTimeString();
};

export default function UnmonitoredDetailPage() {
  // Type the state for data, loading, and error
  const [data, setData] = useState<UnmonitoredMovement[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/unmonitored-detail')
      .then((res) => {
        if (!res.ok) {
          throw new Error('Failed to fetch data');
        }
        return res.json();
      })
      .then((data: UnmonitoredMovement[]) => {
        setData(data);
        setIsLoading(false);
      })
      .catch((err: Error) => {
        setError(err.message);
        setIsLoading(false);
      });
  }, []);

  if (isLoading) return <p className="p-8">Loading unmonitored movement data...</p>;
  if (error) return <p className="p-8 text-red-500">Error: {error}</p>;

  return (
    <div className="p-4 sm:p-6 lg:p-8">
      <h1 className="text-2xl font-bold mb-4">Unmonitored Movement Details</h1>
      <p className="mb-6 text-gray-600">Displaying the last 200 unmonitored movement events.</p>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Start Time</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">End Time</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Vehicle ID</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Route Mode</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Duration</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Distance (m)</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {data.map((row, index) => (
              <tr key={index}>
                <td className="px-6 py-4 whitespace-nowrap text-sm">{formatTime(row.movement_start_utc)}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm">{formatTime(row.movement_end_utc)}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-mono">{row.vehicle_id}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm">{row.route_mode}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm">{formatDuration(row.gap_duration_seconds)}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm">{Math.round(row.gap_distance_m)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}