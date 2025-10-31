import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Transit Analytics",
  description: "Public UI for transit analytics",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen text-slate-900">
        <div className="max-w-6xl mx-auto p-6">{children}</div>
      </body>
    </html>
  );
}
