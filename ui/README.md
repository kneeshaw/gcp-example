# Transit UI (Next.js)

This is a minimal Next.js + Tailwind UI that reads BigQuery views via server API routes.

## Prereqs
- Node 18+
- gcloud CLI authenticated for ADC: `gcloud auth application-default login`

## Setup
1. Install deps

```bash
npm install
```

2. Create `.env.local`

```bash
GCP_PROJECT=regal-dynamo-470908-v9
BQ_DATASET=auckland_data_dev
BQ_LOCATION=australia-southeast1
TZ=Pacific/Auckland
NEXT_PUBLIC_BASE_URL=http://localhost:3000
```

3. Run

```bash
npm run dev
```

## Deploy (Cloud Run)

- Ensure the runtime service account has roles:
  - bigquery.jobUser
  - bigquery.dataViewer
- Set env vars in the service (same as `.env.local`).

## Troubleshooting

- Error: "Cannot parse as CloudRegion"
  - Cause: An env var like `GOOGLE_CLOUD_REGION` or `GOOGLE_CLOUD_LOCATION` is set to a non-GCP region value (e.g., a timezone like `Pacific/Auckland`).
  - Fix: Unset or correct it, and set `BQ_LOCATION` to your BigQuery dataset's location (e.g., `US`, `EU`, `australia-southeast1`). For zsh:

    ```zsh
    unset GOOGLE_CLOUD_REGION
    unset GOOGLE_CLOUD_LOCATION
    ```

  - Note: `TZ` controls Node's timezone and is safe to set (e.g., `Pacific/Auckland`).