# Daily Schedule Builder

Builds a complete transit schedule for a given date from GTFS data stored in BigQuery.

## Usage

### Basic Usage
```bash
cd /Users/martin/Projects/mak-group/gcp-example/src/daily_schedule
python daily_schedule.py --date 20250914
```

### With Custom Configuration
```bash
python daily_schedule.py \
  --project your-project-id \
  --dataset your-dataset \
  --date 20250914 \
  --output schedule_20250914.csv
```

### Direct Execution
```bash
./daily_schedule.py --date 20250914
```

## Command Line Options

- `--project`: GCP project ID (default: regal-dynamo-470908-v9)
- `--dataset`: BigQuery dataset name (default: auckland_data_dev)
- `--date`: Service date in YYYYMMDD format (default: 20250914)
- `--output`: Optional output file path for CSV export

## Output

The script will:
1. Find the applicable GTFS feed for the given date
2. Identify active services (regular + exceptions)
3. Get trips for active services
4. Get stop times and convert GTFS times to UTC
5. Build and sort the final schedule
6. Display summary statistics
7. Optionally save to CSV file

## Features

- ✅ Handles GTFS time complexities (times > 24:00)
- ✅ Converts to UTC datetime and epoch timestamps
- ✅ Proper timezone handling
- ✅ Comprehensive error handling and logging
- ✅ Command line interface
- ✅ CSV export capability
