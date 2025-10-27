# src/ingest/main.py

import base64
import json
import os
import zipfile
import io
import pandas as pd


from common.logging_utils import logger
from ingest.fetch import get_data
from gcs_storage.store import upload_schedule_response, upload_data_response
from ingest.transform import transform_realtime, transform_schedule
from big_query.upload import upload_to_bigquery


def get_globals():

    # project variables
    project_id = os.getenv("PROJECT_ID")
    bucket = os.getenv("BUCKET")
    bq_dataset = os.getenv("BQ_DATASET")

    # dataset variables
    dataset = os.getenv("DATASET")
    spec = os.getenv("SPEC")

    # data fetch variables
    url = os.getenv("URL")
    headers_b64 = os.getenv("HEADERS")
    headers = {}
    if headers_b64:
        try:
            headers = json.loads(base64.b64decode(headers_b64).decode("utf-8"))
        except Exception:
            headers = {}
    response_type = os.getenv("RESPONSE_TYPE").lower()

    return {
        "project_id": project_id,
        "bucket": bucket,
        "bq_dataset": bq_dataset,
        "dataset": dataset,
        "spec": spec,
        "url": url,
        "headers": headers,
        "response_type": response_type
    }

def _get_deduplication_mode(dataset: str) -> str:
    """Determine the appropriate deduplication mode based on dataset type."""
    # Dataset type to deduplication mode mapping
    dataset_mode_map = {
        "vehicle-positions": "skip_duplicates",
        "service-alerts": "merge_tracking",
        "trip-updates": "merge_tracking",
        "schedule": "none"
    }
    
    # Return the mode for the dataset, or default to merge_tracking
    return dataset_mode_map.get(dataset, "merge_tracking")


def run(request):

    config = get_globals()

    logger.info(f"Starting data ingestion for {config['dataset']}...")

    try:

        # 1. Get data
        data_bytes = get_data(config['url'], config['headers'], config['response_type'])
        logger.info(f"Fetched and processed data for {config['dataset']}, size: {len(data_bytes) if data_bytes else '0'} bytes.")

        # Branching by dataset type
        
        if config['dataset'] == 'schedule':
             # 2a. Schedule dataset processing with hash-based deduplication
            object_name, feed_hash, is_new = upload_schedule_response(config['bucket'], config['dataset'], data_bytes, config['response_type'], config['spec'])
            
            if not is_new:
                    logger.info(f"GTFS static feed for {config['dataset']} is a duplicate. Halting ingestion.")
                    return ({"status": "duplicate", "dataset": config['dataset'], "feed_hash": feed_hash}, 200)

            logger.info(f"Uploaded schedule to GCS: {object_name}. Feed hash: {feed_hash}")

            # 3a. Unpack zip and load each file to BigQuery with the feed_hash
            results = {"tables_loaded": [], "files_skipped": [], "errors": []}
            
            try:
                with zipfile.ZipFile(io.BytesIO(data_bytes)) as z:
                    file_list = z.namelist()
                    for filename in file_list:
                        with z.open(filename) as file:
                            df_raw = pd.read_csv(file)

                            if df_raw.empty:
                                results["files_skipped"].append(filename)
                                continue
                            
                            # Add feed_hash column to schedule data
                            df_raw['feed_hash'] = feed_hash

                            # Clean and validate data using Pandera schemas
                            file = filename.split('.')[0]
                            df_transformed = transform_schedule(file, df_raw)

                            # 4a. Upload to BigQuery
                            table_name = f"{config['spec']}_{file.replace('-', '_')}"
                            deduplication_mode = _get_deduplication_mode(config['dataset'])
                            logger.info(f"Using deduplication mode '{deduplication_mode}' for dataset '{file}'")
                            
                            result = upload_to_bigquery(
                                df_transformed, 
                                table_name, 
                                config['project_id'], 
                                config['bq_dataset'],
                                upload_method='batch',
                                deduplication_mode=deduplication_mode
                            )
                            
                            logger.info(f"BigQuery ingestion result: {result}")

            
            except Exception as e:
                logger.error(f"Error processing files in zip for {config['dataset']}: {e}")
                return ({"status": "error", "dataset": config['dataset'], "error": str(e)}, 500)

        else:
            # 2b. Real-time dataset processing
            object_name = upload_data_response(config["bucket"], config["dataset"], data_bytes, config["response_type"], config["spec"])
            logger.info(f"Uploaded real-time data to GCS: {object_name}")

            # 3b. Clean and validate data using Pandera schemas
            df_transformed = transform_realtime(config['dataset'], data_bytes)
            if df_transformed.empty:
                logger.warning("Transformed DataFrame is empty.")
                return ({"status": "no_data", "dataset": config['dataset'], "object": object_name, "rows": 0}, 200)

            row_count = len(df_transformed)
            logger.info(f"Transformed data into DataFrame with {row_count} records.")

            # 4b. Upload to BigQuery
            table_name = f"{config['spec']}_{config['dataset'].replace('-', '_')}"
            deduplication_mode = _get_deduplication_mode(config['dataset'])
            logger.info(f"Using deduplication mode '{deduplication_mode}' for dataset '{config['dataset']}'")
            
            # additional debug to inspect transformed DataFrame
            logger.info(f"Stopped prior to BigQuery upload")
            #return df_transformed

            result = upload_to_bigquery(
                df_transformed, 
                table_name, 
                config['project_id'], 
                config['bq_dataset'],
                upload_method="storage_api",
                deduplication_mode=deduplication_mode
            )
            
            logger.info(f"BigQuery ingestion result: {result}")

            logger.info(f"Data ingestion for {config['dataset']} completed successfully.")
            return ({"status": "ok", "dataset": config['dataset'], "object": object_name, "rows": row_count, "bq_ingest": result}, 200)
    
    except Exception as e:
        logger.exception("Ingestion run failed")
        return ({"status": "error", "error": str(e), "dataset": config['dataset']}, 500)
