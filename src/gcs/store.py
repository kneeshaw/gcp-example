# Library imports
import io
import gzip
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from google.cloud import storage
import hashlib

from common.logging_utils import logger


CONTENT_TYPE_MAP = {
    "json": "application/json",
    "protobuf": "application/x-protobuf",
    "zip": "application/zip",
}

EXT_MAP = {
    "json": "json",
    "protobuf": "pb",
    "zip": "zip",
}


def ts_parts(ts: datetime) -> Dict[str, str]:
    """Break a UTC timestamp into formatted parts for path construction.

    Args:
        ts: A timezone-aware UTC ``datetime`` instance.

    Returns:
        Dict with keys Y, M, D, h, stamp (compact timestamp) used in object names.
    """
    return {
        "Y": ts.strftime("%Y"),
        "M": ts.strftime("%m"),
        "D": ts.strftime("%d"),
        "h": ts.strftime("%H"),
        "stamp": ts.strftime("%Y%m%dT%H%M%SZ"),
    }


def build_object_name(dataset: str, parts: Dict[str, str], response_type: str, gzipped: bool, spec: str) -> str:
    """Compose a partitioned GCS object path for real-time style datasets.

    Layout:
        {dataset}/year=YYYY/month=MM/day=DD/hour=HH/{spec}-{dataset}-<timestamp>.<ext>[.gz]

    Args:
        dataset: Dataset identifier (e.g. vehicle_positions).
        parts: Dict produced by ``ts_parts``.
        response_type: Original response type (json, protobuf, zip, etc.).
        gzipped: Whether a gzip suffix should be appended (non-zip only).
        spec: Dataset specification prefix (e.g. 'rt', 'sc').

    Returns:
        A fully qualified object name relative to the bucket.
    """
    
    ext = EXT_MAP.get(response_type, "bin")
    suffix = f".{ext}"
    
    if gzipped and response_type != "zip":
        suffix += ".gz"
    return (f"{spec}-{dataset}/year={parts['Y']}/month={parts['M']}/day={parts['D']}/hour={parts['h']}/"
            f"{dataset}-{parts['stamp']}{suffix}")


def gzip_bytes(data: bytes, level: int = 6) -> bytes:
    """Gzip-compress a bytes payload with deterministic mtime.

    Args:
        data: Raw bytes to compress.
        level: gzip compression level (1-9).

    Returns:
        Compressed bytes.
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level, mtime=0) as gz:
        gz.write(data)
    return buf.getvalue()


def upload_gcs(bucket: str, object_name: str, data_bytes: bytes, content_type: str, gzipped: bool, metadata: Optional[Dict[str, str]] = None):
    """Upload a byte payload to GCS.

    Args:
        bucket: Destination GCS bucket name.
        object_name: Full object path inside the bucket.
        data_bytes: Bytes to upload.
        content_type: MIME type for the object.
        gzipped: Whether the bytes are gzip-compressed (sets content_encoding="gzip").
        metadata: Optional dict of custom metadata to attach to the object.
    """
    client = storage.Client()
    blob = client.bucket(bucket).blob(object_name)
    blob.cache_control = "no-store"
    if gzipped:
        blob.content_encoding = "gzip"
    if metadata:
        blob.metadata = metadata
    blob.upload_from_string(data_bytes, content_type=content_type)


def upload_data_response(bucket: str, dataset: str, data: bytes, response_type: str, spec: str):
    """Upload a real-time style response (with time-partitioned path).

    Automatically gzips non-zip content and derives the correct extension &
    content type. Returns the created object path.

    Args:
        bucket: Destination GCS bucket name.
        dataset: Dataset identifier.
        data: Raw (uncompressed) payload bytes.
        response_type: Declared response type (json|protobuf|zip|...).
        spec: Dataset specification prefix (e.g. 'rt', 'sc').

    Returns:
        The object name written to in GCS.
    """
    capture_ts = datetime.now(timezone.utc)
    parts = ts_parts(capture_ts)

    gzipped = response_type != "zip"
    if gzipped:
        data = gzip_bytes(data)

    content_type = CONTENT_TYPE_MAP.get(response_type, "application/octet-stream")
    object_name = build_object_name(dataset, parts, response_type, gzipped, spec)

    upload_gcs(bucket, object_name, data, content_type, gzipped)
    return object_name


def upload_schedule_response(bucket: str, dataset: str, zip_bytes: bytes, response_type: str, spec: str) -> tuple[str, str, bool]:
    """Upload GTFS static feed (schedule) ZIP with hash-based deduplication.

    Creates (if new):
        {dataset}/year=YYYY/{spec}-{hash}.zip  (immutable, content addressed)
        {dataset}/{spec}-latest.zip          (always points to most recent content)

    Args:
        bucket: Destination GCS bucket name.
        dataset: Dataset identifier (expected 'gtfs-schedule').
        zip_bytes: Raw ZIP file bytes.
        response_type: Should be 'zip'; retained for signature symmetry.
        spec: Dataset specification prefix (e.g. 'rt', 'sc').

    Returns:
        A tuple of (hashed_object_name, hash_hex, is_new).
    """
    capture_ts = datetime.now(timezone.utc)
    year = capture_ts.strftime("%Y")
    hash_hex = hashlib.md5(zip_bytes).hexdigest()

    hashed_object_name = f"{spec}-{dataset}/year={year}/{hash_hex}.zip"
    latest_object_name = f"{spec}-{dataset}/latest.zip"

    # Existence check (avoid re-uploading identical content)
    client = storage.Client()
    if client.bucket(bucket).blob(hashed_object_name).exists():
        logger.info(f"GTFS static feed unchanged (hash={hash_hex})")
        return hashed_object_name, hash_hex, False

    meta = {"hash": hash_hex, "captured": capture_ts.isoformat()}

    # Upload hashed object
    upload_gcs(
        bucket=bucket,
        object_name=hashed_object_name,
        data_bytes=zip_bytes,
        content_type="application/zip",
        gzipped=False,
        metadata=meta,
    )

    # Upload / overwrite latest pointer
    upload_gcs(
        bucket=bucket,
        object_name=latest_object_name,
        data_bytes=zip_bytes,
        content_type="application/zip",
        gzipped=False,
        metadata=meta,
    )

    logger.info(f"Uploaded new GTFS static feed (hash={hash_hex})")
    return hashed_object_name, hash_hex, True