#  Moved from tasks_enqueuer/main.py (package renamed to enqueuer)
from __future__ import annotations
import os, json
from typing import Optional
import urllib.request
from datetime import datetime, timedelta, timezone
from google.cloud import tasks_v2
import google.auth
from google.auth.transport import requests as auth_requests
from google.oauth2 import id_token
import sys
from google.protobuf import timestamp_pb2

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is not None:
            return default
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def _minute_base(now: datetime) -> datetime:
    return now.replace(second=0, microsecond=0)

def enqueue(request):
    req_payload = {}
    try:
        if request and getattr(request, 'is_json', False):
            req_payload = request.get_json(silent=True) or {}
    except Exception:
        req_payload = {}
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or _env("PROJECT_ID")
    location = _env("QUEUE_LOCATION")
    queue_name = _env("QUEUE_NAME")
    worker_url = _env("WORKER_URL")
    sa_email = _env("SERVICE_ACCOUNT_EMAIL")
    offsets_str = os.getenv("OFFSETS", "0,5,10,15,20,25,30,35,40,45,50,55")
    offsets = []
    for tok in offsets_str.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            offsets.append(int(tok))
        except ValueError:
            continue
    # Shift all offsets by +5s so we avoid the exact minute boundary (0) and add a 60s slot.
    # Example: 0,5,10,...,55 -> 5,10,15,...,60
    offsets = sorted({o + 5 for o in offsets})
    env_payload = {}
    pj = os.getenv("PAYLOAD_JSON")
    if pj:
        try:
            env_payload = json.loads(pj)
        except Exception:
            env_payload = {}
    payload = {**env_payload, **req_payload}
    creds, detected_project = google.auth.default()
    client = tasks_v2.CloudTasksClient(credentials=creds)
    parent = client.queue_path(project, location, queue_name)
    detected_email = getattr(creds, 'service_account_email', 'unknown')
    meta_email = "unknown"
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:  # nosec B310 - metadata internal
            meta_email = resp.read().decode().strip()
    except Exception:
        pass
    print(f"Enqueue debug: project={project} detected_project={detected_project} queue={queue_name} location={location} configured_sa={sa_email} creds_sa={detected_email} meta_sa={meta_email}", file=sys.stderr)
    # Pre-mint an ID token for the worker (avoid Cloud Tasks OIDC token block => no actAs requirement on create_task)
    auth_req = auth_requests.Request()
    try:
        worker_id_token = id_token.fetch_id_token(auth_req, worker_url)
    except Exception as e:
        print(f"ID token fetch failed: {e}", file=sys.stderr)
        worker_id_token = None
    now = datetime.now(timezone.utc)
    base = _minute_base(now)
    created = 0
    grace = timedelta(seconds=2)
    for off in offsets:
        scheduled = base + timedelta(seconds=off)
        # Allow small startup delay; only skip if we're more than grace past the intended time.
        if scheduled + grace < now:
            continue
        # If we're slightly late (scheduled < now but within grace), bump forward to now+0.5s.
        if scheduled < now:
            scheduled = now + timedelta(milliseconds=500)
        ts = timestamp_pb2.Timestamp(); ts.FromDatetime(scheduled)
        headers = {"Content-Type": "application/json"}
        if worker_id_token:
            headers["Authorization"] = f"Bearer {worker_id_token}"
        task = {"http_request": {"http_method": tasks_v2.HttpMethod.POST, "url": worker_url, "headers": headers, "body": json.dumps(payload).encode("utf-8")}, "schedule_time": ts}
        client.create_task(request={"parent": parent, "task": task}); created += 1
    return ({"status": "ok", "created": created, "queue": queue_name, "location": location}, 200, {"Content-Type": "application/json"})
