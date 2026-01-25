import base64
import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timezone

import functions_framework
from cloudevents.http import from_http
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage

# =========================
# Environment
# =========================

PROJECT_ID = os.environ["PROJECT_ID"]
INBOX_BUCKET = os.environ["INBOX_BUCKET"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
CANONICAL_BUCKET = os.environ["CANONICAL_BUCKET"]
DOCAI_REQUESTS_TOPIC = os.environ["DOCAI_REQUESTS_TOPIC"]

storage_client = storage.Client()
fs = firestore.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, DOCAI_REQUESTS_TOPIC)

# =========================
# Helpers
# =========================

def _ascii_safe_name(filename: str) -> str:
    if "." in filename:
        base, ext = filename.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        base, ext = filename, ""

    norm = unicodedata.normalize("NFKD", base)
    norm = norm.encode("ascii", "ignore").decode("ascii")
    norm = re.sub(r"[^A-Za-z0-9_-]+", "_", norm).strip("_")

    if not norm:
        norm = "file"

    return norm[:120] + ext


def _sha256_gcs(bucket: str, name: str) -> str:
    b = storage_client.bucket(bucket)
    blob = b.blob(name)
    h = hashlib.sha256()
    with blob.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_eventarc_cloudevent(request):
    """
    Robust Eventarc CloudEvent parser.
    Handles:
    - Native CloudEvents
    - GCS_NOTIFICATION mode
    - bytes / wrapped payloads
    """
    try:
        event = from_http(request.headers, request.get_data())
        data = event.data

        if isinstance(data, (bytes, bytearray)):
            data = json.loads(data.decode("utf-8"))

        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            data = data["data"]

        return data, None

    except Exception as e:
        return None, str(e)

# =========================
# Entry point
# =========================

@functions_framework.http
def ingest_etl(request):
    if request.method != "POST":
        return ("ok", 200)

    data, err = _parse_eventarc_cloudevent(request)

    if err or not isinstance(data, dict):
        try:
            raw = request.get_json(silent=True)
            if isinstance(raw, dict):
                data = raw
        except Exception:
            data = None

    if not isinstance(data, dict):
        print("Bad event payload (not dict).")
        return ("bad event", 400)

    bucket = data.get("bucket")
    name = data.get("name")

    if not bucket or not name:
        print("Missing bucket/name in event")
        return ("bad event", 400)

    if bucket != INBOX_BUCKET:
        print(f"Ignoring event from bucket={bucket}")
        return ("ignored", 200)

    if name.endswith("/") or name.endswith(".keep"):
        print(f"Ignoring placeholder object: {name}")
        return ("ignored", 200)

    b_in = storage_client.bucket(INBOX_BUCKET)
    blob = b_in.blob(name)
    blob.reload()

    size = int(blob.size or 0)
    content_type = blob.content_type or ""

    sha = _sha256_gcs(INBOX_BUCKET, name)
    doc_id = sha

    doc_ref = fs.collection("documents").document(doc_id)
    existing = doc_ref.get()

    if existing.exists:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        raw_name = f"duplicates/{ts}_{os.path.basename(name)}"
        storage_client.bucket(RAW_BUCKET).blob(raw_name).rewrite(blob)
        blob.delete()
        return ("duplicate archived", 200)

    safe = _ascii_safe_name(os.path.basename(name))
    if "." in safe:
        base, ext = safe.rsplit(".", 1)
        canonical_name = f"{base}_{sha[:12]}.{ext}"
    else:
        canonical_name = f"{safe}_{sha[:12]}"

    canonical_path = f"canonical/{canonical_name}"

    b_can = storage_client.bucket(CANONICAL_BUCKET)
    b_can.blob(canonical_path).rewrite(blob)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = f"raw/{ts}/{name}"
    storage_client.bucket(RAW_BUCKET).blob(raw_path).rewrite(blob)

    blob.delete()

    canonical_gcs_uri = f"gs://{CANONICAL_BUCKET}/{canonical_path}"
    original_gcs_uri = f"gs://{RAW_BUCKET}/{raw_path}"

    doc_ref.set(
        {
            "docId": doc_id,
            "sha256": sha,
            "sizeBytes": size,
            "contentType": content_type,
            "original": {
                "gcsUri": original_gcs_uri,
                "filename": os.path.basename(name),
                "inboxObject": name,
            },
            "canonical": {
                "gcsUri": canonical_gcs_uri,
                "filename": canonical_name,
                "object": canonical_path,
            },
            "status": "CANONICALIZED",
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
    )

    msg = {
        "docId": doc_id,
        "canonicalGcsUri": canonical_gcs_uri,
        "canonicalObject": canonical_path,
        "canonicalBucket": CANONICAL_BUCKET,
        "rawGcsUri": original_gcs_uri,
    }

    publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))

    print(f"OK docId={doc_id} canonical={canonical_gcs_uri}")
    return ("ok", 200)
