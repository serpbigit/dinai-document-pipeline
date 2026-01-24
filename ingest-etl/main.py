import base64
import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timezone

from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage

PROJECT_ID = os.environ["PROJECT_ID"]
INBOX_BUCKET = os.environ["INBOX_BUCKET"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
CANONICAL_BUCKET = os.environ["CANONICAL_BUCKET"]
DOCAI_REQUESTS_TOPIC = os.environ["DOCAI_REQUESTS_TOPIC"]

storage_client = storage.Client()
fs = firestore.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, DOCAI_REQUESTS_TOPIC)

def _ascii_safe_name(filename: str) -> str:
    # Keep extension
    if "." in filename:
        base, ext = filename.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        base, ext = filename, ""

    # Normalize, drop diacritics, keep ascii only
    norm = unicodedata.normalize("NFKD", base)
    norm = norm.encode("ascii", "ignore").decode("ascii")

    # Replace anything not alnum/_/- with underscore
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

def ingest_etl(cloud_event):
    # CloudEvent from Eventarc (GCS finalize)
    data = cloud_event.data
    bucket = data.get("bucket")
    name = data.get("name")

    if not bucket or not name:
        print("Missing bucket/name in event")
        return ("bad event", 400)

    # Only handle inbox bucket
    if bucket != INBOX_BUCKET:
        print(f"Ignoring event from bucket={bucket}")
        return ("ignored", 200)

    # Ignore "folders" / placeholders
    if name.endswith("/") or name.endswith(".keep"):
        print(f"Ignoring placeholder object: {name}")
        return ("ignored", 200)

    b_in = storage_client.bucket(INBOX_BUCKET)
    blob = b_in.blob(name)
    blob.reload()

    size = int(blob.size or 0)
    content_type = blob.content_type or ""

    sha = _sha256_gcs(INBOX_BUCKET, name)
    doc_id = sha  # deterministic for v0

    # Dedup check
    doc_ref = fs.collection("documents").document(doc_id)
    existing = doc_ref.get()
    if existing.exists:
        print(f"Duplicate detected (sha256 exists): {doc_id}. Archiving upload event without reprocessing.")
        # Move original to RAW with unique suffix to preserve audit of upload
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        raw_name = f"duplicates/{ts}_{os.path.basename(name)}"
        storage_client.bucket(RAW_BUCKET).blob(raw_name).rewrite(blob)
        blob.delete()
        return ("duplicate archived", 200)

    safe = _ascii_safe_name(os.path.basename(name))
    canonical_name = f"{safe.rsplit('.',1)[0]}_{sha[:12]}{('.'+safe.rsplit('.',1)[1]) if '.' in safe else ''}"
    canonical_path = f"canonical/{canonical_name}"

    # Copy to canonical
    b_can = storage_client.bucket(CANONICAL_BUCKET)
    b_can.blob(canonical_path).rewrite(blob)

    # Move original to RAW (immutable evidence)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = f"raw/{ts}/{name}"
    storage_client.bucket(RAW_BUCKET).blob(raw_path).rewrite(blob)

    # Delete from inbox (inbox is transient trigger)
    blob.delete()

    canonical_gcs_uri = f"gs://{CANONICAL_BUCKET}/{canonical_path}"
    original_gcs_uri  = f"gs://{RAW_BUCKET}/{raw_path}"

    # Store mapping
    doc_ref.set({
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
    })

    # Publish docai request
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
