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

# =============================================================================
# VERSION
# =============================================================================
VERSION = "2026-01-27T-ingest-AtoZ-v4-docai-raw+geometry+quality-contract"

# =========================
# Environment
# =========================

PROJECT_ID = os.environ["PROJECT_ID"]
INBOX_BUCKET = os.environ["INBOX_BUCKET"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
CANONICAL_BUCKET = os.environ["CANONICAL_BUCKET"]
DOCAI_REQUESTS_TOPIC = os.environ["DOCAI_REQUESTS_TOPIC"]

# Optional override; default matches your naming convention:
# dinai-tenant-internal-001 -> dinai-tenant-internal-001-docai-out
DOCAI_OUT_BUCKET = os.environ.get("DOCAI_OUT_BUCKET", f"{PROJECT_ID}-docai-out")

storage_client = storage.Client()
fs = firestore.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, DOCAI_REQUESTS_TOPIC)

# =========================
# Helpers
# =========================

def _utc_run_id() -> str:
    """UTC run id suitable for folder prefixes."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _ascii_safe_name(filename: str) -> str:
    """
    Turn any filename into an ASCII-safe version while keeping the extension.
    NOTE: used only for canonical naming; we always preserve the original unicode name in Firestore.
    """
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

def _infer_case(inbox_object_name: str) -> str:
    """
    Case is the first path segment of the inbox object:
      shemesh/file.pdf  -> shemesh
      file.pdf          -> _default
    """
    if "/" in inbox_object_name:
        first = inbox_object_name.split("/", 1)[0].strip()
        return first if first else "_default"
    return "_default"

def _docai_case_prefix(case: str) -> str:
    # Case-rooted prefix (runner can place job subfolders under this)
    return f"gs://{DOCAI_OUT_BUCKET}/{case}/"

def _docai_run_prefix(case: str, run_id: str) -> str:
    # Run-rooted prefix (preferred: all outputs for this run stay under the run folder)
    return f"gs://{DOCAI_OUT_BUCKET}/{case}/{run_id}/"

def _docai_latest_link_index_uri(case: str) -> str:
    return f"gs://{DOCAI_OUT_BUCKET}/{case}/latest/link_index.jsonl"

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

    case = _infer_case(name)
    original_filename = os.path.basename(name)  # unicode-safe (Hebrew preserved)
    ascii_safe = _ascii_safe_name(original_filename)

    b_in = storage_client.bucket(INBOX_BUCKET)
    blob = b_in.blob(name)
    blob.reload()

    size = int(blob.size or 0)
    content_type = blob.content_type or ""

    # Stable doc identity = sha256 of inbox object contents
    sha = _sha256_gcs(INBOX_BUCKET, name)
    doc_id = sha

    doc_ref = fs.collection("documents").document(doc_id)
    existing = doc_ref.get()

    # Duplicate: archive into RAW duplicates/ and delete inbox object
    if existing.exists:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dup_name = f"duplicates/{ts}/{case}/{sha[:12]}_{ascii_safe}"
        storage_client.bucket(RAW_BUCKET).blob(dup_name).rewrite(blob)
        blob.delete()
        print(f"DUPLICATE docId={doc_id} -> gs://{RAW_BUCKET}/{dup_name}")
        return ("duplicate archived", 200)

    # Canonical filename: ASCII-safe + short hash suffix
    if "." in ascii_safe:
        base, ext = ascii_safe.rsplit(".", 1)
        canonical_name = f"{base}_{sha[:12]}.{ext}"
    else:
        canonical_name = f"{ascii_safe}_{sha[:12]}"

    # Keep canonical separated by case
    canonical_path = f"canonical/{case}/{canonical_name}"

    b_can = storage_client.bucket(CANONICAL_BUCKET)
    b_can.blob(canonical_path).rewrite(blob)

    # Raw path keeps full original inbox object key for provenance
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = f"raw/{ts}/{name}"
    storage_client.bucket(RAW_BUCKET).blob(raw_path).rewrite(blob)

    # Inbox object consumed
    blob.delete()

    canonical_gcs_uri = f"gs://{CANONICAL_BUCKET}/{canonical_path}"
    raw_gcs_uri = f"gs://{RAW_BUCKET}/{raw_path}"

    # Provide a run id for downstream grouping
    run_id = _utc_run_id()

    # Contract for runner: explicitly request RAW docai output + geometry + quality score preservation
    docai_expectations = {
        "persistRawDocaiDocument": True,   # store full DocAI Document JSON (geometry, anchors, etc.)
        "persistPageGeometry": True,       # bounding polys, text anchors, etc.
        "persistQualityScores": True,      # image quality scores -> docQualityScore
        "persistExtractedText": True,      # full doc text where available
        "persistDerivedLayoutBlocks": True # your simplified block list is fine, but NOT instead of RAW
    }

    # Persist metadata for provenance + monitoring
    doc_ref.set(
        {
            "docId": doc_id,
            "sha256": sha,
            "case": case,
            "sizeBytes": size,
            "contentType": content_type,
            "ingest": {
                "version": VERSION,
                "atUtc": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                "runId": run_id,
            },
            "original": {
                "gcsUri": raw_gcs_uri,
                "filenameOriginal": original_filename,   # unicode (Hebrew preserved)
                "filenameAsciiSafe": ascii_safe,
                "inboxObject": name,                    # full key (case/path/filename)
            },
            "canonical": {
                "gcsUri": canonical_gcs_uri,
                "filename": canonical_name,
                "object": canonical_path,
                "bucket": CANONICAL_BUCKET,
            },
            "docai": {
                "outBucket": DOCAI_OUT_BUCKET,
                "casePrefix": _docai_case_prefix(case),
                "runPrefix": _docai_run_prefix(case, run_id),
                "latestLinkIndexUri": _docai_latest_link_index_uri(case),
                "expectations": docai_expectations,
            },
            "status": "CANONICALIZED",
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
    )

    # Publish to docai-runner (preserve unicode fields)
    msg = {
        "docId": doc_id,
        "case": case,
        "runId": run_id,

        # The PDF we want Search/RAG citations to open (stable path)
        "sourcePdfUri": canonical_gcs_uri,

        # Canonical
        "canonicalGcsUri": canonical_gcs_uri,
        "canonicalObject": canonical_path,
        "canonicalBucket": CANONICAL_BUCKET,

        # Raw provenance
        "rawGcsUri": raw_gcs_uri,
        "rawObject": raw_path,
        "rawBucket": RAW_BUCKET,

        # Original / inbox
        "originalFilename": original_filename,
        "originalInboxObject": name,

        # Sizing
        "sizeBytes": size,
        "contentType": content_type,

        # Output routing for DocAI (runner uses these to set gcs_output_uri_prefix)
        "docaiOutBucket": DOCAI_OUT_BUCKET,
        "docaiCasePrefix": _docai_case_prefix(case),        # gs://.../<case>/
        "docaiRunPrefix": _docai_run_prefix(case, run_id),  # gs://.../<case>/<runId>/
        "latestLinkIndexUri": _docai_latest_link_index_uri(case),

        # What we expect runner to persist
        "docaiExpectations": docai_expectations,

        # Versioning
        "ingestVersion": VERSION,
    }

    publisher.publish(topic_path, json.dumps(msg, ensure_ascii=False).encode("utf-8"))

    print(
        "OK "
        f"docId={doc_id} "
        f"case={case} "
        f"runId={run_id} "
        f"canonical={canonical_gcs_uri} "
        f"original={original_filename}"
    )
    return ("ok", 200)
