import base64
import json
import os
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List

import functions_framework
from flask import Request

from google.api_core import exceptions as gexceptions
from google.api_core import operation as ga_operation
from google.cloud import documentai_v1 as documentai
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
from google.protobuf.json_format import MessageToDict

# =============================================================================
# Version
# =============================================================================
VERSION = "2026-01-29-save-all-raw-doc+manifest+sourcepdf+geometry"

# =============================================================================
# Environment (required)
# =============================================================================
# PROJECT_ID is not automatically injected into Cloud Run.
# Prefer explicit PROJECT_ID, but fall back to GOOGLE_CLOUD_PROJECT.
PROJECT_ID = (os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
if not PROJECT_ID:
    raise RuntimeError("Missing PROJECT_ID. Set PROJECT_ID or ensure GOOGLE_CLOUD_PROJECT is present.")

DOCAI_PROCESSOR = os.environ.get("DOCAI_PROCESSOR", "").strip()  # full resource name or AUTO/empty
DOCAI_PROCESSOR_FALLBACK_FORM = os.environ.get("DOCAI_PROCESSOR_FALLBACK_FORM", "").strip()

# AUTO mode => use fallback FORM processor
if (not DOCAI_PROCESSOR) or (DOCAI_PROCESSOR.upper() == "AUTO"):
    if not DOCAI_PROCESSOR_FALLBACK_FORM:
        raise RuntimeError(
            "DOCAI_PROCESSOR is not set. Set DOCAI_PROCESSOR to full processor name, "
            "or set DOCAI_PROCESSOR_FALLBACK_FORM for AUTO mode."
        )
    DOCAI_PROCESSOR = DOCAI_PROCESSOR_FALLBACK_FORM

DOCAI_RESULTS_TOPIC = os.environ["DOCAI_RESULTS_TOPIC"]  # topic name (not full path)
DOCAI_OUT_BUCKET = os.environ["DOCAI_OUT_BUCKET"]        # bucket name (no gs://)

LOCK_MINUTES = int(os.environ.get("DOCAI_LOCK_MINUTES", "15"))
DEFAULT_CASE = os.environ.get("DEFAULT_CASE", "").strip()

# =============================================================================
# Clients
# =============================================================================
_fs = firestore.Client(project=PROJECT_ID)
_storage = storage.Client(project=PROJECT_ID)
_publisher = pubsub_v1.PublisherClient()
_results_topic_path = _publisher.topic_path(PROJECT_ID, DOCAI_RESULTS_TOPIC)

# =============================================================================
# Helpers
# =============================================================================
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _ensure_trailing_slash(uri: str) -> str:
    return uri if uri.endswith("/") else (uri + "/")

def _parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not isinstance(gs_uri, str) or not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with gs://")
    rest = gs_uri[5:]
    bucket, _, name = rest.partition("/")
    if not bucket or not name:
        raise ValueError("invalid gs:// uri")
    return bucket, name

def _gcs_list(prefix_uri: str) -> List[str]:
    bkt, prefix = _parse_gs_uri(prefix_uri)
    bucket = _storage.bucket(bkt)
    out = []
    for blob in bucket.list_blobs(prefix=prefix):
        out.append(f"gs://{bkt}/{blob.name}")
    return out

def _gcs_read_json(gs_uri: str) -> dict:
    bkt, name = _parse_gs_uri(gs_uri)
    blob = _storage.bucket(bkt).blob(name)
    data = blob.download_as_bytes()
    return json.loads(data.decode("utf-8"))

def _write_json_gs(prefix_gs_uri: str, name: str, obj: Any):
    prefix_gs_uri = _ensure_trailing_slash(prefix_gs_uri)
    bkt, prefix = _parse_gs_uri(prefix_gs_uri)
    blob = _storage.bucket(bkt).blob(prefix + name)
    blob.upload_from_string(
        json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"),
        content_type="application/json; charset=utf-8",
    )

def _publish_results(payload: dict):
    _publisher.publish(
        _results_topic_path,
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )

def _acquire_lock(doc_id: str, pubsub_message_id: Optional[str]) -> bool:
    lock_ref = _fs.collection("docaiLocks").document(doc_id)
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=LOCK_MINUTES)

    @firestore.transactional
    def txn_fn(txn):
        snap = lock_ref.get(transaction=txn)
        if snap.exists:
            d = snap.to_dict() or {}
            exp = d.get("expiresAtUtc")
            if exp:
                try:
                    exp_dt = exp
                    if isinstance(exp, str):
                        exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                    if exp_dt > now:
                        return False
                except Exception:
                    pass

        txn.set(
            lock_ref,
            {
                "docId": doc_id,
                "lockedAtUtc": now.isoformat(),
                "expiresAtUtc": expires.isoformat(),
                "pubsubMessageId": pubsub_message_id,
                "version": VERSION,
            },
            merge=True,
        )
        return True

    txn = _fs.transaction()
    return txn_fn(txn)

def _release_lock(doc_id: str):
    try:
        _fs.collection("docaiLocks").document(doc_id).delete()
    except Exception:
        pass

def _unwrap_doc_payload(doc_like: dict) -> dict:
    # REST sometimes wraps as {"document": {...}}
    if not isinstance(doc_like, dict):
        return {}
    if "document" in doc_like and isinstance(doc_like.get("document"), dict):
        return doc_like["document"]
    return doc_like

def _doc_geometry_counts(doc_like: dict) -> Tuple[int, int]:
    # robust scan without deep traversal assumptions
    try:
        s = json.dumps(doc_like, ensure_ascii=False)
    except Exception:
        s = str(doc_like)
    return s.count('"boundingPoly"'), s.count('"textAnchor"')

def _compute_page_scores_from_document_layout(doc_like: dict):
    """
    Heuristic score (NOT DocAI confidence). Keeps your behavior but doesn’t replace raw storage.
    """
    if not isinstance(doc_like, dict):
        return [], 0.0

    doc = _unwrap_doc_payload(doc_like)

    # 1) Layout-only payloads (custom)
    layout = doc.get("documentLayout") or doc.get("document_layout") or {}
    blocks = layout.get("blocks") or []
    pages_acc = {}

    for b in blocks:
        if not isinstance(b, dict):
            continue
        span = b.get("pageSpan") or b.get("page_span") or {}
        p = span.get("pageStart") or span.get("page_start")
        if not p:
            continue
        tb = b.get("textBlock") or b.get("text_block") or {}
        txt = tb.get("text") or ""
        pages_acc.setdefault(p, {"page": p, "textLen": 0, "units": 0})
        pages_acc[p]["textLen"] += len(txt)
        pages_acc[p]["units"] += 1

    # 2) Full Document pages
    if not pages_acc:
        pages = doc.get("pages") if isinstance(doc, dict) else None
        if isinstance(pages, list):
            for i, p in enumerate(pages, start=1):
                if not isinstance(p, dict):
                    continue
                n_par = len(p.get("paragraphs") or [])
                n_blk = len(p.get("blocks") or [])
                n_lin = len(p.get("lines") or [])
                n_tok = len(p.get("tokens") or [])
                units = n_par + n_blk + n_lin + n_tok
                bp = json.dumps(p, ensure_ascii=False)
                approx_text = bp.count('"textAnchor"') * 15
                pages_acc[i] = {"page": i, "textLen": approx_text, "units": units}

    results = []
    for p in sorted(pages_acc.keys()):
        tl = pages_acc[p]["textLen"]
        units = pages_acc[p]["units"]
        s_len = min(1.0, tl / 3000.0)
        s_units = min(1.0, units / 40.0)
        score = round(s_len * s_units, 4)
        flags = []
        if tl < 50:
            flags.append("empty_or_near_empty")
        if units < 3:
            flags.append("low_structure")
        results.append({"page": p, "score": score, "textLen": tl, "numUnits": units, "flags": flags})

    if not results:
        return [], 0.0

    doc_score = round(sum(r["score"] for r in results) / len(results), 4)
    return results, doc_score

def _docai_online_process_pdf(source_pdf_uri: str) -> dict:
    """
    Online process using RAW bytes from GCS (RawDocument).
    This avoids client/library mismatch around inline_document/gcs_document fields.
    Returns the processed Document as dict (preserving proto field names).
    """
    client = documentai.DocumentProcessorServiceClient()

    # Download the source PDF bytes from GCS
    bkt, name = _parse_gs_uri(source_pdf_uri)
    blob = _storage.bucket(bkt).blob(name)
    pdf_bytes = blob.download_as_bytes()

    raw = documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf")
    req = documentai.ProcessRequest(
        name=DOCAI_PROCESSOR,
        raw_document=raw,
        skip_human_review=True,
    )

    result = client.process_document(request=req)
    return MessageToDict(result.document._pb, preserving_proto_field_name=True)
def _submit_batch_for_pdf(source_pdf_uri: str, output_gcs_prefix: str) -> str:
    client = documentai.DocumentProcessorServiceClient()
    gcs_documents = documentai.GcsDocuments(
        documents=[documentai.GcsDocument(gcs_uri=source_pdf_uri, mime_type="application/pdf")]
    )
    input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)
    output_config = documentai.DocumentOutputConfig(
        gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(gcs_uri=output_gcs_prefix)
    )
    req = documentai.BatchProcessRequest(
        name=DOCAI_PROCESSOR,
        input_documents=input_config,
        document_output_config=output_config,
    )
    op = client.batch_process_documents(request=req)
    return op.operation.name

def _wait_for_operation(op_name: str, timeout_seconds: int = 3600):
    client = documentai.DocumentProcessorServiceClient()
    op = ga_operation.Operation(
        op_name,
        client._transport.operations_client,
        documentai.BatchProcessResponse,
        documentai.BatchProcessMetadata,
    )
    op.result(timeout=timeout_seconds)
    return op

def _pick_batch_output_json(batch_output_prefix: str) -> Tuple[dict, List[str], str]:
    """
    Choose best JSON shard from batch output preferring geometry/pages.
    """
    out_files = _gcs_list(batch_output_prefix)
    json_files = [u for u in out_files if u.lower().endswith(".json")]

    if not json_files:
        raise RuntimeError(f"Batch finished but no .json output found under {batch_output_prefix}")

    json_files_sorted = sorted(json_files)
    best = None
    best_score = None

    for uri in json_files_sorted:
        try:
            raw = _gcs_read_json(uri)
        except Exception:
            continue

        doc = _unwrap_doc_payload(raw)
        bp, ta = _doc_geometry_counts(doc)
        has_geom = 1 if (bp > 0 and ta > 0) else 0

        pages = doc.get("pages") if isinstance(doc, dict) else None
        has_pages = 1 if isinstance(pages, list) and len(pages) > 0 else 0

        layout = doc.get("documentLayout") or doc.get("document_layout") or {}
        blocks = layout.get("blocks") if isinstance(layout, dict) else None
        has_blocks = 1 if isinstance(blocks, list) and len(blocks) > 0 else 0

        score = (has_geom, has_pages, has_blocks, uri)
        if (best_score is None) or (score > best_score):
            best_score = score
            best = (raw, uri)

    if not best:
        chosen = json_files_sorted[-1]
        doc_dict = _gcs_read_json(chosen)
        return doc_dict, json_files_sorted, chosen

    doc_dict, chosen = best
    return doc_dict, json_files_sorted, chosen

def _finalize_doc(
    doc_id: str,
    source_pdf_uri: str,
    pubsub_message_id: Optional[str],
    doc_dict: dict,
    mode_used: str,
    batch_output_prefix: Optional[str] = None,
    batch_json_files: Optional[List[str]] = None,
    batch_chosen_json: Optional[str] = None,
    case: Optional[str] = None,
    run_id: Optional[str] = None,
    docai_run_prefix: Optional[str] = None,
):
    ts = _utc_now_iso()
    case = (case or DEFAULT_CASE or "").strip()

    # Prefer contract-provided run prefix; fallback to per-doc prefix
    if docai_run_prefix and docai_run_prefix.startswith("gs://"):
        run_prefix_uri = _ensure_trailing_slash(docai_run_prefix)
    else:
        run_prefix_uri = f"gs://{DOCAI_OUT_BUCKET}/{doc_id}/{ts}/"

    latest_prefix_uri = f"gs://{DOCAI_OUT_BUCKET}/{doc_id}/latest/"

    # SAVE ALL: keep both envelope and unwrapped document
    doc_payload = _unwrap_doc_payload(doc_dict)
    pages, doc_score = _compute_page_scores_from_document_layout(doc_payload)

    bounding_poly_count, text_anchor_count = _doc_geometry_counts(doc_payload)
    has_geometry = bool(bounding_poly_count and text_anchor_count)

    # Persist raw artifacts
    _write_json_gs(run_prefix_uri, "docai_document.json", doc_payload)   # the actual Document (best for coords)
    _write_json_gs(run_prefix_uri, "docai_envelope.json", doc_dict)      # wrapper, if any (debug/back-compat)

    manifest = {
        "docId": doc_id,
        "case": case,
        "runId": run_id,
        "sourcePdfUri": source_pdf_uri,
        "processor": DOCAI_PROCESSOR,
        "createdAtUtc": ts,
        "docQualityScore": doc_score,
        "pageScores": pages,
        "hasGeometry": has_geometry,
        "geometryCounts": {"boundingPoly": bounding_poly_count, "textAnchor": text_anchor_count},
        "pubsubMessageId": pubsub_message_id,
        "version": VERSION,
        "mode": mode_used,
        "docaiRunPrefix": run_prefix_uri,
        "docaiDocumentJsonUri": run_prefix_uri + "docai_document.json",
        "docaiEnvelopeJsonUri": run_prefix_uri + "docai_envelope.json",
    }
    if mode_used == "batch":
        manifest["batchOutputPrefix"] = batch_output_prefix
        manifest["batchJsonFiles"] = batch_json_files or []
        manifest["batchChosenJson"] = batch_chosen_json

    _write_json_gs(run_prefix_uri, "manifest.json", manifest)
    _write_json_gs(latest_prefix_uri, "manifest.json", manifest)

    # Also write a single record for easy downstream ingestion
    try:
        link_rec = {
            "docId": doc_id,
            "case": case,
            "runId": run_id,
            "createdAtUtc": ts,
            "sourcePdfUri": source_pdf_uri,
            "docaiRunPrefix": run_prefix_uri,
            "manifestUri": run_prefix_uri + "manifest.json",
            "docaiDocumentJsonUri": run_prefix_uri + "docai_document.json",
            "processor": DOCAI_PROCESSOR,
            "mode": mode_used,
            "hasGeometry": has_geometry,
            "geometryCounts": {"boundingPoly": bounding_poly_count, "textAnchor": text_anchor_count},
        }
        _write_json_gs(run_prefix_uri, "link_index_record.json", link_rec)
    except Exception:
        pass

    _publish_results({
        "docId": doc_id,
        "case": case,
        "runId": run_id,
        "sourcePdfUri": source_pdf_uri,
        "docaiRunPrefix": run_prefix_uri,
        "manifestUri": run_prefix_uri + "manifest.json",
        "docaiDocumentJsonUri": run_prefix_uri + "docai_document.json",
        "docQualityScore": doc_score,
        "hasGeometry": has_geometry,
        "geometryCounts": {"boundingPoly": bounding_poly_count, "textAnchor": text_anchor_count},
        "mode": mode_used,
        "processor": DOCAI_PROCESSOR,
        "version": VERSION,
    })

def _process_one(
    doc_id: str,
    source_pdf_uri: str,
    pubsub_message_id: Optional[str],
    case: Optional[str],
    run_id: Optional[str],
    docai_run_prefix: Optional[str],
) -> dict:
    # 1) Online
    try:
        doc_dict = _docai_online_process_pdf(source_pdf_uri)
        _finalize_doc(
            doc_id=doc_id,
            source_pdf_uri=source_pdf_uri,
            pubsub_message_id=pubsub_message_id,
            doc_dict=doc_dict,
            mode_used="online",
            case=case,
            run_id=run_id,
            docai_run_prefix=docai_run_prefix,
        )
        return {"mode": "online", "ok": True}
    except gexceptions.InvalidArgument as e:
        msg = str(e).lower()
        if ("page limit" not in msg) and ("too many pages" not in msg):
            raise

    # 2) Batch fallback
    ts = _utc_now_iso()
    batch_output_prefix = f"gs://{DOCAI_OUT_BUCKET}/{doc_id}/{ts}/batch_output/"
    op_name = _submit_batch_for_pdf(source_pdf_uri, batch_output_prefix)
    _wait_for_operation(op_name, timeout_seconds=3600)

    doc_dict, json_files, chosen = _pick_batch_output_json(batch_output_prefix)
    _finalize_doc(
        doc_id=doc_id,
        source_pdf_uri=source_pdf_uri,
        pubsub_message_id=pubsub_message_id,
        doc_dict=doc_dict,
        mode_used="batch",
        batch_output_prefix=batch_output_prefix,
        batch_json_files=json_files,
        batch_chosen_json=chosen,
        case=case,
        run_id=run_id,
        docai_run_prefix=docai_run_prefix,
    )
    return {"mode": "batch", "ok": True, "operation": op_name, "batchChosenJson": chosen}

def _parse_pubsub_envelope(req_json: dict) -> Tuple[dict, Optional[str]]:
    msg = (req_json or {}).get("message") or {}
    data_b64 = msg.get("data") or ""
    mid = msg.get("messageId")
    if not data_b64:
        raise ValueError("missing Pub/Sub message.data")
    data = base64.b64decode(data_b64).decode("utf-8")
    payload = json.loads(data)
    return payload, mid

@functions_framework.http
def docai_runner(request: Request):
    try:
        req_json = request.get_json(silent=True) or {}
        is_pubsub = "message" in req_json and isinstance(req_json.get("message"), dict)

        if is_pubsub:
            payload, message_id = _parse_pubsub_envelope(req_json)
        else:
            payload = req_json
            message_id = None

        doc_id = payload.get("docId") or payload.get("doc_id")
        source_pdf_uri = payload.get("sourcePdfUri") or payload.get("source_pdf_uri")
        case = payload.get("case")
        run_id = payload.get("runId") or payload.get("run_id")
        docai_run_prefix = payload.get("docaiRunPrefix") or payload.get("docai_run_prefix")

        if not doc_id or not source_pdf_uri:
            return (json.dumps({"ok": False, "error": "missing docId or sourcePdfUri"}), 400, {"Content-Type": "application/json"})

        if not _acquire_lock(doc_id, message_id):
            return (json.dumps({"ok": True, "skipped": True, "reason": "lock_held"}), 200, {"Content-Type": "application/json"})

        try:
            result = _process_one(
                doc_id=doc_id,
                source_pdf_uri=source_pdf_uri,
                pubsub_message_id=message_id,
                case=case,
                run_id=run_id,
                docai_run_prefix=docai_run_prefix,
            )
            return (json.dumps({"ok": True, **result}), 200, {"Content-Type": "application/json"})
        finally:
            _release_lock(doc_id)

    except Exception as e:
        traceback.print_exc()
        return (json.dumps({"ok": False, "error": str(e)}), 500, {"Content-Type": "application/json"})

