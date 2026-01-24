import base64
import json
import os
from datetime import datetime, timezone

from google.cloud import documentai_v1 as documentai
from google.cloud import firestore
from google.cloud import storage
from google.cloud import pubsub_v1

PROJECT_ID = os.environ["PROJECT_ID"]
DOCAI_LOCATION = os.environ["DOCAI_LOCATION"]   # e.g. "us"
DOCAI_PROCESSOR = os.environ["DOCAI_PROCESSOR"] # full resource name: projects/.../locations/.../processors/...
DOCAI_RESULTS_TOPIC = os.environ["DOCAI_RESULTS_TOPIC"]
DOCAI_OUT_BUCKET = os.environ["DOCAI_OUT_BUCKET"]

storage_client = storage.Client()
fs = firestore.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, DOCAI_RESULTS_TOPIC)

def _compute_page_scores_from_document_layout(doc_like: dict):
    # Works with your observed Layout Parser output: documentLayout.blocks[*].pageSpan + textBlock.text
    layout = doc_like.get("documentLayout") or {}
    blocks = layout.get("blocks") or []

    # Aggregate per page
    pages = {}
    for b in blocks:
        span = b.get("pageSpan") or {}
        p = span.get("pageStart")
        if not p:
            continue
        txt = ((b.get("textBlock") or {}).get("text")) or ""
        pages.setdefault(p, {"page": p, "textLen": 0, "numBlocks": 0})
        pages[p]["textLen"] += len(txt)
        pages[p]["numBlocks"] += 1

    # Score heuristic v0
    results = []
    for p in sorted(pages.keys()):
        tl = pages[p]["textLen"]
        nb = pages[p]["numBlocks"]
        s_len = min(1.0, tl / 3000.0)
        s_blk = min(1.0, nb / 10.0)
        score = round(s_len * s_blk, 4)

        flags = []
        if tl < 50:
            flags.append("empty_or_near_empty")

        results.append({
            "page": p,
            "score": score,
            "textLen": tl,
            "numBlocks": nb,
            "flags": flags,
        })

    if not results:
        return [], 0.0

    doc_score = round(sum(r["score"] for r in results) / len(results), 4)
    return results, doc_score

def _write_json_to_gcs(prefix: str, obj_name: str, payload: dict):
    b = storage_client.bucket(DOCAI_OUT_BUCKET)
    blob = b.blob(f"{prefix}/{obj_name}")
    blob.upload_from_string(json.dumps(payload, ensure_ascii=False), content_type="application/json")

def docai_runner(cloud_event):
    # Eventarc Pub/Sub CloudEvent: data.message.data is base64
    data = cloud_event.data
    message = data.get("message") or {}
    b64 = message.get("data")
    if not b64:
        print("No Pub/Sub data found")
        return ("bad event", 400)

    msg = json.loads(base64.b64decode(b64).decode("utf-8"))
    doc_id = msg["docId"]
    canonical_gcs_uri = msg["canonicalGcsUri"]

    # Update status
    doc_ref = fs.collection("documents").document(doc_id)
    doc_ref.set({"status": "DOCAI_RUNNING", "updatedAt": firestore.SERVER_TIMESTAMP}, merge=True)

    # Use Document AI ONLINE PROCESS (v0). For large PDFs we can switch to batchProcess later.
    client = documentai.DocumentProcessorServiceClient()
    name = DOCAI_PROCESSOR

    req = documentai.ProcessRequest(
        name=name,
        raw_document=None,
        inline_document=None,
        gcs_document=documentai.GcsDocument(gcs_uri=canonical_gcs_uri, mime_type="application/pdf"),
    )

    result = client.process_document(request=req)

    # Convert proto to dict
    # result.document is a proto; easiest is to use MessageToDict
    from google.protobuf.json_format import MessageToDict
    doc_dict = MessageToDict(result.document._pb, preserving_proto_field_name=True)

    # Compute quality scores from documentLayout if present
    pages, doc_score = _compute_page_scores_from_document_layout(doc_dict)

    # Write outputs to GCS
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"{doc_id}/{ts}"
    _write_json_to_gcs(prefix, "docai_document.json", doc_dict)

    manifest = {
        "docId": doc_id,
        "canonicalGcsUri": canonical_gcs_uri,
        "rawGcsUri": msg.get("rawGcsUri"),
        "processor": DOCAI_PROCESSOR,
        "createdAtUtc": ts,
        "docQualityScore": doc_score,
    }
    _write_json_to_gcs(prefix, "manifest.json", manifest)

    # Persist to Firestore
    doc_ref.set({
        "status": "DOCAI_DONE",
        "docai": {
            "outputPrefix": f"gs://{DOCAI_OUT_BUCKET}/{prefix}/",
            "docQualityScore": doc_score,
            "pages": pages,
            "processor": DOCAI_PROCESSOR,
        },
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }, merge=True)

    # Emit results message (for future SearchApp indexer)
    out_msg = {
        "docId": doc_id,
        "docaiOutputPrefix": f"gs://{DOCAI_OUT_BUCKET}/{prefix}/",
        "docQualityScore": doc_score,
        "canonicalGcsUri": canonical_gcs_uri,
    }
    publisher.publish(topic_path, json.dumps(out_msg).encode("utf-8"))

    print(f"OK docId={doc_id} docScore={doc_score} pages={len(pages)} out=gs://{DOCAI_OUT_BUCKET}/{prefix}/")
    return ("ok", 200)
