from typing import Dict, List, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from api.labeling import router as labeling_router
from api.retrieval import default_clauses_path, get_hybrid_retriever, load_jsonl


def load_clause_records() -> List[dict]:
    return load_jsonl(default_clauses_path())


def summarize_sources(records: List[dict]) -> List[dict]:
    by_source: Dict[str, dict] = {}
    for rec in records:
        source_id = rec.get("source_id") or "unknown"
        entry = by_source.setdefault(
            source_id,
            {
                "source_id": source_id,
                "document_id": rec.get("document_id"),
                "document_type": rec.get("document_type"),
                "carrier": rec.get("carrier"),
                "supplier": rec.get("supplier"),
                "offer_or_circular_id": rec.get("offer_or_circular_id"),
                "effective_from": rec.get("effective_from"),
                "effective_to": rec.get("effective_to"),
                "record_count": 0,
            },
        )
        entry["record_count"] += 1
    return sorted(by_source.values(), key=lambda x: x["record_count"], reverse=True)


class AskRequest(BaseModel):
    query: str = Field(..., description="User question for retrieval")
    top_k: int = Field(5, ge=1, le=20)
    document_type: Optional[str] = None
    carrier: Optional[str] = None
    supplier: Optional[str] = None


class AskCitation(BaseModel):
    chunk_id: str
    source_id: str
    document_id: str
    document_type: Optional[str] = None
    carrier: Optional[str] = None
    supplier: Optional[str] = None
    offer_or_circular_id: Optional[str] = None
    section_title: Optional[str] = None
    chunk_text: str
    matched_clauses: List[dict]
    score: float
    score_breakdown: Dict[str, float]


class AskResponse(BaseModel):
    mode: str = "retrieval_only"
    query: str
    answer: str
    hits: int
    citations: List[AskCitation]

app = FastAPI(
    title="RAG Policy Assistant API",
    description="Backend service for retrieval-augmented travel policy assistant",
    version="0.1.0",
)
app.include_router(labeling_router)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/ask")
def ask_question(req: AskRequest):
    retriever = get_hybrid_retriever()
    filters = {
        "document_type": req.document_type,
        "carrier": req.carrier,
        "supplier": req.supplier,
    }
    hits = retriever.search(req.query, top_k=req.top_k, filters=filters)
    if not hits:
        return AskResponse(
            query=req.query,
            answer="No matching policy chunks were found in the indexed records.",
            hits=0,
            citations=[],
        )

    answer_lines = []
    citations = [
        _chunk_to_citation(hit, idx, answer_lines)
        for idx, hit in enumerate(hits, start=1)
    ]
    return AskResponse(
        query=req.query,
        answer="\n".join(answer_lines),
        hits=len(citations),
        citations=citations,
    )


@app.get("/sources")
def list_sources(
    limit: int = Query(50, ge=1, le=500),
    document_type: Optional[str] = None,
    carrier: Optional[str] = None,
    supplier: Optional[str] = None,
):
    records = load_clause_records()
    filtered = []
    for rec in records:
        if document_type and str(rec.get("document_type", "")).lower() != document_type.lower():
            continue
        if carrier and str(rec.get("carrier", "")).lower() != carrier.lower():
            continue
        if supplier and str(rec.get("supplier", "")).lower() != supplier.lower():
            continue
        filtered.append(rec)

    sources = summarize_sources(filtered)[:limit]
    return {
        "total_records": len(filtered),
        "total_sources": len(sources),
        "sources": sources,
    }


def _chunk_to_citation(hit: dict, idx: int, answer_lines: List[str]) -> AskCitation:
    chunk_meta = hit.get("metadata") or {}
    chunk_text = str(hit.get("text", "") or "")
    clauses = hit.get("clauses") or []
    matched_clauses = [
        {
            "record_id": clause.get("record_id"),
            "policy_topic": clause.get("policy_topic"),
            "action": clause.get("action"),
            "clause_text": clause.get("clause_text"),
        }
        for clause in clauses[:5]
    ]
    preview = " | ".join(
        c.get("clause_text", "") for c in matched_clauses[:2] if c.get("clause_text")
    ) or chunk_text
    answer_lines.append(f"{idx}. {preview}")

    primary_clause = clauses[0] if clauses else {}
    source_id = primary_clause.get("source_id") or chunk_meta.get("source_id") or "unknown"
    document_id = primary_clause.get("document_id") or chunk_meta.get("document_id") or source_id
    document_type = primary_clause.get("document_type") or chunk_meta.get("document_type")
    carrier = primary_clause.get("carrier") or chunk_meta.get("carrier")
    supplier = primary_clause.get("supplier") or chunk_meta.get("supplier")
    offer_or_circular_id = primary_clause.get("offer_or_circular_id") or chunk_meta.get("offer_or_circular_id")
    section_title = primary_clause.get("section_title") or chunk_meta.get("section_title")
    score_breakdown_raw = hit.get("_scores") or {}
    score_breakdown = {
        "hybrid": float(score_breakdown_raw.get("hybrid", 0.0)),
        "bm25": float(score_breakdown_raw.get("bm25", 0.0)),
        "vector": float(score_breakdown_raw.get("vector", 0.0)),
        "clause_boost": float(score_breakdown_raw.get("clause_boost", 0.0)),
    }
    return AskCitation(
        chunk_id=str(hit.get("chunk_id")),
        source_id=str(source_id),
        document_id=str(document_id),
        document_type=document_type,
        carrier=carrier,
        supplier=supplier,
        offer_or_circular_id=offer_or_circular_id,
        section_title=section_title,
        chunk_text=chunk_text,
        matched_clauses=matched_clauses,
        score=score_breakdown["hybrid"],
        score_breakdown=score_breakdown,
    )
