import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_chunks_path() -> Path:
    primary = _repo_root() / "data" / "chunks.jsonl"
    if primary.exists():
        return primary
    fallback = _repo_root() / "data" / "pilot_eval_chunks.jsonl"
    return fallback


def default_clauses_path() -> Path:
    return _repo_root() / "data" / "policy_clauses.jsonl"


def load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 1]


def _safe_float(value: Optional[float]) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class HybridRetrieverBundle:
    chunks_path: Path
    clauses_path: Path
    chunks_mtime_ns: int
    clauses_mtime_ns: int
    retriever: "InMemoryHybridChunkRetriever"


class InMemoryHybridChunkRetriever:
    """Hybrid chunk retriever using BM25 + TF-IDF cosine with clause-aware boosting."""

    def __init__(self, chunk_records: List[dict], clause_records: List[dict]):
        self.chunk_records = chunk_records
        self.clause_records = clause_records
        self.chunk_by_id: Dict[str, dict] = {str(r.get("chunk_id")): dict(r) for r in chunk_records if r.get("chunk_id")}
        self.clauses_by_chunk: Dict[str, List[dict]] = {}
        for clause in clause_records:
            cid = str(clause.get("chunk_id") or "")
            if not cid:
                continue
            self.clauses_by_chunk.setdefault(cid, []).append(clause)

        # attach clause lists to chunks
        for chunk_id, chunk in self.chunk_by_id.items():
            chunk["clauses"] = self.clauses_by_chunk.get(chunk_id, [])

        self._doc_ids: List[str] = []
        self._doc_texts: List[str] = []
        self._doc_tokens: List[List[str]] = []
        self._doc_tf: List[Dict[str, int]] = []
        self._doc_lens: List[int] = []
        self._df: Dict[str, int] = {}
        self._avgdl: float = 0.0
        self._tfidf_vectorizer = None
        self._tfidf_matrix = None

        self._build_indexes()

    def _combined_chunk_text(self, chunk: dict) -> str:
        chunk_text = str(chunk.get("text", "") or "")
        clauses = chunk.get("clauses") or []
        clause_text = " ".join(str(c.get("clause_text", "") or "") for c in clauses)
        topic_action_text = " ".join(
            f"{c.get('policy_topic', '')} {c.get('action', '')}".replace("_", " ")
            for c in clauses
        )
        return " ".join(p for p in [chunk_text, clause_text, topic_action_text] if p).strip()

    def _build_indexes(self):
        docs = [chunk for chunk in self.chunk_by_id.values()]
        for chunk in docs:
            chunk_id = str(chunk.get("chunk_id"))
            combined_text = self._combined_chunk_text(chunk)
            tokens = tokenize(combined_text)
            tf: Dict[str, int] = {}
            for tok in tokens:
                tf[tok] = tf.get(tok, 0) + 1
            seen = set(tokens)
            for tok in seen:
                self._df[tok] = self._df.get(tok, 0) + 1

            self._doc_ids.append(chunk_id)
            self._doc_texts.append(combined_text)
            self._doc_tokens.append(tokens)
            self._doc_tf.append(tf)
            self._doc_lens.append(len(tokens))

        if self._doc_lens:
            self._avgdl = sum(self._doc_lens) / len(self._doc_lens)

        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
        except Exception:
            return

        if self._doc_texts:
            self._tfidf_vectorizer = TfidfVectorizer(lowercase=True, ngram_range=(1, 2))
            self._tfidf_matrix = self._tfidf_vectorizer.fit_transform(self._doc_texts)

    def _bm25_score(self, query: str, doc_index: int, k1: float = 1.5, b: float = 0.75) -> float:
        tokens = tokenize(query)
        if not tokens or not self._doc_lens:
            return 0.0
        tf = self._doc_tf[doc_index]
        dl = self._doc_lens[doc_index]
        n_docs = len(self._doc_lens)
        score = 0.0
        for tok in tokens:
            f = tf.get(tok, 0)
            if f == 0:
                continue
            df = self._df.get(tok, 0)
            idf = math.log(1 + ((n_docs - df + 0.5) / (df + 0.5)))
            denom = f + k1 * (1 - b + b * (dl / (self._avgdl or 1.0)))
            score += idf * ((f * (k1 + 1)) / (denom or 1.0))
        return score

    def _vector_scores(self, query: str) -> List[float]:
        if not self._doc_ids or not self._tfidf_vectorizer or self._tfidf_matrix is None:
            return [0.0 for _ in self._doc_ids]
        q_vec = self._tfidf_vectorizer.transform([query])
        # sparse dot product; rows are L2-normalized by TfidfVectorizer by default
        sims = (self._tfidf_matrix @ q_vec.T).toarray().ravel()
        return [float(x) for x in sims]

    def _clause_boost(self, query: str, chunk: dict) -> float:
        q_tokens = set(tokenize(query))
        if not q_tokens:
            return 0.0
        boost = 0.0
        for clause in chunk.get("clauses", []):
            clause_text = str(clause.get("clause_text", "") or "")
            clause_tokens = set(tokenize(clause_text))
            overlap = len(q_tokens & clause_tokens)
            if overlap:
                boost += min(1.5, 0.3 * overlap)
            topic = str(clause.get("policy_topic", "") or "").replace("_", " ").lower()
            if topic and topic in query.lower():
                boost += 0.5
            action = str(clause.get("action", "") or "").replace("_", " ").lower()
            if action and action in query.lower():
                boost += 0.2
        return boost

    def _match_filters(self, chunk: dict, filters: Dict[str, str]) -> bool:
        chunk_meta = chunk.get("metadata") or {}
        clauses = chunk.get("clauses", [])
        for key, expected in filters.items():
            expected_lower = expected.lower()
            # try chunk metadata first
            chunk_val = chunk_meta.get(key)
            if chunk_val is not None and str(chunk_val).lower() == expected_lower:
                continue
            # fallback to top-level on chunk (some test fixtures)
            if str(chunk.get(key, "") or "").lower() == expected_lower:
                continue
            # fallback to any clause metadata
            if any(str(cl.get(key, "") or "").lower() == expected_lower for cl in clauses):
                continue
            return False
        return True

    def search(self, query: str, top_k: int = 5, filters: Optional[Dict[str, str]] = None) -> List[dict]:
        filters = {k: v for k, v in (filters or {}).items() if v}
        vector_scores = self._vector_scores(query)

        candidates = []
        for i, chunk_id in enumerate(self._doc_ids):
            chunk = self.chunk_by_id[chunk_id]
            if filters and not self._match_filters(chunk, filters):
                continue
            bm25 = self._bm25_score(query, i)
            vec = vector_scores[i] if i < len(vector_scores) else 0.0
            clause_boost = self._clause_boost(query, chunk)
            if bm25 <= 0 and vec <= 0 and clause_boost <= 0:
                continue
            hybrid_score = (1.0 * bm25) + (4.0 * vec) + clause_boost
            candidates.append(
                (
                    hybrid_score,
                    {
                        **chunk,
                        "_scores": {
                            "hybrid": hybrid_score,
                            "bm25": bm25,
                            "vector": vec,
                            "clause_boost": clause_boost,
                        },
                    },
                )
            )

        candidates.sort(key=lambda x: x[0], reverse=True)
        return [row for _, row in candidates[:top_k]]


_RETRIEVER_CACHE: Optional[HybridRetrieverBundle] = None


def _mtime_ns(path: Path) -> int:
    return path.stat().st_mtime_ns if path.exists() else -1


def get_hybrid_retriever(
    chunks_path: Optional[Path] = None,
    clauses_path: Optional[Path] = None,
) -> InMemoryHybridChunkRetriever:
    global _RETRIEVER_CACHE
    chunks_path = chunks_path or Path(os.getenv("POLICY_CHUNKS_PATH", str(default_chunks_path())))
    clauses_path = clauses_path or Path(os.getenv("POLICY_CLAUSES_PATH", str(default_clauses_path())))
    chunks_mtime = _mtime_ns(chunks_path)
    clauses_mtime = _mtime_ns(clauses_path)

    if (
        _RETRIEVER_CACHE
        and _RETRIEVER_CACHE.chunks_path == chunks_path
        and _RETRIEVER_CACHE.clauses_path == clauses_path
        and _RETRIEVER_CACHE.chunks_mtime_ns == chunks_mtime
        and _RETRIEVER_CACHE.clauses_mtime_ns == clauses_mtime
    ):
        return _RETRIEVER_CACHE.retriever

    chunk_records = load_jsonl(chunks_path)
    clause_records = load_jsonl(clauses_path)
    retriever = InMemoryHybridChunkRetriever(chunk_records, clause_records)
    _RETRIEVER_CACHE = HybridRetrieverBundle(
        chunks_path=chunks_path,
        clauses_path=clauses_path,
        chunks_mtime_ns=chunks_mtime,
        clauses_mtime_ns=clauses_mtime,
        retriever=retriever,
    )
    return retriever

