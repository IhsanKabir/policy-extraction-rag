"""Retrieval before extraction, scored against the existing human labels (2026-11-03).

The paper's extraction results give every system the right clause. This script
measures the step a real pipeline needs first: finding that clause in the
document. Queries are one per (document, topic) for which annotator A labelled
at least one clause with that topic (topic "other" has no query); the query
wording per topic is fixed in QUERIES and was not tuned on the results.

Two retrieval units are searched within each document, with BM25, a dense
encoder and their reciprocal-rank fusion:
  clause  the atomic clauses (data/policy_clauses_fine.jsonl, exact-duplicate
          texts removed); strict, since many clauses are fragments of a rule
  chunk   the passages the clauses were split from (data/chunks.jsonl); a
          labelled clause is found when its chunk is retrieved, as in a
          retrieve-then-extract pipeline

The labelled clauses are a sample of each document, so recall of the
known-relevant clauses is measured, not precision; at clause level, precision
among the clauses that were judged at all (labelled or skipped) is reported as
well. End-to-end, a clause's existing extraction counts only if the clause was
found, which gives field recall at k without new model calls.

Two steps, so the scored numbers reproduce exactly without re-embedding:
    python scripts/retrieval_eval.py --rank     # writes data/retrieval_runs.jsonl
    python scripts/retrieval_eval.py            # scores it -> results/v1c_retrieval.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sqlite3
import statistics
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))  # build_queue_v1c imports its sibling directly
from evals import v1b_eval as E  # noqa: E402
from scripts.build_queue_v1c import PURPOSIVE_SQ_IDS  # noqa: E402

QUERIES = {
    "refund": "Can the ticket be refunded, and what refund fee or penalty applies?",
    "cancellation": "What happens if the booking is cancelled, and what cancellation fee applies?",
    "reissue_change": "Can the flight or travel date be changed, and what change fee applies?",
    "no_show": "What happens if the passenger does not show up for the flight?",
    "go_show": "Can the passenger fly on an earlier flight on the day of travel?",
    "waiver": "When are change or refund fees waived?",
}
DENSE_MODEL = "BAAI/bge-small-en-v1.5"
DENSE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
RETRIEVERS = ("bm25", "dense", "hybrid")
LEVELS = {"clause": (1, 3, 5, 10, 20), "chunk": (1, 2, 3, 5)}  # level -> cut-offs k
DEPTH = 50  # rankings stored per query
RRF_K = 60
BM25_K1, BM25_B = 1.5, 0.75
V1_PREFIX = 62  # first queue positions: v1 records, not part of the uniform random sample
PREDICTIONS = {"Rule baseline": "data/policy_clauses_fine.jsonl",
               "Opus 5.5 + cite + guide": "data/v1b_llm_cited_guided_opus55.jsonl"}


# ---------------------------------------------------------------------------
# Units, keys and queries
def unit_key(level: str, row: dict) -> str:
    """What counts as the same unit: the clause's normalised text (hashed, so
    rankings carry no clause text) or the clause's chunk."""
    if level == "clause":
        return hashlib.sha1(E._norm_text(row["clause_text"]).encode("utf-8")).hexdigest()[:16]
    return row["chunk_id"]


def load_units(clauses_path: Path, chunks_path: Path) -> dict:
    """units[level][doc] = [{"key", "text"}], each unit once per document."""
    chunk_text = {c["chunk_id"]: c["text"] for c in _jsonl(chunks_path)}
    units: dict = {level: {} for level in LEVELS}
    seen: set = set()
    for row in E.load_pred(clauses_path).values():
        if not E._norm_text(row["clause_text"]):
            continue
        for level in LEVELS:
            k = unit_key(level, row)
            if (level, row["document_id"], k) not in seen:
                seen.add((level, row["document_id"], k))
                text = row["clause_text"] if level == "clause" else chunk_text[row["chunk_id"]]
                units[level].setdefault(row["document_id"], []).append({"key": k, "text": text})
    return units


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def build_queries(gold: list[dict], doc_of: dict[str, str]) -> list[dict]:
    groups: dict[tuple[str, str], list[str]] = {}
    for g in gold:
        topic = g.get("policy_topic")
        if topic in QUERIES:
            groups.setdefault((doc_of[g["record_id"]], topic), []).append(g["record_id"])
    return [{"query_id": f"{doc}:{topic}", "document_id": doc, "topic": topic, "relevant": ids}
            for (doc, topic), ids in sorted(groups.items())]


# ---------------------------------------------------------------------------
# Retrievers: each returns unit indices, best first
def tokenize(text: str) -> list[str]:
    return [t for t in E._norm_text(text).split() if len(t) > 1]


def bm25_rank(query: str, texts: list[str]) -> list[int]:
    docs = [tokenize(t) for t in texts]
    avgdl = sum(map(len, docs)) / max(len(docs), 1)
    df: dict[str, int] = {}
    for d in docs:
        for tok in set(d):
            df[tok] = df.get(tok, 0) + 1
    n, q = len(docs), set(tokenize(query))
    scores = []
    for d in docs:
        s = 0.0
        for tok in q & set(d):
            tf, idf = d.count(tok), math.log(1 + (n - df[tok] + 0.5) / (df[tok] + 0.5))
            s += idf * tf * (BM25_K1 + 1) / (tf + BM25_K1 * (1 - BM25_B + BM25_B * len(d) / avgdl))
        scores.append(s)
    return sorted(range(n), key=lambda i: (-scores[i], i))


def dense_rank(query_vec, doc_vecs) -> list[int]:
    scores = doc_vecs @ query_vec
    return sorted(range(len(scores)), key=lambda i: (-float(scores[i]), i))


def rrf(*rankings: list[int]) -> list[int]:
    score: dict[int, float] = {}
    for ranking in rankings:
        for r, i in enumerate(ranking):
            score[i] = score.get(i, 0.0) + 1 / (RRF_K + r + 1)
    return sorted(score, key=lambda i: (-score[i], i))


def rank_all(queries: list[dict], units: dict) -> list[dict]:
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(DENSE_MODEL)
    runs = []
    for level, by_doc in units.items():
        for doc in sorted({q["document_id"] for q in queries}):
            items = by_doc[doc]
            texts = [u["text"] for u in items]
            doc_vecs = model.encode(texts, normalize_embeddings=True, batch_size=32)
            for q in (q for q in queries if q["document_id"] == doc):
                text = QUERIES[q["topic"]]
                qvec = model.encode(DENSE_QUERY_PREFIX + text, normalize_embeddings=True)
                ranked = {"bm25": bm25_rank(text, texts), "dense": dense_rank(qvec, doc_vecs)}
                ranked["hybrid"] = rrf(ranked["bm25"], ranked["dense"])
                for name, order in ranked.items():
                    runs.append({"level": level, "query_id": q["query_id"], "retriever": name,
                                 "pool_size": len(items), "ranking": [items[i]["key"] for i in order[:DEPTH]]})
    return runs


# ---------------------------------------------------------------------------
# Scoring
def judgments(db: Path, queue: list[dict]) -> dict[str, set[str]]:
    """Clause unit key -> topics A labelled it with ("skip" if skipped)."""
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as con:
        rows = {rid: (status, json.loads(p)) for rid, p, status in
                con.execute("select record_id, payload_json, label_status from manual_labels")}
    out: dict[str, set[str]] = {}
    for q in queue:
        status, payload = rows.get(q["record_id"], (None, None))
        if status in ("labeled", "skipped"):
            topic = payload.get("policy_topic") if status == "labeled" else "skip"
            out.setdefault(unit_key("clause", q), set()).add(topic)
    return out


def found(relevant: list[str], ranking: list[str], k: int, key: dict) -> set[str]:
    """Relevant record ids whose unit is among the top k unit keys."""
    top = set(ranking[:k])
    return {rid for rid in relevant if key[rid] in top}


def bootstrap_ratio(pairs: list[tuple[int, int]], samples: int = E.BOOTSTRAP_SAMPLES) -> list[float]:
    rng = random.Random(E.SEED)
    vals = sorted(sum(h for h, _ in s) / sum(n for _, n in s)
                  for s in ([rng.choice(pairs) for _ in pairs] for _ in range(samples)))
    return [vals[int(0.025 * samples)], vals[int(0.975 * samples) - 1]]


def recall_stats(queries, runs, k, key, keep=lambda rid: True) -> dict:
    pairs, success, capped = [], [], []
    for q in queries:
        rel = [r for r in q["relevant"] if keep(r)]
        if not rel:
            continue
        hit = found(rel, runs[q["query_id"]]["ranking"], k, key)
        pairs.append((len(hit), len(rel)))
        success.append(bool(hit))
        distinct = {key[r] for r in rel}
        capped.append(len({key[r] for r in hit}) / min(k, len(distinct)))
    return {"recall": sum(h for h, _ in pairs) / sum(n for _, n in pairs), "recall_ci": bootstrap_ratio(pairs),
            "success": statistics.mean(success), "capped_recall": statistics.mean(capped)}


def judged_precision(queries, runs, k, key, judged) -> dict:
    rel = judged_n = 0
    for q in queries:
        for u in runs[q["query_id"]]["ranking"][:k]:
            topics = judged.get(u)
            if topics:
                judged_n += 1
                rel += q["topic"] in topics
    return {"judged": judged_n, "relevant": rel, "precision": rel / judged_n if judged_n else None}


def random_recall(queries, k) -> float:
    """Expected recall of a random ordering: each relevant clause's unit is in the top k with probability k/N."""
    n_rel = sum(len(q["relevant"]) for q in queries)
    return sum(len(q["relevant"]) * min(k, q["pool_size"]) / q["pool_size"] for q in queries) / n_rel


def field_recall(queries, runs, k, key, gold_by_id, preds) -> float:
    counts = []
    for q in queries:
        hit = found(q["relevant"], runs[q["query_id"]]["ranking"], k, key) if runs else set(q["relevant"])
        counts += [E._record_counts(gold_by_id[r], preds.get(r) if r in hit else None) for r in q["relevant"]]
    return E._aggregate(counts)["micro_recall"]


def score_level(level, queries, runs_rows, key, ctx) -> dict:
    sizes = {r["query_id"]: r["pool_size"] for r in runs_rows}
    queries = [{**q, "pool_size": sizes[q["query_id"]]} for q in queries]
    out = {"pool_units": sum({r["query_id"].split(":")[0]: r["pool_size"] for r in runs_rows}.values()),
           "random_order_recall": {k: random_recall(queries, k) for k in LEVELS[level]}, "retrievers": {}}
    for name in RETRIEVERS:
        runs = {r["query_id"]: r for r in runs_rows if r["retriever"] == name}
        res = {}
        for k in LEVELS[level]:
            res[k] = {**recall_stats(queries, runs, k, key),
                      "random_sample": recall_stats(queries, runs, k, key, keep=lambda rid: rid in ctx["random_ids"]),
                      "field_recall": {m: field_recall(queries, runs, k, key, ctx["gold_by_id"], p)
                                       for m, p in ctx["preds"].items()}}
            if level == "clause":
                res[k]["judged_precision"] = judged_precision(queries, runs, k, key, ctx["judged"])
        out["retrievers"][name] = res
    return out


def score(queries, runs_rows, key, gold, queue, judged, preds) -> dict:
    ctx = {"random_ids": {q["record_id"] for q in queue[V1_PREFIX:]} - set(PURPOSIVE_SQ_IDS),
           "gold_by_id": {g["record_id"]: g for g in gold}, "judged": judged, "preds": preds}
    report = {"n_queries": len(queries), "n_relevant": sum(len(q["relevant"]) for q in queries),
              "n_relevant_random_sample": sum(r in ctx["random_ids"] for q in queries for r in q["relevant"]),
              "queries": {t: QUERIES[t] for t in sorted({q["topic"] for q in queries})},
              "dense_model": DENSE_MODEL,
              "field_recall_oracle": {m: field_recall(queries, {}, 0, key["clause"], ctx["gold_by_id"], p)
                                      for m, p in preds.items()},
              "levels": {}}
    for level in LEVELS:
        rows = [r for r in runs_rows if r["level"] == level]
        report["levels"][level] = score_level(level, queries, rows, key[level], ctx)
    return report


def print_report(report: dict) -> None:
    pct = lambda x: f"{100 * x:5.1f}"  # noqa: E731
    print(f"{report['n_queries']} queries, {report['n_relevant']} known-relevant clauses "
          f"({report['n_relevant_random_sample']} from the random sample); oracle field recall "
          + " / ".join(pct(v) for v in report["field_recall_oracle"].values()))
    for level, lv in report["levels"].items():
        print(f"\n[{level}] {lv['pool_units']} units. recall [CI] | random-sample recall | success | capped | "
              "judged prec | field recall (rules / opus)")
        for k in LEVELS[level]:
            print(f"  k={k:<2} random order {pct(lv['random_order_recall'][k])}")
            for name in RETRIEVERS:
                r = lv["retrievers"][name][k]
                jp = r.get("judged_precision", {}).get("precision")
                print(f"        {name:6s} {pct(r['recall'])} [{pct(r['recall_ci'][0])}, {pct(r['recall_ci'][1])}] | "
                      f"{pct(r['random_sample']['recall'])} | {pct(r['success'])} | {pct(r['capped_recall'])} | "
                      f"{pct(jp) if jp is not None else '    -'} | "
                      + " / ".join(pct(v) for v in r["field_recall"].values()))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rank", action="store_true", help="run the retrievers and write --runs")
    ap.add_argument("--clauses", type=Path, default=Path("data/policy_clauses_fine.jsonl"))
    ap.add_argument("--chunks", type=Path, default=Path("data/chunks.jsonl"))
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--runs", type=Path, default=Path("data/retrieval_runs.jsonl"))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_retrieval.json"))
    args = ap.parse_args()

    queue = E.load_queue(args.queue)
    gold = E.dedup(E.load_gold(args.labels_db, queue))
    queries = build_queries(gold, {q["record_id"]: q["document_id"] for q in queue})
    if args.rank:
        rows = rank_all(queries, load_units(args.clauses, args.chunks))
        args.runs.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        print(f"wrote {len(rows)} rankings for {len(queries)} queries to {args.runs}")
        return 0

    preds = {m: E.load_pred(Path(p)) for m, p in PREDICTIONS.items()}
    key = {level: {q["record_id"]: unit_key(level, q) for q in queue} for level in LEVELS}
    report = score(queries, _jsonl(args.runs), key, gold, queue, judgments(args.labels_db, queue), preds)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
