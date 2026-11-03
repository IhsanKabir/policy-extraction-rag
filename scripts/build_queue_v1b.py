"""Build the v1b labelling queue (2026-10-28).

v1b = the 62 records already labelled in v1, kept first and in their original
order, plus a seeded random sample of new records from the expanded corpus:

  * fare-rule genre (the main set): Nok Air, Air Arabia, AirAsia, Qatar
    Airways, SpiceJet
  * policy-text genre (a smaller genre-shift set): Oman Air, Malaysia Airlines,
    Thai Airways, Singapore Airlines, Emirates

Before sampling, candidates are cleaned and the counts are written to a report
for the paper's data section:
  * short   : under 20 normalised characters (fragments)
  * long    : over 700 characters (failed atomic splits)
  * junk    : website chrome (cookie banners, copyright, menus, social links)
  * dup     : same normalised text as an already-labelled or earlier record
              (chunks overlap, so the same clause repeats)

Sampling is uniform within each document, so no extraction method influences
which records are selected.

Usage:
    python scripts/build_queue_v1b.py --labels-db data/labeling_fine.db
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import re
import sqlite3
from pathlib import Path

SEED = 20261028

FARE_RULE_QUOTA = {  # source_id -> new records to sample (None = all available)
    "src_008": None, "src_010": None,  # Nok Air (remaining from v1)
    "src_012": 25,  # Air Arabia
    "src_013": 20,  # AirAsia
    "src_024": 25,  # Qatar Airways (trade-facing public penalties page)
    "src_014": 8,   # SpiceJet (FAQ prose)
}
POLICY_TEXT_QUOTA = {
    "src_011": 14,  # Oman Air conditions of carriage
    "src_023": 7,   # Malaysia Airlines T&C
    "src_027": 6,   # Thai Airways reservation & ticketing T&C
    "src_022": 3,   # Singapore Airlines cancellations & refunds
    "src_026": 5,   # Emirates cancel/change FAQ
}

JUNK = re.compile(
    r"(?i)copyright|©|all rights? reserved|cookie|consent|skip to|search flights|"
    r"follow us|subscribe|newsletter|download now|switch to app|choose your region|"
    r"business licen[cs]e|back button|filter icon|plan travel flying with us|"
    r"privacy (policy|notice|statement)|instagram|facebook|twitter|youtube"
)


def norm(text: str | None) -> str:
    return re.sub(r"\W+", " ", (text or "").lower()).strip()


def load_jsonl(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def queue_row(clause: dict, chunk: dict) -> dict:
    """Same row shape as evals.real_pilot_labeling.build_labeling_queue."""
    return {
        "record_id": clause.get("record_id"),
        "source_id": clause.get("source_id"),
        "document_id": clause.get("document_id"),
        "document_type": clause.get("document_type"),
        "carrier": clause.get("carrier"),
        "supplier": clause.get("supplier"),
        "offer_or_circular_id": clause.get("offer_or_circular_id"),
        "chunk_id": clause.get("chunk_id"),
        "chunk_text": chunk.get("text"),
        "clause_text": clause.get("clause_text"),
        "heuristic": {k: clause.get(k) for k in (
            "policy_topic", "action", "penalty_type", "penalty_amount", "penalty_currency",
            "penalty_percent", "fare_difference_required", "time_window",
            "before_after_departure", "tax_refund_rule")}
        | {"eligibility_conditions": clause.get("eligibility_conditions", []),
           "exceptions": clause.get("exceptions", [])},
        "manual_labeling_note": "v1b queue: label the CLAUSE only; the chunk is context.",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--old-queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_fine.jsonl"))
    ap.add_argument("--chunks", type=Path, default=Path("data/chunks.jsonl"))
    ap.add_argument("--clauses", type=Path, default=Path("data/policy_clauses_fine.jsonl"))
    ap.add_argument("--output", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1b.jsonl"))
    ap.add_argument("--report", type=Path, default=Path("data/queue_v1b_selection_report.json"))
    args = ap.parse_args()

    with sqlite3.connect(f"file:{args.labels_db}?mode=ro", uri=True) as con:
        labelled = {rid for (rid,) in con.execute("select record_id from manual_labels")}

    old_queue = load_jsonl(args.old_queue)
    kept = [r for r in old_queue if r["record_id"] in labelled]
    missing = labelled - {r["record_id"] for r in kept}
    if missing:
        raise SystemExit(f"{len(missing)} labelled records are not in the old queue: {sorted(missing)[:5]}")

    chunks = {c["chunk_id"]: c for c in load_jsonl(args.chunks)}
    clauses = load_jsonl(args.clauses)
    quota = FARE_RULE_QUOTA | POLICY_TEXT_QUOTA

    seen = {norm(r["clause_text"]) for r in kept}
    excluded = collections.Counter()
    available: dict[str, list[dict]] = collections.defaultdict(list)
    for c in clauses:
        sid = c.get("source_id")
        if c["record_id"] in labelled or sid not in quota:
            continue
        text = c.get("clause_text") or ""
        n = norm(text)
        reason = ("short" if len(n) < 20 else "long" if len(text) > 700
                  else "junk" if JUNK.search(text) else "dup" if n in seen else None)
        if reason:
            excluded[f"{sid}:{reason}"] += 1
            continue
        seen.add(n)
        available[sid].append(c)

    rng = random.Random(SEED)
    new_rows: list[dict] = []
    sampled_counts = {}
    for sid, k in quota.items():
        pool = available.get(sid, [])
        pick = pool if k is None or k >= len(pool) else rng.sample(pool, k)
        pick.sort(key=lambda c: (c["chunk_id"], c["record_id"]))
        sampled_counts[sid] = {"available": len(pool), "sampled": len(pick)}
        new_rows += [queue_row(c, chunks[c["chunk_id"]]) for c in pick]

    rows = kept + new_rows
    args.output.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    report = {
        "seed": SEED,
        "labelled_kept_first": len(kept),
        "new_records": len(new_rows),
        "total": len(rows),
        "genre": {"fare_rule": sorted(FARE_RULE_QUOTA), "policy_text": sorted(POLICY_TEXT_QUOTA)},
        "per_source": sampled_counts,
        "excluded_before_sampling": dict(sorted(excluded.items())),
    }
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
