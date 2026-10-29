"""Build the v1c labelling queue: v1b (208 records, unchanged order) + two
disclosed additions that address known gaps in v1b.

1. Purposive Singapore Airlines (SQ) addition: the v1b random sample drew only
   website boilerplate from the SQ page, so SQ had 0 labelled rules. v1c adds
   every SQ clause that states a refund/cancellation rule (selection rule:
   PURPOSIVE_SQ_IDS below, chosen by reading the 13 unsampled SQ clauses and
   keeping the 7 complete rule sentences; fragments and navigation excluded).
   Report these separately from the random sample.
2. Random policy-text expansion: v1b had only 17 labelled policy-text
   records. v1c samples more from the policy-text documents with the same
   cleaning as v1b (short/long/junk/duplicate filters) and a new seed.

Usage:
    python scripts/build_queue_v1c.py
"""
from __future__ import annotations

import collections
import json
import random
from pathlib import Path

from build_queue_v1b import JUNK, load_jsonl, norm, queue_row

SEED = 20261030
V1B_QUEUE = Path("data/real_pilot_manual_labeling_queue_v1b.jsonl")
OUT = Path("data/real_pilot_manual_labeling_queue_v1c.jsonl")
REPORT = Path("data/queue_v1c_selection_report.json")

PURPOSIVE_SQ_IDS = [
    "3a983975-6872-4192-8e7f-ae57b44c694c:0",
    "3a983975-6872-4192-8e7f-ae57b44c694c:1",
    "3a983975-6872-4192-8e7f-ae57b44c694c:2",
    "3a983975-6872-4192-8e7f-ae57b44c694c:3",
    "3a983975-6872-4192-8e7f-ae57b44c694c:4",
    "3a983975-6872-4192-8e7f-ae57b44c694c:5",
    "3a983975-6872-4192-8e7f-ae57b44c694c:6"
]
SQ_SOURCE = "src_022"
POLICY_TEXT_QUOTA = {"src_011": 12, "src_023": 10, "src_027": 7, "src_026": 6}


def main() -> int:
    v1b = load_jsonl(V1B_QUEUE)
    chunks = {c["chunk_id"]: c for c in load_jsonl(Path("data/chunks.jsonl"))}
    clauses = load_jsonl(Path("data/policy_clauses_fine.jsonl"))
    in_queue = {r["record_id"] for r in v1b}
    seen = {norm(r["clause_text"]) for r in v1b}

    # 1. purposive SQ: the chunk whose clauses carry the refund/cancellation rules
    sq_ids = PURPOSIVE_SQ_IDS
    by_id = {c["record_id"]: c for c in clauses}
    sq_rows = []
    for rid in sq_ids:
        c = by_id[rid]
        if rid in in_queue or norm(c["clause_text"]) in seen:
            continue
        seen.add(norm(c["clause_text"]))
        sq_rows.append(queue_row(c, chunks[c["chunk_id"]]))

    # 2. random policy-text expansion with v1b's cleaning
    excluded = collections.Counter()
    pools = collections.defaultdict(list)
    for c in clauses:
        sid = c.get("source_id")
        if sid not in POLICY_TEXT_QUOTA or c["record_id"] in in_queue:
            continue
        text = c.get("clause_text") or ""
        n = norm(text)
        reason = ("short" if len(n) < 20 else "long" if len(text) > 700
                  else "junk" if JUNK.search(text) else "dup" if n in seen else None)
        if reason:
            excluded[f"{sid}:{reason}"] += 1
            continue
        seen.add(n)
        pools[sid].append(c)
    rng = random.Random(SEED)
    pt_rows, sampled = [], {}
    for sid, k in POLICY_TEXT_QUOTA.items():
        pick = rng.sample(pools[sid], min(k, len(pools[sid])))
        pick.sort(key=lambda c: (c["chunk_id"], c["record_id"]))
        sampled[sid] = {"available": len(pools[sid]), "sampled": len(pick)}
        pt_rows += [queue_row(c, chunks[c["chunk_id"]]) for c in pick]

    for r in sq_rows:
        r["manual_labeling_note"] = "v1c purposive SQ addition: label the CLAUSE only."
    for r in pt_rows:
        r["manual_labeling_note"] = "v1c random policy-text addition: label the CLAUSE only."
    rows = v1b + sq_rows + pt_rows
    OUT.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    report = {"seed": SEED, "v1b_records_first": len(v1b), "purposive_sq": len(sq_rows),
              "purposive_sq_record_ids": [r["record_id"] for r in sq_rows],
              "random_policy_text": len(pt_rows), "per_source": sampled,
              "excluded_before_sampling": dict(sorted(excluded.items())), "total": len(rows)}
    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
