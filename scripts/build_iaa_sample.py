"""Build the second-annotator (IAA) sample for the v1c gold set (2026-10-29).

An independent annotator (B) labels a stratified sample blind, so the paper can
report inter-annotator agreement and re-score every method against labels that
neither annotator A nor any AI assistant influenced.

Sample (seed 20261030):
  * labelled records: drawn from the deduplicated v1c gold, stratified by
    genre x topic of A's label (largest-remainder allocation);
  * skipped records: unique skipped clauses, so the skip decision is tested too.

The queue is shuffled and the heuristic's guess is removed from every row, so
B sees only the clause, its chunk, carrier and document type.

Usage:
    python scripts/build_iaa_sample.py --labels-db data/labeling_fine.db
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402

SEED = 20261030
N_LABELLED = 42
N_SKIPPED = 8
QUEUE_KEYS = ("record_id", "source_id", "document_id", "document_type", "carrier",
              "supplier", "chunk_id", "chunk_text", "clause_text")


def allocate(strata: dict[str, int], total: int) -> dict[str, int]:
    """Largest-remainder proportional allocation, at least 1 per non-empty stratum."""
    size = sum(strata.values())
    raw = {k: total * n / size for k, n in strata.items()}
    alloc = {k: max(1, int(v)) for k, v in raw.items()}
    by_remainder = sorted(strata, key=lambda k: raw[k] - int(raw[k]), reverse=True)
    i = 0
    while sum(alloc.values()) < total:
        k = by_remainder[i % len(by_remainder)]
        if alloc[k] < strata[k]:
            alloc[k] += 1
        i += 1
    while sum(alloc.values()) > total:  # minimum-1 rule can overshoot
        k = max(alloc, key=lambda s: alloc[s] - raw[s])
        alloc[k] -= 1
    return {k: min(v, strata[k]) for k, v in alloc.items()}


def blind_row(queue_row: dict) -> dict:
    """Queue row with everything except the source text and its metadata removed."""
    row = {k: queue_row.get(k) for k in QUEUE_KEYS}
    row["heuristic"] = {}
    return row


def skipped_ids(db_path: Path, queue: list[dict]) -> list[str]:
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True) as con:
        skipped = {rid for (rid,) in con.execute(
            "SELECT record_id FROM manual_labels WHERE label_status = 'skip'")}
    seen: set[str] = set()
    out = []
    for q in queue:
        norm = E._norm_text(q.get("clause_text") or "")
        if q["record_id"] in skipped and norm not in seen:
            seen.add(norm)
            out.append(q["record_id"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels-db", type=Path, required=True)
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--out-queue", type=Path, default=Path("data/queue_iaa_b.jsonl"))
    ap.add_argument("--out-report", type=Path, default=Path("data/queue_iaa_b_report.json"))
    args = ap.parse_args()

    if args.out_queue.exists():
        print(f"error: {args.out_queue} exists; the sample must not change once B has started", file=sys.stderr)
        return 1

    queue = E.load_queue(args.queue)
    by_id = {q["record_id"]: q for q in queue}
    gold = E.dedup(E.load_gold(args.labels_db, queue))
    rng = random.Random(SEED)

    strata: dict[str, list[str]] = collections.defaultdict(list)
    for g in gold:
        strata[f"{g['genre']}|{g.get('policy_topic') or 'none'}"].append(g["record_id"])
    alloc = allocate({k: len(v) for k, v in strata.items()}, N_LABELLED)
    labelled = [rid for k in sorted(strata) for rid in rng.sample(strata[k], alloc[k])]
    skipped = rng.sample(skipped_ids(args.labels_db, queue), N_SKIPPED)

    chosen = labelled + skipped
    rng.shuffle(chosen)
    args.out_queue.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_queue, "w", encoding="utf-8") as f:
        for rid in chosen:
            f.write(json.dumps(blind_row(by_id[rid]), ensure_ascii=False) + "\n")

    report = {"seed": SEED, "n_labelled": len(labelled), "n_skipped": len(skipped),
              "strata_population": {k: len(v) for k, v in sorted(strata.items())},
              "strata_sampled": dict(sorted(alloc.items())),
              "labelled_ids": labelled, "skipped_ids": skipped}
    args.out_report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {len(chosen)} records to {args.out_queue} ({len(labelled)} labelled by A, {len(skipped)} skipped by A)")
    for k, n in sorted(alloc.items()):
        print(f"  {k:32s} {n:2d} of {len(strata[k])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
