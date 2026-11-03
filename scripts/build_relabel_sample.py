"""Build the blind self-relabel sample (intra-annotator agreement, 2026-10-29).

The first 62 v1 records were labelled with the heuristic's guess pre-filled.
Annotator A relabels a random sample of them blind (empty form, heuristic
hidden) into a separate database; scripts/iaa_agreement.py then compares the
two passes. Low agreement on fields the heuristic pre-filled would point to
anchoring.

Usage:
    python scripts/build_relabel_sample.py --labels-db data/labeling_fine.db
    python scripts/iaa_agreement.py --a-db data/labeling_fine.db \
        --b-db data/labeling_relabel_a.db --iaa-queue data/queue_relabel_a.jsonl \
        --out results/v1c_self_relabel.json
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from scripts.build_iaa_sample import blind_row  # noqa: E402

SEED = 20261031
N_PREFILLED = 62  # queue positions 1-62 were labelled with heuristic pre-fill
N_SAMPLE = 30


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels-db", type=Path, required=True)
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--out-queue", type=Path, default=Path("data/queue_relabel_a.jsonl"))
    args = ap.parse_args()

    if args.out_queue.exists():
        print(f"error: {args.out_queue} exists; the sample must not change once relabelling has started", file=sys.stderr)
        return 1

    queue = E.load_queue(args.queue)
    prefilled = queue[:N_PREFILLED]
    candidates = E.dedup(E.load_gold(args.labels_db, prefilled))
    by_id = {q["record_id"]: q for q in prefilled}
    chosen = random.Random(SEED).sample([g["record_id"] for g in candidates], min(N_SAMPLE, len(candidates)))
    with open(args.out_queue, "w", encoding="utf-8") as f:
        for rid in chosen:
            f.write(json.dumps(blind_row(by_id[rid]), ensure_ascii=False) + "\n")
    print(f"wrote {len(chosen)} of {len(candidates)} unique pre-filled labelled records to {args.out_queue}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
