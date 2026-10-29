"""Label-provenance audit for the v1c gold set (2026-10-29).

Several consistency passes over the gold labels were drafted with Claude's help
and approved by the annotator. Because Claude models are also evaluated on this
gold set, the paper must show the model ranking does not depend on those edits.

For every record, the label as first entered by the annotator is the record's
value in the earliest snapshot that contains it (snapshots are taken before
each fix pass). A record is "touched" when its label fields in the final
database differ from that first-entered value. Methods are then scored on the
untouched records only.

Limitation: snapshots cannot detect advice given while a label was first being
entered (e.g. assistant "watch for" tips during a batch); only post-hoc edits.

Usage:
    python scripts/audit_label_provenance.py \
        --snapshot ../labeling_fine_backup_2026-10-27.db \
        --snapshot ../labeling_fine_BEFORE_FIXES_2026-10-27.db \
        --snapshot data/labeling_fine_BEFORE_AUDIT_2026-10-28.db \
        --snapshot data/backups/BEFORE_BATCH2_FIX_2026-10-28_labeling_fine.db \
        --snapshot data/backups/BEFORE_FINAL_FIX_2026-10-29_labeling_fine.db \
        --snapshot data/backups/BEFORE_V1C_REVIEW_2026-10-29_labeling_fine.db \
        --final data/labeling_fine.db
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402

# Payload keys that identify or annotate a record but are not labels.
NON_LABEL_KEYS = {"record_id", "source_id", "document_id", "document_type", "carrier",
                  "supplier", "chunk_id", "clause_text", "notes", "updated_at"}

METHODS = {  # method name -> prediction file
    "Rule-based": "data/policy_clauses_fine.jsonl",
    "Qwen 7B": "data/v1b_llm_qwen7b.jsonl",
    "Qwen 7B + cite": "data/v1b_llm_cited_qwen7b.jsonl",
    "Qwen 7B + cite + guide": "data/v1b_llm_cited_guided_qwen7b.jsonl",
    "Sonnet 5.5": "data/v1b_llm_sonnet55.jsonl",
    "Sonnet 5.5 + cite": "data/v1b_llm_cited_sonnet55.jsonl",
    "Sonnet 5.5 + cite + ctx": "data/v1b_llm_cited_ctx_sonnet55.jsonl",
    "Opus 5.5": "data/v1b_llm_opus55.jsonl",
    "Opus 5.5 + cite": "data/v1b_llm_cited_opus55.jsonl",
    "Opus 5.5 + guide": "data/v1b_llm_guided_opus55.jsonl",
    "Opus 5.5 + cite + guide": "data/v1b_llm_cited_guided_opus55.jsonl",
}


def load_snapshot(path: Path) -> dict[str, dict]:
    """record_id -> label view (label fields + label_status), read-only."""
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute("SELECT record_id, payload_json, label_status FROM manual_labels").fetchall()
    finally:
        conn.close()
    return {rid: label_view(json.loads(payload), status) for rid, payload, status in rows}


def label_view(payload: dict, status: str | None) -> dict:
    """Label fields only, with empty values dropped so '' / [] / None compare equal."""
    view = {k: v for k, v in payload.items() if k not in NON_LABEL_KEYS and not E.is_empty(v)}
    view["label_status"] = status
    return view


def first_entered(snapshots: list[dict[str, dict]]) -> dict[str, dict]:
    """Each record's value in the earliest snapshot that contains it."""
    first: dict[str, dict] = {}
    for snap in snapshots:
        for rid, view in snap.items():
            first.setdefault(rid, view)
    return first


def changed_fields(before: dict, after: dict) -> list[str]:
    return sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))


def classify(first: dict[str, dict], final: dict[str, dict]) -> dict[str, list[str] | None]:
    """record_id -> changed fields ([] = untouched); None when no snapshot holds the record."""
    return {rid: (changed_fields(first[rid], view) if rid in first else None)
            for rid, view in final.items()}


# (baseline, comparison) pairs behind the paper's claims, re-tested per subset.
PAIRED = [
    ("Rule-based", "Opus 5.5"), ("Qwen 7B", "Opus 5.5"), ("Sonnet 5.5", "Opus 5.5"),
    ("Qwen 7B", "Qwen 7B + cite"), ("Sonnet 5.5", "Sonnet 5.5 + cite"),
    ("Opus 5.5", "Opus 5.5 + cite"), ("Opus 5.5", "Opus 5.5 + guide"),
    ("Opus 5.5 + cite", "Opus 5.5 + cite + guide"),
]
PAIRED_SAMPLES = 2000


def paired_delta(gold: list[dict], base_path: Path, comp_path: Path) -> dict:
    """Comparison minus baseline (micro F1/P/R) with a paired bootstrap 95% CI."""
    base_pred, comp_pred = E.load_pred(base_path), E.load_pred(comp_path)
    base = [E._record_counts(g, base_pred.get(g["record_id"])) for g in gold]
    comp = [E._record_counts(g, comp_pred.get(g["record_id"])) for g in gold]
    keys = ("micro_f1", "micro_precision", "micro_recall")

    def deltas(idx: list[int]) -> dict:
        a = E._aggregate([base[i] for i in idx])
        b = E._aggregate([comp[i] for i in idx])
        return {k: (b[k] or 0.0) - (a[k] or 0.0) for k in keys}

    rng = random.Random(E.SEED)
    n = len(gold)
    point = deltas(list(range(n)))
    samples = [deltas([rng.randrange(n) for _ in range(n)]) for _ in range(PAIRED_SAMPLES)]
    return {k: {"delta": point[k], "ci95": E._ci([s[k] for s in samples])} for k in keys}


def score_subset(gold: list[dict], pred_path: Path) -> dict:
    res = E.score(gold, E.load_pred(pred_path))
    return {"n": res["n_records"], "micro_f1": res["micro_f1"], "ci95": res["ci95"]["micro_f1"],
            "topic": res["topic_accuracy"], "action": res["action_accuracy"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--snapshot", action="append", required=True, type=Path,
                    help="pre-fix snapshot DB, oldest first (repeatable)")
    ap.add_argument("--final", required=True, type=Path)
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_label_provenance.json"))
    args = ap.parse_args()

    final = load_snapshot(args.final)
    status = classify(first_entered([load_snapshot(p) for p in args.snapshot]), final)
    if any(v is None for v in status.values()):
        missing = [rid for rid, v in status.items() if v is None]
        print(f"error: {len(missing)} final records are in no snapshot, e.g. {missing[:3]}", file=sys.stderr)
        return 1

    gold = E.dedup(E.load_gold(args.final, E.load_queue(args.queue)))
    subsets = {
        "all_dedup": gold,
        "untouched_dedup": [g for g in gold if not status[g["record_id"]]],
        "touched_dedup": [g for g in gold if status[g["record_id"]]],
    }
    field_counts: dict[str, int] = {}
    for fields in status.values():
        for f in fields:
            field_counts[f] = field_counts.get(f, 0) + 1

    scores = {m: {name: score_subset(sub, Path(p)) for name, sub in subsets.items()}
              for m, p in METHODS.items()}
    paired = {f"{comp} - {base}": {name: paired_delta(sub, Path(METHODS[base]), Path(METHODS[comp]))
                                   for name, sub in subsets.items() if name != "touched_dedup"}
              for base, comp in PAIRED}
    report = {
        "snapshots": [str(p) for p in args.snapshot], "final": str(args.final),
        "records_total": len(status),
        "records_touched": sum(1 for v in status.values() if v),
        "changed_field_counts": dict(sorted(field_counts.items(), key=lambda kv: -kv[1])),
        "subset_sizes": {k: len(v) for k, v in subsets.items()},
        "touched_record_ids": sorted(rid for rid, v in status.items() if v),
        "scores": scores,
        "paired": paired,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"touched {report['records_touched']}/{report['records_total']} records; "
          f"subsets {report['subset_sizes']}")
    print(f"{'method':26s} " + " ".join(f"{k:>24s}" for k in subsets))
    for m, by in scores.items():
        cells = [f"{100*s['micro_f1']:5.1f} [{100*s['ci95'][0]:.1f}-{100*s['ci95'][1]:.1f}]" for s in by.values()]
        print(f"{m:26s} " + " ".join(f"{c:>24s}" for c in cells))
    print("\npaired micro F1 delta [95% CI]   (* = CI excludes 0)")
    for label, by in paired.items():
        cells = []
        for name, d in by.items():
            f1 = d["micro_f1"]
            lo, hi = f1["ci95"]
            sig = "*" if lo > 0 or hi < 0 else " "
            cells.append(f"{name}: {100*f1['delta']:+5.1f} [{100*lo:+.1f},{100*hi:+.1f}]{sig}")
        print(f"{label:38s} " + "   ".join(cells))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
