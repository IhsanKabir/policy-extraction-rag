"""Inter-annotator agreement between annotator A (gold) and B (IAA sample).

Reports, on the IAA sample:
  * skip-decision agreement and Cohen's kappa over all sampled records;
  * Cohen's kappa for policy_topic and action on records both annotators labelled;
  * field-level micro F1 between A and B with the v1b scorer's typed matching
    (micro F1 is symmetric, so it does not matter which side is "gold");
  * every method re-scored against B's labels and against A's, on the same
    records, so a model advantage that only exists on A's labels shows up.

Usage:
    python scripts/iaa_agreement.py --a-db data/labeling_fine.db --b-db data/labeling_iaa_b.db
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from scripts.audit_label_provenance import METHODS  # noqa: E402


def cohen_kappa(a: list, b: list) -> float | None:
    """Cohen's kappa for two equal-length label lists; None when undefined."""
    if not a or len(a) != len(b):
        return None
    n = len(a)
    observed = sum(x == y for x, y in zip(a, b)) / n
    ca, cb = collections.Counter(a), collections.Counter(b)
    expected = sum(ca[k] * cb[k] for k in ca.keys() | cb.keys()) / (n * n)
    if expected == 1:
        return None
    return (observed - expected) / (1 - expected)


def statuses(db_path: Path) -> dict[str, str]:
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True) as con:
        return dict(con.execute("SELECT record_id, label_status FROM manual_labels"))


def agreement(gold_a: list[dict], gold_b: list[dict]) -> dict:
    b_by_id = {g["record_id"]: g for g in gold_b}
    pairs = [(a, b_by_id[a["record_id"]]) for a in gold_a if a["record_id"] in b_by_id]
    per = [E._record_counts(a, b) for a, b in pairs]
    agg = E._aggregate(per) if per else {}
    return {
        "n_both_labelled": len(pairs),
        "topic_kappa": cohen_kappa([a.get("policy_topic") for a, _ in pairs], [b.get("policy_topic") for _, b in pairs]),
        "action_kappa": cohen_kappa([a.get("action") for a, _ in pairs], [b.get("action") for _, b in pairs]),
        "topic_agreement": agg.get("topic_accuracy"),
        "action_agreement": agg.get("action_accuracy"),
        "field_micro_f1": agg.get("micro_f1"),
        "field_f1": {f: v["f1"] for f, v in agg.get("fields", {}).items()},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--a-db", type=Path, required=True)
    ap.add_argument("--b-db", type=Path, required=True)
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--iaa-queue", type=Path, default=Path("data/queue_iaa_b.jsonl"))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_iaa.json"))
    args = ap.parse_args()

    sample = [q["record_id"] for q in E.load_queue(args.iaa_queue)]
    st_a, st_b = statuses(args.a_db), statuses(args.b_db)
    unlabelled = [rid for rid in sample if rid not in st_b]
    if unlabelled:
        print(f"warning: B has not labelled {len(unlabelled)}/{len(sample)} sampled records", file=sys.stderr)
    done = [rid for rid in sample if rid in st_b]

    queue = [q for q in E.load_queue(args.queue) if q["record_id"] in set(done)]
    gold_a, gold_b = E.load_gold(args.a_db, queue), E.load_gold(args.b_db, queue)
    both = {g["record_id"] for g in gold_a} & {g["record_id"] for g in gold_b}
    on_both_a = [g for g in gold_a if g["record_id"] in both]
    on_both_b = [g for g in gold_b if g["record_id"] in both]

    skip_a = [st_a.get(rid) == "skip" for rid in done]
    skip_b = [st_b.get(rid) == "skip" for rid in done]
    report = {
        "n_sampled": len(sample), "n_done_by_b": len(done),
        "skip_agreement": (sum(x == y for x, y in zip(skip_a, skip_b)) / len(done)) if done else None,
        "skip_kappa": cohen_kappa(skip_a, skip_b),
        **agreement(gold_a, gold_b),
        "methods": {m: {"vs_A": E.score(on_both_a, E.load_pred(Path(p)))["micro_f1"],
                        "vs_B": E.score(on_both_b, E.load_pred(Path(p)))["micro_f1"]}
                    for m, p in METHODS.items()},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    fmt = lambda x: "n/a" if x is None else f"{x:.3f}"  # noqa: E731
    print(f"B labelled {len(done)}/{len(sample)}; both labelled {report['n_both_labelled']}")
    print(f"skip kappa {fmt(report['skip_kappa'])}  topic kappa {fmt(report['topic_kappa'])}  "
          f"action kappa {fmt(report['action_kappa'])}  field micro F1 A-vs-B {fmt(report['field_micro_f1'])}")
    print(f"{'method':26s} {'vs A':>6s} {'vs B':>6s}")
    for m, s in report["methods"].items():
        print(f"{m:26s} {100*(s['vs_A'] or 0):6.1f} {100*(s['vs_B'] or 0):6.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
