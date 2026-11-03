"""Rule detection and end-to-end scoring over all 250 queue clauses (2026-11-03).

The main results score only the 166 clauses annotator A labelled; the 84 she
skipped as stating no rule (menus, contact details, fragments) are left out.
This script adds them back:

  detection   a system "predicts a rule" for a clause when any of the 12 fields
              is non-empty (definition fixed before the no-rule runs were made);
              precision, recall and F1 of the rule class, and how often a
              no-rule clause gets no values (specificity)
  end to end  field-level micro F1 over all clauses, where every value a system
              extracts from a no-rule clause is a false positive

Labelled clauses are deduplicated as in the main results (132); no-rule clauses
that duplicate one of them, or each other, are dropped.
The Claude systems were run on the no-rule clauses separately
(data/v1b_llm_<run>_norule.jsonl, same prompts and settings); the rule baseline
covers every clause. Qwen is not included until its no-rule runs exist.

Usage:
    python scripts/rule_detection.py
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

SYSTEMS = {  # name -> prediction files (labelled run, no-rule run)
    "Rule-based": ["data/policy_clauses_fine.jsonl"],
    **{name: [f"data/v1b_llm_{run}.jsonl", f"data/v1b_llm_{run}_norule.jsonl"] for name, run in (
        ("Sonnet 5.5", "sonnet55"), ("Sonnet 5.5 + cite", "cited_sonnet55"),
        ("Sonnet 5.5 + cite + ctx", "cited_ctx_sonnet55"), ("Opus 5.5", "opus55"),
        ("Opus 5.5 + cite", "cited_opus55"), ("Opus 5.5 + guide", "guided_opus55"),
        ("Opus 5.5 + cite + ctx", "cited_ctx_opus55"), ("Opus 5.5 + cite + guide", "cited_guided_opus55"))},
}
SAMPLES = 1000


def all_gold(db: Path, queue: list[dict]) -> list[dict]:
    """Every queue clause: A's labels, or all fields empty when A skipped it."""
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as con:
        rows = {rid: (status, json.loads(p)) for rid, p, status in
                con.execute("select record_id, payload_json, label_status from manual_labels")}
    gold = []
    for q in queue:
        status, payload = rows[q["record_id"]]
        rec = dict(payload) if status == "labeled" else {f: None for f in E.FIELDS}
        gold.append({**rec, "record_id": q["record_id"], "clause_text": q["clause_text"],
                     "document_id": q["document_id"], "has_rule": status == "labeled"})
    return gold


def predicts_rule(pred: dict | None) -> bool:
    """Primary definition, fixed before the no-rule runs: any field non-empty."""
    return pred is not None and any(not E.is_empty(pred.get(f)) for f in E.FIELDS)


def predicts_rule_lenient(pred: dict | None) -> bool:
    """Post hoc, chosen after seeing the outputs: topic "other" with action
    "unknown" and nothing else filled is read as "no rule" (the guide-prompted
    models answer most no-rule clauses this way). Reported as a sensitivity
    check only; A also uses topic "other" for real rules."""
    if not predicts_rule(pred):
        return False
    rest = [f for f in E.FIELDS if f not in ("policy_topic", "action")]
    return not (pred.get("policy_topic") in ("other", None) and pred.get("action") in ("unknown", None)
                and all(E.is_empty(pred.get(f)) for f in rest))


def detection(gold: list[dict], preds: dict[str, dict], rule=predicts_rule) -> dict:
    tp = sum(g["has_rule"] and rule(preds.get(g["record_id"])) for g in gold)
    fp = sum(not g["has_rule"] and rule(preds.get(g["record_id"])) for g in gold)
    pos = sum(g["has_rule"] for g in gold)
    neg = len(gold) - pos
    prec = tp / (tp + fp) if tp + fp else None
    rec = tp / pos
    return {"tp": tp, "fp": fp, "rules": pos, "no_rules": neg, "precision": prec, "recall": rec,
            "f1": 2 * prec * rec / (prec + rec) if prec and rec else 0.0, "specificity": (neg - fp) / neg}


def doc_bootstrap(gold: list[dict], stat) -> list[float]:
    """Document-level bootstrap interval of stat(gold subset)."""
    rng = random.Random(E.SEED)
    by_doc: dict[str, list[dict]] = {}
    for g in gold:
        by_doc.setdefault(g["document_id"], []).append(g)
    docs = sorted(by_doc)
    vals = sorted(stat([g for d in (rng.choice(docs) for _ in docs) for g in by_doc[d]]) for _ in range(SAMPLES))
    return [vals[int(0.025 * SAMPLES)], vals[int(0.975 * SAMPLES) - 1]]


def system_report(gold: list[dict], labelled: list[dict], preds: dict[str, dict]) -> dict:
    spurious_on_no_rule = sum(sum(c["spurious"] for c in E._record_counts(g, preds.get(g["record_id"])).values())
                              for g in gold if not g["has_rule"])
    return {"detection": detection(gold, preds),
            "detection_post_hoc_lenient": detection(gold, preds, rule=predicts_rule_lenient),
            "detection_f1_document_ci": doc_bootstrap(gold, lambda s: detection(s, preds)["f1"]),
            "values_on_no_rule_clauses": spurious_on_no_rule,
            "micro_f1_labelled_only": E.score(labelled, preds, bootstrap=0)["micro_f1"],
            "micro_f1_all_clauses": E.score(gold, preds, bootstrap=0)["micro_f1"],
            "micro_f1_all_clauses_document_ci": doc_bootstrap(
                gold, lambda s: E.score(s, preds, bootstrap=0)["micro_f1"] or 0.0)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_rule_detection.json"))
    args = ap.parse_args()

    every = all_gold(args.labels_db, E.load_queue(args.queue))
    # labelled clauses deduplicated exactly as in the main results; then no-rule
    # clauses that duplicate a kept clause (or each other) are dropped
    labelled = E.dedup([g for g in every if g["has_rule"]])
    gold = E.dedup(labelled + [g for g in every if not g["has_rule"]])
    report = {"n_clauses": len(gold), "n_rule": len(labelled), "n_no_rule": len(gold) - len(labelled),
              "systems": {}}
    for name, paths in SYSTEMS.items():
        preds = {rid: row for p in paths for rid, row in E.load_pred(Path(p)).items()}
        missing = [g["record_id"] for g in gold if g["record_id"] not in preds]
        report["systems"][name] = {**system_report(gold, labelled, preds), "missing_predictions": len(missing)}
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    pct = lambda x: f"{100 * x:5.1f}" if x is not None else "    -"  # noqa: E731
    print(f"{report['n_clauses']} clauses after near-duplicate removal: {report['n_rule']} with a rule, "
          f"{report['n_no_rule']} without")
    print(f"{'system':26s} {'prec':>5s} {'rec':>5s} {'F1':>5s} {'spec':>5s} {'FPvals':>6s} "
          f"{'F1 lab':>6s} {'F1 all':>6s} [doc CI]  missing")
    for name, r in report["systems"].items():
        d, ci = r["detection"], r["micro_f1_all_clauses_document_ci"]
        print(f"{name:26s} {pct(d['precision'])} {pct(d['recall'])} {pct(d['f1'])} {pct(d['specificity'])} "
              f"{r['values_on_no_rule_clauses']:6d} {pct(r['micro_f1_labelled_only'])} "
              f"{pct(r['micro_f1_all_clauses'])} [{pct(ci[0])},{pct(ci[1])}] {r['missing_predictions']}")
    print("post hoc (other/unknown only = no rule): detection F1 / specificity")
    for name, r in report["systems"].items():
        d = r["detection_post_hoc_lenient"]
        print(f"  {name:26s} {pct(d['f1'])} {pct(d['specificity'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
