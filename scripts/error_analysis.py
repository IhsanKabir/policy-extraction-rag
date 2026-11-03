"""Error analysis of v1c predictions against the gold labels (2026-11-02).

For one method (default: Opus 5.5 + cite + guide) on the headline view (all
gold, near-duplicates removed), every scored field is classed as
  correct  - gold present, prediction matches;
  wrong    - both present, no match;
  missed   - gold present, prediction empty;
  spurious - gold empty, prediction present.
It also gives topic and action confusions, and the same counts per genre.

For every citation-gated method, each value the gate nulled (model asserted it,
no quote grounded it) is classed against gold: a "cost" when the raw value was
right, a "benefit" when it was wrong or gold is empty. This is the mechanism
behind the citation-gate result.

Counts go to results/v1c_error_analysis.json. Example clauses (airline text)
go to handoff/error_examples.md, which is gitignored.

Usage:
    python scripts/error_analysis.py
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from scripts.audit_label_provenance import METHODS, paired_delta_preds  # noqa: E402

KINDS = ("correct", "wrong", "missed", "spurious")
EXAMPLES_PER_CELL = 6


def classify(field: str, gold, pred) -> str | None:
    """Error kind for one field, or None when both sides are empty."""
    if E.is_empty(gold):
        return None if E.is_empty(pred) else "spurious"
    if E.is_empty(pred):
        return "missed"
    return "correct" if E.field_match(field, gold, pred)[0] else "wrong"


def field_errors(gold: list[dict], preds: dict[str, dict]) -> tuple[dict, list[dict]]:
    counts = {f: collections.Counter() for f in E.FIELDS}
    errors = []
    for g in gold:
        p = preds.get(g["record_id"]) or {}
        for f in E.FIELDS:
            kind = classify(f, g.get(f), p.get(f))
            if kind:
                counts[f][kind] += 1
            if kind and kind != "correct":
                errors.append({"record_id": g["record_id"], "genre": g["genre"], "carrier": g["carrier"],
                               "field": f, "kind": kind, "gold": g.get(f), "pred": p.get(f),
                               "clause": g["clause_text"]})
    return {f: dict(c) for f, c in counts.items()}, errors


def confusion(gold: list[dict], preds: dict[str, dict], field: str) -> dict[str, int]:
    pairs = collections.Counter()
    for g in gold:
        if E.is_empty(g.get(field)):
            continue
        p = (preds.get(g["record_id"]) or {}).get(field)
        if str(p).lower() != str(g[field]).lower():
            pairs[f"{g[field]} -> {p if not E.is_empty(p) else '(empty)'}"] += 1
    return dict(pairs.most_common())


def gate_effect(gold: list[dict], preds: dict[str, dict]) -> dict:
    """Class every value the citation gate nulled by what gold says about it."""
    out = {f: collections.Counter() for f in E.FIELDS}
    for g in gold:
        p = preds.get(g["record_id"]) or {}
        raw = p.get("model_raw_fields") or {}
        nulled = [f for f, v in ((p.get("grounding") or {}).get("fields") or {}).items()
                  if not v.get("grounded")]
        for f in nulled:
            if f not in out or E.is_empty(raw.get(f)):
                continue
            kind = classify(f, g.get(f), raw.get(f))
            out[f]["cost: right value removed" if kind == "correct" else
                   "benefit: spurious value removed" if kind == "spurious" else
                   "benefit: wrong value removed"] += 1
    total = sum((c for c in out.values()), collections.Counter())
    return {"total": dict(total), "by_field": {f: dict(c) for f, c in out.items() if c}}


def ungated(preds: dict[str, dict]) -> dict[str, dict]:
    """The same run with the citation gate off: the model's raw values for every field."""
    return {rid: {**p, **{f: (p.get("model_raw_fields") or {}).get(f, p.get(f)) for f in E.FIELDS}}
            for rid, p in preds.items()}


def gate_ablation(gold: list[dict], preds: dict[str, dict]) -> dict:
    """Micro F1 of a cited run with and without the gate, and the paired delta."""
    raw = ungated(preds)
    return {"gated_f1": E.score(gold, preds, bootstrap=0)["micro_f1"],
            "ungated_f1": E.score(gold, raw, bootstrap=0)["micro_f1"],
            "gate_delta": paired_delta_preds(gold, raw, preds)["micro_f1"]}


def write_examples(path: Path, method: str, errors: list[dict]) -> None:
    cells = collections.defaultdict(list)
    for e in errors:
        cells[(e["field"], e["kind"])].append(e)
    lines = [f"# Error examples: {method} (local only, contains airline text)\n"]
    for (field, kind), items in sorted(cells.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"\n## {field} / {kind} ({len(items)})\n")
        for e in items[:EXAMPLES_PER_CELL]:
            lines.append(f"- [{e['carrier']}] `{e['record_id']}` gold=`{e['gold']}` pred=`{e['pred']}`\n"
                         f"  > {' '.join(str(e['clause']).split())[:300]}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--method", default="Opus 5.5 + cite + guide", choices=list(METHODS))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_error_analysis.json"))
    ap.add_argument("--examples", type=Path, default=Path("handoff/error_examples.md"))
    args = ap.parse_args()

    gold = E.dedup(E.load_gold(args.labels_db, E.load_queue(args.queue)))
    preds = E.load_pred(Path(METHODS[args.method]))
    counts, errors = field_errors(gold, preds)
    by_genre = {genre: field_errors([g for g in gold if g["genre"] == genre], preds)[0]
                for genre in ("fare_rule", "policy_text")}
    cited = {m: E.load_pred(Path(p)) for m, p in METHODS.items() if "cite" in m}
    gates = {m: {**gate_effect(gold, p), **gate_ablation(gold, p)} for m, p in cited.items()}
    report = {"method": args.method, "n_records": len(gold), "fields": counts, "by_genre": by_genre,
              "topic_confusion": confusion(gold, preds, "policy_topic"),
              "action_confusion": confusion(gold, preds, "action"),
              "penalty_type_confusion": confusion(gold, preds, "penalty_type"),
              "citation_gate": gates}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_examples(args.examples, args.method, errors)

    print(f"{args.method} on {len(gold)} records (dedup)")
    print(f"{'field':26s} " + " ".join(f"{k:>8s}" for k in KINDS))
    for f, c in counts.items():
        print(f"{f:26s} " + " ".join(f"{c.get(k, 0):8d}" for k in KINDS))
    print("citation gate: values nulled (classed against gold); micro F1 without -> with the gate")
    for m, g in gates.items():
        d, (lo, hi) = g["gate_delta"]["delta"], g["gate_delta"]["ci95"]
        print(f"  {m:24s} {g['total']}\n  {'':24s} F1 {100 * g['ungated_f1']:.1f} -> {100 * g['gated_f1']:.1f}"
              f"  delta {100 * d:+.1f} [{100 * lo:+.1f}, {100 * hi:+.1f}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
