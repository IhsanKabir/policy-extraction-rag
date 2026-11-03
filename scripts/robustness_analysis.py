"""Per-field scores and document-level robustness of the main claims (2026-11-03).

Answers two points of the 2026-11-03 review:

1. What the headline micro F1 measures. Topic and action are present in every
   scored record, so they dominate micro F1. Per system this reports every
   field's support, precision, recall and F1; macro F1 over the fields with at
   least MACRO_MIN_SUPPORT gold values (fixed before looking at results); micro
   F1 over the value fields only (all fields except topic and action); and
   whole-record accuracy (all 12 fields right, nothing spurious).

2. Document-level dependence. The 132 scored clauses come from 12 documents,
   and clauses of one document share wording, so resampling clauses
   understates uncertainty about new documents. For each key claim this
   reports the paired difference with a clause-level and a document-level
   (cluster) bootstrap interval, and the range of the difference when each
   document is left out in turn; on all records and on the untouched subset
   (records never edited after first entry, from the provenance audit).

Usage:
    python scripts/robustness_analysis.py
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from scripts.audit_label_provenance import METHODS  # noqa: E402
from scripts.error_analysis import ungated  # noqa: E402

MACRO_MIN_SUPPORT = 10
CLASSIFICATION_FIELDS = ("policy_topic", "action")
SAMPLES = 2000
# (claim, base system, comparison system); "gate off" = the cited run with its pre-gate values
CLAIMS = [
    ("Citation prompt, Sonnet", "Sonnet 5.5", "Sonnet 5.5 + cite (gate off)"),
    ("Citation prompt, Opus", "Opus 5.5", "Opus 5.5 + cite (gate off)"),
    ("Citation prompt, Opus + guide", "Opus 5.5 + guide", "Opus 5.5 + cite + guide (gate off)"),
    ("Gate, Qwen 7B", "Qwen 7B + cite (gate off)", "Qwen 7B + cite"),
    ("Gate, Sonnet", "Sonnet 5.5 + cite (gate off)", "Sonnet 5.5 + cite"),
    ("Gate, Opus", "Opus 5.5 + cite (gate off)", "Opus 5.5 + cite"),
    ("Opus - rules", "Rule-based", "Opus 5.5"),
    ("Opus + cite + guide - rules", "Rule-based", "Opus 5.5 + cite + guide"),
    ("Opus - Qwen 7B", "Qwen 7B", "Opus 5.5"),
]


# ---------------------------------------------------------------------------
# What the headline measures
def record_correct(counts: dict[str, dict]) -> bool:
    return all(c["correct"] == c["support"] and not c["spurious"] for c in counts.values())


def field_report(gold: list[dict], preds: dict[str, dict]) -> dict:
    per_record = [E._record_counts(g, preds.get(g["record_id"])) for g in gold]
    agg = E._aggregate(per_record)
    fields = {f: {k: agg["fields"][f][k] for k in ("support", "precision", "recall", "f1")} for f in E.FIELDS}
    macro_fields = [f for f in E.FIELDS if fields[f]["support"] >= MACRO_MIN_SUPPORT]
    value_counts = [{f: c for f, c in r.items() if f not in CLASSIFICATION_FIELDS} for r in per_record]
    value_tot = {k: sum(c[k] for r in value_counts for c in r.values()) for k in ("correct", "pred", "support")}
    vp = value_tot["correct"] / value_tot["pred"] if value_tot["pred"] else 0.0
    vr = value_tot["correct"] / value_tot["support"] if value_tot["support"] else 0.0
    return {"micro_f1": agg["micro_f1"],
            "macro_f1": sum(fields[f]["f1"] or 0.0 for f in macro_fields) / len(macro_fields),
            "macro_fields": macro_fields,
            "value_fields_micro_f1": 2 * vp * vr / (vp + vr) if vp + vr else 0.0,
            "record_accuracy": sum(map(record_correct, per_record)) / len(per_record),
            "fields": fields}


# ---------------------------------------------------------------------------
# Document-level robustness
def delta(gold: list[dict], base: dict, comp: dict) -> float:
    return (E.score(gold, comp, bootstrap=0)["micro_f1"] or 0.0) - (E.score(gold, base, bootstrap=0)["micro_f1"] or 0.0)


def interval(values: list[float]) -> list[float]:
    values = sorted(values)
    return [values[int(0.025 * len(values))], values[int(0.975 * len(values)) - 1]]


def bootstrap_delta(gold, base, comp, doc_of, cluster: bool) -> list[float]:
    """Paired bootstrap of the F1 difference, resampling clauses or whole documents."""
    rng = random.Random(E.SEED)
    if not cluster:
        return interval([delta([rng.choice(gold) for _ in gold], base, comp) for _ in range(SAMPLES)])
    by_doc: dict[str, list[dict]] = {}
    for g in gold:
        by_doc.setdefault(doc_of[g["record_id"]], []).append(g)
    docs = sorted(by_doc)
    return interval([delta([g for d in (rng.choice(docs) for _ in docs) for g in by_doc[d]], base, comp)
                     for _ in range(SAMPLES)])


def leave_one_out(gold, base, comp, doc_of) -> dict:
    out = {d: delta([g for g in gold if doc_of[g["record_id"]] != d], base, comp)
           for d in sorted({doc_of[g["record_id"]] for g in gold})}
    return {"min": min(out.values()), "max": max(out.values()), "per_document": out}


def claim_report(gold, systems, doc_of) -> dict:
    out = {}
    for name, base_name, comp_name in CLAIMS:
        base, comp = systems[base_name], systems[comp_name]
        clause_ci = bootstrap_delta(gold, base, comp, doc_of, cluster=False)
        doc_ci = bootstrap_delta(gold, base, comp, doc_of, cluster=True)
        out[name] = {"delta": delta(gold, base, comp), "clause_ci": clause_ci, "document_ci": doc_ci,
                     "significant_clause": clause_ci[0] > 0 or clause_ci[1] < 0,
                     "significant_document": doc_ci[0] > 0 or doc_ci[1] < 0,
                     "leave_one_document_out": leave_one_out(gold, base, comp, doc_of)}
    return out


def load_systems() -> dict[str, dict]:
    systems = {name: E.load_pred(Path(p)) for name, p in METHODS.items()}
    for name in ("Qwen 7B + cite", "Sonnet 5.5 + cite", "Opus 5.5 + cite", "Opus 5.5 + cite + guide"):
        systems[f"{name} (gate off)"] = ungated(systems[name])
    return systems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--provenance", type=Path, default=Path("results/v1c_label_provenance.json"))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_robustness.json"))
    args = ap.parse_args()

    queue = E.load_queue(args.queue)
    doc_of = {q["record_id"]: q["document_id"] for q in queue}
    gold = E.dedup(E.load_gold(args.labels_db, queue))
    touched = set(json.loads(args.provenance.read_text(encoding="utf-8"))["touched_record_ids"])
    untouched = [g for g in gold if g["record_id"] not in touched]
    systems = load_systems()

    report = {"n_records": len(gold), "n_untouched": len(untouched),
              "n_documents": len({doc_of[g["record_id"]] for g in gold}),
              "macro_min_support": MACRO_MIN_SUPPORT,
              "fields_by_system": {name: field_report(gold, systems[name]) for name in METHODS},
              "claims": {"all": claim_report(gold, systems, doc_of),
                         "untouched": claim_report(untouched, systems, doc_of)}}
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    pct = lambda x: f"{100 * x:6.1f}"  # noqa: E731
    print(f"{len(gold)} records from {report['n_documents']} documents; macro over "
          f"{report['fields_by_system']['Rule-based']['macro_fields']}")
    print(f"{'system':28s} {'micro':>6s} {'macro':>6s} {'values':>6s} {'record':>6s}")
    for name, r in report["fields_by_system"].items():
        print(f"{name:28s} {pct(r['micro_f1'])} {pct(r['macro_f1'])} {pct(r['value_fields_micro_f1'])} "
              f"{pct(r['record_accuracy'])}")
    for subset, claims in report["claims"].items():
        print(f"\nclaims ({subset}): delta [clause CI] [document CI] leave-one-doc-out range")
        for name, c in claims.items():
            lo = c["leave_one_document_out"]
            print(f"  {name:30s} {pct(c['delta'])} [{pct(c['clause_ci'][0])},{pct(c['clause_ci'][1])}] "
                  f"[{pct(c['document_ci'][0])},{pct(c['document_ci'][1])}]{'*' if c['significant_document'] else ' '}"
                  f" [{pct(lo['min'])},{pct(lo['max'])}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
