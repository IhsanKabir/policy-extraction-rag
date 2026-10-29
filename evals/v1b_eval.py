"""Scorer for the v1b gold set (labels in a SQLite labelling DB + the v1b queue).

Kept separate from extraction_eval.py so the published v0 numbers stay
reproducible. Differences from the v0 scorer:

* Field-type-aware matching:
    - categorical fields (topic, action, penalty type, before/after): exact;
    - fare_difference_required: boolean exact;
    - penalty_amount / penalty_percent: numeric (tolerance 1e-6);
    - penalty_currency: upper-cased exact;
    - time_window: normalised ("08-30 Days" == "8-30 days", "24 Hrs." == "24 hours");
    - tax_refund_rule, eligibility_conditions, exceptions (free text): token F1
      between gold and prediction; counted correct when F1 >= TEXT_MATCH_F1,
      and the mean F1 is reported as a soft score.
* Null handling as in v0: a field only counts towards recall when the gold
  value is present; a prediction where gold is empty counts as spurious and
  lowers precision.
* Near-duplicate removal: records whose normalised clause text equals, or is
  contained in, an earlier record's text (min 30 chars), or has token
  Jaccard >= NEAR_DUP_JACCARD with it, are dropped in the "dedup" view.
* Bootstrap 95% confidence intervals over records.
* Separate reports for the fare-rule and policy-text genres.

Usage:
    python -m evals.v1b_eval --pred data/policy_clauses_fine.jsonl --method heuristic
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

CATEGORICAL = ["policy_topic", "action", "penalty_type", "before_after_departure"]
BOOLEAN = ["fare_difference_required"]
NUMERIC = ["penalty_amount", "penalty_percent"]
CURRENCY = ["penalty_currency"]
TIME = ["time_window"]
TEXT = ["tax_refund_rule", "eligibility_conditions", "exceptions"]
FIELDS = CATEGORICAL + BOOLEAN + NUMERIC + CURRENCY + TIME + TEXT

FARE_RULE_CARRIERS = {"VZ", "DD", "G9", "AK", "SG", "QR"}
TEXT_MATCH_F1 = 0.5
NEAR_DUP_JACCARD = 0.85
NEAR_DUP_MIN_CHARS = 30
BOOTSTRAP_SAMPLES = 1000
SEED = 20261029

_STOP = {"a", "an", "the", "of", "to", "and", "or", "for", "in", "on", "is", "are", "be", "by", "with", "if", "any"}


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalize_time_window(raw) -> Optional[str]:
    """Same rules as the labelling UI's normalizeTimeWindow, minus the
    direction words (those are not part of the value)."""
    if raw is None:
        return None
    s = str(raw).lower().strip()
    if not s:
        return None
    s = re.sub(r"^(within|less than|more than|above|over|under|at least|up to)\s+", "", s)
    s = re.sub(r"\s+(before|prior to|after)\s+(the\s+)?(scheduled\s+)?(flight\s+)?departure.*$", "", s)
    s = re.sub(r"\bhrs?\b\.?", "hours", s)
    s = re.sub(r"\bhour\b", "hours", s)
    s = re.sub(r"\bday\b", "days", s)
    s = re.sub(r"\b0+(\d)", r"\1", s)
    s = re.sub(r"\s*[-–—]\s*", "-", s)
    s = re.sub(r"\s+to\s+", "-", s)
    s = re.sub(r"(\d)\s*(hours|days|weeks|months|years)", r"\1 \2", s)
    s = re.sub(r"\.+$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"(^|-)1 (hour|day|week|month|year)s\b", r"\g<1>1 \2", s)
    return s or None


def tokens(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        value = " ".join(str(v) for v in value)
    words = re.findall(r"[a-z0-9]+", str(value).lower())
    return [w for w in words if w not in _STOP]


def token_f1(gold, pred) -> float:
    g, p = tokens(gold), tokens(pred)
    if not g or not p:
        return 0.0
    gc: Dict[str, int] = {}
    for w in g:
        gc[w] = gc.get(w, 0) + 1
    overlap = 0
    for w in p:
        if gc.get(w, 0) > 0:
            overlap += 1
            gc[w] -= 1
    if overlap == 0:
        return 0.0
    prec, rec = overlap / len(p), overlap / len(g)
    return 2 * prec * rec / (prec + rec)


def is_empty(value) -> bool:
    return value is None or value == "" or value == [] or (isinstance(value, str) and not value.strip())


def field_match(field: str, gold, pred) -> Tuple[bool, float]:
    """Return (counted_correct, soft_score) for a gold value that is present."""
    if is_empty(pred):
        return False, 0.0
    if field in CATEGORICAL:
        ok = str(gold).strip().lower() == str(pred).strip().lower()
        return ok, float(ok)
    if field in BOOLEAN:
        ok = bool(gold) == bool(pred) and isinstance(pred, bool)
        return ok, float(ok)
    if field in NUMERIC:
        try:
            ok = abs(float(gold) - float(pred)) < 1e-6
        except (TypeError, ValueError):
            ok = False
        return ok, float(ok)
    if field in CURRENCY:
        ok = str(gold).strip().upper() == str(pred).strip().upper()
        return ok, float(ok)
    if field in TIME:
        ok = normalize_time_window(gold) == normalize_time_window(pred)
        return ok, float(ok)
    f1 = token_f1(gold, pred)
    return f1 >= TEXT_MATCH_F1, f1


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_queue(path: Path) -> List[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def load_gold(db_path: Path, queue: Sequence[dict]) -> List[dict]:
    """Labelled (not skipped) gold records in queue order, each with clause_text,
    carrier and genre attached."""
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as con:
        rows = {rid: (status, json.loads(payload)) for rid, payload, status in
                con.execute("select record_id, payload_json, label_status from manual_labels")}
    gold = []
    for q in queue:
        status, payload = rows.get(q["record_id"], (None, None))
        if status != "labeled":
            continue
        rec = dict(payload)
        rec["clause_text"] = q.get("clause_text") or rec.get("clause_text") or ""
        rec["carrier"] = q.get("carrier")
        rec["genre"] = "fare_rule" if q.get("carrier") in FARE_RULE_CARRIERS else "policy_text"
        gold.append(rec)
    return gold


def load_pred(path: Path) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row = json.loads(line)
                out[row["record_id"]] = row
    return out


# ---------------------------------------------------------------------------
# Near-duplicate removal
# ---------------------------------------------------------------------------

def _norm_text(text: str) -> str:
    return re.sub(r"\W+", " ", (text or "").lower()).strip()


def dedup(records: Sequence[dict]) -> List[dict]:
    kept: List[dict] = []
    kept_norm: List[Tuple[str, set]] = []
    for r in records:
        n = _norm_text(r["clause_text"])
        toks = set(n.split())
        dup = False
        for kn, ktoks in kept_norm:
            if n == kn:
                dup = True
            elif len(n) >= NEAR_DUP_MIN_CHARS and len(kn) >= NEAR_DUP_MIN_CHARS and (n in kn or kn in n):
                dup = True
            elif toks and ktoks and len(toks & ktoks) / len(toks | ktoks) >= NEAR_DUP_JACCARD:
                dup = True
            if dup:
                break
        if not dup:
            kept.append(r)
            kept_norm.append((n, toks))
    return kept


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _record_counts(gold: dict, pred: Optional[dict]) -> Dict[str, dict]:
    pred = pred or {}
    out = {}
    for f in FIELDS:
        g, p = gold.get(f), pred.get(f)
        c = {"support": 0, "correct": 0, "soft": 0.0, "pred": 0, "spurious": 0}
        if not is_empty(p):
            c["pred"] = 1
        if is_empty(g):
            if not is_empty(p):
                c["spurious"] = 1
        else:
            c["support"] = 1
            ok, soft = field_match(f, g, p)
            c["correct"] = int(ok)
            c["soft"] = soft
        out[f] = c
    return out


def _aggregate(per_record: Sequence[Dict[str, dict]]) -> dict:
    fields = {}
    tot = {"support": 0, "correct": 0, "pred": 0, "spurious": 0}
    for f in FIELDS:
        s = {k: sum(r[f][k] for r in per_record) for k in ("support", "correct", "soft", "pred", "spurious")}
        prec = s["correct"] / s["pred"] if s["pred"] else None
        rec = s["correct"] / s["support"] if s["support"] else None
        f1 = None
        if prec is not None and rec is not None:
            f1 = 0.0 if prec + rec == 0 else 2 * prec * rec / (prec + rec)
        fields[f] = {"support": s["support"], "correct": s["correct"], "predicted": s["pred"],
                     "spurious": s["spurious"], "precision": prec, "recall": rec, "f1": f1,
                     "soft_score": (s["soft"] / s["support"]) if (f in TEXT and s["support"]) else None}
        for k in tot:
            tot[k] += s[k]
    mp = tot["correct"] / tot["pred"] if tot["pred"] else None
    mr = tot["correct"] / tot["support"] if tot["support"] else None
    mf = None if mp is None or mr is None else (0.0 if mp + mr == 0 else 2 * mp * mr / (mp + mr))
    return {"fields": fields, "micro_precision": mp, "micro_recall": mr, "micro_f1": mf,
            "topic_accuracy": fields["policy_topic"]["recall"], "action_accuracy": fields["action"]["recall"]}


def _ci(values: List[float]) -> Optional[Tuple[float, float]]:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    lo = vals[int(0.025 * (len(vals) - 1))]
    hi = vals[int(0.975 * (len(vals) - 1))]
    return (lo, hi)


def score(gold: Sequence[dict], preds: Dict[str, dict], bootstrap: int = BOOTSTRAP_SAMPLES) -> dict:
    per = [_record_counts(g, preds.get(g["record_id"])) for g in gold]
    result = _aggregate(per)
    result["n_records"] = len(gold)
    result["n_missing_predictions"] = sum(1 for g in gold if g["record_id"] not in preds)
    rng = random.Random(SEED)
    samples = {"micro_f1": [], "topic_accuracy": [], "action_accuracy": []}
    if per:
        for _ in range(bootstrap):
            agg = _aggregate([per[rng.randrange(len(per))] for _ in per])
            for k in samples:
                samples[k].append(agg[k])
    result["ci95"] = {k: _ci(v) for k, v in samples.items()}
    return result


def evaluate(labels_db: Path, queue_path: Path, pred_path: Path) -> dict:
    queue = load_queue(queue_path)
    gold_all = load_gold(labels_db, queue)
    preds = load_pred(pred_path)
    views = {"all": gold_all, "all_dedup": dedup(gold_all)}
    for genre in ("fare_rule", "policy_text"):
        sub = [g for g in gold_all if g["genre"] == genre]
        views[genre] = sub
        views[f"{genre}_dedup"] = dedup(sub)
    return {name: score(recs, preds) for name, recs in views.items()}


def _fmt(x) -> str:
    return "  -  " if x is None else f"{x * 100:5.1f}"


def summary_table(method: str, results: dict) -> str:
    lines = [f"{'view':<18}{'n':>4}  {'topic':>6}  {'action':>6}  {'microF1':>7}  95% CI (micro F1)"]
    for view, r in results.items():
        ci = r["ci95"]["micro_f1"]
        ci_s = "-" if ci is None else f"{ci[0] * 100:.1f}-{ci[1] * 100:.1f}"
        lines.append(f"{view:<18}{r['n_records']:>4}  {_fmt(r['topic_accuracy']):>6}  {_fmt(r['action_accuracy']):>6}  "
                     f"{_fmt(r['micro_f1']):>7}  {ci_s}")
    return f"== {method} ==\n" + "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1b.jsonl"))
    ap.add_argument("--pred", type=Path, required=True)
    ap.add_argument("--method", default="method")
    ap.add_argument("--out", type=Path, default=None, help="write full JSON results here")
    args = ap.parse_args()
    results = evaluate(args.labels_db, args.queue, args.pred)
    if args.out:
        args.out.write_text(json.dumps({"method": args.method, "pred": str(args.pred), "results": results},
                                       indent=2), encoding="utf-8")
    print(summary_table(args.method, results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
