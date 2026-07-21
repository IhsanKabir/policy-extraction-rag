import argparse
import json
from pathlib import Path
from typing import Dict, Optional, Sequence


DEFAULT_FIELDS = [
    "policy_topic",
    "action",
    "penalty_type",
    "penalty_amount",
    "penalty_currency",
    "penalty_percent",
    "fare_difference_required",
    "time_window",
    "before_after_departure",
    "tax_refund_rule",
    "eligibility_conditions",
    "exceptions",
]


def _load_jsonl_by_record_id(path: Path) -> Dict[str, dict]:
    rows: Dict[str, dict] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if str(row.get("label_status", "")).lower() == "skip":
                continue
            record_id = row.get("record_id")
            if not record_id:
                raise ValueError(f"Missing record_id in {path}")
            rows[record_id] = row
    return rows


def _normalize_value(value):
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, list):
        # An empty list carries no assertion: treat it exactly like null so
        # trivial []-vs-[] matches can't inflate accuracy/precision/recall,
        # and a predicted [] against real gold conditions counts as missing.
        # Non-empty lists compare as ordered tuples (order-sensitive; noted
        # as a limitation in the eval docs).
        return tuple(value) if value else None
    return value


def evaluate_extraction(gold_rows: Dict[str, dict], pred_rows: Dict[str, dict], fields: Sequence[str]) -> dict:
    gold_ids = set(gold_rows)
    pred_ids = set(pred_rows)
    matched_ids = sorted(gold_ids & pred_ids)

    field_stats = {
        field: {"correct": 0, "total": 0, "missing_pred_value": 0,
                "pred_non_null": 0, "spurious_pred": 0}
        for field in fields
    }

    for record_id in matched_ids:
        gold = gold_rows[record_id]
        pred = pred_rows[record_id]
        for field in fields:
            gold_val = _normalize_value(gold.get(field))
            pred_val = _normalize_value(pred.get(field))
            if pred_val is not None:
                field_stats[field]["pred_non_null"] += 1
            if gold_val is None:
                # Spurious assertion: model claims a value the gold says absent.
                if pred_val is not None:
                    field_stats[field]["spurious_pred"] += 1
                continue
            field_stats[field]["total"] += 1
            if pred_val is None:
                field_stats[field]["missing_pred_value"] += 1
                continue
            if pred_val == gold_val:
                field_stats[field]["correct"] += 1

    per_field_accuracy = {}
    per_field_prf = {}
    for field, stats in field_stats.items():
        total = stats["total"]
        per_field_accuracy[field] = None if total == 0 else stats["correct"] / total
        tp = stats["correct"]
        pred_n = stats["pred_non_null"]
        precision = None if pred_n == 0 else tp / pred_n
        recall = None if total == 0 else tp / total
        # F1 is 0.0 (worst score), not None, when P and R are both defined and
        # zero: None means "no data" and would let a worst-case field silently
        # drop out of any downstream average.
        f1 = None
        if precision is not None and recall is not None:
            f1 = 0.0 if (precision + recall) == 0 else (
                2 * precision * recall / (precision + recall))
        per_field_prf[field] = {"precision": precision, "recall": recall, "f1": f1}

    overall_correct = sum(stats["correct"] for stats in field_stats.values())
    overall_total = sum(stats["total"] for stats in field_stats.values())
    overall_pred_n = sum(stats["pred_non_null"] for stats in field_stats.values())
    overall_accuracy = None if overall_total == 0 else overall_correct / overall_total
    micro_precision = None if overall_pred_n == 0 else overall_correct / overall_pred_n
    micro_recall = overall_accuracy
    micro_f1 = None
    if micro_precision is not None and micro_recall is not None:
        micro_f1 = 0.0 if (micro_precision + micro_recall) == 0 else (
            2 * micro_precision * micro_recall / (micro_precision + micro_recall))

    return {
        "gold_records": len(gold_ids),
        "pred_records": len(pred_ids),
        "matched_records": len(matched_ids),
        "missing_predictions_for_gold_records": len(gold_ids - pred_ids),
        "extra_prediction_records": len(pred_ids - gold_ids),
        "fields": list(fields),
        "per_field_accuracy": per_field_accuracy,
        "per_field_prf": per_field_prf,
        "field_stats": field_stats,
        "overall_field_accuracy": overall_accuracy,
        "micro_precision": micro_precision,
        "micro_recall": micro_recall,
        "micro_f1": micro_f1,
    }


# ---------------------------------------------------------------------------
# Faithfulness metrics (paper §2.5): grounding + hallucination.
# ---------------------------------------------------------------------------

# Fields whose value should literally occur in the clause text when asserted.
VALUE_GROUNDABLE_FIELDS = [
    "penalty_amount",
    "penalty_percent",
    "penalty_currency",
    "time_window",
]


def _value_occurs_in_text(value, text_lower: str) -> bool:
    if isinstance(value, float):
        # 25.0 should match "25" or "25.0"; 12.5 should match "12.5"
        candidates = {f"{value}", f"{value:g}"}
        if value == int(value):
            candidates.add(str(int(value)))
        return any(c in text_lower for c in candidates)
    if isinstance(value, str):
        return value.strip().lower() in text_lower
    return False


def value_grounding_check(pred_rows: Dict[str, dict], fields: Optional[Sequence[str]] = None) -> dict:
    """Post-hoc hallucination check applicable to ANY method (heuristic or LLM):
    for value-bearing fields, does the asserted value literally occur in the
    record's clause_text? Reported per method as `hallucination_rate` =
    asserted-but-not-found / asserted."""
    use_fields = list(fields or VALUE_GROUNDABLE_FIELDS)
    asserted = 0
    ungrounded = 0
    per_field = {f: {"asserted": 0, "ungrounded": 0} for f in use_fields}
    for row in pred_rows.values():
        text_lower = str(row.get("clause_text") or "").lower()
        for field in use_fields:
            value = row.get(field)
            if value is None or value == []:
                continue
            asserted += 1
            per_field[field]["asserted"] += 1
            if not _value_occurs_in_text(value, text_lower):
                ungrounded += 1
                per_field[field]["ungrounded"] += 1
    return {
        "fields": use_fields,
        "asserted_values": asserted,
        "ungrounded_values": ungrounded,
        "hallucination_rate": None if asserted == 0 else ungrounded / asserted,
        "per_field": per_field,
    }


def citation_grounding_summary(pred_rows: Dict[str, dict]) -> Optional[dict]:
    """Aggregate the per-record `grounding` reports written by the llm_cited
    pipeline (asserted / grounded / ungrounded_nulled). Returns None when no
    record carries a grounding report (i.e., not a cited-mode file)."""
    asserted = grounded = nulled = 0
    seen = False
    for row in pred_rows.values():
        rep = row.get("grounding")
        if not isinstance(rep, dict):
            continue
        seen = True
        asserted += int(rep.get("asserted", 0))
        grounded += int(rep.get("grounded", 0))
        nulled += int(rep.get("ungrounded_nulled", 0))
    if not seen:
        return None
    return {
        "asserted_fields": asserted,
        "grounded_fields": grounded,
        "ungrounded_nulled_fields": nulled,
        "citation_grounding_rate": None if asserted == 0 else grounded / asserted,
        "raw_hallucination_rate": None if asserted == 0 else nulled / asserted,
    }


def evaluate_files(gold_path: Path, pred_path: Path, fields: Optional[Sequence[str]] = None) -> dict:
    fields = list(fields or DEFAULT_FIELDS)
    gold_rows = _load_jsonl_by_record_id(gold_path)
    pred_rows = _load_jsonl_by_record_id(pred_path)
    return evaluate_extraction(gold_rows, pred_rows, fields)


def main():
    parser = argparse.ArgumentParser(description="Evaluate extracted policy clause fields against gold JSONL")
    parser.add_argument("--gold", required=True, help="Gold JSONL with record_id and target fields")
    parser.add_argument("--pred", required=True, help="Predicted JSONL with record_id and target fields")
    parser.add_argument(
        "--fields",
        nargs="*",
        default=DEFAULT_FIELDS,
        help="Fields to score (default: common classification + penalty/time fields)",
    )
    args = parser.parse_args()

    results = evaluate_files(Path(args.gold), Path(args.pred), fields=args.fields)
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
