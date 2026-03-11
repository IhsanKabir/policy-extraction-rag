import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence


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
    return value


def evaluate_extraction(gold_rows: Dict[str, dict], pred_rows: Dict[str, dict], fields: Sequence[str]) -> dict:
    gold_ids = set(gold_rows)
    pred_ids = set(pred_rows)
    matched_ids = sorted(gold_ids & pred_ids)

    field_stats = {
        field: {"correct": 0, "total": 0, "missing_pred_value": 0}
        for field in fields
    }

    for record_id in matched_ids:
        gold = gold_rows[record_id]
        pred = pred_rows[record_id]
        for field in fields:
            gold_val = _normalize_value(gold.get(field))
            pred_val = _normalize_value(pred.get(field))
            if gold_val is None:
                continue
            field_stats[field]["total"] += 1
            if pred_val is None:
                field_stats[field]["missing_pred_value"] += 1
                continue
            if pred_val == gold_val:
                field_stats[field]["correct"] += 1

    per_field_accuracy = {}
    for field, stats in field_stats.items():
        total = stats["total"]
        per_field_accuracy[field] = None if total == 0 else stats["correct"] / total

    overall_correct = sum(stats["correct"] for stats in field_stats.values())
    overall_total = sum(stats["total"] for stats in field_stats.values())
    overall_accuracy = None if overall_total == 0 else overall_correct / overall_total

    return {
        "gold_records": len(gold_ids),
        "pred_records": len(pred_ids),
        "matched_records": len(matched_ids),
        "missing_predictions_for_gold_records": len(gold_ids - pred_ids),
        "extra_prediction_records": len(pred_ids - gold_ids),
        "fields": list(fields),
        "per_field_accuracy": per_field_accuracy,
        "field_stats": field_stats,
        "overall_field_accuracy": overall_accuracy,
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
