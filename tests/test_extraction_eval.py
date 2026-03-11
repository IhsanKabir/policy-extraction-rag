import json

from evals.extraction_eval import evaluate_files


def test_evaluate_files_basic(tmp_path):
    gold_path = tmp_path / "gold.jsonl"
    pred_path = tmp_path / "pred.jsonl"

    gold_rows = [
        {
            "record_id": "r1",
            "policy_topic": "refund",
            "action": "conditional",
            "penalty_type": "fixed_amount",
            "penalty_amount": 50.0,
            "penalty_currency": "USD",
        },
        {
            "record_id": "r2",
            "policy_topic": "no_show",
            "action": "not_allowed",
            "penalty_type": "forfeiture",
        },
    ]
    pred_rows = [
        {
            "record_id": "r1",
            "policy_topic": "refund",
            "action": "conditional",
            "penalty_type": "fixed_amount",
            "penalty_amount": 50.0,
            "penalty_currency": "USD",
        },
        {
            "record_id": "r2",
            "policy_topic": "no_show",
            "action": "unknown",
            "penalty_type": "forfeiture",
        },
    ]

    gold_path.write_text("\n".join(json.dumps(r) for r in gold_rows) + "\n", encoding="utf-8")
    pred_path.write_text("\n".join(json.dumps(r) for r in pred_rows) + "\n", encoding="utf-8")

    res = evaluate_files(gold_path, pred_path, fields=["policy_topic", "action", "penalty_type"])
    assert res["matched_records"] == 2
    assert res["per_field_accuracy"]["policy_topic"] == 1.0
    assert res["per_field_accuracy"]["penalty_type"] == 1.0
    assert res["per_field_accuracy"]["action"] == 0.5
    assert res["overall_field_accuracy"] == 5 / 6


def test_evaluate_files_ignores_skipped_gold_rows(tmp_path):
    gold_path = tmp_path / "gold.jsonl"
    pred_path = tmp_path / "pred.jsonl"

    gold_rows = [
        {"record_id": "r1", "policy_topic": "refund", "action": "allowed"},
        {"record_id": "r2", "label_status": "skip", "policy_topic": "no_show", "action": "not_allowed"},
    ]
    pred_rows = [
        {"record_id": "r1", "policy_topic": "refund", "action": "allowed"},
        {"record_id": "r2", "policy_topic": "other", "action": "unknown"},
    ]

    gold_path.write_text("\n".join(json.dumps(r) for r in gold_rows) + "\n", encoding="utf-8")
    pred_path.write_text("\n".join(json.dumps(r) for r in pred_rows) + "\n", encoding="utf-8")

    res = evaluate_files(gold_path, pred_path, fields=["policy_topic", "action"])
    assert res["gold_records"] == 1
    assert res["matched_records"] == 1
    assert res["overall_field_accuracy"] == 1.0
