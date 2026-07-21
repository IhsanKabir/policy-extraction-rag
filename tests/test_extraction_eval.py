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


def test_prf_counts_spurious_predictions():
    from evals.extraction_eval import evaluate_extraction

    gold = {
        "r1": {"record_id": "r1", "policy_topic": "refund", "penalty_amount": None},
        "r2": {"record_id": "r2", "policy_topic": "no_show", "penalty_amount": 100.0},
    }
    pred = {
        "r1": {"record_id": "r1", "policy_topic": "refund", "penalty_amount": 50.0},
        "r2": {"record_id": "r2", "policy_topic": "refund", "penalty_amount": 100.0},
    }
    res = evaluate_extraction(gold, pred, ["policy_topic", "penalty_amount"])
    # policy_topic: 2 predicted non-null, 1 correct -> P=0.5, R=0.5
    assert res["per_field_prf"]["policy_topic"]["precision"] == 0.5
    assert res["per_field_prf"]["policy_topic"]["recall"] == 0.5
    # penalty_amount: r1 spurious (gold null), r2 correct
    assert res["field_stats"]["penalty_amount"]["spurious_pred"] == 1
    assert res["per_field_prf"]["penalty_amount"]["precision"] == 0.5
    assert res["per_field_prf"]["penalty_amount"]["recall"] == 1.0
    # micro: correct=2, pred_non_null=4, gold_total=3
    assert res["micro_precision"] == 2 / 4
    assert res["micro_recall"] == 2 / 3


def test_value_grounding_check_flags_hallucinated_values():
    from evals.extraction_eval import value_grounding_check

    pred = {
        "r1": {
            "record_id": "r1",
            "clause_text": "Change fee THB 1500 applies 3 hours before departure.",
            "penalty_amount": 1500.0,
            "penalty_currency": "THB",
            "time_window": "3 hours before departure",
        },
        "r2": {
            "record_id": "r2",
            "clause_text": "Refund not permitted.",
            "penalty_amount": 999.0,  # not in text -> hallucinated
            "penalty_currency": None,
            "time_window": None,
        },
    }
    res = value_grounding_check(pred)
    assert res["asserted_values"] == 4
    assert res["ungrounded_values"] == 1
    assert res["hallucination_rate"] == 0.25
    assert res["per_field"]["penalty_amount"]["ungrounded"] == 1


def test_citation_grounding_summary_aggregates_reports():
    from evals.extraction_eval import citation_grounding_summary

    pred = {
        "r1": {"record_id": "r1",
               "grounding": {"asserted": 3, "grounded": 2, "ungrounded_nulled": 1}},
        "r2": {"record_id": "r2",
               "grounding": {"asserted": 2, "grounded": 2, "ungrounded_nulled": 0}},
    }
    res = citation_grounding_summary(pred)
    assert res["asserted_fields"] == 5
    assert res["grounded_fields"] == 4
    assert res["citation_grounding_rate"] == 0.8
    assert res["raw_hallucination_rate"] == 0.2

    assert citation_grounding_summary({"r": {"record_id": "r"}}) is None


def test_empty_lists_do_not_inflate_metrics():
    """CRITICAL fix: []-vs-[] carries no information and must not count as a
    correct prediction; predicted [] against real gold conditions counts as
    missing."""
    from evals.extraction_eval import evaluate_extraction

    gold = {
        "r1": {"record_id": "r1", "eligibility_conditions": []},
        "r2": {"record_id": "r2", "eligibility_conditions": ["must book direct"]},
    }
    pred = {
        "r1": {"record_id": "r1", "eligibility_conditions": []},
        "r2": {"record_id": "r2", "eligibility_conditions": []},
    }
    res = evaluate_extraction(gold, pred, ["eligibility_conditions"])
    stats = res["field_stats"]["eligibility_conditions"]
    assert stats["total"] == 1            # only r2 has real gold content
    assert stats["correct"] == 0
    assert stats["missing_pred_value"] == 1
    assert stats["pred_non_null"] == 0    # empty lists assert nothing
    assert res["per_field_accuracy"]["eligibility_conditions"] == 0.0


def test_matching_nonempty_lists_score_correct():
    from evals.extraction_eval import evaluate_extraction

    gold = {"r1": {"record_id": "r1", "exceptions": ["infants exempt"]}}
    pred = {"r1": {"record_id": "r1", "exceptions": ["infants exempt"]}}
    res = evaluate_extraction(gold, pred, ["exceptions"])
    assert res["per_field_accuracy"]["exceptions"] == 1.0
    assert res["per_field_prf"]["exceptions"]["f1"] == 1.0


def test_f1_is_zero_not_none_when_all_predictions_wrong():
    """HIGH fix: worst-case F1 must be 0.0 so it can't drop out of averages."""
    from evals.extraction_eval import evaluate_extraction

    gold = {"r1": {"record_id": "r1", "policy_topic": "refund"},
            "r2": {"record_id": "r2", "policy_topic": "no_show"}}
    pred = {"r1": {"record_id": "r1", "policy_topic": "waiver"},
            "r2": {"record_id": "r2", "policy_topic": "waiver"}}
    res = evaluate_extraction(gold, pred, ["policy_topic"])
    prf = res["per_field_prf"]["policy_topic"]
    assert prf["precision"] == 0.0
    assert prf["recall"] == 0.0
    assert prf["f1"] == 0.0
    assert res["micro_f1"] == 0.0


def test_value_grounding_ignores_digit_commas():
    from evals.extraction_eval import value_grounding_check

    pred = {"r1": {"record_id": "r1",
                   "clause_text": "Change fee THB 1,500 per passenger.",
                   "penalty_amount": 1500.0}}
    res = value_grounding_check(pred)
    assert res["ungrounded_values"] == 0
