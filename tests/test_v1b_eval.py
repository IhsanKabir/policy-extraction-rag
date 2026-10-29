from evals.v1b_eval import (
    dedup,
    field_match,
    normalize_time_window,
    score,
    token_f1,
)


def test_time_window_normalisation_matches_ui_rules():
    assert normalize_time_window("08-30 Days") == "8-30 days"
    assert normalize_time_window("24 Hours- 7 Days") == "24 hours-7 days"
    assert normalize_time_window("Within 24 Hours") == "24 hours"
    assert normalize_time_window("24 Hrs.") == "24 hours"
    assert normalize_time_window("6 Hours before departure") == "6 hours"
    assert normalize_time_window("1 day") == "1 day"
    assert normalize_time_window("") is None


def test_token_f1_ignores_case_order_and_stopwords():
    assert token_f1(["Value fare", "one modification only"], ["one modification only", "value fare"]) == 1.0
    assert token_f1("if any", "anything") == 0.0
    assert 0 < token_f1("flights to or from the USA", "USA flights") < 1


def test_field_match_by_type():
    assert field_match("policy_topic", "refund", "Refund")[0]
    assert field_match("time_window", "8-30 days", "08-30 Days")[0]
    assert field_match("penalty_amount", 1500.0, 1500)[0]
    assert field_match("penalty_currency", "THB", "thb")[0]
    assert field_match("fare_difference_required", True, True)[0]
    assert not field_match("fare_difference_required", True, "true")[0]
    assert not field_match("action", "allowed", None)[0]
    ok, soft = field_match("eligibility_conditions", ["Value fare"], ["value fare only"])
    assert ok and 0.5 <= soft <= 1


def test_null_handling_spurious_and_missing():
    gold = [{"record_id": "a", "clause_text": "x", "policy_topic": "refund", "action": "allowed"}]
    preds = {"a": {"record_id": "a", "policy_topic": "refund", "action": "not_allowed", "penalty_type": "free"}}
    r = score(gold, preds, bootstrap=10)
    assert r["topic_accuracy"] == 1.0
    assert r["action_accuracy"] == 0.0
    assert r["fields"]["penalty_type"]["support"] == 0
    assert r["fields"]["penalty_type"]["spurious"] == 1
    # 1 correct out of 3 asserted; 1 correct out of 2 gold values
    assert abs(r["micro_precision"] - 1 / 3) < 1e-9
    assert abs(r["micro_recall"] - 1 / 2) < 1e-9


def test_missing_prediction_counts_as_miss():
    gold = [{"record_id": "a", "clause_text": "x", "policy_topic": "refund"}]
    r = score(gold, {}, bootstrap=5)
    assert r["topic_accuracy"] == 0.0
    assert r["n_missing_predictions"] == 1


def test_dedup_drops_exact_contained_and_near_duplicates():
    recs = [
        {"record_id": "1", "clause_text": "Passenger has to be responsible for the differences of the fare between the moment of booking and the moment of change."},
        {"record_id": "2", "clause_text": "Passenger has to be responsible for the differences of the fare between the moment of booking and the moment of"},
        {"record_id": "3", "clause_text": "Refund/Cancellation: Not Available"},
        {"record_id": "4", "clause_text": "refund cancellation not available"},
        {"record_id": "5", "clause_text": "Changes in name: Not permitted"},
    ]
    assert [r["record_id"] for r in dedup(recs)] == ["1", "3", "5"]
