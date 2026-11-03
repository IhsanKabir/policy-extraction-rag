from scripts.robustness_analysis import record_correct
from scripts.rule_detection import detection, predicts_rule, predicts_rule_lenient


def test_any_filled_field_counts_as_a_rule():
    assert not predicts_rule(None)
    assert not predicts_rule({"policy_topic": None, "eligibility_conditions": []})
    assert predicts_rule({"policy_topic": "other"})


def test_lenient_reading_treats_other_unknown_as_no_rule():
    assert not predicts_rule_lenient({"policy_topic": "other", "action": "unknown"})
    assert predicts_rule_lenient({"policy_topic": "other", "action": "unknown", "time_window": "24 hours"})
    assert predicts_rule_lenient({"policy_topic": "refund", "action": "unknown"})


def test_detection_counts():
    gold = [{"record_id": "a", "has_rule": True}, {"record_id": "b", "has_rule": False},
            {"record_id": "c", "has_rule": False}]
    preds = {"a": {"policy_topic": "refund"}, "b": {"policy_topic": "other"}, "c": {}}
    d = detection(gold, preds)
    assert (d["tp"], d["fp"], d["precision"], d["recall"], d["specificity"]) == (1, 1, 0.5, 1.0, 0.5)


def test_record_correct_needs_every_field_and_no_spurious():
    ok = {"support": 1, "correct": 1, "spurious": 0}
    assert record_correct({"policy_topic": ok, "action": ok})
    assert not record_correct({"policy_topic": ok, "action": {"support": 1, "correct": 0, "spurious": 0}})
    assert not record_correct({"policy_topic": ok, "time_window": {"support": 0, "correct": 0, "spurious": 1}})
