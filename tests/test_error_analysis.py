from scripts.error_analysis import classify, gate_effect, ungated


def test_classify_kinds():
    assert classify("action", None, None) is None
    assert classify("action", None, "allowed") == "spurious"
    assert classify("action", "allowed", None) == "missed"
    assert classify("action", "allowed", "Allowed") == "correct"
    assert classify("action", "allowed", "unknown") == "wrong"


PRED = {"r1": {"record_id": "r1", "policy_topic": None, "action": "allowed", "time_window": None,
               "model_raw_fields": {"policy_topic": "refund", "action": "allowed", "time_window": "6 hours"},
               "grounding": {"fields": {"policy_topic": {"grounded": False}, "action": {"grounded": True},
                                        "time_window": {"grounded": False}}}}}


def test_ungated_restores_raw_values():
    raw = ungated(PRED)["r1"]
    assert raw["policy_topic"] == "refund" and raw["time_window"] == "6 hours" and raw["action"] == "allowed"


def test_gate_effect_classes_nulled_values_against_gold():
    gold = [{"record_id": "r1", "policy_topic": "refund", "action": "allowed", "time_window": None}]
    effect = gate_effect(gold, PRED)
    assert effect["by_field"]["policy_topic"] == {"cost: right value removed": 1}
    assert effect["by_field"]["time_window"] == {"benefit: spurious value removed": 1}
