from scripts.adjudication_sheet import DECISION, adjudicate, disagreements, parse_other

Q = [{"record_id": "r1", "clause_text": "c1"}, {"record_id": "r2", "clause_text": "c2"},
     {"record_id": "r3", "clause_text": "c3"}]
A = {"r1": ("labeled", {"policy_topic": "reissue_change", "action": "conditional", "notes": "a note"}),
     "r2": ("skip", {"notes": "menu"}),
     "r3": ("labeled", {"policy_topic": "refund", "action": "not_allowed"})}
B = {"r1": ("labeled", {"policy_topic": "other", "action": "conditional"}),
     "r2": ("labeled", {"policy_topic": "other", "action": "unknown"}),
     "r3": ("labeled", {"policy_topic": "refund", "action": "not_allowed"})}


def test_disagreements_lists_only_differing_fields_and_skip_decisions():
    rows = {(r["q"]["record_id"], r["field"]) for r in disagreements(Q, A, B)}
    assert rows == {("r1", "policy_topic"), ("r2", DECISION), ("r2", "policy_topic"), ("r2", "action")}


def test_adjudicate_applies_agreed_values_and_keeps_agreed_records():
    decisions = [{"record_id": "r1", "field": "policy_topic", "value": "reissue_change"},
                 {"record_id": "r2", "field": DECISION, "value": "labeled"},
                 {"record_id": "r2", "field": "policy_topic", "value": "other"},
                 {"record_id": "r2", "field": "action", "value": None}]
    out = {p["record_id"]: p for p in adjudicate(Q, A, B, decisions)}
    assert out["r1"]["policy_topic"] == "reissue_change" and out["r1"]["notes"] == "a note"
    assert out["r2"]["label_status"] == "labeled" and out["r2"]["policy_topic"] == "other"
    assert out["r2"]["action"] is None
    assert out["r3"] == {**A["r3"][1], "record_id": "r3", "label_status": "labeled"}


def test_adjudicate_skip_drops_label_fields():
    decisions = [{"record_id": "r2", "field": DECISION, "value": "skip"}]
    out = {p["record_id"]: p for p in adjudicate(Q, A, B, decisions)}
    assert out["r2"]["label_status"] == "skip" and "policy_topic" not in out["r2"]


def test_parse_other_validates_and_types_values():
    errors = []
    assert parse_other("policy_topic", "Refund", "w", errors) == "refund"
    assert parse_other("fare_difference_required", "TRUE", "w", errors) is True
    assert parse_other("penalty_currency", "thb", "w", errors) == "THB"
    assert parse_other("exceptions", "groups\nreward tickets", "w", errors) == ["groups", "reward tickets"]
    assert parse_other("action", "", "w", errors) is None
    assert errors == []
    parse_other("policy_topic", "refunds", "w", errors)
    assert errors
