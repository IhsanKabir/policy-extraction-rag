from scripts.audit_label_provenance import changed_fields, classify, first_entered, label_view


def test_label_view_drops_metadata_and_empty_values():
    payload = {"record_id": "r1", "clause_text": "x", "notes": "n", "policy_topic": "refund",
               "eligibility_conditions": [], "time_window": None}
    assert label_view(payload, "labeled") == {"policy_topic": "refund", "label_status": "labeled"}


def test_first_entered_keeps_earliest_snapshot_value():
    old = {"r1": {"action": "allowed"}}
    new = {"r1": {"action": "conditional"}, "r2": {"action": "unknown"}}
    assert first_entered([old, new]) == {"r1": {"action": "allowed"}, "r2": {"action": "unknown"}}


def test_classify_flags_changed_fields_and_untouched_records():
    first = {"r1": {"action": "allowed", "label_status": "labeled"},
             "r2": {"action": "unknown", "label_status": "labeled"}}
    final = {"r1": {"action": "conditional", "label_status": "labeled"},
             "r2": {"action": "unknown", "label_status": "labeled"},
             "r3": {"label_status": "skip"}}
    assert classify(first, final) == {"r1": ["action"], "r2": [], "r3": None}


def test_changed_fields_detects_status_change_to_skip():
    before = {"policy_topic": "other", "label_status": "labeled"}
    after = {"label_status": "skip"}
    assert changed_fields(before, after) == ["label_status", "policy_topic"]
