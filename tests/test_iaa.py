from scripts.build_iaa_sample import allocate, blind_row
from scripts.iaa_agreement import agreement, cohen_kappa


def test_allocate_is_proportional_and_sums_to_total():
    alloc = allocate({"a": 60, "b": 30, "c": 10}, 10)
    assert alloc == {"a": 6, "b": 3, "c": 1}


def test_allocate_gives_every_stratum_at_least_one():
    alloc = allocate({"big": 98, "tiny": 2}, 10)
    assert alloc["tiny"] == 1 and sum(alloc.values()) == 10


def test_blind_row_removes_heuristic_and_notes():
    row = blind_row({"record_id": "r1", "clause_text": "x", "heuristic": {"action": "allowed"},
                     "manual_labeling_note": "copy fields"})
    assert row["heuristic"] == {} and "manual_labeling_note" not in row and row["clause_text"] == "x"


def test_cohen_kappa_perfect_and_chance():
    assert cohen_kappa(["a", "b", "a", "b"], ["a", "b", "a", "b"]) == 1.0
    assert cohen_kappa(["a", "a", "b", "b"], ["a", "b", "a", "b"]) == 0.0
    assert cohen_kappa([], []) is None


def test_agreement_counts_only_records_both_labelled():
    a = [{"record_id": "r1", "policy_topic": "refund", "action": "not_allowed", "clause_text": ""},
         {"record_id": "r2", "policy_topic": "other", "action": "unknown", "clause_text": ""}]
    b = [{"record_id": "r1", "policy_topic": "refund", "action": "not_allowed", "clause_text": ""}]
    res = agreement(a, b)
    assert res["n_both_labelled"] == 1 and res["field_micro_f1"] == 1.0
