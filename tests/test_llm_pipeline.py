import json

from extractors.llm_pipeline import (
    build_prompt,
    coerce_fields,
    extract_records,
    parse_model_json,
    verify_citations,
)


def test_parse_model_json_plain_and_fenced():
    assert parse_model_json('{"policy_topic": "refund"}') == {"policy_topic": "refund"}
    fenced = "```json\n{\"action\": \"allowed\"}\n```"
    assert parse_model_json(fenced) == {"action": "allowed"}
    assert parse_model_json("no json here") is None
    assert parse_model_json("") is None
    # prose-wrapped
    assert parse_model_json('Sure! {"policy_topic": "no_show"} hope that helps') == {
        "policy_topic": "no_show"
    }


def test_coerce_fields_validates_enums_and_numbers():
    raw = {
        "policy_topic": "refund",
        "action": "made_up_value",
        "penalty_type": "percentage",
        "penalty_amount": "1,500 THB",
        "penalty_percent": 25,
        "penalty_currency": " THB ",
        "fare_difference_required": "yes",  # not a bool -> null
        "time_window": "3 hours before departure",
        "tax_refund_rule": "",
        "before_after_departure": "before_departure",
        "eligibility_conditions": ["  cond one ", ""],
        "exceptions": "not-a-list",
    }
    out = coerce_fields(raw)
    assert out["policy_topic"] == "refund"
    assert out["action"] is None  # invalid enum rejected
    assert out["penalty_type"] == "percentage"
    assert out["penalty_amount"] == 1500.0
    assert out["penalty_percent"] == 25.0
    assert out["penalty_currency"] == "THB"
    assert out["fare_difference_required"] is None
    assert out["time_window"] == "3 hours before departure"
    assert out["tax_refund_rule"] is None
    assert out["eligibility_conditions"] == ["cond one"]
    assert out["exceptions"] == []


def test_coerce_fields_rejects_bool_as_number():
    out = coerce_fields({"penalty_amount": True})
    assert out["penalty_amount"] is None


def test_verify_citations_nulls_ungrounded_fields():
    clause = "Cancellation fee THB 1500 applies 3 hours before departure."
    fields = {
        "policy_topic": "cancellation",
        "penalty_amount": 1500.0,
        "penalty_currency": "THB",
        "time_window": "24 hours before departure",  # NOT in text
        "eligibility_conditions": [],
        "exceptions": [],
    }
    citations = {
        "policy_topic": "Cancellation fee",
        "penalty_amount": "THB 1500",
        "penalty_currency": "THB 1500",
        "time_window": "24 hours before departure",  # quote not in clause
    }
    report = verify_citations(fields, citations, clause)
    assert report["asserted"] == 4
    assert report["grounded"] == 3
    assert report["ungrounded_nulled"] == 1
    assert fields["time_window"] is None  # nulled by the constraint
    assert fields["penalty_amount"] == 1500.0  # grounded survives


def test_verify_citations_missing_quote_counts_ungrounded():
    fields = {"policy_topic": "refund"}
    report = verify_citations(fields, {}, "Refund not permitted.")
    assert report["ungrounded_nulled"] == 1
    assert fields["policy_topic"] is None


def test_build_prompt_cited_mode_mentions_citations():
    plain = build_prompt("some clause", "VZ", "fare_rule", cited=False)
    cited = build_prompt("some clause", "VZ", "fare_rule", cited=True)
    assert "citations" not in plain[0]["content"]
    assert "citations" in cited[0]["content"]
    assert "some clause" in cited[1]["content"]


def test_extract_records_resumable_and_writes_records(tmp_path):
    rows = [
        {"record_id": "r1", "source_id": "s", "document_id": "d",
         "document_type": "fare_rule", "carrier": "VZ", "chunk_id": "c1",
         "clause_text": "Refund not permitted."},
        {"record_id": "r2", "source_id": "s", "document_id": "d",
         "document_type": "fare_rule", "carrier": "VZ", "chunk_id": "c1",
         "clause_text": "Change fee THB 1500."},
    ]

    def fake_chat(messages, model):
        return json.dumps({"policy_topic": "refund", "action": "not_allowed"})

    out_path = tmp_path / "pred.jsonl"
    stats = extract_records(rows, mode="llm", model="fake", output_path=out_path,
                            chat=fake_chat)
    assert stats["extracted"] == 2
    written = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines()]
    assert {w["record_id"] for w in written} == {"r1", "r2"}
    assert written[0]["extraction_mode"] == "llm"
    assert written[0]["policy_topic"] == "refund"
    assert written[0]["clause_text"] == "Refund not permitted."

    # resume: nothing new to do
    stats2 = extract_records(rows, mode="llm", model="fake", output_path=out_path,
                             chat=fake_chat)
    assert stats2["extracted"] == 0
    assert stats2["skipped_existing"] == 2


def test_extract_records_survives_bad_json_and_call_failure(tmp_path):
    rows = [
        {"record_id": "r1", "document_type": "fare_rule", "clause_text": "text one"},
        {"record_id": "r2", "document_type": "fare_rule", "clause_text": "text two"},
        {"record_id": "r3", "document_type": "fare_rule", "clause_text": "text three"},
    ]
    calls = {"n": 0}

    def flaky_chat(messages, model):
        calls["n"] += 1
        if calls["n"] == 1:
            return "not json at all"
        if calls["n"] == 2:
            raise OSError("connection refused")
        return json.dumps({"policy_topic": "other", "action": "unknown"})

    out_path = tmp_path / "pred.jsonl"
    stats = extract_records(rows, mode="llm", model="fake", output_path=out_path,
                            chat=flaky_chat)
    assert stats["parse_failures"] == 1
    assert stats["call_failures"] == 1
    assert stats["extracted"] == 1


def test_extract_records_cited_mode_stores_grounding(tmp_path):
    rows = [{"record_id": "r1", "document_type": "fare_rule",
             "clause_text": "Refund not permitted for promo fares."}]

    def fake_chat(messages, model):
        return json.dumps({
            "policy_topic": "refund",
            "action": "not_allowed",
            "citations": {
                "policy_topic": "Refund not permitted",
                "action": "Refund not permitted",
            },
        })

    out_path = tmp_path / "pred.jsonl"
    extract_records(rows, mode="llm_cited", model="fake", output_path=out_path,
                    chat=fake_chat)
    rec = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
    assert rec["extraction_mode"] == "llm_cited"
    assert rec["grounding"]["asserted"] == 2
    assert rec["grounding"]["grounded"] == 2
    assert rec["policy_topic"] == "refund"


# --- regression tests for review findings ---------------------------------


def test_verify_citations_rejects_irrelevant_quote_for_value_fields():
    """CRITICAL fix: a real-but-irrelevant quote must not launder an invented
    number/currency through the grounding gate."""
    clause = "Cancellation fee applies as per fare rules. Non-refundable ticket."
    fields = {"penalty_amount": 9999.0, "penalty_currency": "USD"}
    citations = {
        "penalty_amount": "Cancellation fee applies",   # real quote, no 9999
        "penalty_currency": "Non-refundable ticket",    # real quote, no USD
    }
    report = verify_citations(fields, citations, clause)
    assert report["asserted"] == 2
    assert report["grounded"] == 0
    assert report["ungrounded_nulled"] == 2
    assert fields["penalty_amount"] is None
    assert fields["penalty_currency"] is None


def test_verify_citations_list_items_verified_individually():
    """HIGH fix: list items are grounded per item; hallucinated items drop,
    grounded items survive."""
    clause = "Valid only for infants under 2 years. Not applicable on codeshare."
    fields = {"eligibility_conditions": [
        "infants under 2 years",       # in text -> kept
        "minimum 3 passengers",        # invented -> dropped
    ]}
    report = verify_citations(fields, {}, clause)
    assert report["asserted"] == 2
    assert report["grounded"] == 1
    assert report["ungrounded_nulled"] == 1
    assert fields["eligibility_conditions"] == ["infants under 2 years"]


def test_coerce_fields_keeps_decimal_with_thousands_separator():
    """HIGH fix: '1,500.50' must coerce to 1500.5, not 1500.0."""
    out = coerce_fields({"penalty_amount": "1,500.50 THB"})
    assert out["penalty_amount"] == 1500.5
    out2 = coerce_fields({"penalty_amount": "THB 1,234.56"})
    assert out2["penalty_amount"] == 1234.56


def test_extract_records_resume_tolerates_truncated_last_line(tmp_path):
    """MEDIUM fix: a truncated final JSONL line (killed mid-write) must not
    break the resume path."""
    out_path = tmp_path / "pred.jsonl"
    out_path.write_text(
        json.dumps({"record_id": "r1", "policy_topic": "refund"}) + "\n"
        + '{"record_id": "r2", "policy_t',  # truncated write
        encoding="utf-8",
    )
    rows = [
        {"record_id": "r1", "document_type": "fare_rule", "clause_text": "a"},
        {"record_id": "r2", "document_type": "fare_rule", "clause_text": "b"},
    ]

    def fake_chat(messages, model):
        return json.dumps({"policy_topic": "other"})

    stats = extract_records(rows, mode="llm", model="fake", output_path=out_path,
                            chat=fake_chat)
    # r1 skipped (already done), r2 re-extracted despite the truncated line
    assert stats["extracted"] == 1
    assert stats["skipped_existing"] == 1


def test_extract_records_none_record_id_not_marked_done(tmp_path):
    """MEDIUM fix: a None record_id in the output must not mark future
    id-less rows as already done."""
    out_path = tmp_path / "pred.jsonl"
    out_path.write_text(json.dumps({"record_id": None, "policy_topic": "x"}) + "\n",
                        encoding="utf-8")
    rows = [{"record_id": None, "document_type": "fare_rule", "clause_text": "a"}]

    def fake_chat(messages, model):
        return json.dumps({"policy_topic": "other"})

    stats = extract_records(rows, mode="llm", model="fake", output_path=out_path,
                            chat=fake_chat)
    assert stats["extracted"] == 1  # processed, not silently skipped


# --- pre-v1 follow-up features --------------------------------------------


def test_value_in_text_ignores_digit_commas():
    """Follow-up: 1500.0 must ground against "THB 1,500"."""
    from extractors.llm_pipeline import _norm, _value_in_text

    assert _value_in_text(1500.0, _norm("Change fee THB 1,500 per passenger"))
    assert _value_in_text(1234.56, _norm("fee of 1,234.56 applies"))
    # a comma that is not a digit separator must not be collapsed
    assert not _value_in_text(12.0, _norm("one, two, three"))


def test_verify_citations_grounds_comma_separated_number():
    clause = "Cancellation fee THB 1,500 applies."
    fields = {"penalty_amount": 1500.0}
    citations = {"penalty_amount": "THB 1,500"}
    report = verify_citations(fields, citations, clause)
    assert report["grounded"] == 1
    assert fields["penalty_amount"] == 1500.0


def test_extract_records_stores_raw_model_fields(tmp_path):
    rows = [{"record_id": "r1", "document_type": "fare_rule",
             "clause_text": "Refund not permitted."}]

    def fake_chat(messages, model):
        return json.dumps({"policy_topic": "Refund_Policy",  # invalid enum
                           "action": "not_allowed"})

    out_path = tmp_path / "pred.jsonl"
    extract_records(rows, mode="llm", model="fake", output_path=out_path,
                    chat=fake_chat)
    rec = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
    # coerced field nulled, but the raw model output is preserved for audit
    assert rec["policy_topic"] is None
    assert rec["model_raw_fields"]["policy_topic"] == "Refund_Policy"


def test_chunk_context_included_in_prompt_and_flagged(tmp_path):
    rows = [{"record_id": "r1", "document_type": "fare_rule", "chunk_id": "c9",
             "clause_text": "Only applicable to unused tickets."}]
    chunk_map = {"c9": "Refund policy section. Only applicable to unused "
                       "tickets. Fees per fare family apply."}
    seen = {}

    def fake_chat(messages, model):
        seen["user"] = messages[1]["content"]
        return json.dumps({"policy_topic": "refund"})

    out_path = tmp_path / "pred.jsonl"
    extract_records(rows, mode="llm", model="fake", output_path=out_path,
                    chat=fake_chat, chunk_map=chunk_map)
    assert "SURROUNDING CONTEXT" in seen["user"]
    assert "Refund policy section" in seen["user"]
    rec = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
    assert rec["with_chunk_context"] is True


def test_chunk_context_omitted_when_identical_to_clause(tmp_path):
    rows = [{"record_id": "r1", "document_type": "fare_rule", "chunk_id": "c9",
             "clause_text": "Whole chunk text."}]
    chunk_map = {"c9": "Whole chunk text."}
    seen = {}

    def fake_chat(messages, model):
        seen["user"] = messages[1]["content"]
        return json.dumps({"policy_topic": "other"})

    out_path = tmp_path / "pred.jsonl"
    extract_records(rows, mode="llm", model="fake", output_path=out_path,
                    chat=fake_chat, chunk_map=chunk_map)
    assert "SURROUNDING CONTEXT" not in seen["user"]
    rec = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
    assert rec["with_chunk_context"] is False
