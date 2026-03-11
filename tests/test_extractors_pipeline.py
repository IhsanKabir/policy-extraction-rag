import json

from extractors.pipeline import (
    build_clause_record,
    build_stub_clause_record,
    classify_action,
    classify_policy_topic,
    extract_records_from_jsonl,
    extract_stub_records_from_jsonl,
    extract_eligibility_conditions,
    extract_exceptions,
    extract_tax_refund_rule,
    split_chunk_into_clauses,
)
from extractors.schema import BeforeAfterDeparture, PenaltyType, PolicyAction, PolicyTopic, SourceType


def test_build_stub_clause_record():
    chunk_record = {
        "chunk_id": "chunk-1",
        "text": "Refunds not permitted after no-show.",
        "metadata": {
            "source_id": "src_1",
            "document_id": "doc_1",
            "document_type": "circular",
            "carrier": "AA",
            "offer_or_circular_id": "CIRC-1",
            "section_title": "Refunds",
        },
    }

    record = build_stub_clause_record(chunk_record)
    assert record.record_id == "chunk-1:0"
    assert record.document_type == SourceType.circular
    assert record.policy_topic == PolicyTopic.other
    assert record.action == PolicyAction.unknown
    assert record.needs_review is True
    assert record.clause_text == "Refunds not permitted after no-show."


def test_extract_stub_records_from_jsonl(tmp_path):
    input_path = tmp_path / "chunks.jsonl"
    output_path = tmp_path / "clauses.jsonl"

    chunk = {
        "chunk_id": "chunk-1",
        "text": "No-show clause text",
        "metadata": {
            "source_id": "src_1",
            "document_id": "doc_1",
            "document_type": "fare_rule",
        },
        "start_token": 0,
        "end_token": 4,
    }
    input_path.write_text(json.dumps(chunk) + "\n", encoding="utf-8")

    total = extract_stub_records_from_jsonl(input_path, output_path)
    assert total == 1

    out = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(out) == 1
    assert out[0]["chunk_id"] == "chunk-1"
    assert out[0]["policy_topic"] == "other"
    assert out[0]["action"] == "unknown"
    assert out[0]["needs_review"] is True


def test_classify_topic_and_action():
    text = "Refund permitted before departure with USD 50 fee."
    assert classify_policy_topic(text) == PolicyTopic.refund
    assert classify_action(text) == PolicyAction.conditional


def test_build_clause_record_heuristics():
    chunk_record = {
        "chunk_id": "chunk-2",
        "text": "Refund permitted before departure with USD 50 fee.",
        "metadata": {
            "source_id": "src_2",
            "document_id": "doc_2",
            "document_type": "fare_rule",
        },
    }
    record = build_clause_record(chunk_record)
    assert record.policy_topic == PolicyTopic.refund
    assert record.action == PolicyAction.conditional
    assert record.penalty_type == PenaltyType.fixed_amount
    assert record.penalty_currency == "USD"
    assert record.penalty_amount == 50.0
    assert record.before_after_departure == BeforeAfterDeparture.before_departure
    assert record.time_window == "before departure"
    assert record.needs_review is False


def test_extract_records_from_jsonl_heuristic_mode(tmp_path):
    input_path = tmp_path / "chunks.jsonl"
    output_path = tmp_path / "clauses.jsonl"
    chunk = {
        "chunk_id": "chunk-3",
        "text": "No-show results in full forfeiture of fare value.",
        "metadata": {
            "source_id": "src_3",
            "document_id": "doc_3",
            "document_type": "supplier_offer",
        },
    }
    input_path.write_text(json.dumps(chunk) + "\n", encoding="utf-8")
    total = extract_records_from_jsonl(input_path, output_path, mode="heuristic")
    assert total == 1
    row = json.loads(output_path.read_text(encoding="utf-8").strip())
    assert row["policy_topic"] == "no_show"
    assert row["action"] == "not_allowed"
    assert row["penalty_type"] == "forfeiture"


def test_split_chunk_into_multiple_clauses():
    text = (
        "Refund permitted before departure with USD 50 fee; "
        "After departure no refund. Taxes are refundable except YQ surcharge."
    )
    clauses = split_chunk_into_clauses(text)
    assert len(clauses) >= 2
    assert "Refund permitted before departure with USD 50 fee" in clauses[0]


def test_field_specific_parsers():
    text = (
        "Refund allowed only if ticket is unused and subject to carrier approval. "
        "Taxes are refundable except YQ surcharge. Does not apply to group bookings."
    )
    eligibility = extract_eligibility_conditions(text)
    exceptions = extract_exceptions(text)
    tax_rule = extract_tax_refund_rule(text)

    assert any("only if ticket is unused" in s.lower() for s in eligibility)
    assert any("except yq surcharge" in s.lower() for s in exceptions)
    assert any("does not apply to group bookings" in s.lower() for s in exceptions)
    assert tax_rule == "taxes refundable"


def test_extract_records_from_jsonl_multiclause(tmp_path):
    input_path = tmp_path / "chunks.jsonl"
    output_path = tmp_path / "clauses.jsonl"
    chunk = {
        "chunk_id": "chunk-4",
        "text": "Refund permitted before departure with USD 50 fee; No-show results in full forfeiture.",
        "metadata": {
            "source_id": "src_4",
            "document_id": "doc_4",
            "document_type": "fare_rule",
        },
    }
    input_path.write_text(json.dumps(chunk) + "\n", encoding="utf-8")
    total = extract_records_from_jsonl(input_path, output_path, mode="heuristic")
    assert total == 2
    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["record_id"] == "chunk-4:0"
    assert rows[1]["record_id"] == "chunk-4:1"
