import json

from fastapi.testclient import TestClient

from api.main import app


def test_labeling_page_loads():
    client = TestClient(app)
    res = client.get("/labeling")
    assert res.status_code == 200
    assert "Manual Labeling" in res.text


def test_labeling_queue_and_save(monkeypatch, tmp_path):
    queue_path = tmp_path / "queue.jsonl"
    manual_path = tmp_path / "manual_gold.jsonl"
    queue_row = {
        "record_id": "r1",
        "source_id": "src_001",
        "document_id": "doc_001",
        "document_type": "fare_rule",
        "carrier": "AA",
        "chunk_id": "chunk_001",
        "chunk_text": "Refund permitted before departure with USD 50 fee.",
        "clause_text": "Refund permitted before departure with USD 50 fee.",
        "heuristic": {"policy_topic": "refund", "action": "conditional"},
    }
    queue_path.write_text(json.dumps(queue_row) + "\n", encoding="utf-8")
    manual_path.write_text("", encoding="utf-8")

    monkeypatch.setattr("api.labeling.default_labeling_queue_path", lambda: queue_path)
    monkeypatch.setattr("api.labeling.labeling_storage_backend", lambda: "jsonl")
    monkeypatch.setattr("api.labeling.default_manual_gold_path", lambda: manual_path)

    client = TestClient(app)

    q1 = client.get("/labeling/api/queue")
    assert q1.status_code == 200
    body1 = q1.json()
    assert body1["returned_records"] == 1
    assert body1["rows"][0]["is_labeled"] is False

    save = client.post(
        "/labeling/api/save",
        json={
            "record_id": "r1",
            "source_id": "src_001",
            "document_id": "doc_001",
            "document_type": "fare_rule",
            "carrier": "AA",
            "chunk_id": "chunk_001",
            "clause_text": "Refund permitted before departure with USD 50 fee.",
            "policy_topic": "refund",
            "action": "conditional",
            "penalty_type": "fixed_amount",
            "penalty_amount": 50,
            "penalty_currency": "USD",
            "penalty_tiers": [
                {
                    "time_window": "72 hours before departure",
                    "before_after_departure": "before_departure",
                    "penalty_type": "fixed_amount",
                    "penalty_amount": 50,
                    "penalty_currency": "USD",
                },
                {
                    "time_window": "within 24 hours of departure",
                    "before_after_departure": "before_departure",
                    "penalty_type": "fixed_amount",
                    "penalty_amount": 75,
                    "penalty_currency": "USD",
                },
            ],
            "refund_payout_options": [
                {
                    "method": "original_payment",
                    "allowed": True,
                    "fee_type": "fixed_amount",
                    "fee_amount": 55,
                    "fee_currency": "USD",
                },
                {
                    "method": "travel_credit",
                    "allowed": True,
                    "fee_type": "free",
                    "notes": "Credit voucher in wallet",
                },
            ],
            "eligibility_conditions": ["if ticket is unused", "if ticket is unused"],  # dedupe
            "exceptions": [],
        },
    )
    assert save.status_code == 200
    save_body = save.json()
    assert save_body["saved"] is True
    assert save_body["updated"] is False
    assert save_body["record"]["policy_topic"] == "refund"
    assert save_body["record"]["eligibility_conditions"] == ["if ticket is unused"]
    assert len(save_body["record"]["penalty_tiers"]) == 2
    assert save_body["record"]["penalty_tiers"][1]["penalty_amount"] == 75
    assert len(save_body["record"]["refund_payout_options"]) == 2
    assert save_body["record"]["refund_payout_options"][1]["fee_type"] == "free"

    q2 = client.get("/labeling/api/queue")
    assert q2.status_code == 200
    body2 = q2.json()
    assert body2["rows"][0]["is_labeled"] is True
    assert body2["rows"][0]["manual_label"]["penalty_currency"] == "USD"

    # upsert/update
    save2 = client.post(
        "/labeling/api/save",
        json={
            "record_id": "r1",
            "policy_topic": "refund",
            "action": "not_allowed",
        },
    )
    assert save2.status_code == 200
    assert save2.json()["updated"] is True
    saved_lines = [json.loads(line) for line in manual_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(saved_lines) == 1
    assert saved_lines[0]["action"] == "not_allowed"


def test_labeling_mark_skip(monkeypatch, tmp_path):
    queue_path = tmp_path / "queue.jsonl"
    manual_path = tmp_path / "manual_gold.jsonl"
    queue_row = {
        "record_id": "r_skip",
        "source_id": "src_001",
        "document_id": "doc_001",
        "document_type": "fare_rule",
        "chunk_id": "chunk_skip",
        "chunk_text": "Ambiguous policy wording",
        "clause_text": "Ambiguous policy wording",
        "heuristic": {"policy_topic": "other", "action": "unknown"},
    }
    queue_path.write_text(json.dumps(queue_row) + "\n", encoding="utf-8")
    manual_path.write_text("", encoding="utf-8")

    monkeypatch.setattr("api.labeling.default_labeling_queue_path", lambda: queue_path)
    monkeypatch.setattr("api.labeling.labeling_storage_backend", lambda: "jsonl")
    monkeypatch.setattr("api.labeling.default_manual_gold_path", lambda: manual_path)

    client = TestClient(app)
    res = client.post(
        "/labeling/api/save",
        json={
            "record_id": "r_skip",
            "source_id": "src_001",
            "document_id": "doc_001",
            "document_type": "fare_rule",
            "chunk_id": "chunk_skip",
            "clause_text": "Ambiguous policy wording",
            "label_status": "skip",
            "notes": "Ambiguous",
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["record"]["label_status"] == "skip"

    q = client.get("/labeling/api/queue").json()
    assert q["rows"][0]["is_labeled"] is True
    assert q["rows"][0]["is_skipped"] is True
    assert q["skipped_records_in_response"] == 1


def test_labeling_queue_and_save_sqlite(monkeypatch, tmp_path):
    queue_path = tmp_path / "queue.jsonl"
    db_path = tmp_path / "labels.db"
    queue_row = {
        "record_id": "r_sqlite",
        "source_id": "src_001",
        "document_id": "doc_001",
        "document_type": "fare_rule",
        "carrier": "AA",
        "chunk_id": "chunk_sqlite",
        "chunk_text": "No-show fee applies. Refund not allowed after departure.",
        "clause_text": "No-show fee applies. Refund not allowed after departure.",
        "heuristic": {"policy_topic": "no_show", "action": "conditional"},
    }
    queue_path.write_text(json.dumps(queue_row) + "\n", encoding="utf-8")

    monkeypatch.setattr("api.labeling.default_labeling_queue_path", lambda: queue_path)
    monkeypatch.setattr("api.labeling.labeling_storage_backend", lambda: "sqlite")
    monkeypatch.setattr("api.labeling.default_labeling_db_path", lambda: db_path)
    monkeypatch.setattr("api.labeling.default_manual_gold_path", lambda: tmp_path / "legacy_manual.jsonl")

    client = TestClient(app)
    q1 = client.get("/labeling/api/queue").json()
    assert q1["manual_gold_backend"] == "sqlite"
    assert q1["rows"][0]["is_labeled"] is False

    save = client.post(
        "/labeling/api/save",
        json={"record_id": "r_sqlite", "policy_topic": "no_show", "action": "not_allowed", "label_status": "labeled"},
    )
    assert save.status_code == 200
    body = save.json()
    assert body["backend"] == "sqlite"

    q2 = client.get("/labeling/api/queue").json()
    assert q2["rows"][0]["is_labeled"] is True
    assert q2["rows"][0]["manual_label"]["action"] == "not_allowed"


def test_labeling_queue_and_save_postgres(monkeypatch, tmp_path):
    queue_path = tmp_path / "queue.jsonl"
    queue_row = {
        "record_id": "r_pg",
        "source_id": "src_010",
        "document_id": "doc_010",
        "document_type": "terms_page",
        "carrier": "DD",
        "chunk_id": "chunk_pg",
        "chunk_text": "No-show charge applies and refund is not allowed.",
        "clause_text": "No-show charge applies and refund is not allowed.",
        "heuristic": {"policy_topic": "no_show", "action": "conditional"},
    }
    queue_path.write_text(json.dumps(queue_row) + "\n", encoding="utf-8")

    fake_store = {}

    def fake_import_if_empty(dsn, path):
        return 0

    def fake_load_map(dsn):
        return dict(fake_store)

    def fake_upsert(dsn, payload):
        updated = payload["record_id"] in fake_store
        fake_store[payload["record_id"]] = payload
        return updated

    monkeypatch.setattr("api.labeling.default_labeling_queue_path", lambda: queue_path)
    monkeypatch.setattr("api.labeling.labeling_storage_backend", lambda: "postgres")
    monkeypatch.setattr("api.labeling.default_labeling_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr("api.labeling.postgres_import_sqlite_if_empty", fake_import_if_empty)
    monkeypatch.setattr("api.labeling.postgres_import_jsonl_if_empty", fake_import_if_empty)
    monkeypatch.setattr("api.labeling.postgres_load_manual_label_map", fake_load_map)
    monkeypatch.setattr("api.labeling.postgres_upsert_manual_label", fake_upsert)

    client = TestClient(app)
    q1 = client.get("/labeling/api/queue").json()
    assert q1["manual_gold_backend"] == "postgres"
    assert q1["labeling_postgres_configured"] is True
    assert q1["rows"][0]["is_labeled"] is False

    s1 = client.post("/labeling/api/save", json={"record_id": "r_pg", "policy_topic": "no_show", "action": "not_allowed"})
    assert s1.status_code == 200
    b1 = s1.json()
    assert b1["backend"] == "postgres"
    assert b1["updated"] is False

    s2 = client.post("/labeling/api/save", json={"record_id": "r_pg", "policy_topic": "no_show", "action": "conditional"})
    assert s2.status_code == 200
    assert s2.json()["updated"] is True

    q2 = client.get("/labeling/api/queue").json()
    assert q2["rows"][0]["is_labeled"] is True
    assert q2["rows"][0]["manual_label"]["action"] == "conditional"
