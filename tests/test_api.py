from fastapi.testclient import TestClient

from api.main import app


def test_sources_endpoint_returns_aggregated_sources(monkeypatch):
    sample_records = [
        {
            "record_id": "r1",
            "source_id": "src_001",
            "document_id": "doc_001",
            "document_type": "fare_rule",
            "carrier": "AA",
            "clause_text": "Refund permitted before departure with USD 50 fee.",
        },
        {
            "record_id": "r2",
            "source_id": "src_001",
            "document_id": "doc_001",
            "document_type": "fare_rule",
            "carrier": "AA",
            "clause_text": "No-show results in full forfeiture.",
        },
        {
            "record_id": "r3",
            "source_id": "src_003",
            "document_id": "doc_003",
            "document_type": "supplier_offer",
            "supplier": "ExampleSupplier",
            "clause_text": "Taxes are refundable except YQ surcharge.",
        },
    ]
    monkeypatch.setattr("api.main.load_clause_records", lambda: sample_records)
    client = TestClient(app)

    res = client.get("/sources")
    assert res.status_code == 200
    body = res.json()
    assert body["total_records"] == 3
    assert body["total_sources"] == 2
    assert body["sources"][0]["source_id"] == "src_001"
    assert body["sources"][0]["record_count"] == 2


def test_ask_endpoint_retrieval_only(monkeypatch):
    class DummyRetriever:
        def search(self, query, top_k=5, filters=None):
            return [
                {
                    "chunk_id": "chunk-1",
                    "text": "Refund permitted before departure with USD 50 fee.",
                    "metadata": {"source_id": "src_001", "document_id": "doc_001", "document_type": "fare_rule", "carrier": "AA"},
                    "clauses": [
                        {
                            "record_id": "r1",
                            "source_id": "src_001",
                            "document_id": "doc_001",
                            "document_type": "fare_rule",
                            "carrier": "AA",
                            "clause_text": "Refund permitted before departure with USD 50 fee.",
                            "policy_topic": "refund",
                            "action": "conditional",
                        }
                    ],
                    "_scores": {"hybrid": 6.2, "bm25": 1.8, "vector": 0.9, "clause_boost": 0.8},
                }
            ]

    monkeypatch.setattr("api.main.get_hybrid_retriever", lambda: DummyRetriever())
    client = TestClient(app)

    res = client.post("/ask", json={"query": "AA refund before departure fee", "top_k": 3})
    assert res.status_code == 200
    body = res.json()
    assert body["mode"] == "retrieval_only"
    assert body["hits"] >= 1
    assert "Refund permitted before departure" in body["answer"]
    assert body["citations"][0]["chunk_id"] == "chunk-1"
    assert body["citations"][0]["matched_clauses"][0]["record_id"] == "r1"
