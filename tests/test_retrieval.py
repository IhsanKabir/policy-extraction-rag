from api.retrieval import InMemoryHybridChunkRetriever


def test_hybrid_retriever_chunk_and_clause_scoring():
    chunks = [
        {
            "chunk_id": "c1",
            "text": "General fare rules text",
            "metadata": {"source_id": "src_1", "document_id": "doc_1", "document_type": "fare_rule", "carrier": "AA"},
        },
        {
            "chunk_id": "c2",
            "text": "Miscellaneous baggage text",
            "metadata": {"source_id": "src_2", "document_id": "doc_2", "document_type": "circular", "carrier": "AA"},
        },
    ]
    clauses = [
        {
            "record_id": "r1",
            "chunk_id": "c1",
            "source_id": "src_1",
            "document_id": "doc_1",
            "document_type": "fare_rule",
            "carrier": "AA",
            "clause_text": "Refund permitted before departure with USD 50 fee.",
            "policy_topic": "refund",
            "action": "conditional",
        },
        {
            "record_id": "r2",
            "chunk_id": "c2",
            "source_id": "src_2",
            "document_id": "doc_2",
            "document_type": "circular",
            "carrier": "AA",
            "clause_text": "Baggage allowance is 20kg.",
            "policy_topic": "other",
            "action": "unknown",
        },
    ]

    retriever = InMemoryHybridChunkRetriever(chunks, clauses)
    hits = retriever.search("AA refund before departure fee", top_k=2)
    assert hits
    assert hits[0]["chunk_id"] == "c1"
    assert hits[0]["clauses"][0]["record_id"] == "r1"
    assert hits[0]["_scores"]["hybrid"] > 0


def test_hybrid_retriever_filters():
    chunks = [
        {"chunk_id": "c1", "text": "Refund text", "metadata": {"carrier": "AA", "document_type": "fare_rule"}},
        {"chunk_id": "c2", "text": "Refund text", "metadata": {"carrier": "BA", "document_type": "fare_rule"}},
    ]
    clauses = [
        {"record_id": "r1", "chunk_id": "c1", "carrier": "AA", "document_type": "fare_rule", "clause_text": "Refund allowed", "policy_topic": "refund", "action": "allowed"},
        {"record_id": "r2", "chunk_id": "c2", "carrier": "BA", "document_type": "fare_rule", "clause_text": "Refund allowed", "policy_topic": "refund", "action": "allowed"},
    ]
    retriever = InMemoryHybridChunkRetriever(chunks, clauses)
    hits = retriever.search("refund", top_k=5, filters={"carrier": "AA"})
    assert len(hits) == 1
    assert hits[0]["chunk_id"] == "c1"
