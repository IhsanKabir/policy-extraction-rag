import json
from pathlib import Path

from labeling_store import sqlite_upsert_manual_label
from evals.real_pilot_labeling import build_labeling_queue, compare_heuristic_vs_manual_gold


def test_build_labeling_queue_blocks_synthetic_chunks(tmp_path):
    manifest = tmp_path / "manifest.csv"
    manifest.write_text(
        "source_id,document_id,document_type,source_filename\nsrc_001,doc_001,fare_rule,file.pdf\n",
        encoding="utf-8",
    )
    chunks = tmp_path / "chunks.jsonl"
    chunks.write_text(
        json.dumps({"chunk_id": "pilot_chunk_001", "text": "Refund text", "metadata": {"source_id": "src_001"}}) + "\n",
        encoding="utf-8",
    )
    clauses = tmp_path / "clauses.jsonl"
    clauses.write_text(
        json.dumps({"record_id": "pilot_chunk_001:0", "chunk_id": "pilot_chunk_001", "source_id": "src_001", "clause_text": "Refund allowed"}) + "\n",
        encoding="utf-8",
    )
    queue_out = tmp_path / "queue.jsonl"

    result = build_labeling_queue(manifest, chunks, clauses, queue_out, limit=10)
    assert result["status"] == "blocked"
    assert result["reason"] == "synthetic_chunks_detected"


def test_compare_heuristic_vs_manual_gold_blocks_when_empty(tmp_path):
    pred = tmp_path / "pred.jsonl"
    pred.write_text(json.dumps({"record_id": "r1", "policy_topic": "refund"}) + "\n", encoding="utf-8")
    manual_gold = tmp_path / "manual.jsonl"
    manual_gold.write_text("", encoding="utf-8")

    result = compare_heuristic_vs_manual_gold(pred, manual_gold, manual_gold_db_path=tmp_path / "labels.db")
    assert result["status"] == "blocked"
    assert result["reason"] == "manual_gold_empty"


def test_compare_heuristic_vs_manual_gold_reads_sqlite(tmp_path):
    pred = tmp_path / "pred.jsonl"
    pred.write_text(
        "\n".join(
            [
                json.dumps({"record_id": "r1", "policy_topic": "refund", "action": "conditional"}),
                json.dumps({"record_id": "r2", "policy_topic": "no_show", "action": "not_allowed"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    manual_gold = tmp_path / "manual.jsonl"
    manual_gold.write_text("", encoding="utf-8")
    db_path = tmp_path / "labels.db"
    sqlite_upsert_manual_label(db_path, {"record_id": "r1", "policy_topic": "refund", "action": "conditional"})
    sqlite_upsert_manual_label(db_path, {"record_id": "r2", "label_status": "skip", "policy_topic": "no_show", "action": "not_allowed"})

    result = compare_heuristic_vs_manual_gold(pred, manual_gold, manual_gold_db_path=db_path, fields=["policy_topic", "action"])
    assert result["status"] == "ok"
    assert result["gold_records"] == 1
    assert result["matched_records"] == 1
    assert result["overall_field_accuracy"] == 1.0
    assert result["manual_gold_db_path"] == str(Path(db_path))


def test_compare_heuristic_vs_manual_gold_reads_postgres(monkeypatch, tmp_path):
    pred = tmp_path / "pred.jsonl"
    pred.write_text(
        "\n".join(
            [
                json.dumps({"record_id": "r1", "policy_topic": "refund", "action": "conditional"}),
                json.dumps({"record_id": "r2", "policy_topic": "no_show", "action": "not_allowed"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    manual_gold = tmp_path / "manual.jsonl"
    manual_gold.write_text("", encoding="utf-8")

    monkeypatch.setenv("LABELING_STORAGE_BACKEND", "postgres")
    monkeypatch.setenv("LABELING_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr("evals.real_pilot_labeling.postgres_manual_label_count", lambda dsn: 2)
    monkeypatch.setattr(
        "evals.real_pilot_labeling.postgres_load_manual_label_map",
        lambda dsn: {
            "r1": {"record_id": "r1", "policy_topic": "refund", "action": "conditional"},
            "r2": {"record_id": "r2", "label_status": "skip", "policy_topic": "no_show", "action": "not_allowed"},
        },
    )

    result = compare_heuristic_vs_manual_gold(
        pred,
        manual_gold,
        manual_gold_postgres_dsn="postgresql://test",
        fields=["policy_topic", "action"],
    )
    assert result["status"] == "ok"
    assert result["manual_gold_backend"] == "postgres"
    assert result["gold_records"] == 1
    assert result["matched_records"] == 1
    assert result["overall_field_accuracy"] == 1.0
