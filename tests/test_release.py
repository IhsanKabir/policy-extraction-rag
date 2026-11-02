import json
import sqlite3

from labeling_store import sqlite_load_manual_label_map, sqlite_upsert_manual_label
from scripts.export_release import label_rows
from scripts.reproduce import build_db, diff


def test_diff_ignores_paths_and_float_noise():
    a = {"pred": "data/x.jsonl", "results": {"f1": 0.5, "n": [1, 2]}}
    b = {"pred": "other/x.jsonl", "results": {"f1": 0.5 + 1e-12, "n": [1, 2]}}
    assert diff(a, b) == []


def test_diff_reports_value_length_and_key_changes():
    problems = diff({"f1": 0.5, "n": [1], "new": 1}, {"f1": 0.6, "n": [1, 2]})
    assert any("/f1" in p for p in problems)
    assert any("length" in p for p in problems)
    assert any("only in reproduced" in p for p in problems)


def test_label_rows_round_trip_through_build_db(tmp_path):
    src = tmp_path / "src.db"
    sqlite_upsert_manual_label(src, {"record_id": "r1", "label_status": "labeled", "policy_topic": "refund"})
    sqlite_upsert_manual_label(src, {"record_id": "r2", "label_status": "skip", "notes": "menu"})
    rows = label_rows(src)
    out = tmp_path / "out.db"
    build_db(rows, out)
    assert sqlite_load_manual_label_map(out) == sqlite_load_manual_label_map(src)
    statuses = dict(sqlite3.connect(out).execute("select record_id, label_status from manual_labels"))
    assert statuses == {"r1": "labeled", "r2": "skip"}
    assert json.loads(json.dumps(rows))[0]["record_id"] == "r1"


def test_label_store_closes_its_connections(tmp_path):
    db = tmp_path / "labels.db"
    sqlite_upsert_manual_label(db, {"record_id": "r1", "label_status": "labeled"})
    db.unlink()  # fails on Windows if a connection were left open
    assert not db.exists()
