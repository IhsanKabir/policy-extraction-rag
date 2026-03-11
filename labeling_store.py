import json
import sqlite3
from pathlib import Path
from typing import Dict, List


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_manual_label_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_labels (
                record_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                label_status TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_manual_labels_status
            ON manual_labels(label_status)
            """
        )
        conn.commit()


def sqlite_manual_label_count(db_path: Path) -> int:
    ensure_manual_label_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM manual_labels").fetchone()
        return int(row["n"] if row else 0)


def sqlite_load_manual_label_map(db_path: Path) -> Dict[str, dict]:
    ensure_manual_label_db(db_path)
    out: Dict[str, dict] = {}
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT record_id, payload_json FROM manual_labels").fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError:
                continue
            record_id = str(payload.get("record_id") or row["record_id"] or "")
            if not record_id:
                continue
            out[record_id] = payload
    return out


def sqlite_upsert_manual_label(db_path: Path, payload: dict) -> bool:
    ensure_manual_label_db(db_path)
    record_id = str(payload.get("record_id") or "")
    if not record_id:
        raise ValueError("manual label payload missing record_id")
    label_status = str(payload.get("label_status") or "").lower() or None
    payload_json = json.dumps(payload)
    with _connect(db_path) as conn:
        exists = conn.execute(
            "SELECT 1 FROM manual_labels WHERE record_id = ?",
            (record_id,),
        ).fetchone() is not None
        conn.execute(
            """
            INSERT INTO manual_labels (record_id, payload_json, label_status, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(record_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                label_status = excluded.label_status,
                updated_at = CURRENT_TIMESTAMP
            """,
            (record_id, payload_json, label_status),
        )
        conn.commit()
    return exists


def sqlite_export_manual_labels_jsonl(db_path: Path, out_path: Path) -> int:
    rows = list(sqlite_load_manual_label_map(db_path).values())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return len(rows)


def _load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def sqlite_import_jsonl_if_empty(db_path: Path, jsonl_path: Path) -> int:
    ensure_manual_label_db(db_path)
    if sqlite_manual_label_count(db_path) > 0:
        return 0
    imported = 0
    for row in _load_jsonl(jsonl_path):
        if not row.get("record_id"):
            continue
        sqlite_upsert_manual_label(db_path, row)
        imported += 1
    return imported


def _pg_connect(dsn: str):
    try:
        import psycopg
    except ImportError as e:
        raise RuntimeError(
            "Postgres backend requires `psycopg` (psycopg3). Install it with `pip install \"psycopg[binary]\"`."
        ) from e
    conn = psycopg.connect(dsn, autocommit=False)
    return conn


def ensure_manual_label_postgres_table(dsn: str) -> None:
    if not dsn:
        raise ValueError("Postgres DSN is required for postgres labeling backend")
    with _pg_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS manual_labels (
                    record_id TEXT PRIMARY KEY,
                    payload_json JSONB NOT NULL,
                    label_status TEXT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manual_labels_status
                ON manual_labels(label_status)
                """
            )
        conn.commit()


def postgres_manual_label_count(dsn: str) -> int:
    ensure_manual_label_postgres_table(dsn)
    with _pg_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM manual_labels")
            row = cur.fetchone()
        conn.commit()
    return int(row[0] if row else 0)


def postgres_load_manual_label_map(dsn: str) -> Dict[str, dict]:
    ensure_manual_label_postgres_table(dsn)
    out: Dict[str, dict] = {}
    with _pg_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT record_id, payload_json FROM manual_labels")
            rows = cur.fetchall()
        conn.commit()
    for record_id, payload_json in rows:
        payload = payload_json
        if isinstance(payload_json, str):
            try:
                payload = json.loads(payload_json)
            except json.JSONDecodeError:
                continue
        if not isinstance(payload, dict):
            continue
        rid = str(payload.get("record_id") or record_id or "")
        if not rid:
            continue
        out[rid] = payload
    return out


def postgres_upsert_manual_label(dsn: str, payload: dict) -> bool:
    ensure_manual_label_postgres_table(dsn)
    record_id = str(payload.get("record_id") or "")
    if not record_id:
        raise ValueError("manual label payload missing record_id")
    label_status = str(payload.get("label_status") or "").lower() or None
    payload_json = json.dumps(payload)
    with _pg_connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM manual_labels WHERE record_id = %s", (record_id,))
            exists = cur.fetchone() is not None
            cur.execute(
                """
                INSERT INTO manual_labels (record_id, payload_json, label_status, updated_at)
                VALUES (%s, %s::jsonb, %s, NOW())
                ON CONFLICT (record_id) DO UPDATE SET
                    payload_json = EXCLUDED.payload_json,
                    label_status = EXCLUDED.label_status,
                    updated_at = NOW()
                """,
                (record_id, payload_json, label_status),
            )
        conn.commit()
    return exists


def postgres_export_manual_labels_jsonl(dsn: str, out_path: Path) -> int:
    rows = list(postgres_load_manual_label_map(dsn).values())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return len(rows)


def postgres_import_jsonl_if_empty(dsn: str, jsonl_path: Path) -> int:
    ensure_manual_label_postgres_table(dsn)
    if postgres_manual_label_count(dsn) > 0:
        return 0
    imported = 0
    for row in _load_jsonl(jsonl_path):
        if not row.get("record_id"):
            continue
        postgres_upsert_manual_label(dsn, row)
        imported += 1
    return imported


def postgres_import_sqlite_if_empty(dsn: str, sqlite_db_path: Path) -> int:
    ensure_manual_label_postgres_table(dsn)
    if postgres_manual_label_count(dsn) > 0:
        return 0
    if not sqlite_db_path.exists():
        return 0
    imported = 0
    for row in sqlite_load_manual_label_map(sqlite_db_path).values():
        if not row.get("record_id"):
            continue
        postgres_upsert_manual_label(dsn, row)
        imported += 1
    return imported
