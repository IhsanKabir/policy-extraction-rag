import argparse
import csv
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from evals.extraction_eval import DEFAULT_FIELDS, _load_jsonl_by_record_id, evaluate_extraction, evaluate_files
from labeling_store import (
    postgres_load_manual_label_map,
    postgres_manual_label_count,
    sqlite_load_manual_label_map,
    sqlite_manual_label_count,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_manifest() -> Path:
    return _repo_root() / "data" / "pilot_source_manifest.csv"


def _default_chunks() -> Path:
    return _repo_root() / "data" / "chunks.jsonl"


def _default_clauses() -> Path:
    return _repo_root() / "data" / "policy_clauses.jsonl"


def _default_queue_out() -> Path:
    return _repo_root() / "data" / "real_pilot_manual_labeling_queue.jsonl"


def _default_manual_gold() -> Path:
    return _repo_root() / "data" / "manual_gold_real_pilot_policy_clauses.jsonl"


def _default_labeling_db() -> Path:
    return Path(
        os.getenv(
            "LABELING_DB_PATH",
            str(_repo_root() / "data" / "labeling.db"),
        )
    )


def _default_labeling_postgres_dsn() -> str:
    return os.getenv("LABELING_POSTGRES_DSN", "").strip()


def _labeling_storage_backend() -> str:
    backend = (os.getenv("LABELING_STORAGE_BACKEND") or "sqlite").strip().lower()
    if backend not in {"sqlite", "jsonl", "postgres"}:
        return "sqlite"
    return backend


def _load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _ensure_file(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")


def _manual_gold_id_set() -> set:
    backend = _labeling_storage_backend()
    if backend == "sqlite":
        db_path = _default_labeling_db()
        if db_path.exists() and sqlite_manual_label_count(db_path) > 0:
            return set(sqlite_load_manual_label_map(db_path).keys())
    if backend == "postgres":
        dsn = _default_labeling_postgres_dsn()
        if dsn and postgres_manual_label_count(dsn) > 0:
            return set(postgres_load_manual_label_map(dsn).keys())
    return {r.get("record_id") for r in _load_jsonl(_default_manual_gold()) if r.get("record_id")}


def load_manifest_source_ids(manifest_path: Path) -> List[str]:
    if not manifest_path.exists():
        return []
    source_ids: List[str] = []
    with open(manifest_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = (row.get("source_id") or "").strip()
            if sid:
                source_ids.append(sid)
    return source_ids


def detect_synthetic_chunks(chunk_rows: List[dict]) -> bool:
    if not chunk_rows:
        return False
    sample_ids = [str(r.get("chunk_id", "")) for r in chunk_rows[:20]]
    return all(cid.startswith("pilot_chunk_") for cid in sample_ids if cid)


def build_labeling_queue(
    manifest_path: Path,
    chunks_path: Path,
    clauses_path: Path,
    output_path: Path,
    limit: int = 50,
) -> dict:
    manifest_source_ids = set(load_manifest_source_ids(manifest_path))
    chunk_rows = _load_jsonl(chunks_path)
    clause_rows = _load_jsonl(clauses_path)

    if _labeling_storage_backend() == "jsonl":
        _ensure_file(_default_manual_gold())

    if not chunk_rows or not clause_rows:
        _ensure_file(output_path)
        return {
            "status": "blocked",
            "reason": "missing_chunks_or_clauses",
            "chunks_path": str(chunks_path),
            "clauses_path": str(clauses_path),
            "queued_records": 0,
        }

    if detect_synthetic_chunks(chunk_rows):
        _ensure_file(output_path)
        return {
            "status": "blocked",
            "reason": "synthetic_chunks_detected",
            "message": "Current chunks look synthetic (pilot eval chunks). Add real ingested pilot docs and regenerate data/chunks.jsonl + data/policy_clauses.jsonl.",
            "queued_records": 0,
        }

    chunk_by_id = {str(c.get("chunk_id")): c for c in chunk_rows if c.get("chunk_id")}
    manual_gold_ids = _manual_gold_id_set()

    queue_rows: List[dict] = []
    for clause in clause_rows:
        if clause.get("record_id") in manual_gold_ids:
            continue
        if manifest_source_ids and clause.get("source_id") not in manifest_source_ids:
            continue
        chunk = chunk_by_id.get(str(clause.get("chunk_id")))
        if chunk is None:
            continue
        queue_rows.append(
            {
                "record_id": clause.get("record_id"),
                "source_id": clause.get("source_id"),
                "document_id": clause.get("document_id"),
                "document_type": clause.get("document_type"),
                "carrier": clause.get("carrier"),
                "supplier": clause.get("supplier"),
                "offer_or_circular_id": clause.get("offer_or_circular_id"),
                "chunk_id": clause.get("chunk_id"),
                "chunk_text": chunk.get("text"),
                "clause_text": clause.get("clause_text"),
                "heuristic": {
                    "policy_topic": clause.get("policy_topic"),
                    "action": clause.get("action"),
                    "penalty_type": clause.get("penalty_type"),
                    "penalty_amount": clause.get("penalty_amount"),
                    "penalty_currency": clause.get("penalty_currency"),
                    "penalty_percent": clause.get("penalty_percent"),
                    "fare_difference_required": clause.get("fare_difference_required"),
                    "time_window": clause.get("time_window"),
                    "before_after_departure": clause.get("before_after_departure"),
                    "tax_refund_rule": clause.get("tax_refund_rule"),
                    "eligibility_conditions": clause.get("eligibility_conditions", []),
                    "exceptions": clause.get("exceptions", []),
                },
                "manual_labeling_note": "Copy labeled fields into data/manual_gold_real_pilot_policy_clauses.jsonl using the same record_id.",
            }
        )

    queue_rows.sort(key=lambda r: (str(r.get("source_id")), str(r.get("chunk_id")), str(r.get("record_id"))))
    queue_rows = queue_rows[:limit]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for row in queue_rows:
            f.write(json.dumps(row) + "\n")

    return {
        "status": "ok",
        "queued_records": len(queue_rows),
        "output_path": str(output_path),
        "manual_gold_path": str(_default_manual_gold()),
    }


def compare_heuristic_vs_manual_gold(
    pred_path: Path,
    manual_gold_path: Path,
    fields: Optional[Sequence[str]] = None,
    manual_gold_db_path: Optional[Path] = None,
    manual_gold_postgres_dsn: Optional[str] = None,
) -> dict:
    use_fields = list(fields or DEFAULT_FIELDS)
    db_path = manual_gold_db_path
    backend = _labeling_storage_backend()
    if db_path is None and backend == "sqlite":
        db_path = _default_labeling_db()
    pg_dsn = (manual_gold_postgres_dsn or "").strip()
    if not pg_dsn and backend == "postgres":
        pg_dsn = _default_labeling_postgres_dsn()

    if pg_dsn and backend == "postgres" and postgres_manual_label_count(pg_dsn) > 0:
        gold_rows = {
            rid: row
            for rid, row in postgres_load_manual_label_map(pg_dsn).items()
            if str(row.get("label_status", "")).lower() != "skip"
        }
        if not gold_rows:
            return {
                "status": "blocked",
                "reason": "manual_gold_empty",
                "manual_gold_backend": "postgres",
                "message": "Postgres manual gold store has no non-skipped labels. Label records first, then rerun compare.",
            }
        pred_rows = _load_jsonl_by_record_id(pred_path)
        result = evaluate_extraction(gold_rows, pred_rows, use_fields)
        result["manual_gold_backend"] = "postgres"
    elif db_path and db_path.exists() and sqlite_manual_label_count(db_path) > 0:
        gold_rows = {
            rid: row
            for rid, row in sqlite_load_manual_label_map(db_path).items()
            if str(row.get("label_status", "")).lower() != "skip"
        }
        if not gold_rows:
            return {
                "status": "blocked",
                "reason": "manual_gold_empty",
                "manual_gold_db_path": str(db_path),
                "message": "SQLite manual gold store has no non-skipped labels. Label records first, then rerun compare.",
            }
        pred_rows = _load_jsonl_by_record_id(pred_path)
        result = evaluate_extraction(gold_rows, pred_rows, use_fields)
        result["manual_gold_db_path"] = str(db_path)
        result["manual_gold_backend"] = "sqlite"
    else:
        _ensure_file(manual_gold_path)
        raw = manual_gold_path.read_text(encoding="utf-8").strip()
        if not raw:
            return {
                "status": "blocked",
                "reason": "manual_gold_empty",
                "manual_gold_path": str(manual_gold_path),
                "message": "Manual gold file is empty. Label records first, then rerun compare.",
            }
        result = evaluate_files(manual_gold_path, pred_path, fields=use_fields)
        result["manual_gold_backend"] = "jsonl"
    result["status"] = "ok"
    result["manual_gold_path"] = str(manual_gold_path)
    result["pred_path"] = str(pred_path)
    return result


def main():
    parser = argparse.ArgumentParser(description="Manual labeling queue + comparison for real pilot documents")
    sub = parser.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("queue", help="Create a labeling queue from real pilot extracted clauses")
    q.add_argument("--manifest", default=str(_default_manifest()))
    q.add_argument("--chunks", default=str(_default_chunks()))
    q.add_argument("--clauses", default=str(_default_clauses()))
    q.add_argument("--output", default=str(_default_queue_out()))
    q.add_argument("--limit", type=int, default=50)

    c = sub.add_parser("compare", help="Compare heuristic predictions against manual gold")
    c.add_argument("--pred", default=str(_default_clauses()))
    c.add_argument("--manual-gold", default=str(_default_manual_gold()))
    c.add_argument("--manual-gold-db", default=str(_default_labeling_db()))
    c.add_argument("--manual-gold-postgres-dsn", default="")
    c.add_argument("--fields", nargs="*", default=DEFAULT_FIELDS)

    args = parser.parse_args()

    if args.cmd == "queue":
        result = build_labeling_queue(
            manifest_path=Path(args.manifest),
            chunks_path=Path(args.chunks),
            clauses_path=Path(args.clauses),
            output_path=Path(args.output),
            limit=args.limit,
        )
    else:
        result = compare_heuristic_vs_manual_gold(
            pred_path=Path(args.pred),
            manual_gold_path=Path(args.manual_gold),
            fields=args.fields,
            manual_gold_db_path=Path(args.manual_gold_db) if args.manual_gold_db else None,
            manual_gold_postgres_dsn=args.manual_gold_postgres_dsn or None,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
