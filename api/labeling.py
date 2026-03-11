import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from labeling_store import (
    postgres_import_sqlite_if_empty,
    postgres_import_jsonl_if_empty,
    postgres_load_manual_label_map,
    postgres_upsert_manual_label,
    sqlite_import_jsonl_if_empty,
    sqlite_load_manual_label_map,
    sqlite_upsert_manual_label,
)

router = APIRouter(tags=["labeling"])


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_labeling_page_path() -> Path:
    return _repo_root() / "web" / "labeling.html"


def default_labeling_queue_path() -> Path:
    return Path(
        os.getenv(
            "LABELING_QUEUE_PATH",
            str(_repo_root() / "data" / "real_pilot_manual_labeling_queue.jsonl"),
        )
    )


def default_manual_gold_path() -> Path:
    return Path(
        os.getenv(
            "MANUAL_GOLD_PATH",
            str(_repo_root() / "data" / "manual_gold_real_pilot_policy_clauses.jsonl"),
        )
    )


def default_labeling_db_path() -> Path:
    return Path(
        os.getenv(
            "LABELING_DB_PATH",
            str(_repo_root() / "data" / "labeling.db"),
        )
    )


def default_labeling_postgres_dsn() -> str:
    return os.getenv("LABELING_POSTGRES_DSN", "").strip()


def labeling_storage_backend() -> str:
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
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _write_jsonl(path: Path, rows: List[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _init_sqlite_store() -> None:
    # One-time lazy migration path for users with existing JSONL labels.
    sqlite_import_jsonl_if_empty(default_labeling_db_path(), default_manual_gold_path())


def _init_postgres_store() -> None:
    dsn = default_labeling_postgres_dsn()
    if not dsn:
        raise RuntimeError("LABELING_POSTGRES_DSN is required when LABELING_STORAGE_BACKEND=postgres")
    # One-time lazy bootstrap from existing local stores if the table is empty.
    postgres_import_sqlite_if_empty(dsn, default_labeling_db_path())
    postgres_import_jsonl_if_empty(dsn, default_manual_gold_path())


def _manual_gold_map() -> Dict[str, dict]:
    backend = labeling_storage_backend()
    if backend == "sqlite":
        _init_sqlite_store()
        return sqlite_load_manual_label_map(default_labeling_db_path())
    if backend == "postgres":
        _init_postgres_store()
        return postgres_load_manual_label_map(default_labeling_postgres_dsn())
    return {
        str(row.get("record_id")): row
        for row in _load_jsonl(default_manual_gold_path())
        if row.get("record_id")
    }


def _normalize_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def _normalize_list(values: Optional[List[str]]) -> List[str]:
    if not values:
        return []
    out: List[str] = []
    seen = set()
    for item in values:
        v = (item or "").strip()
        if not v:
            continue
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _normalize_bool_tri(value: Optional[bool]) -> Optional[bool]:
    if value is None:
        return None
    return bool(value)


def _clean_optional_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in data.items():
        if v is None:
            continue
        if isinstance(v, list) and not v:
            continue
        out[k] = v
    return out


class PenaltyTierInput(BaseModel):
    time_window: Optional[str] = None
    before_after_departure: Optional[str] = None
    penalty_type: Optional[str] = None
    penalty_amount: Optional[float] = None
    penalty_currency: Optional[str] = None
    penalty_percent: Optional[float] = None
    fare_difference_required: Optional[bool] = None
    notes: Optional[str] = None


class RefundPayoutOptionInput(BaseModel):
    method: Optional[str] = None
    allowed: Optional[bool] = None
    fee_type: Optional[str] = None
    fee_amount: Optional[float] = None
    fee_currency: Optional[str] = None
    fee_percent: Optional[float] = None
    time_window: Optional[str] = None
    before_after_departure: Optional[str] = None
    notes: Optional[str] = None


def _normalize_penalty_tiers(rows: Optional[List[PenaltyTierInput]]) -> List[dict]:
    if not rows:
        return []
    out: List[dict] = []
    for row in rows:
        item = _clean_optional_dict(
            {
                "time_window": _normalize_str(row.time_window),
                "before_after_departure": _normalize_str(row.before_after_departure),
                "penalty_type": _normalize_str(row.penalty_type),
                "penalty_amount": row.penalty_amount,
                "penalty_currency": _normalize_str(row.penalty_currency),
                "penalty_percent": row.penalty_percent,
                "fare_difference_required": _normalize_bool_tri(row.fare_difference_required),
                "notes": _normalize_str(row.notes),
            }
        )
        if item:
            out.append(item)
    return out


def _normalize_refund_payout_options(rows: Optional[List[RefundPayoutOptionInput]]) -> List[dict]:
    if not rows:
        return []
    out: List[dict] = []
    for row in rows:
        item = _clean_optional_dict(
            {
                "method": _normalize_str(row.method),
                "allowed": _normalize_bool_tri(row.allowed),
                "fee_type": _normalize_str(row.fee_type),
                "fee_amount": row.fee_amount,
                "fee_currency": _normalize_str(row.fee_currency),
                "fee_percent": row.fee_percent,
                "time_window": _normalize_str(row.time_window),
                "before_after_departure": _normalize_str(row.before_after_departure),
                "notes": _normalize_str(row.notes),
            }
        )
        if item:
            out.append(item)
    return out


def _queue_rows(include_labeled: bool = True) -> List[dict]:
    queue_rows = _load_jsonl(default_labeling_queue_path())
    gold_map = _manual_gold_map()

    rows: List[dict] = []
    for row in queue_rows:
        record_id = str(row.get("record_id") or "")
        if not record_id:
            continue
        manual = gold_map.get(record_id)
        item = dict(row)
        item["is_labeled"] = manual is not None
        item["is_skipped"] = bool(manual and str(manual.get("label_status", "")).lower() == "skip")
        item["manual_label"] = manual
        if not include_labeled and item["is_labeled"]:
            continue
        rows.append(item)
    return rows


class ManualLabelSaveRequest(BaseModel):
    record_id: str
    source_id: Optional[str] = None
    document_id: Optional[str] = None
    document_type: Optional[str] = None
    carrier: Optional[str] = None
    supplier: Optional[str] = None
    offer_or_circular_id: Optional[str] = None
    chunk_id: Optional[str] = None
    clause_text: Optional[str] = None
    label_status: Optional[str] = None

    policy_topic: Optional[str] = None
    action: Optional[str] = None
    penalty_type: Optional[str] = None
    penalty_amount: Optional[float] = None
    penalty_currency: Optional[str] = None
    penalty_percent: Optional[float] = None
    fare_difference_required: Optional[bool] = None
    time_window: Optional[str] = None
    before_after_departure: Optional[str] = None
    tax_refund_rule: Optional[str] = None
    eligibility_conditions: List[str] = Field(default_factory=list)
    exceptions: List[str] = Field(default_factory=list)
    penalty_tiers: List[PenaltyTierInput] = Field(default_factory=list)
    refund_payout_options: List[RefundPayoutOptionInput] = Field(default_factory=list)
    notes: Optional[str] = None


def _build_manual_gold_record(payload: ManualLabelSaveRequest) -> dict:
    label_status = _normalize_str(payload.label_status)
    if label_status:
        label_status = label_status.lower()
    record = {
        "record_id": payload.record_id,
        "source_id": _normalize_str(payload.source_id),
        "document_id": _normalize_str(payload.document_id),
        "document_type": _normalize_str(payload.document_type),
        "carrier": _normalize_str(payload.carrier),
        "supplier": _normalize_str(payload.supplier),
        "offer_or_circular_id": _normalize_str(payload.offer_or_circular_id),
        "chunk_id": _normalize_str(payload.chunk_id),
        "clause_text": _normalize_str(payload.clause_text),
        "label_status": label_status,
        "policy_topic": _normalize_str(payload.policy_topic),
        "action": _normalize_str(payload.action),
        "penalty_type": _normalize_str(payload.penalty_type),
        "penalty_amount": payload.penalty_amount,
        "penalty_currency": _normalize_str(payload.penalty_currency),
        "penalty_percent": payload.penalty_percent,
        "fare_difference_required": _normalize_bool_tri(payload.fare_difference_required),
        "time_window": _normalize_str(payload.time_window),
        "before_after_departure": _normalize_str(payload.before_after_departure),
        "tax_refund_rule": _normalize_str(payload.tax_refund_rule),
        "eligibility_conditions": _normalize_list(payload.eligibility_conditions),
        "exceptions": _normalize_list(payload.exceptions),
        "penalty_tiers": _normalize_penalty_tiers(payload.penalty_tiers),
        "refund_payout_options": _normalize_refund_payout_options(payload.refund_payout_options),
        "notes": _normalize_str(payload.notes),
    }
    # remove keys with None/empty list except record_id (eval script ignores extras)
    cleaned = {"record_id": payload.record_id}
    for k, v in record.items():
        if k == "record_id":
            continue
        if v is None:
            continue
        if isinstance(v, list) and not v:
            continue
        cleaned[k] = v
    return cleaned


def upsert_manual_gold_record(payload: ManualLabelSaveRequest) -> dict:
    new_record = _build_manual_gold_record(payload)
    backend = labeling_storage_backend()

    if backend == "sqlite":
        _init_sqlite_store()
        db_path = default_labeling_db_path()
        updated = sqlite_upsert_manual_label(db_path, new_record)
        return {
            "saved": True,
            "updated": updated,
            "record": new_record,
            "backend": "sqlite",
            "path": str(db_path),
        }
    if backend == "postgres":
        _init_postgres_store()
        updated = postgres_upsert_manual_label(default_labeling_postgres_dsn(), new_record)
        return {
            "saved": True,
            "updated": updated,
            "record": new_record,
            "backend": "postgres",
            "path": "postgres:manual_labels",
        }

    path = default_manual_gold_path()
    rows = _load_jsonl(path)
    replaced = False
    updated_rows: List[dict] = []
    for row in rows:
        if str(row.get("record_id")) == payload.record_id:
            updated_rows.append(new_record)
            replaced = True
        else:
            updated_rows.append(row)
    if not replaced:
        updated_rows.append(new_record)

    _write_jsonl(path, updated_rows)
    return {
        "saved": True,
        "updated": replaced,
        "record": new_record,
        "backend": "jsonl",
        "path": str(path),
    }


@router.get("/labeling", response_class=HTMLResponse)
def labeling_page():
    page_path = default_labeling_page_path()
    if not page_path.exists():
        raise HTTPException(status_code=404, detail="Labeling page not found")
    return HTMLResponse(page_path.read_text(encoding="utf-8"))


@router.get("/labeling/api/queue")
def labeling_queue(include_labeled: bool = True):
    queue_path = default_labeling_queue_path()
    rows = _queue_rows(include_labeled=include_labeled)
    total = len(_load_jsonl(queue_path))
    labeled = sum(1 for row in rows if row.get("is_labeled"))
    skipped = sum(1 for row in rows if row.get("is_skipped"))
    unlabeled = sum(1 for row in rows if not row.get("is_labeled"))
    return {
        "queue_path": str(queue_path),
        "manual_gold_path": str(default_manual_gold_path()),
        "manual_gold_backend": labeling_storage_backend(),
        "labeling_db_path": str(default_labeling_db_path()),
        "labeling_postgres_configured": bool(default_labeling_postgres_dsn()),
        "exists": queue_path.exists(),
        "total_records": total if include_labeled else len(rows),
        "returned_records": len(rows),
        "labeled_records_in_response": labeled,
        "skipped_records_in_response": skipped,
        "unlabeled_records_in_response": unlabeled,
        "rows": rows,
    }


@router.post("/labeling/api/save")
def labeling_save(req: ManualLabelSaveRequest):
    result = upsert_manual_gold_record(req)
    return result
