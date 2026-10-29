"""Spreadsheet hand-off for the second annotator (2026-10-29).

`build` writes the IAA sample (data/queue_iaa_b.jsonl) to an .xlsx workbook the
annotator fills in Excel, with drop-downs for every categorical field and no
model or heuristic output. `import` validates the returned workbook and writes
it to annotator B's own database (default data/labeling_iaa_b.db), in the same
payload shape the labelling tool saves, so scripts/iaa_agreement.py reads it
unchanged.

Only the scored fields (evals.v1b_eval.FIELDS) are separate columns. Penalty
tiers and refund payout options are not scored, so they go in a free-text
column that is kept in the notes.

Usage:
    python scripts/iaa_workbook.py build --out output/iaa_annotator_b.xlsx
    python scripts/iaa_workbook.py import --workbook returned.xlsx
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from extractors.schema import BeforeAfterDeparture, PenaltyType, PolicyAction, PolicyTopic  # noqa: E402
from labeling_store import sqlite_manual_label_count, sqlite_upsert_manual_label  # noqa: E402

DECISIONS = ["label", "skip"]
CHOICES = {
    "policy_topic": [e.value for e in PolicyTopic],
    "action": [e.value for e in PolicyAction],
    "penalty_type": [e.value for e in PenaltyType],
    "before_after_departure": [e.value for e in BeforeAfterDeparture],
    "fare_difference_required": ["true", "false"],
}
# (header, payload key or None for read-only, column width)
COLUMNS = [
    ("#", None, 5), ("Record ID", None, 14), ("Carrier", None, 8), ("Document type", None, 12),
    ("CLAUSE (label this)", None, 60), ("Surrounding text (context only)", None, 70),
    ("Decision", "decision", 10), ("Topic", "policy_topic", 16), ("Action", "action", 13),
    ("Penalty type", "penalty_type", 20), ("Penalty amount", "penalty_amount", 11),
    ("Currency", "penalty_currency", 9), ("Penalty % (lost)", "penalty_percent", 11),
    ("Fare difference", "fare_difference_required", 11), ("Tax refund rule", "tax_refund_rule", 22),
    ("Time window", "time_window", 15), ("Before/after departure", "before_after_departure", 18),
    ("Eligibility (one per line)", "eligibility_conditions", 32), ("Exceptions (one per line)", "exceptions", 26),
    ("Tiers / refund options (optional)", "tiers_text", 30), ("Notes", "notes", 32),
]
FIRST_ROW = 2
INPUT_FILL = PatternFill("solid", fgColor="FFF7D6")
INSTRUCTIONS = """Labelling workbook: second annotator

Rules (these make the result valid)
1. Work alone. Don't discuss the records with anyone until you've finished all 50.
2. No AI tools (ChatGPT, Claude, Copilot...). Don't look anything up on airline websites.
3. Use only the labelling guide (sheet "Guide"). Read it once fully before you start.
4. When unsure, choose your best answer and explain in Notes. Don't leave a row half done.

How to fill the "Label" sheet
- Column E is the clause to label. Column F is only there to help you understand it.
- Yellow columns are yours. Columns A-F must not be edited.
- Decision: "skip" if the clause states no rule (menus, contact details, cut-off fragments: guide Step 0).
  Otherwise "label", then fill Topic and Action (required) and any other field the CLAUSE itself states.
- Empty means "the clause doesn't say". That is often the right answer.
- Drop-downs: Topic, Action, Penalty type, Fare difference, Before/after. Type other fields.
- Eligibility / Exceptions: one item per line (Alt+Enter for a new line inside a cell).
- Tiers / refund options: optional. If the rule has different fees per time band, or says how a refund
  is paid, describe it in plain words here (e.g. "31 days: 0% lost; 8-30 days: 25% lost").
- The guide mentions form features (locked fields, pop-ups, "+ Tier" buttons). Those are for the web
  tool; in this workbook just follow the same rules by hand.

When you're done: save the file and return it. Please don't change answers afterwards.
"""


def build(queue_path: Path, guide_path: Path, out: Path) -> int:
    rows = E.load_queue(queue_path)
    wb = Workbook()
    ws_info = wb.active
    ws_info.title = "Instructions"
    headings = {"Labelling workbook: second annotator", "Rules (these make the result valid)",
                'How to fill the "Label" sheet'}
    for i, line in enumerate(INSTRUCTIONS.splitlines(), 1):
        ws_info.cell(row=i, column=1, value=line).font = Font(bold=line in headings)
    ws_info.column_dimensions["A"].width = 110

    ws = wb.create_sheet("Label")
    for c, (header, key, width) in enumerate(COLUMNS, 1):
        cell = ws.cell(row=1, column=c, value=header)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.column_dimensions[cell.column_letter].width = width
    for r, q in enumerate(rows, FIRST_ROW):
        values = [r - 1, q["record_id"], q.get("carrier"), q.get("document_type"),
                  q.get("clause_text"), q.get("chunk_text")]
        for c, v in enumerate(values, 1):
            ws.cell(row=r, column=c, value=v).alignment = Alignment(wrap_text=True, vertical="top")
        for c in range(len(values) + 1, len(COLUMNS) + 1):
            cell = ws.cell(row=r, column=c)
            cell.fill = INPUT_FILL
            cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.row_dimensions[r].height = 120
    ws.freeze_panes = "G2"

    ws_lists = wb.create_sheet("Lists")
    lists = {"decision": DECISIONS, **CHOICES}
    last = FIRST_ROW + len(rows) - 1
    for c, (key, values) in enumerate(lists.items(), 1):
        for r, v in enumerate(values, 1):
            ws_lists.cell(row=r, column=c, value=v)
        letter = ws_lists.cell(row=1, column=c).column_letter
        dv = DataValidation(type="list", formula1=f"=Lists!${letter}$1:${letter}${len(values)}",
                            allow_blank=True, showErrorMessage=True,
                            errorTitle="Not in the list", error="Pick a value from the drop-down, or leave it empty.")
        ws.add_data_validation(dv)
        col = next(i for i, (_, k, _) in enumerate(COLUMNS, 1) if k == key)
        dv.add(f"{ws.cell(row=FIRST_ROW, column=col).column_letter}{FIRST_ROW}:"
               f"{ws.cell(row=last, column=col).column_letter}{last}")
    ws_lists.sheet_state = "hidden"

    ws_guide = wb.create_sheet("Guide")
    ws_guide.column_dimensions["A"].width = 140
    for i, line in enumerate(guide_path.read_text(encoding="utf-8").splitlines(), 1):
        cell = ws_guide.cell(row=i, column=1, value=line)
        cell.alignment = Alignment(wrap_text=True)
        if line.startswith("#"):
            cell.font = Font(bold=True)

    ws.protection.sheet = False  # editing must stay simple; columns A-F are checked on import
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    print(f"wrote {len(rows)} records to {out}")
    return 0


def _lines(value) -> list[str]:
    return [s.strip() for s in re.split(r"[\r\n]+", str(value or "")) if s.strip()]


def _number(value, field: str, errors: list[str], where: str) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        errors.append(f"{where}: {field} '{value}' is not a number")
        return None


def parse_row(cells: dict, queue_row: dict, where: str, errors: list[str]) -> dict | None:
    """One workbook row -> labelling-tool payload; appends problems to `errors`."""
    text = {k: (str(v).strip() if v is not None else "") for k, v in cells.items()}
    decision = text["decision"].lower()
    if decision not in DECISIONS:
        errors.append(f"{where}: Decision must be 'label' or 'skip' (got '{text['decision']}')")
        return None
    base = {k: queue_row.get(k) for k in ("record_id", "source_id", "document_id", "document_type",
                                           "carrier", "chunk_id", "clause_text")}
    notes = "\n".join(filter(None, [text["notes"], f"Tiers/refund options: {text['tiers_text']}" if text["tiers_text"] else ""]))
    if decision == "skip":
        return {**base, "label_status": "skip", "notes": notes or None}

    for key, allowed in CHOICES.items():
        if text[key] and text[key].lower() not in allowed:
            errors.append(f"{where}: {key} '{text[key]}' is not one of {allowed}")
    for key in ("policy_topic", "action"):
        if not text[key]:
            errors.append(f"{where}: {key} is required when Decision is 'label'")
    fare_diff = {"true": True, "false": False}.get(text["fare_difference_required"].lower())
    return {
        **base, "label_status": "labeled",
        "policy_topic": text["policy_topic"].lower() or None,
        "action": text["action"].lower() or None,
        "penalty_type": text["penalty_type"].lower() or None,
        "penalty_amount": _number(cells["penalty_amount"], "penalty_amount", errors, where),
        "penalty_currency": text["penalty_currency"].upper() or None,
        "penalty_percent": _number(cells["penalty_percent"], "penalty_percent", errors, where),
        "fare_difference_required": fare_diff,
        "tax_refund_rule": text["tax_refund_rule"] or None,
        "time_window": text["time_window"] or None,
        "before_after_departure": text["before_after_departure"].lower() or None,
        "eligibility_conditions": _lines(cells["eligibility_conditions"]),
        "exceptions": _lines(cells["exceptions"]),
        "notes": notes or None,
    }


def import_workbook(workbook: Path, queue_path: Path, db_path: Path, force: bool) -> int:
    if db_path.exists() and sqlite_manual_label_count(db_path) > 0 and not force:
        print(f"error: {db_path} already has labels; pass --force to overwrite them", file=sys.stderr)
        return 1
    queue = {q["record_id"]: q for q in E.load_queue(queue_path)}
    ws = load_workbook(workbook, data_only=True)["Label"]
    headers = [c.value for c in ws[1]]
    if headers != [h for h, _, _ in COLUMNS]:
        print("error: the Label sheet's columns were changed; expected the original layout", file=sys.stderr)
        return 1

    errors: list[str] = []
    payloads: list[dict] = []
    seen: set[str] = set()
    for r, row in enumerate(ws.iter_rows(min_row=FIRST_ROW, values_only=True), FIRST_ROW):
        if not any(row):
            continue
        rid = str(row[1] or "")
        where = f"row {r} ({rid})"
        if rid not in queue:
            errors.append(f"{where}: record ID is not in the sample")
            continue
        if row[4] != queue[rid].get("clause_text"):
            errors.append(f"{where}: the clause text was edited")
        seen.add(rid)
        cells = {key: row[c] for c, (_, key, _) in enumerate(COLUMNS) if key}
        payload = parse_row(cells, queue[rid], where, errors)
        if payload:
            payloads.append(payload)
    missing = sorted(set(queue) - seen)
    if missing:
        errors.append(f"{len(missing)} sampled records are missing from the workbook: {missing[:3]}")
    if errors:
        print(f"{len(errors)} problem(s); nothing imported:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    for p in payloads:
        sqlite_upsert_manual_label(db_path, p)
    n_skip = sum(p["label_status"] == "skip" for p in payloads)
    print(f"imported {len(payloads)} records into {db_path} ({len(payloads) - n_skip} labelled, {n_skip} skipped)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--queue", type=Path, default=Path("data/queue_iaa_b.jsonl"))
    b.add_argument("--guide", type=Path, default=Path("docs/LABELING_GUIDE.md"))
    b.add_argument("--out", type=Path, required=True)
    i = sub.add_parser("import")
    i.add_argument("--workbook", type=Path, required=True)
    i.add_argument("--queue", type=Path, default=Path("data/queue_iaa_b.jsonl"))
    i.add_argument("--db", type=Path, default=Path("data/labeling_iaa_b.db"))
    i.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if args.cmd == "build":
        return build(args.queue, args.guide, args.out)
    return import_workbook(args.workbook, args.queue, args.db, args.force)


if __name__ == "__main__":
    raise SystemExit(main())
