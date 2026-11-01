"""Adjudication of the IAA sample: annotator A vs annotator B (2026-11-01).

`build` lists every disagreement between A (data/labeling_fine.db) and B
(data/labeling_iaa_b.db) on the IAA sample as one row per (record, field), with
both values side by side. The two annotators fill in the agreed value together
and the guide rule that decided it. A field counts as a disagreement when the
v1b scorer would not match A's value to B's (one side empty, or no field_match).
A skip/label difference is a "decision" row; when B labelled a record A skipped,
B's filled fields are listed too, and are used only if the decision becomes
"label".

`import` validates the returned workbook and writes the adjudicated labels for
the whole sample to data/labeling_adjudicated.db (agreed records keep A's label
unchanged), plus a log of every decision to results/v1c_adjudication.json.
Score against it with:
    python scripts/iaa_agreement.py --a-db data/labeling_fine.db --b-db data/labeling_adjudicated.db \
        --out results/v1c_iaa_adjudicated.json

Usage:
    python scripts/adjudication_sheet.py build --out handoff/adjudication.xlsx
    python scripts/adjudication_sheet.py import --workbook returned.xlsx
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font
from openpyxl.worksheet.datavalidation import DataValidation

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from labeling_store import sqlite_manual_label_count, sqlite_upsert_manual_label  # noqa: E402
from scripts.iaa_workbook import CHOICES, INPUT_FILL, _lines, _number  # noqa: E402

DECISION = "decision"
AGREED = ["A", "B", "other"]
LIST_FIELDS = {"eligibility_conditions", "exceptions"}
# (header, key, width); keys with None are read-only
COLUMNS = [
    ("#", None, 5), ("Record ID", None, 14), ("Carrier", None, 8), ("CLAUSE", None, 55),
    ("Surrounding text", None, 55), ("Field", None, 18), ("A (gold)", None, 22), ("B (second annotator)", None, 22),
    ("Agreed: A / B / other", "agreed", 12), ("If other: value", "other_value", 20),
    ("Guide rule or reason (required)", "reason", 36), ("Guide gap? (yes/no)", "gap", 10), ("Notes", "notes", 26),
]
FIRST_ROW = 2
INSTRUCTIONS = """Adjudication: annotator A and annotator B together

Each row is one field where your two labels differ. Decide together, using only the labelling guide.
- Agreed: "A" keeps A's value, "B" keeps B's, "other" uses the value you type in "If other: value"
  (leave that empty to mean "the field should be empty").
- Guide rule or reason: which guide rule decided it (e.g. "Step 1: topic = transaction", "example B").
  If the guide doesn't decide it, say so and write "yes" in "Guide gap?".
- A "decision" row is skip vs label. If you agree on "skip", the other rows for that record are ignored.
- Lists (eligibility, exceptions): one item per line.
- Don't use AI tools, and don't look at any model output.
- Columns A-H must not be edited.
"""


def load_db(db: Path) -> dict[str, tuple[str, dict]]:
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as con:
        return {rid: (status, json.loads(p)) for rid, p, status in
                con.execute("SELECT record_id, payload_json, label_status FROM manual_labels")}


def same(field: str, a, b) -> bool:
    if E.is_empty(a) or E.is_empty(b):
        return E.is_empty(a) and E.is_empty(b)
    return E.field_match(field, a, b)[0] and E.field_match(field, b, a)[0]


def show(value) -> str:
    if E.is_empty(value):
        return "(empty)"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list):
        return "\n".join(str(v) for v in value)
    return str(value)


def disagreements(queue: list[dict], a: dict, b: dict) -> list[dict]:
    rows = []
    for q in queue:
        rid = q["record_id"]
        (sa, pa), (sb, pb) = a[rid], b[rid]
        if sa != sb:
            rows.append({"q": q, "field": DECISION, "a": sa, "b": sb})
        if sb != "labeled" or sa != "labeled" and sa == sb:
            continue
        pa = pa if sa == "labeled" else {}
        rows += [{"q": q, "field": f, "a": pa.get(f), "b": pb.get(f)}
                 for f in E.FIELDS if not same(f, pa.get(f), pb.get(f))]
    return rows


def _sample(queue_path: Path, a_db: Path, b_db: Path) -> tuple[list[dict], dict, dict]:
    queue = E.load_queue(queue_path)
    a, b = load_db(a_db), load_db(b_db)
    missing = [q["record_id"] for q in queue if q["record_id"] not in a or q["record_id"] not in b]
    if missing:
        raise SystemExit(f"error: {len(missing)} sampled records lack an A or B label: {missing[:3]}")
    return queue, a, b


def read_only_values(n: int, row: dict) -> list:
    q = row["q"]
    return [n, q["record_id"], q.get("carrier"), q.get("clause_text"), q.get("chunk_text"),
            row["field"], show(row["a"]), show(row["b"])]


def build(queue_path: Path, a_db: Path, b_db: Path, out: Path) -> int:
    queue, a, b = _sample(queue_path, a_db, b_db)
    rows = disagreements(queue, a, b)
    wb = Workbook()
    info = wb.active
    info.title = "Instructions"
    for i, line in enumerate(INSTRUCTIONS.splitlines(), 1):
        info.cell(row=i, column=1, value=line).font = Font(bold=i == 1)
    info.column_dimensions["A"].width = 110

    ws = wb.create_sheet("Adjudicate")
    for c, (header, _, width) in enumerate(COLUMNS, 1):
        cell = ws.cell(row=1, column=c, value=header)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.column_dimensions[cell.column_letter].width = width
    for r, row in enumerate(rows, FIRST_ROW):
        values = read_only_values(r - 1, row)
        for c in range(1, len(COLUMNS) + 1):
            cell = ws.cell(row=r, column=c, value=values[c - 1] if c <= len(values) else None)
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            if c > len(values):
                cell.fill = INPUT_FILL
        ws.row_dimensions[r].height = 90
    ws.freeze_panes = "I2"
    last = FIRST_ROW + len(rows) - 1
    for key, options in (("agreed", AGREED), ("gap", ["yes", "no"])):
        col = next(i for i, (_, k, _) in enumerate(COLUMNS, 1) if k == key)
        letter = ws.cell(row=1, column=col).column_letter
        dv = DataValidation(type="list", formula1='"' + ",".join(options) + '"', allow_blank=True)
        ws.add_data_validation(dv)
        dv.add(f"{letter}{FIRST_ROW}:{letter}{last}")
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    n_rec = len({r["q"]["record_id"] for r in rows})
    print(f"wrote {len(rows)} disagreements on {n_rec}/{len(queue)} records to {out}")
    return 0


def parse_other(field: str, value, where: str, errors: list[str]):
    """The typed 'other' value for a field; empty means the field is empty."""
    if E.is_empty(value):
        return None
    text = str(value).strip()
    if field == DECISION:
        if text.lower() not in ("skip", "label"):
            errors.append(f"{where}: decision must be 'skip' or 'label'")
        return {"skip": "skip", "label": "labeled"}.get(text.lower())
    if field in CHOICES:
        if text.lower() not in CHOICES[field]:
            errors.append(f"{where}: {field} '{text}' is not one of {CHOICES[field]}")
            return None
        return text.lower() == "true" if field == "fare_difference_required" else text.lower()
    if field in E.NUMERIC:
        return _number(text, field, errors, where)
    if field in E.CURRENCY:
        return text.upper()
    return _lines(text) if field in LIST_FIELDS else text


def read_decisions(workbook: Path, expected: list[dict]) -> tuple[list[dict], list[str]]:
    ws = load_workbook(workbook, data_only=True)["Adjudicate"]
    if [c.value for c in ws[1]] != [h for h, _, _ in COLUMNS]:
        return [], ["the Adjudicate sheet's columns were changed; expected the original layout"]
    got = [row for row in ws.iter_rows(min_row=FIRST_ROW, values_only=True) if any(row)]
    if len(got) != len(expected):
        return [], [f"expected {len(expected)} rows, found {len(got)}"]
    errors: list[str] = []
    decisions = []
    n_ro = len(read_only_values(0, expected[0])) if expected else 0
    for r, (row, exp) in enumerate(zip(got, expected), FIRST_ROW):
        where = f"row {r} ({exp['q']['record_id']} {exp['field']})"
        if [str(v) if v is not None else None for v in row[1:n_ro]] != \
                [str(v) if v is not None else None for v in read_only_values(0, exp)[1:]]:
            errors.append(f"{where}: read-only columns were edited or rows reordered")
            continue
        cells = {key: row[i] for i, (_, key, _) in enumerate(COLUMNS) if key}
        agreed = str(cells["agreed"] or "").strip()
        if agreed not in AGREED:
            errors.append(f"{where}: 'Agreed' must be A, B or other")
            continue
        if E.is_empty(cells["reason"]):
            errors.append(f"{where}: the guide rule or reason is required")
        value = {"A": exp["a"], "B": exp["b"]}.get(agreed)
        if agreed == "other":
            value = parse_other(exp["field"], cells["other_value"], where, errors)
        decisions.append({"record_id": exp["q"]["record_id"], "field": exp["field"], "agreed": agreed,
                          "value": value, "reason": cells["reason"],
                          "guide_gap": str(cells["gap"] or "").strip().lower() == "yes", "notes": cells["notes"]})
    return decisions, errors


def adjudicate(queue: list[dict], a: dict, b: dict, decisions: list[dict]) -> list[dict]:
    """One payload per sampled record: A's label with the agreed values applied."""
    by_rec = collections.defaultdict(dict)
    for d in decisions:
        by_rec[d["record_id"]][d["field"]] = d["value"]
    payloads = []
    for q in queue:
        rid = q["record_id"]
        (sa, pa), (_, pb) = a[rid], b[rid]
        fixes = by_rec.get(rid, {})
        status = fixes.get(DECISION, sa)
        if status == "labeled":
            base = dict(pa) if sa == "labeled" else dict(pb)
            if sa != "labeled":  # B's label is the starting point; only adjudicated fields come from it
                base = {k: v for k, v in base.items() if k not in E.FIELDS}
            payload = {**base, **{f: v for f, v in fixes.items() if f != DECISION}}
        else:
            payload = {k: v for k, v in pa.items() if k not in E.FIELDS}
        payloads.append({**payload, "record_id": rid, "label_status": status})
    return payloads


def import_workbook(workbook: Path, queue_path: Path, a_db: Path, b_db: Path,
                    out_db: Path, log: Path, force: bool) -> int:
    if out_db.exists() and sqlite_manual_label_count(out_db) > 0 and not force:
        print(f"error: {out_db} already has labels; pass --force to overwrite them", file=sys.stderr)
        return 1
    queue, a, b = _sample(queue_path, a_db, b_db)
    decisions, errors = read_decisions(workbook, disagreements(queue, a, b))
    if errors:
        print(f"{len(errors)} problem(s); nothing imported:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    payloads = adjudicate(queue, a, b, decisions)
    for p in payloads:
        sqlite_upsert_manual_label(out_db, p)
    wins = collections.Counter(d["agreed"] for d in decisions)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(json.dumps({"n_records": len(payloads), "n_disagreements": len(decisions),
                               "agreed_counts": dict(wins),
                               "n_guide_gaps": sum(d["guide_gap"] for d in decisions),
                               "decisions": decisions}, indent=2, default=str), encoding="utf-8")
    print(f"adjudicated {len(decisions)} disagreements (A {wins['A']}, B {wins['B']}, other {wins['other']}); "
          f"wrote {len(payloads)} records to {out_db} and the log to {log}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("build", "import"):
        p = sub.add_parser(name)
        p.add_argument("--queue", type=Path, default=Path("data/queue_iaa_b.jsonl"))
        p.add_argument("--a-db", type=Path, default=Path("data/labeling_fine.db"))
        p.add_argument("--b-db", type=Path, default=Path("data/labeling_iaa_b.db"))
        if name == "build":
            p.add_argument("--out", type=Path, required=True)
        else:
            p.add_argument("--workbook", type=Path, required=True)
            p.add_argument("--db", type=Path, default=Path("data/labeling_adjudicated.db"))
            p.add_argument("--log", type=Path, default=Path("results/v1c_adjudication.json"))
            p.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if args.cmd == "build":
        return build(args.queue, args.a_db, args.b_db, args.out)
    return import_workbook(args.workbook, args.queue, args.a_db, args.b_db, args.db, args.log, args.force)


if __name__ == "__main__":
    raise SystemExit(main())
