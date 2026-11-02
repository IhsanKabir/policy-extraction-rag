"""Adjudication of the IAA sample: annotator A vs annotator B (2026-11-01).

`build` lists every disagreement between A (data/labeling_fine.db) and B
(data/labeling_iaa_b.db) on the IAA sample as one row per (record, field), with
both values side by side. The two annotators fill in the agreed value together
and the ID of the guide rule that decided it (sheet "Rules": the current
guide split into numbered rules). A field counts as a disagreement when the
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
import re
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
GAP_ID = "GAP"
GAP_TEXT = "No rule in the guide decides this (guide gap). Explain in Notes."
RULE_TEXT_HEADER = "Rule text (auto, click to open)"
# (header, key, width); keys with None are read-only or formulas
COLUMNS = [
    ("#", None, 5), ("Record ID", None, 14), ("Carrier", None, 8), ("CLAUSE", None, 55),
    ("Surrounding text", None, 55), ("Field", None, 18), ("A (gold)", None, 22), ("B (second annotator)", None, 22),
    ("Agreed: A / B / other", "agreed", 12), ("If other: value", "other_value", 20),
    ("Guide rule ID (required)", "rule_id", 12), (RULE_TEXT_HEADER, None, 45), ("Notes", "notes", 30),
]
FIRST_ROW = 2
INSTRUCTIONS = """Adjudication: annotator A and annotator B together

Each row is one field where your two labels differ. Decide together, using only the labelling guide.
- Agreed: "A" keeps A's value, "B" keeps B's, "other" uses the value you type in "If other: value"
  (leave that empty to mean "the field should be empty").
- Guide rule ID: pick the rule that decided it from the drop-down. The sheet "Rules" is the full current
  guide (incl. the 2026-11-01 addendum), split into numbered rules. The next column shows the rule's text;
  click it to jump to the rule. If several rules apply, pick the main one and name the others in Notes.
  If no rule decides it, pick GAP and explain in Notes.
- A "decision" row is skip vs label. If you agree on "skip", the other rows for that record are ignored.
- Lists (eligibility, exceptions): one item per line.
- Help from a person is fine (say who in Notes). Don't use AI tools, and don't look at any model output.
- Columns A-H, column L and the Rules sheet must not be edited.
"""


def parse_rules(guide_md: str) -> list[tuple[str, str, str]]:
    """Split the labelling guide into numbered rules: (id, section, text).

    A rule is a bullet, a numbered item, a table row (header rows excluded) or a
    paragraph; indented lines continue the rule above. A lead-in line such as
    "Action:" right before a table prefixes each of its rows instead. Each
    worked example is one rule, including its table."""
    rules: list[list[str]] = []
    section, prefix, in_examples, prev_blank = "", "", False, True
    lines = guide_md.splitlines()
    for i, raw in enumerate(lines):
        line = raw.strip().replace("**", "")
        if not line:
            prev_blank = True
            continue
        if line.startswith("#"):
            section = line.lstrip("#").strip()
            in_examples = section.lower().startswith("worked examples")
            prev_blank = True
            continue
        nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
        indented = raw.startswith((" ", "\t"))
        is_table = line.startswith("|")
        if is_table and (set(line) <= set("|-: ") or nxt.startswith("|-")):
            continue  # separator or header row
        if not is_table:
            prefix = ""
        if not in_examples and not indented and line.endswith(":") and nxt.startswith("|"):
            prefix = line.rstrip(":").strip()
            continue
        if in_examples:
            starts_rule = bool(re.match(r"^[A-Z]\. ", line))
        else:
            starts_rule = not indented and bool(re.match(r"^(- |\d+\. )", line) or is_table or prev_blank)
        if starts_rule or not rules:
            text = line
            if is_table:
                text = " : ".join(c.strip() for c in line.strip("|").split("|"))
                text = f"{prefix} {text}" if prefix else text
            rules.append([section, re.sub(r"^(- |\d+\. )", "", text)])
        else:
            rules[-1][1] += "\n" + line
        prev_blank = False
    return [(f"R{n}", sec, text) for n, (sec, text) in enumerate(rules, 1)] + [(GAP_ID, "", GAP_TEXT)]


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


def _col(key_or_header: str) -> int:
    return next(i for i, (h, k, _) in enumerate(COLUMNS, 1) if key_or_header in (h, k))


def add_rules_sheet(wb: Workbook, rules: list[tuple[str, str, str]]) -> None:
    ws = wb.create_sheet("Rules")
    for c, (header, width) in enumerate((("Rule ID", 9), ("Guide section", 28), ("Rule (guide wording)", 120)), 1):
        cell = ws.cell(row=1, column=c, value=header)
        cell.font = Font(bold=True)
        ws.column_dimensions[cell.column_letter].width = width
    for r, rule in enumerate(rules, 2):
        for c, v in enumerate(rule, 1):
            ws.cell(row=r, column=c, value=v).alignment = Alignment(wrap_text=True, vertical="top")
    ws.freeze_panes = "A2"


def link_rule_columns(ws, n_rules: int, last: int) -> None:
    """Rule-ID drop-down, plus a formula that shows the rule and links to it."""
    id_letter = ws.cell(row=1, column=_col("rule_id")).column_letter
    dv = DataValidation(type="list", formula1=f"=Rules!$A$2:$A${n_rules + 1}", allow_blank=True,
                        showErrorMessage=True, errorTitle="Not a rule ID", error="Pick a rule ID from the list.")
    ws.add_data_validation(dv)
    dv.add(f"{id_letter}{FIRST_ROW}:{id_letter}{last}")
    text_col = _col(RULE_TEXT_HEADER)
    for r in range(FIRST_ROW, last + 1):
        ref = f"{id_letter}{r}"
        ws.cell(row=r, column=text_col, value=(
            f'=IF({ref}="","",IFERROR(HYPERLINK("#Rules!A"&MATCH({ref},Rules!$A:$A,0),'
            f'VLOOKUP({ref},Rules!$A:$C,3,FALSE)),""))'))


def build(queue_path: Path, a_db: Path, b_db: Path, guide: Path, out: Path) -> int:
    queue, a, b = _sample(queue_path, a_db, b_db)
    rows = disagreements(queue, a, b)
    rules = parse_rules(guide.read_text(encoding="utf-8"))
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
            if c > len(values) and COLUMNS[c - 1][1]:
                cell.fill = INPUT_FILL
        ws.row_dimensions[r].height = 90
    ws.freeze_panes = "I2"
    last = FIRST_ROW + len(rows) - 1
    letter = ws.cell(row=1, column=_col("agreed")).column_letter
    dv = DataValidation(type="list", formula1='"' + ",".join(AGREED) + '"', allow_blank=True)
    ws.add_data_validation(dv)
    dv.add(f"{letter}{FIRST_ROW}:{letter}{last}")
    add_rules_sheet(wb, rules)
    link_rule_columns(ws, len(rules), last)
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
    if field in E.TIME:
        norm = E.normalize_time_window(text)
        if not E._DURATION.search(norm) or E._DURATION.sub("", norm).strip(" -"):
            errors.append(f"{where}: time window '{text}' must be only a duration like '24 hours' or "
                          f"'8-30 days' (guide R37); put any explanation in Notes")
            return None
        return norm
    return _lines(text) if field in LIST_FIELDS else text


def read_decisions(workbook: Path, expected: list[dict],
                   rules: list[tuple[str, str, str]]) -> tuple[list[dict], list[str]]:
    wb = load_workbook(workbook, data_only=True)
    ws = wb["Adjudicate"]
    if [c.value for c in ws[1]] != [h for h, _, _ in COLUMNS]:
        return [], ["the Adjudicate sheet's columns were changed; expected the original layout"]
    in_wb = [tuple(v or "" for v in row[:3]) for row in wb["Rules"].iter_rows(min_row=2, values_only=True) if any(row)]
    if in_wb != rules:
        return [], ["the Rules sheet differs from the current labelling guide (sheet edited, or guide "
                    "changed since the workbook was built)"]
    rule_text = {rid: text for rid, _, text in rules}
    got = [row for row in ws.iter_rows(min_row=FIRST_ROW, values_only=True) if any(row)]
    if len(got) != len(expected):
        return [], [f"expected {len(expected)} rows, found {len(got)}"]
    errors: list[str] = []
    decisions = []
    n_ro = len(read_only_values(0, expected[0])) if expected else 0
    for r, (row, exp) in enumerate(zip(got, expected, strict=True), FIRST_ROW):
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
        rule_id = str(cells["rule_id"] or "").strip().upper()
        if rule_id not in rule_text:
            errors.append(f"{where}: guide rule ID '{rule_id}' is required and must be on the Rules sheet")
        if rule_id == GAP_ID and E.is_empty(cells["notes"]):
            errors.append(f"{where}: GAP needs an explanation in Notes")
        value = {"A": exp["a"], "B": exp["b"]}.get(agreed)
        if agreed == "other":
            value = parse_other(exp["field"], cells["other_value"], where, errors)
        decisions.append({"record_id": exp["q"]["record_id"], "field": exp["field"], "agreed": agreed,
                          "value": value, "rule_id": rule_id, "rule_text": rule_text.get(rule_id),
                          "guide_gap": rule_id == GAP_ID, "notes": cells["notes"]})
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


def import_workbook(workbook: Path, queue_path: Path, a_db: Path, b_db: Path, guide: Path,
                    out_db: Path, log: Path, force: bool) -> int:
    if out_db.exists() and sqlite_manual_label_count(out_db) > 0 and not force:
        print(f"error: {out_db} already has labels; pass --force to overwrite them", file=sys.stderr)
        return 1
    queue, a, b = _sample(queue_path, a_db, b_db)
    rules = parse_rules(guide.read_text(encoding="utf-8"))
    decisions, errors = read_decisions(workbook, disagreements(queue, a, b), rules)
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
                               "rule_counts": dict(collections.Counter(d["rule_id"] for d in decisions)
                                                   .most_common()),
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
        p.add_argument("--guide", type=Path, default=Path("docs/LABELING_GUIDE.md"))
        if name == "build":
            p.add_argument("--out", type=Path, required=True)
        else:
            p.add_argument("--workbook", type=Path, required=True)
            p.add_argument("--db", type=Path, default=Path("data/labeling_adjudicated.db"))
            p.add_argument("--log", type=Path, default=Path("results/v1c_adjudication.json"))
            p.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if args.cmd == "build":
        return build(args.queue, args.a_db, args.b_db, args.guide, args.out)
    return import_workbook(args.workbook, args.queue, args.a_db, args.b_db, args.guide,
                           args.db, args.log, args.force)


if __name__ == "__main__":
    raise SystemExit(main())
