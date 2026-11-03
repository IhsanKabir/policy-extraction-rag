"""Workbooks for two human checks requested in the 2026-11-03 review.

1. Time-window direction (handoff/time_window_direction.xlsx): the gold time
   window stores only a duration, so "within 24 hours" and "at least 24 hours"
   look the same. For each scored clause with a gold time window, the
   annotator records the direction and the reference event.

2. Citation audit (handoff/citation_audit.xlsx): the gate checks that a quote
   occurs in the clause, not that it supports the value. A stratified sample
   of values the gate accepted (seeded) is shown with clause, value and quote;
   the annotator judges whether the quote supports the value.

Both are filled in by a person; nothing is pre-filled.

Usage:
    python scripts/build_audit_sheets.py
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.worksheet.datavalidation import DataValidation

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402

SEED = 20261103
DIRECTIONS = ["at_least_before", "within", "range", "unclear"]
EVENTS = ["departure", "booking_or_purchase", "ticket_issue", "other"]
SUPPORT = ["yes", "partly", "no"]
AUDIT_RUNS = {"Opus 5.5 + cite + guide": "data/v1b_llm_cited_guided_opus55.jsonl",
              "Sonnet 5.5 + cite": "data/v1b_llm_cited_sonnet55.jsonl"}
# values sampled per field across both runs (all available when fewer)
AUDIT_QUOTA = {"action": 6, "policy_topic": 4, "penalty_type": 6, "before_after_departure": 4,
               "time_window": 6, "fare_difference_required": 3, "penalty_amount": 4, "penalty_currency": 2,
               "penalty_percent": 1, "tax_refund_rule": 2, "eligibility_conditions": 6, "exceptions": 2}


def sheet(wb: Workbook, title: str, headers: list[str], rows: list[list], widths: list[int],
          choices: dict[int, list[str]], instructions: list[str]):
    ws = wb.active if wb.active.title == "Sheet" else wb.create_sheet()
    ws.title = title
    for i, line in enumerate(instructions, 1):
        ws.cell(row=i, column=1, value=line).font = Font(italic=True)
    top = len(instructions) + 2
    for c, (h, w) in enumerate(zip(headers, widths, strict=True), 1):
        ws.cell(row=top, column=c, value=h).font = Font(bold=True)
        ws.column_dimensions[chr(64 + c)].width = w
    for r, row in enumerate(rows, top + 1):
        for c, v in enumerate(row, 1):
            ws.cell(row=r, column=c, value=v).alignment = Alignment(wrap_text=True, vertical="top")
    for col, options in choices.items():
        dv = DataValidation(type="list", formula1='"' + ",".join(options) + '"', allow_blank=True)
        letter = chr(64 + col)
        dv.add(f"{letter}{top + 1}:{letter}{top + len(rows)}")
        ws.add_data_validation(dv)
    ws.freeze_panes = ws.cell(row=top + 1, column=1)


def time_window_rows(gold: list[dict]) -> list[list]:
    return [[g["record_id"], g["clause_text"], g["time_window"], "", "", ""]
            for g in gold if not E.is_empty(g.get("time_window"))]


def audit_rows(gold: list[dict]) -> list[list]:
    scored = {g["record_id"] for g in gold}
    pool = []
    for run, path in AUDIT_RUNS.items():
        for rid, row in E.load_pred(Path(path)).items():
            if rid not in scored:
                continue
            for field, info in row["grounding"]["fields"].items():
                if "items" in info:
                    pool += [(field, run, rid, row["clause_text"], it["item"], "(list item: checked against the clause)")
                             for it in info["items"] if it["grounded"]]
                elif info.get("grounded"):
                    pool.append((field, run, rid, row["clause_text"], row.get(field), info.get("quote")))
    rng = random.Random(SEED)
    rows = []
    for field, quota in AUDIT_QUOTA.items():
        cands = [p for p in pool if p[0] == field]
        for f, run, rid, clause, value, quote in rng.sample(cands, min(quota, len(cands))):
            rows.append([rid, run, clause, f, json.dumps(value, ensure_ascii=False), quote, "", ""])
    rng.shuffle(rows)
    return rows


def main() -> int:
    queue = E.load_queue(Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    gold = E.dedup(E.load_gold(Path("data/labeling_fine.db"), queue))
    out = Path("handoff")
    out.mkdir(exist_ok=True)

    wb = Workbook()
    tw = time_window_rows(gold)
    sheet(wb, "time_windows", ["record_id", "clause", "gold time window", "direction", "reference event", "notes"],
          tw, [14, 70, 16, 18, 20, 30], {4: DIRECTIONS, 5: EVENTS},
          ["Time-window direction. For each clause, choose from the drop-downs:",
           "direction: at_least_before = the rule needs at least that long before the event "
           "('up to 12 hours before', 'at least 24 hours prior'); within = the rule applies inside that period "
           "('within 8 hours of departure', 'less than 3 hours'); range = both bounds; unclear = cannot tell.",
           "reference event: what the time is measured from. Use notes for anything else. Read only the clause."])
    wb.save(out / "time_window_direction.xlsx")

    wb = Workbook()
    au = audit_rows(gold)
    sheet(wb, "citation_audit", ["record_id", "system", "clause", "field", "value", "quote", "supports?", "notes"],
          au, [14, 20, 60, 18, 28, 40, 12, 30], {7: SUPPORT},
          ["Citation audit. The gate accepted each value because its quote occurs in the clause.",
           "Judge only whether the quote (and the clause) actually supports the value for that field:",
           "yes = supports it; partly = supports part of it or needs other text; no = the quote does not "
           "mean this value. Ignore whether you would have labelled the clause differently overall."])
    wb.save(out / "citation_audit.xlsx")
    print(f"wrote {len(tw)} time-window rows and {len(au)} citation-audit rows to {out}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
