import json
import sqlite3

from openpyxl import load_workbook

from labeling_store import sqlite_upsert_manual_label
from scripts.adjudication_sheet import (COLUMNS, DECISION, FIRST_ROW, GAP_ID, adjudicate, build, disagreements,
                                        import_workbook, parse_other, parse_rules)

GUIDE = """# Guide
## Golden rules
1. Only what the clause says.
2. Empty is better than a guess.

## Step 1
Action:
| Action | Use when |
|---|---|
| `allowed` | **nothing** attached |
| `unknown` | not stated |

- Fee + percentage: use `mixed`.
  - note the base
  | Clause | Type |
  |---|---|
  | "THB 500 plus 10%" | `mixed` |

## Worked examples
**A. `Refund: Not Available`**
topic `refund` · action `not_allowed`
| Time | % |
|---|---|
| `31 days` | `0` |
"""

Q = [{"record_id": "r1", "clause_text": "c1"}, {"record_id": "r2", "clause_text": "c2"},
     {"record_id": "r3", "clause_text": "c3"}]
A = {"r1": ("labeled", {"policy_topic": "reissue_change", "action": "conditional", "notes": "a note"}),
     "r2": ("skip", {"notes": "menu"}),
     "r3": ("labeled", {"policy_topic": "refund", "action": "not_allowed"})}
B = {"r1": ("labeled", {"policy_topic": "other", "action": "conditional"}),
     "r2": ("labeled", {"policy_topic": "other", "action": "unknown"}),
     "r3": ("labeled", {"policy_topic": "refund", "action": "not_allowed"})}


def test_disagreements_lists_only_differing_fields_and_skip_decisions():
    rows = {(r["q"]["record_id"], r["field"]) for r in disagreements(Q, A, B)}
    assert rows == {("r1", "policy_topic"), ("r2", DECISION), ("r2", "policy_topic"), ("r2", "action")}


def test_adjudicate_applies_agreed_values_and_keeps_agreed_records():
    decisions = [{"record_id": "r1", "field": "policy_topic", "value": "reissue_change"},
                 {"record_id": "r2", "field": DECISION, "value": "labeled"},
                 {"record_id": "r2", "field": "policy_topic", "value": "other"},
                 {"record_id": "r2", "field": "action", "value": None}]
    out = {p["record_id"]: p for p in adjudicate(Q, A, B, decisions)}
    assert out["r1"]["policy_topic"] == "reissue_change" and out["r1"]["notes"] == "a note"
    assert out["r2"]["label_status"] == "labeled" and out["r2"]["policy_topic"] == "other"
    assert out["r2"]["action"] is None
    assert out["r3"] == {**A["r3"][1], "record_id": "r3", "label_status": "labeled"}


def test_adjudicate_skip_drops_label_fields():
    decisions = [{"record_id": "r2", "field": DECISION, "value": "skip"}]
    out = {p["record_id"]: p for p in adjudicate(Q, A, B, decisions)}
    assert out["r2"]["label_status"] == "skip" and "policy_topic" not in out["r2"]


def test_parse_other_validates_and_types_values():
    errors = []
    assert parse_other("policy_topic", "Refund", "w", errors) == "refund"
    assert parse_other("fare_difference_required", "TRUE", "w", errors) is True
    assert parse_other("penalty_currency", "thb", "w", errors) == "THB"
    assert parse_other("exceptions", "groups\nreward tickets", "w", errors) == ["groups", "reward tickets"]
    assert parse_other("action", "", "w", errors) is None
    assert parse_other("time_window", "Within 24 Hrs", "w", errors) == "24 hours"
    assert parse_other("time_window", "8 - 30 Days", "w", errors) == "8-30 days"
    assert errors == []
    parse_other("time_window", "should be 24 hours from the booking time", "w", errors)
    assert len(errors) == 1
    parse_other("policy_topic", "refunds", "w", errors)
    assert errors


def test_parse_rules_numbers_bullets_table_rows_and_examples():
    rules = parse_rules(GUIDE)
    texts = [t for _, _, t in rules]
    assert [r[0] for r in rules] == ["R1", "R2", "R3", "R4", "R5", "R6", GAP_ID]
    assert texts[0] == "Only what the clause says."
    assert texts[2] == "Action `allowed` : nothing attached"
    assert texts[4].startswith("Fee + percentage") and '"THB 500 plus 10%"' in texts[4]
    assert "| Clause | Type |" not in texts[4]
    assert rules[5][1] == "Worked examples" and "`31 days`" in texts[5]


def _db(path, labels):
    for rid, (status, payload) in labels.items():
        sqlite_upsert_manual_label(path, {**payload, "record_id": rid, "label_status": status})
    return path


def test_build_fill_import_round_trip(tmp_path):
    queue = tmp_path / "q.jsonl"
    queue.write_text("".join(json.dumps(q) + "\n" for q in Q), encoding="utf-8")
    guide = tmp_path / "guide.md"
    guide.write_text(GUIDE, encoding="utf-8")
    a_db, b_db = _db(tmp_path / "a.db", A), _db(tmp_path / "b.db", B)
    xlsx = tmp_path / "adj.xlsx"
    assert build(queue, a_db, b_db, guide, xlsx) == 0

    wb = load_workbook(xlsx)
    ws = wb["Adjudicate"]
    col = {key: i for i, (_, key, _) in enumerate(COLUMNS, 1) if key}
    choices = {("r1", "policy_topic"): ("A", "R1"), ("r2", DECISION): ("A", "R2"),
               ("r2", "policy_topic"): ("B", GAP_ID), ("r2", "action"): ("B", "R3")}
    for r in range(FIRST_ROW, ws.max_row + 1):
        agreed, rule = choices[(ws.cell(row=r, column=2).value, ws.cell(row=r, column=6).value)]
        ws.cell(row=r, column=col["agreed"], value=agreed)
        ws.cell(row=r, column=col["rule_id"], value=rule)
    assert str(ws.cell(row=FIRST_ROW, column=col["rule_id"] + 1).value).startswith("=IF(")
    wb.save(xlsx)

    out_db, log = tmp_path / "adj.db", tmp_path / "log.json"
    assert import_workbook(xlsx, queue, a_db, b_db, guide, out_db, log, force=False) == 1  # GAP without notes
    ws.cell(row=next(r for r in range(FIRST_ROW, ws.max_row + 1)
                     if ws.cell(row=r, column=col["rule_id"]).value == GAP_ID),
            column=col["notes"], value="guide silent on this")
    wb.save(xlsx)
    assert import_workbook(xlsx, queue, a_db, b_db, guide, out_db, log, force=False) == 0

    rows = dict(sqlite3.connect(out_db).execute("select record_id, label_status from manual_labels"))
    assert rows == {"r1": "labeled", "r2": "skip", "r3": "labeled"}
    report = json.loads(log.read_text(encoding="utf-8"))
    assert report["n_guide_gaps"] == 1 and report["rule_counts"]["R1"] == 1

    guide.write_text(GUIDE + "\n- a new rule\n", encoding="utf-8")
    assert import_workbook(xlsx, queue, a_db, b_db, guide, out_db, log, force=True) == 1  # guide changed
