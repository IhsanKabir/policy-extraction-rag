"""Score the two human audits requested in the 2026-11-03 review.

1. Time-window direction. The scorer credits a time window on its duration
   alone. Annotator A recorded, for each scored clause with a gold time window,
   the direction of the deadline (at least / within / range) and what it is
   measured from (handoff/time_window_direction.xlsx). For each system, every
   duration match is checked for whether the predicted text still states the
   direction. The direction of a predicted string is read with a fixed keyword
   rule (DIRECTION_RULES below); a string with none of the keywords, such as
   "8 hours before departure", counts as not stating a direction. The same rule
   is applied to the clause, so a prediction that drops a qualifier the clause
   has is counted separately.

2. Citation audit. A judged whether each sampled quote that passed the gate
   supports its value (handoff/citation_audit.xlsx, built by
   scripts/build_audit_sheets.py). Counts are reported as judged.

The filled workbooks are imported once into JSONL (needs openpyxl); scoring
reads only the JSONL, so scripts/reproduce.py can rerun it from release/.

Usage:
    python scripts/score_audits.py --import-time-windows TW.xlsx --import-citations CA.xlsx
    python scripts/score_audits.py
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from scripts.export_release import EVAL_RUNS  # noqa: E402

TW_JSONL = Path("data/v1c_time_window_direction.jsonl")
CA_JSONL = Path("data/v1c_citation_audit.jsonl")
OUT = Path("results/v1c_audits.json")
# checked in order; the first match decides
DIRECTION_RULES = [
    ("range", re.compile(r"\bminimum\b.*\bmaximum\b|\bmaximum\b.*\bminimum\b|\bbetween\b")),
    ("within", re.compile(r"\bwithin\b|\bless than\b|\binside\b")),
    ("at_least_before", re.compile(r"\bat least\b|\bup ?to\b|\bminimum\b|\bno later than\b|\bnot later than\b")),
]
FIELD_GROUPS = {"categorical": E.CATEGORICAL, "text": E.TEXT}


def direction(text) -> str:
    if not isinstance(text, str) or not text.strip():
        return "none"
    low = text.lower()
    return next((name for name, rx in DIRECTION_RULES if rx.search(low)), "not_stated")


def sheet_rows(xlsx: Path, headers: list[str]) -> list[dict]:
    from openpyxl import load_workbook
    ws = load_workbook(xlsx, read_only=True).active
    rows = list(ws.iter_rows(values_only=True))
    top = next(i for i, r in enumerate(rows) if r and r[0] == "record_id")
    if list(rows[top][:len(headers)]) != headers:
        raise ValueError(f"{xlsx}: unexpected header {rows[top]}")
    return [dict(zip(headers, r[:len(headers)], strict=True)) for r in rows[top + 1:] if r and r[0]]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def import_sheets(tw: Path | None, ca: Path | None) -> None:
    if tw:
        rows = sheet_rows(tw, ["record_id", "clause", "gold time window", "direction", "reference event", "notes"])
        write_jsonl(TW_JSONL, [{"record_id": r["record_id"], "direction": r["direction"],
                                "reference_event": r["reference event"], "notes": r["notes"]} for r in rows])
        print(f"imported {len(rows)} time-window rows to {TW_JSONL}")
    if ca:
        rows = sheet_rows(ca, ["record_id", "system", "clause", "field", "value", "quote", "supports?", "notes"])
        write_jsonl(CA_JSONL, [{"record_id": r["record_id"], "system": r["system"], "field": r["field"],
                                "value": json.loads(r["value"]), "quote": r["quote"],
                                "supports": r["supports?"], "notes": r["notes"]} for r in rows])
        print(f"imported {len(rows)} citation-audit rows to {CA_JSONL}")


def read_jsonl(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def wilson(k: int, n: int, z: float = 1.96) -> list[float] | None:
    if not n:
        return None
    p, d = k / n, 1 + z * z / n
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    centre = (p + z * z / (2 * n)) / d
    return [centre - half, centre + half]


def time_window_report(gold: dict[str, dict], labels: list[dict]) -> dict:
    rows = [(lab, gold[lab["record_id"]]) for lab in labels]
    clause_dir = {lab["record_id"]: direction(g["clause_text"]) for lab, g in rows}
    systems = {}
    for name, (_, path) in EVAL_RUNS.items():
        preds = E.load_pred(Path(path))
        counts = Counter()
        for lab, g in rows:
            pred = preds.get(lab["record_id"], {}).get("time_window")
            if not E.field_match("time_window", g["time_window"], pred)[0]:
                continue
            counts["duration_match"] += 1
            d = direction(pred)
            if d == "not_stated":
                counts["not_stated"] += 1
                counts["dropped_clause_qualifier"] += clause_dir[lab["record_id"]] != "not_stated"
            else:
                counts["agrees" if d == lab["direction"] else "disagrees"] += 1
        systems[name] = dict(counts)
    return {"n": len(rows), "annotator_direction": dict(Counter(lab["direction"] for lab in labels)),
            "annotator_reference_event": dict(Counter(lab["reference_event"] for lab in labels)),
            "clause_states_direction": sum(d != "not_stated" for d in clause_dir.values()),
            "systems": systems}


def citation_report(rows: list[dict]) -> dict:
    def summary(subset: list[dict]) -> dict:
        c = Counter(r["supports"] for r in subset)
        return {"n": len(subset), **{k: c[k] for k in ("yes", "partly", "no")},
                "yes_rate": c["yes"] / len(subset) if subset else None,
                "yes_rate_ci": wilson(c["yes"], len(subset)),
                "with_note": sum(bool(r["notes"]) for r in subset)}
    group = lambda f: next((g for g, fs in FIELD_GROUPS.items() if f in fs), "value")  # noqa: E731
    return {"all": summary(rows),
            "by_system": {s: summary([r for r in rows if r["system"] == s]) for s in sorted({r["system"] for r in rows})},
            "by_field_group": {g: summary([r for r in rows if group(r["field"]) == g]) for g in ("categorical", "value", "text")},
            "not_yes": [{k: r[k] for k in ("record_id", "system", "field", "value", "supports", "notes")}
                        for r in rows if r["supports"] != "yes"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--import-time-windows", type=Path)
    ap.add_argument("--import-citations", type=Path)
    args = ap.parse_args()
    if args.import_time_windows or args.import_citations:
        import_sheets(args.import_time_windows, args.import_citations)

    queue = E.load_queue(Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    gold = {g["record_id"]: g for g in E.dedup(E.load_gold(Path("data/labeling_fine.db"), queue))}
    report = {"direction_rules": [[n, rx.pattern] for n, rx in DIRECTION_RULES],
              "time_window": time_window_report(gold, read_jsonl(TW_JSONL)),
              "citation_audit": citation_report(read_jsonl(CA_JSONL))}
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    tw = report["time_window"]
    print(f"time windows: {tw['n']} gold, annotator {tw['annotator_direction']}, "
          f"clause states direction in {tw['clause_states_direction']}")
    print(f"  {'system':28s} match agree disagree not_stated (dropped)")
    for name, c in tw["systems"].items():
        print(f"  {name:28s} {c.get('duration_match', 0):5d} {c.get('agrees', 0):5d} {c.get('disagrees', 0):8d} "
              f"{c.get('not_stated', 0):10d} ({c.get('dropped_clause_qualifier', 0)})")
    ca = report["citation_audit"]
    for key, s in [("all", ca["all"]), *ca["by_system"].items(), *ca["by_field_group"].items()]:
        print(f"citations {key:26s} n={s['n']:2d} yes={s['yes']:2d} partly={s['partly']} no={s['no']} notes={s['with_note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
