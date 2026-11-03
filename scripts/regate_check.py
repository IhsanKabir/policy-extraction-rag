"""Re-apply the current citation gate to stored cited runs (2026-11-03).

Each cited prediction keeps the model's pre-gate values (model_raw_fields), its
quotes (grounding.fields.<name>.quote) and the clause text, so the gate can be
re-run offline. This checks that a change to the gate (here: whole-number and
whole-word matching instead of substring matching) leaves every stored
prediction unchanged, i.e. that no reported result depended on the old gate.

Usage:
    python scripts/regate_check.py [data/v1b_llm_cited*.jsonl ...]
"""
from __future__ import annotations

import copy
import glob
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from extractors.llm_pipeline import ALL_FIELDS, coerce_fields, verify_citations  # noqa: E402


def regate(row: dict) -> tuple[dict, dict]:
    """Replay the pipeline's post-processing on the stored raw reply (coerce,
    then gate); returns (gated fields, grounding report)."""
    raw = copy.deepcopy(row["model_raw_fields"])
    fields = coerce_fields(raw)
    report = verify_citations(fields, raw.get("citations"), row["clause_text"])
    return {name: fields.get(name) for name in ALL_FIELDS}, report


def differences(row: dict) -> list[str]:
    gated, report = regate(row)
    out = [f"{name}: stored {row.get(name)!r}, regated {gated[name]!r}"
           for name in ALL_FIELDS if row.get(name) != gated[name]]
    if report["grounded"] != row["grounding"]["grounded"]:
        out.append(f"grounded count: stored {row['grounding']['grounded']}, regated {report['grounded']}")
    return out


def main() -> int:
    paths = sys.argv[1:] or sorted(glob.glob("data/v1b_llm_cited*.jsonl"))
    total = 0
    for path in paths:
        rows = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
        diffs = [(r["record_id"], d) for r in rows if r.get("model_raw_fields") for d in differences(r)]
        total += len(diffs)
        print(f"  {'ok      ' if not diffs else 'CHANGED '} {path} ({len(rows)} rows)")
        for rid, d in diffs[:5]:
            print(f"      {rid}: {d}")
    print("gate change leaves every stored prediction unchanged" if not total else f"{total} differences")
    return 1 if total else 0


if __name__ == "__main__":
    raise SystemExit(main())
