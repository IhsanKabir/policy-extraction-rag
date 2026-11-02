"""Run-to-run variance of the Claude conditions (2026-11-02).

Each of Opus 5.5 and Sonnet 5.5, plain and cited, was run three times on the
same 181 target clauses: the original run (data/v1b_llm_*.jsonl) and two
repeats (*_rep2, *_rep3), identical settings. On the headline view (gold,
near-duplicates removed) this reports, per condition, micro F1 for each run with
mean, standard deviation and range; and, per model, the citation-prompt effect
(cited with the gate off minus plain) and the gate effect (on minus off) for
every pairing of runs, so the effects can be compared with the run-to-run spread.

Usage:
    python scripts/variance_analysis.py
"""
from __future__ import annotations

import argparse
import itertools
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from evals import v1b_eval as E  # noqa: E402
from scripts.error_analysis import ungated  # noqa: E402

RUNS = ("", "_rep2", "_rep3")
MODELS = ("opus55", "sonnet55")


def run_paths(model: str, cited: bool) -> list[Path]:
    tag = "cited_" if cited else ""
    return [Path(f"data/v1b_llm_{tag}{model}{rep}.jsonl") for rep in RUNS]


def f1(gold: list[dict], preds: dict[str, dict]) -> float:
    return E.score(gold, preds, bootstrap=0)["micro_f1"]


def spread(values: list[float]) -> dict:
    return {"runs": values, "mean": statistics.mean(values), "sd": statistics.stdev(values),
            "min": min(values), "max": max(values)}


def model_report(gold: list[dict], model: str) -> dict:
    plain = [E.load_pred(p) for p in run_paths(model, cited=False)]
    cited = [E.load_pred(p) for p in run_paths(model, cited=True)]
    raw = [ungated(c) for c in cited]
    plain_f1 = [f1(gold, p) for p in plain]
    cited_f1 = [f1(gold, c) for c in cited]
    raw_f1 = [f1(gold, r) for r in raw]
    prompt = [r - p for r, p in itertools.product(raw_f1, plain_f1)]  # every cited run vs every plain run
    gate = [c - r for c, r in zip(cited_f1, raw_f1, strict=True)]  # same run, gate on vs off
    return {"plain": spread(plain_f1), "cited": spread(cited_f1), "cited_gate_off": spread(raw_f1),
            "prompt_effect_all_pairings": spread(prompt), "gate_effect_per_run": spread(gate)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--labels-db", type=Path, default=Path("data/labeling_fine.db"))
    ap.add_argument("--queue", type=Path, default=Path("data/real_pilot_manual_labeling_queue_v1c.jsonl"))
    ap.add_argument("--out", type=Path, default=Path("results/v1c_variance.json"))
    ap.add_argument("--models", nargs="+", default=list(MODELS), choices=MODELS)
    args = ap.parse_args()
    missing = [str(p) for m in args.models for c in (False, True) for p in run_paths(m, c) if not p.exists()]
    if missing:
        print(f"error: missing runs: {missing}", file=sys.stderr)
        return 1
    gold = E.dedup(E.load_gold(args.labels_db, E.load_queue(args.queue)))
    report = {"n_records": len(gold), "models": {m: model_report(gold, m) for m in args.models}}
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    pct = lambda d: f"{100 * d['mean']:5.1f} sd {100 * d['sd']:.1f} [{100 * d['min']:.1f}, {100 * d['max']:.1f}]"  # noqa: E731
    print(f"micro F1 x100 over 3 runs, {len(gold)} records: mean sd [min, max]")
    for m, r in report["models"].items():
        for key in ("plain", "cited", "cited_gate_off", "prompt_effect_all_pairings", "gate_effect_per_run"):
            print(f"  {m:9s} {key:28s} {pct(r[key])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
