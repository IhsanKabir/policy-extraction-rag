"""Reproduce every reported result from release/ alone, and check it (2026-11-02).

Rebuilds the working files (label databases, queue, samples, predictions) from
release/ into a scratch workspace (default build/repro), runs the scorer and
all analysis scripts there, and compares every number with the committed
results/*.json. Nothing outside the workspace is read or written, except that
the analysis scripts are imported from this repository.

Usage:
    python scripts/reproduce.py              # rebuild, rerun, compare
    python scripts/reproduce.py --keep       # keep the workspace for inspection
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from labeling_store import ensure_manual_label_db  # noqa: E402
from scripts.export_release import AUDITS, EVAL_RUNS, LABEL_SETS, SAMPLES, SNAPSHOTS  # noqa: E402

PATH_KEYS = {"pred", "snapshots", "final"}  # file paths recorded in results; differ by design
TOLERANCE = 1e-9


def read_jsonl(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)


def build_db(rows: list[dict], db: Path) -> None:
    """Same table and payload format as the labelling tool, written in one transaction."""
    ensure_manual_label_db(db)
    con = sqlite3.connect(db)
    try:
        with con:
            con.executemany("INSERT INTO manual_labels (record_id, payload_json, label_status) VALUES (?, ?, ?)",
                            [(r["record_id"], json.dumps(r), str(r.get("label_status") or "").lower() or None)
                             for r in rows])
    finally:
        con.close()


def restore(release: Path, ws: Path) -> dict[str, Path]:
    """Recreate the working files under ws; returns snapshot DB paths, oldest first."""
    write_jsonl(ws / "data/real_pilot_manual_labeling_queue_v1c.jsonl", read_jsonl(release / "clauses.jsonl"))
    for name, db in LABEL_SETS.items():
        build_db(read_jsonl(release / "labels" / f"{name}.jsonl"), ws / db)
    snapshots = {}
    for name in SNAPSHOTS:
        snapshots[name] = ws / "data/provenance" / f"{name}.db"
        build_db(read_jsonl(release / "labels/provenance" / f"{name}.jsonl"), snapshots[name])
    for name, path in SAMPLES.items():
        write_jsonl(ws / path, read_jsonl(release / "samples" / f"{name}.jsonl"))
    write_jsonl(ws / "data/queue_relabel_a_unseen.jsonl", read_jsonl(release / "samples/relabel_a_unseen.jsonl"))
    for pred in (release / "predictions").glob("*.jsonl"):
        shutil.copyfile(pred, ws / "data" / pred.name)
    shutil.copyfile(release / "retrieval/runs.jsonl", ws / "data/retrieval_runs.jsonl")
    for name, path in AUDITS.items():
        shutil.copyfile(release / "audits" / f"{name}.jsonl", ws / path)
    return snapshots


def commands(snapshots: dict[str, Path]) -> list[list[str]]:
    py = [sys.executable, "-W", "ignore"]
    s = lambda name: str(REPO / "scripts" / name)  # noqa: E731
    cmds = [py + [str(REPO / "evals/v1b_eval.py"), "--queue", "data/real_pilot_manual_labeling_queue_v1c.jsonl",
                  "--pred", pred, "--method", method, "--out", f"results/{name}.json"]
            for name, (method, pred) in EVAL_RUNS.items()]
    snap_args = [a for p in snapshots.values() for a in ("--snapshot", str(p))]
    cmds.append(py + [s("audit_label_provenance.py"), *snap_args, "--final", "data/labeling_fine.db"])
    iaa = py + [s("iaa_agreement.py"), "--a-db", "data/labeling_fine.db"]
    cmds += [iaa + ["--b-db", "data/labeling_iaa_b.db"],
             iaa + ["--b-db", "data/labeling_adjudicated.db", "--out", "results/v1c_iaa_adjudicated.json"],
             iaa + ["--b-db", "data/labeling_relabel_a.db", "--iaa-queue", "data/queue_relabel_a.jsonl",
                    "--out", "results/v1c_self_relabel.json"],
             iaa + ["--b-db", "data/labeling_relabel_a.db", "--iaa-queue", "data/queue_relabel_a_unseen.jsonl",
                    "--out", "results/v1c_self_relabel_unseen.json"],
             py + [s("error_analysis.py"), "--examples", "handoff/error_examples.md"],
             py + [s("variance_analysis.py")],
             py + [s("retrieval_eval.py")],
             py + [s("rule_detection.py")],
             py + [s("robustness_analysis.py")],
             py + [s("score_audits.py")]]
    return cmds


def diff(a, b, path: str = "") -> list[str]:
    """Paths where two JSON values differ (numbers within TOLERANCE count as equal)."""
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(a.keys() | b.keys()):
            if k in PATH_KEYS:
                continue
            if k not in a or k not in b:
                out.append(f"{path}/{k}: only in {'reproduced' if k in a else 'committed'}")
            else:
                out += diff(a[k], b[k], f"{path}/{k}")
        return out
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f"{path}: length {len(a)} vs {len(b)}"]
        return [d for i, (x, y) in enumerate(zip(a, b, strict=True)) for d in diff(x, y, f"{path}[{i}]")]
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        return [] if math.isclose(a, b, rel_tol=0, abs_tol=TOLERANCE) else [f"{path}: {a} vs {b}"]
    return [] if a == b else [f"{path}: {a!r} vs {b!r}"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--release", type=Path, default=REPO / "release")
    ap.add_argument("--workspace", type=Path, default=REPO / "build/repro")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    ws = args.workspace.resolve()
    if ws.exists():
        shutil.rmtree(ws)
    (ws / "results").mkdir(parents=True)
    snapshots = restore(args.release.resolve(), ws)
    for cmd in commands(snapshots):
        done = subprocess.run(cmd, cwd=ws, capture_output=True, text=True, encoding="utf-8")
        if done.returncode != 0:
            print(f"FAILED: {' '.join(cmd)}\n{done.stderr[-2000:]}", file=sys.stderr)
            return 1

    failures = 0
    for produced in sorted((ws / "results").glob("*.json")):
        committed = REPO / "results" / produced.name
        if not committed.exists():
            print(f"  new      {produced.name} (no committed copy)")
            continue
        problems = diff(json.loads(produced.read_text(encoding="utf-8")),
                        json.loads(committed.read_text(encoding="utf-8")))
        failures += bool(problems)
        print(f"  {'MISMATCH' if problems else 'ok      '} {produced.name}")
        for p in problems[:5]:
            print(f"           {p}")
    if not args.keep:
        shutil.rmtree(ws)
    print("all results reproduced" if not failures else f"{failures} result file(s) differ")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
