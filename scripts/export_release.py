"""Export the v1c benchmark to release/ so the results can be reproduced (2026-11-02).

Writes plain JSONL (no SQLite) that scripts/reproduce.py turns back into the
working files the scoring scripts expect:

  release/clauses.jsonl                 the 250 queue clauses (+ parent chunk text)
  release/sources.csv                   source manifest (URLs, snapshot dates)
  release/labels/<set>.jsonl            label sets: annotator A (gold), annotator B
                                        (IAA sample), adjudicated, A's blind relabel
  release/labels/provenance/<n>.jsonl   annotator A's label snapshots before each fix pass
  release/samples/<name>.jsonl          IAA and relabel samples (queue rows)
  release/predictions/<file>.jsonl      every system's output on the queue clauses
  release/retrieval/runs.jsonl          retriever rankings (unit keys only, no text)
  release/README.md                     data card (from docs/RELEASE_README.md)
  release/LICENSE-DATA.md               data licence (from docs/RELEASE_LICENSE.md)
  release/SHA256SUMS                    checksums of all of the above

Each label row is the stored payload plus "label_status", exactly as the
labelling tool saved it, so rebuilding a database from it is lossless.

Usage:
    python scripts/export_release.py [--out release]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from evals import v1b_eval as E  # noqa: E402
from scripts.audit_label_provenance import METHODS  # noqa: E402
from scripts.variance_analysis import MODELS, run_paths  # noqa: E402

QUEUE = Path("data/real_pilot_manual_labeling_queue_v1c.jsonl")
QUEUE_KEYS = ("record_id", "source_id", "document_id", "document_type", "carrier",
              "supplier", "chunk_id", "chunk_text", "clause_text")
LABEL_SETS = {  # release name -> working database
    "annotator_a": "data/labeling_fine.db",
    "annotator_b_iaa": "data/labeling_iaa_b.db",
    "adjudicated": "data/labeling_adjudicated.db",
    "annotator_a_relabel": "data/labeling_relabel_a.db",
}
# Snapshots of A's labels before each fix pass, oldest first (as used by the provenance audit).
SNAPSHOTS = {
    "1_backup_2026-10-27": "../labeling_fine_backup_2026-10-27.db",
    "2_before_fixes_2026-10-27": "../labeling_fine_BEFORE_FIXES_2026-10-27.db",
    "3_before_audit_2026-10-28": "data/labeling_fine_BEFORE_AUDIT_2026-10-28.db",
    "4_before_batch2_fix_2026-10-28": "data/backups/BEFORE_BATCH2_FIX_2026-10-28_labeling_fine.db",
    "5_before_final_fix_2026-10-29": "data/backups/BEFORE_FINAL_FIX_2026-10-29_labeling_fine.db",
    "6_before_v1c_review_2026-10-29": "data/backups/BEFORE_V1C_REVIEW_2026-10-29_labeling_fine.db",
}
SAMPLES = {"iaa_b": "data/queue_iaa_b.jsonl", "relabel_a": "data/queue_relabel_a.jsonl"}
# Main-table runs: results/<name>.json -> (method label, prediction file)
EVAL_RUNS = {
    "v1c_heuristic": ("heuristic", "data/policy_clauses_fine.jsonl"),
    **{f"v1c_llm_{run}": (f"llm_{run}", f"data/v1b_llm_{run}.jsonl") for run in (
        "qwen7b", "guided_qwen7b", "cited_qwen7b", "cited_ctx_qwen7b", "cited_guided_qwen7b",
        "sonnet55", "cited_sonnet55", "cited_ctx_sonnet55",
        "opus55", "cited_opus55", "guided_opus55", "cited_ctx_opus55", "cited_guided_opus55")},
}
# Repeat runs of the Claude conditions, for the run-to-run variance analysis.
VARIANCE_RUNS = {str(p.as_posix()) for m in MODELS for c in (False, True) for p in run_paths(m, c)}
RETRIEVAL_RUNS = Path("data/retrieval_runs.jsonl")
# Claude runs on the 84 clauses A marked as stating no rule, for scripts/rule_detection.py.
NORULE_RUNS = {f"data/v1b_llm_{run}_norule.jsonl" for run in (
    "sonnet55", "cited_sonnet55", "cited_ctx_sonnet55", "opus55", "cited_opus55", "guided_opus55",
    "cited_ctx_opus55", "cited_guided_opus55")}
PREDICTION_FILES = sorted({p for _, p in EVAL_RUNS.values()} | set(METHODS.values()) | VARIANCE_RUNS | NORULE_RUNS)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def label_rows(db: Path) -> list[dict]:
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as con:
        rows = con.execute("SELECT record_id, payload_json, label_status FROM manual_labels ORDER BY record_id")
        return [{**json.loads(p), "record_id": rid, "label_status": status} for rid, p, status in rows]


def queue_rows(path: Path) -> list[dict]:
    return [{k: q.get(k) for k in QUEUE_KEYS} for q in E.load_queue(path)]


def export(out: Path) -> int:
    missing = [p for p in [QUEUE, *LABEL_SETS.values(), *SNAPSHOTS.values(), *SAMPLES.values(), *PREDICTION_FILES, RETRIEVAL_RUNS]
               if not Path(p).exists()]
    if missing:
        print(f"error: missing inputs: {missing}", file=sys.stderr)
        return 1
    if out.exists():
        shutil.rmtree(out)
    clauses = queue_rows(QUEUE)
    ids = {c["record_id"] for c in clauses}
    write_jsonl(out / "clauses.jsonl", clauses)
    shutil.copyfile("data/pilot_source_manifest.csv", out / "sources.csv")
    for src, dest in (("docs/RELEASE_README.md", "README.md"), ("docs/RELEASE_LICENSE.md", "LICENSE-DATA.md")):
        # LF regardless of checkout line endings, so the checksums are stable
        text = Path(src).read_text(encoding="utf-8").replace("\r\n", "\n")
        (out / dest).write_text(text, encoding="utf-8", newline="\n")
    for name, db in LABEL_SETS.items():
        write_jsonl(out / "labels" / f"{name}.jsonl", label_rows(Path(db)))
    for name, db in SNAPSHOTS.items():
        write_jsonl(out / "labels" / "provenance" / f"{name}.jsonl", label_rows(Path(db)))
    iaa = {q["record_id"] for q in E.load_queue(Path(SAMPLES["iaa_b"]))}
    for name, path in SAMPLES.items():
        write_jsonl(out / "samples" / f"{name}.jsonl", queue_rows(Path(path)))
    write_jsonl(out / "samples" / "relabel_a_unseen.jsonl",
                [q for q in queue_rows(Path(SAMPLES["relabel_a"])) if q["record_id"] not in iaa])
    for path in PREDICTION_FILES:
        rows = [r for r in E.load_pred(Path(path)).values() if r["record_id"] in ids]
        write_jsonl(out / "predictions" / Path(path).name, rows)
    write_jsonl(out / "retrieval" / "runs.jsonl", [json.loads(line) for line in
                RETRIEVAL_RUNS.read_text(encoding="utf-8").splitlines() if line.strip()])
    sums = sorted(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(out).as_posix()}"
                  for p in out.rglob("*") if p.is_file())
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8", newline="\n")
    print(f"exported {len(clauses)} clauses, {len(LABEL_SETS)} label sets, {len(SNAPSHOTS)} snapshots, "
          f"{len(PREDICTION_FILES)} prediction files to {out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path, default=Path("release"))
    return export(ap.parse_args().out)


if __name__ == "__main__":
    raise SystemExit(main())
