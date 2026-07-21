#!/usr/bin/env bash
# v1 (fine-granularity) evaluation matrix, run AFTER the 200-record gold round.
# Gold: data/labeling_fine.db. Units: data/policy_clauses_fine.jsonl.
# Scope: heuristic-fine compare (instant) + qwen2.5:7b in three variants:
# ungrounded, cited, and cited+chunk-context (the key ablation). The 1.5b
# model is omitted - already characterized on v0 (hallucinates freely).
set -u
cd "$(dirname "$0")/.."
export LABELING_DB_PATH=data/labeling_fine.db

run () {
  local mode="$1" tag="$2"; shift 2
  local out="data/policy_clauses_fine_${tag}.jsonl"
  echo "=== RUN v1 ${tag} (qwen2.5:7b, ${mode}) ==="
  python -m extractors.llm_pipeline --model "qwen2.5:7b" --mode "$mode" \
    --clauses data/policy_clauses_fine.jsonl \
    --only-gold --gold-db data/labeling_fine.db \
    --output "$out" "$@" || { echo "RUN v1 ${tag} FAILED"; return 1; }
  python -m evals.real_pilot_labeling compare \
    --pred "$out" --manual-gold-db data/labeling_fine.db \
    > "data/compare_v1_${tag}.json" || { echo "COMPARE v1 ${tag} FAILED"; return 1; }
  python - "$tag" <<'PY'
import json, sys
tag = sys.argv[1]
d = json.load(open(f"data/compare_v1_{tag}.json"))
vg = d.get("value_grounding") or {}
cg = d.get("citation_grounding") or {}
print(f"RESULT v1 {tag}: topic={d['per_field_accuracy'].get('policy_topic')} "
      f"overall={d.get('overall_field_accuracy')} microF1={d.get('micro_f1')} "
      f"halluc={vg.get('hallucination_rate')} cite_ground={cg.get('citation_grounding_rate')}")
PY
}

# heuristic on fine units (instant, the v1 baseline)
python -m evals.real_pilot_labeling compare \
  --pred data/policy_clauses_fine.jsonl --manual-gold-db data/labeling_fine.db \
  > data/compare_v1_heuristic.json
python - <<'PY'
import json
d = json.load(open("data/compare_v1_heuristic.json"))
print(f"RESULT v1 heuristic: topic={d['per_field_accuracy'].get('policy_topic')} "
      f"overall={d.get('overall_field_accuracy')} microF1={d.get('micro_f1')}")
PY

run "llm"       "llm_qwen7b"
run "llm_cited" "llmcited_qwen7b"
run "llm_cited" "llmcited_ctx_qwen7b" --with-chunk-context

echo "V1 RUNS DONE"
