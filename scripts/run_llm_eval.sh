#!/usr/bin/env bash
# Sequential LLM extraction runs over the v0 gold set + compares.
# Sequential on purpose: CPU-only Ollama, one model in memory at a time.
set -u
cd "$(dirname "$0")/.."

run () {
  local model="$1" mode="$2" tag="$3"
  local out="data/policy_clauses_${tag}.jsonl"
  echo "=== RUN ${tag} (${model}, ${mode}) ==="
  python -m extractors.llm_pipeline --model "$model" --mode "$mode" \
    --only-gold --output "$out" || { echo "RUN ${tag} FAILED"; return 1; }
  python -m evals.real_pilot_labeling compare --pred "$out" \
    > "data/compare_${tag}_2026-07-21.json" || { echo "COMPARE ${tag} FAILED"; return 1; }
  python - "$tag" <<'PY'
import json, sys
tag = sys.argv[1]
d = json.load(open(f"data/compare_{tag}_2026-07-21.json"))
vg = d.get("value_grounding") or {}
cg = d.get("citation_grounding") or {}
print(f"RESULT {tag}: topic={d['per_field_accuracy'].get('policy_topic')} "
      f"overall={d.get('overall_field_accuracy')} microF1={d.get('micro_f1')} "
      f"halluc={vg.get('hallucination_rate')} cite_ground={cg.get('citation_grounding_rate')}")
PY
}

run "qwen2.5:1.5b" "llm"       "llm_qwen15b"
run "qwen2.5:1.5b" "llm_cited" "llmcited_qwen15b"
run "qwen2.5:7b"   "llm"       "llm_qwen7b"
run "qwen2.5:7b"   "llm_cited" "llmcited_qwen7b"

# refresh heuristic compare so it also carries the new faithfulness metrics
python -m evals.real_pilot_labeling compare > data/compare_v0_baseline_2026-07-21.json
echo "ALL RUNS DONE"
