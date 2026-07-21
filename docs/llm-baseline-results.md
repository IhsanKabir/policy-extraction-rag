# Multi-Method v0 Results: Heuristic vs LLM vs LLM+Citation (40-clause gold)

Measured 2026-07-21 on the v0 gold set (40 hand-labeled records, chunk
granularity), after the metric fixes verified in code review (empty-list
normalization, citation value-in-quote check, F1 zero-handling). All methods
covered 40/40 gold records; differences are behavioral, not coverage.

Models: local Ollama qwen2.5:1.5b and qwen2.5:7b, temperature 0, CPU-only.
Raw outputs: `data/compare_*_2026-07-21.json` (untracked; regenerate with
`scripts/run_llm_eval.sh`).

## Results table

| Method | Topic acc | Topic prec (asserted) | Overall acc | Micro P | Micro F1 | Value halluc. rate | Citation grounding |
|---|---|---|---|---|---|---|---|
| Heuristic (baseline) | **79.5%** | 77.5% (31/40) | **46.1%** | 62.5% | **53.0%** | 6.6% (8/121) | n/a |
| qwen2.5:1.5b, ungrounded | 17.9% | 63.6% (7/11) | 11.2% | 34.7% | 16.9% | **100% (2/2)** | n/a |
| qwen2.5:1.5b, cited | 15.4% | 66.7% (6/9) | 3.9% | 60.0% | 7.4% | 0 asserted | 15.2% (raw halluc 84.8%) |
| qwen2.5:7b, ungrounded | 25.6% | **83.3% (10/12)** | 17.8% | 33.8% | 23.3% | 0% (0/9) | n/a |
| qwen2.5:7b, cited | 12.8% | 83.3% (5/6) | 9.2% | 25.9% | 13.6% | 0% (0/3) | **74.5%** (raw halluc 25.5%) |

## Findings

1. **The heuristic dominates v0 accuracy, but the reason is structural, not
   intelligence.** The LLMs abstain (null topic) on ~70% of records. Spot-check
   of raw model output confirmed these are genuine model abstentions, not
   enum-coercion artifacts: v0 clause units are often context-free fragments
   (one sampled unit begins mid-sentence with "and can be used to buy...";
   another is the bare condition "Only applicable to unused tickets."). The
   gold labeler saw the FULL chunk when labeling; the keyword heuristic needs
   no discourse context; the LLM, prompted to never guess, abstains.

2. **On asserted topics, qwen7b matches heuristic precision** (83.3% vs 77.5%)
   at 3.3x lower coverage. The failure mode is recall, not precision.

3. **The citation constraint works as designed.** For the small model it
   blocked 84.8% of asserted fields as ungrounded (the model hallucinates
   freely; the gate catches it). For 7b, 74.5% of asserted fields carried a
   verifying quote. Post-filter value hallucination is 0% in both cited runs
   by construction.

4. **Known biases and limitations (state these in any write-up):**
   - Gold labels were anchored on heuristic suggestions (labeling convention
     "keep the heuristic topic when right") and labeled with full-chunk
     context: both structurally favor the heuristic on exactly this gold set.
   - Tiny CPU models (1.5B/7B); results do not predict frontier-model behavior.
   - Number grounding is comma-naive ("1,500" in a quote does not match
     1500.0) - conservative direction only (deflates grounding, never
     inflates). Follow-up before v1 runs.
   - LLM saw clause_text only, without the parent chunk. A chunk-context
     variant is an obvious ablation.

## Motivated next steps

- **v1 fine-granularity gold round** (in progress, 200 atomic records): each
  unit is a self-contained rule, removing the fragment/context asymmetry that
  drives finding 1.
- Chunk-context ablation for the LLM methods.
- Store raw model replies alongside coerced fields for coercion-loss auditing.
- Comma-normalized number grounding.
