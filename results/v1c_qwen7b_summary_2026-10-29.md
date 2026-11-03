# v1c results: qwen2.5:7b baseline (2026-10-29)

**Setup**
- **Model:** `qwen2.5:7b` via Ollama on Colab, all five conditions, run on the 181 v1c targets.
- **Scored on:** the same 166-record v1c gold; after near-duplicate removal 132 (88 fare-rule, 44 policy-text).
- **Coverage:** 181/181 in four conditions. `llm_guided` has 180: record `c8dfcbf9-0e5e-4898-992e-16bbcc8b3986:1` (a 332-char flattened Nok Air refund table) was dropped by the runner, most likely as unparseable JSON. It is scored as a miss (`n_missing_predictions` = 1), not re-run.
- **Paired tests:** same as the Claude runs (2,000 bootstrap resamples, seed from `evals/v1b_eval.py`, all_dedup). * = 95% CI excludes 0.

| method (all_dedup, n=132) | topic | action | P | R | micro F1 [95% CI] |
|---|---|---|---|---|---|
| Rule-based | 73.5 | 51.5 | 54.3 | 43.9 | 48.6 [44.0-52.8] |
| Qwen 7B | 37.9 | 28.8 | 52.1 | 39.9 | 45.2 [39.1-50.7] |
| Qwen 7B + cite | 9.8 | 6.8 | 49.0 | 17.3 | 25.6 [20.4-30.7] |
| Qwen 7B + cite + ctx | 11.4 | 9.8 | 48.5 | 18.0 | 26.3 [20.6-31.5] |
| Qwen 7B + guide | 43.9 | 40.2 | 62.6 | 45.3 | 52.5 [45.7-58.0] |
| Qwen 7B + cite + guide | 23.5 | 18.2 | 64.3 | 24.8 | 35.8 [28.6-42.2] |
| Sonnet 5.5 | 84.8 | 51.5 | 57.5 | 70.9 | 63.5 [59.1-67.8] |
| Opus 5.5 | 87.1 | 64.4 | 57.9 | 75.7 | 65.6 [61.4-69.7] |
| Opus 5.5 + cite + guide | 87.1 | 79.5 | 72.7 | 78.2 | 75.4 [71.1-79.1] |

**By genre (micro F1):**
- **Fare-rule (n=88):** Qwen 50.6, Qwen + guide 57.7 (rule-based 44.5, Opus + cite + guide 77.2).
- **Policy-text (n=44):** Qwen 30.0, Qwen + guide 38.6 (rule-based 57.7). The small model is worst on the genre shift.

**Paired bootstrap (all_dedup):**
- **Qwen vs rules:** F1 −3.4 [−10.7, +3.9], n.s. With guidelines: +4.0 [−3.9, +11.4], n.s.; precision +8.3*.
- **Citation constraint (Qwen):** F1 **−19.5 [−25.0, −14.1]***; recall **−22.5***; precision −3.0 (n.s.). The gate nulled 205 of 449 asserted fields (46%): Qwen rarely quotes verbatim.
- **Chunk context (Qwen, cited):** +0.6, n.s.
- **Guidelines:** Qwen +7.4 [+3.7, +11.1]*; Qwen cited +10.1 [+3.5, +16.0]*. Precision and recall both rise.
- **Model scale:** Sonnet − Qwen +18.4*; Opus − Qwen +20.5* (recall-driven: +31.1 / +35.8).

**Why topic accuracy is so low:** Qwen never outputs `other` (the gold topic for ~a third of records); it leaves topic null instead, and 41/181 plain-mode replies are entirely empty (mostly baggage-allowance lines). The citation gate then removes most remaining topics. This is model behaviour, not a parsing failure: raw and final fields agree in plain mode.

**Narrative this supports:** the recall cost of the citation constraint shrinks with model capability: Qwen 7B −22.5, Sonnet −7.7, Opus −3.4. Grounding is a precision tool that only pays off once the model can quote reliably. Annotation guidelines help at every scale tested.
