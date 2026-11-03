# Label-provenance audit (2026-10-29): do the results depend on AI-assisted label fixes?

**Why:** the gold labels went through consistency passes drafted with Claude's help and approved by the annotator. Claude models are also evaluated, so a reviewer can ask whether the gold leans toward Claude's reading.

**Method** (`scripts/audit_label_provenance.py`, full output `results/v1c_label_provenance.json`):
- Snapshots exist before every fix pass (Oct 27 ×2, Oct 28 audit, batch 2, final fix, v1c review).
- A record's *first-entered* label is its value in the earliest snapshot holding it. It is **touched** when its final label fields differ from that.
- **68/250 records touched.** Most changed fields: eligibility 28, penalty_type 20, label_status (labelled↔skip) 16, action 14, topic 10, time_window 10.
- **Scored subsets:** all_dedup 132 = **untouched 100** + touched 32.
- **Limitation:** snapshots only catch post-hoc edits, not advice given while a label was first entered.

**Micro F1 by subset**

| method | all (n=132) | untouched (n=100) | touched (n=32) |
|---|---|---|---|
| Rule-based | 48.6 | 50.7 | 43.0 |
| Qwen 7B | 45.2 | 42.5 | 51.0 |
| Sonnet 5.5 | 63.5 | 62.0 | 66.9 |
| Sonnet 5.5 + cite + ctx | 70.5 | 69.6 | 72.5 |
| Opus 5.5 | 65.6 | 62.4 | 73.4 |
| Opus 5.5 + cite | 69.7 | 66.5 | 77.4 |
| Opus 5.5 + guide | 72.5 | 67.6 | 84.5 |
| Opus 5.5 + cite + guide | 75.4 | 72.6 | 81.9 |

**Paired tests on the untouched subset** (2,000 resamples; * = CI excludes 0):

| claim | all | untouched |
|---|---|---|
| Opus − rules (F1) | +17.1* | **+11.8* [+5.1, +18.4]** |
| Opus − Qwen (F1) | +20.5* | +19.9* |
| Opus − Sonnet (F1) | +2.1 n.s. | +0.4 n.s. |
| Citation, Qwen: recall | −22.5* | −19.4* |
| Citation, Sonnet: recall | −7.7* | −8.6* |
| Citation, Opus: recall | −3.4* | −3.8* |
| Citation, Opus: F1 / precision | +4.1* / +9.4* | +4.0* / +9.6* |
| Guidelines, Opus (F1) | +6.8* | +5.2* |
| Guidelines, Opus cited (F1) | +5.6* | +6.2* |

**What this means**
- **Every significant claim survives on labels no AI-assisted fix touched**, including the headline: the recall cost of the citation constraint shrinks with model capability (Qwen −19.4, Sonnet −8.6, Opus −3.8).
- **The concern was not baseless.** All Claude conditions score higher on touched records (Opus + guide 84.5 vs 67.6), while rule-based scores lower (43.0 vs 50.7). The LLM-over-rules gap shrinks from +17.1 to +11.8 on untouched labels. Touched records may also differ in difficulty, so this is not proof of bias, but **the paper should report the untouched subset alongside the full set** and state the gap.
- Guidelines were written alongside these fixes, and their gain is larger on touched records (+11.1 vs +5.2). Report the untouched figure as the conservative estimate.
- The independent second labeler remains the stronger test; this audit is the evidence available without one.
