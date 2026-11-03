# v1c inter-annotator agreement (2026-11-01)

Sample: 50 records (42 labelled + 8 skipped by A), stratified by genre x topic,
seed 20261030 (`data/queue_iaa_b_report.json`). Annotator B labelled all 50 blind
in the workbook (`scripts/iaa_workbook.py`), using only the guide incl. the
2026-11-01 addendum. Raw numbers: `results/v1c_iaa.json`.

Provenance of B's labels (for Methods): B worked with a colleague's help. Separately,
the workbook was also run through Grok and Gemini; B confirmed they never saw those
answers. Those AI files are not used anywhere. B's file matches them no better than it
matches A (topic 26/47 and 23/47), so it is not a copy.

## Agreement (before adjudication)

| | Agreement | Cohen's kappa |
|---|---|---|
| Skip vs label (n=50) | 90% | 0.50 |
| Topic (n=42 both labelled) | 64% | 0.51 |
| Action (n=42) | 62% | 0.44 |
| Field micro F1, A vs B | 0.52 | |

Penalty type F1 is 0.22. Main disagreement patterns: VZ fee-table cells (A
`reissue_change`, B `other`/`unknown`, 7 cases), `unknown` vs empty penalty type,
changes labelled `cancellation` (4), and 5 records A skipped but B labelled.

## Methods scored against each annotator (micro F1 x100, 42 records)

| Method | vs A | vs B |
|---|---|---|
| Rule-based | 50.2 | 44.6 |
| Qwen 7B | 48.5 | 35.9 |
| Qwen 7B + cite | 28.6 | 20.9 |
| Qwen 7B + cite + guide | 40.4 | 27.5 |
| Sonnet 5.5 | 65.2 | 45.2 |
| Sonnet 5.5 + cite | 69.8 | 50.2 |
| Sonnet 5.5 + cite + ctx | 69.7 | 49.1 |
| Opus 5.5 | 68.8 | 45.4 |
| Opus 5.5 + cite | 74.5 | 51.3 |
| Opus 5.5 + guide | 72.9 | 51.4 |
| Opus 5.5 + cite + guide | 75.7 | 52.3 |

## Paired bootstrap (2000 resamples, 95% CI; * = excludes 0)

| Comparison | vs A | vs B |
|---|---|---|
| Opus - Rule-based | +18.7 [+8.9, +28.0]* | +0.8 [-8.3, +9.5] |
| Opus - Qwen 7B | +20.4 [+10.9, +30.2]* | +9.5 [+2.7, +17.7]* |
| Opus - Sonnet | +3.6 [-1.1, +8.8] | +0.2 [-4.5, +5.1] |
| Qwen + cite - Qwen | -19.9 [-30.9, -9.4]* | -15.0 [-24.4, -5.4]* |
| Sonnet + cite - Sonnet | +4.5 [+0.5, +8.2]* | +5.0 [+1.2, +8.5]* |
| Opus + cite - Opus | +5.7 [+1.7, +9.9]* | +5.9 [+2.8, +9.6]* |
| Opus + guide - Opus | +4.1 [-3.7, +11.1] | +6.0 [+0.7, +11.6]* |
| Opus + cite + guide - Opus + cite | +1.1 [-3.9, +6.5] | +1.0 [-4.4, +6.4] |
| Opus + cite + guide - Rule-based | +25.5 [+15.6, +35.3]* | +7.7 [-1.1, +16.0] |

## What this means

- Robust to the annotator: the citation gate helps Sonnet and Opus (+5 to +6) and
  hurts Qwen 7B; Opus beats Qwen 7B.
- Annotator-dependent: LLMs over the rule-based baseline. Against B, no LLM
  condition beats the rule-based baseline significantly.
- Models agree with A (0.70-0.76) more than B does (0.52), consistent with the
  provenance audit (Claude-assisted fixes to A's labels).
- Next: adjudicate the 111 field disagreements on 39 records
  (`scripts/adjudication_sheet.py`), report kappa before adjudication, and score
  against the adjudicated set.

## Adjudication (2026-11-02)

A and B went through all 111 field disagreements on 39 records together, using only
the guide (`scripts/adjudication_sheet.py`, rules sheet R1-R60 + GAP; log:
`results/v1c_adjudication.json`). Every decision names the guide rule that decided it.

- Outcome: A's value kept 94/111 (85%), B's 16, a new value 1; no row marked GAP.
- Most used rules: R39 eligibility (22), R19 action `conditional` (11), R30 empty
  penalty type (10), R12 topic `reissue_change` (10).
- Agreement to report is the pre-adjudication kappa above. Post-adjudication kappa
  (topic 1.0) is the outcome of discussion, not agreement.

Methods scored against the adjudicated set (micro F1 x100; `results/v1c_iaa_adjudicated.json`):

| Method | vs A | vs adjudicated |
|---|---|---|
| Rule-based | 50.2 | 50.0 |
| Qwen 7B | 48.5 | 49.8 |
| Qwen 7B + cite | 28.6 | 30.8 |
| Sonnet 5.5 | 65.2 | 67.3 |
| Sonnet 5.5 + cite | 69.8 | 72.0 |
| Opus 5.5 | 68.8 | 71.3 |
| Opus 5.5 + cite | 74.5 | 75.9 |
| Opus 5.5 + cite + guide | 75.7 | 77.0 |

| Comparison (vs adjudicated) | Delta [95% CI] |
|---|---|
| Opus - Rule-based | +21.3 [+13.2, +29.9]* |
| Opus + cite + guide - Rule-based | +27.0 [+18.1, +36.1]* |
| Qwen + cite - Qwen | -19.0 [-29.4, -8.6]* |
| Sonnet + cite - Sonnet | +4.8 [+0.8, +8.5]* |
| Opus + cite - Opus | +4.5 [+0.7, +8.6]* |
| Opus - Sonnet | +4.1 [-0.5, +9.0] |

Reading: the citation effect holds against A, B and the adjudicated set. LLMs over
rules holds against A and the adjudicated set but not against B alone; because the
adjudication mostly kept A's values, that claim stays secondary.
