# v0 Baseline: Heuristic vs Manual Gold (40-clause set)

Measured 2026-07-21. Raw output: `data/compare_v0_baseline_2026-07-21.json` (local, untracked).

## Setup

- Corpus: 7 public policy documents -> 279 chunks -> 1,662 heuristic clause records (`data/policy_clauses.jsonl`, chunk granularity)
- Gold: 40 records hand-labeled in the `/labeling` UI per the conventions in
  `real-pilot-labeling-session-worksheet.md` (one topic per record; bundled
  Refund/Cancellation -> `refund`; fee variation captured in tier rows)
- Command: `python -m evals.real_pilot_labeling compare`
- Match rate: 40/40 gold records matched to predictions, 0 missing

## Results

| Metric | Accuracy |
|---|---|
| **Overall field accuracy (12 fields)** | **46.1%** |
| policy_topic | **79.5%** |
| action | 56.4% |
| before_after_departure | 50.0% |
| exceptions | 33.3% |
| penalty_percent | 33.3% |
| penalty_type | 29.4% |
| time_window | 20.0% |
| eligibility_conditions | 12.5% |
| fare_difference_required | 10.0% |
| penalty_amount | 0.0% |
| penalty_currency | 0.0% |
| tax_refund_rule | 0.0% |

## Reading

The heuristic classifies the policy **topic** reliably (79.5%) but largely fails
to extract structured **values**: amounts, currencies, time windows, and
conditions. `time_window` errors are dominated by missing predictions (5/10),
and the penalty amount/currency fields never match. This is consistent with the
one-topic-per-chunk design: chunks bundle several rules, so value fields inside
a chunk rarely align with the single gold rule chosen for that record.

## Motivated next step (v1)

Finer clause-level granularity (`--granularity clause`, gazetteer-based label
splitting in `extractors/pipeline.py`) so each record is one atomic rule, then
an independent 200-record gold round against `data/labeling_fine.db`:

```bash
# serve the fine queue against a SEPARATE gold DB (PowerShell)
$env:LABELING_QUEUE_PATH="data/real_pilot_manual_labeling_queue_fine.jsonl"
$env:LABELING_DB_PATH="data/labeling_fine.db"
uvicorn api.main:app

# compare v1
python -m evals.real_pilot_labeling compare --pred data/policy_clauses_fine.jsonl --manual-gold-db data/labeling_fine.db
```
