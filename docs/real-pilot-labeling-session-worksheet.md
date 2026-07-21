# Real-Pilot Labeling Session Worksheet

Use this worksheet during a manual labeling session in the labeling UI (`/labeling`).

## Labeling Conventions (decided 2026-06-08, read first)

These keep the gold set consistent, which is what makes the heuristic-vs-gold comparison trustworthy.

1. **One topic per record.** Each record gets exactly one `policy_topic`. Do not try to capture refund AND reissue/change AND no-show in a single record, even when one block of text mentions all of them. The chunks overlap, so the same fare rule appears across several records: label the refund aspect on one, the change aspect on another. This matches the heuristic (which predicts one topic per chunk) and keeps `compare` apples to apples.

2. **Refund and Cancellation collapse to `refund`.** This corpus writes them together (for example "Refund/Cancellation: Not Available"). When a clause bundles them, label `refund`. Use `cancellation` only when the clause genuinely separates the two, for example "cancellation allowed but non-refundable", which is cancellation `allowed` plus refund `not_available`. Apply this every time.

3. **Multiple fees within one topic go in the tier rows, not new topics.** If one topic has fees that vary by time (72h vs 24h vs no-show) or by method (cash vs travel credit), capture all of them in the Penalty Tiers rows (or Refund Payout Options rows) inside that one record. The repeat-lists are for variation within a topic; they do not cross topics.

4. **Keep the heuristic topic when it is right.** The orange "not-equal heuristic" flag shows where you overrode the machine guess. Override only when it is genuinely wrong. This is faster and keeps the comparison honest.

5. **Genuinely multi-policy chunk, one gold entry wanted?** Label the single most salient policy for that record and note the secondary rule in `notes` if it matters.

## v1: finer one-clause-per-record granularity (optional, run AFTER the baseline)

Per the annotation-methodology research (see `docs/annotation-methodology-research.md`),
the cleanest fix for "one chunk mixes refund, change, and cancellation" is finer
chunking so each record is a single atomic rule, keeping the simple one-topic
schema. This is built and ready, but do the v0 baseline on the current 40-record
queue first so you get the heuristic baseline number, then run v1 as a second,
independent gold round.

The extractor now has a `--granularity clause` mode (default stays `chunk`). It
mines the recurring colon labels (Refund/Cancellation, Changes in Flight, Hand
baggage, etc.) from the corpus and splits each chunk into one rule per record.

Run the v1 round with a SEPARATE gold store so v0 and v1 stay independent:

```bash
# 1. fine clauses + fine queue (already generated; regenerate after re-ingest)
python -m extractors.pipeline --input data/chunks.jsonl --output data/policy_clauses_fine.jsonl --mode heuristic --granularity clause
python -m evals.real_pilot_labeling queue --clauses data/policy_clauses_fine.jsonl --output data/real_pilot_manual_labeling_queue_fine.jsonl --limit 200

# 2. serve the fine queue against a SEPARATE labeling DB (keeps v0 gold intact)
#    PowerShell:  $env:LABELING_QUEUE_PATH="data/real_pilot_manual_labeling_queue_fine.jsonl"; $env:LABELING_DB_PATH="data/labeling_fine.db"; uvicorn api.main:app
LABELING_QUEUE_PATH=data/real_pilot_manual_labeling_queue_fine.jsonl LABELING_DB_PATH=data/labeling_fine.db uvicorn api.main:app

# 3. compare v1 heuristic vs v1 gold
python -m evals.real_pilot_labeling compare --pred data/policy_clauses_fine.jsonl --manual-gold-db data/labeling_fine.db
```

The same Labeling Conventions below still apply (one topic per record), but with
fine granularity each record is already a single rule, so the conventions become
natural rather than a workaround.

## Session Info

- [ ] Session date:
- [ ] Labeler:
- [ ] Queue file:
- [ ] Manual gold file:
- [ ] Sources targeted (min 3):
- [ ] Goal for this session (e.g., first 30 clauses / refund-focus / no-show cleanup):

## Pre-Run Checklist

- [ ] Real pilot docs added to `data/raw/`
- [ ] `data/pilot_source_manifest.csv` updated with matching filenames
- [ ] Ingestion run (`data/chunks.jsonl` generated)
- [ ] Heuristic extraction run (`data/policy_clauses.jsonl` generated)
- [ ] Real labeling queue generated (`python -m evals.real_pilot_labeling queue`)
- [ ] Labeling UI opens at `http://127.0.0.1:8000/labeling`

## Session Commands (Reference)

```bash
python -m ingestion.cli --input data/raw --output data/chunks.jsonl --manifest data/pilot_source_manifest.csv
python -m extractors.pipeline --input data/chunks.jsonl --output data/policy_clauses.jsonl --mode heuristic
python -m evals.real_pilot_labeling queue
uvicorn api.main:app --reload
python -m evals.real_pilot_labeling compare
```

## 30-Clause Target Plan (Checklist)

### Batch A: High-Signal Core (12)

- [ ] 1. Refund + fixed fee (currency amount)
- [ ] 2. Refund + percentage penalty
- [ ] 3. Refund + before departure
- [ ] 4. Refund + after departure
- [ ] 5. Reissue/change + fare difference only
- [ ] 6. Reissue/change + fixed fee
- [ ] 7. Reissue/change + within 24h
- [ ] 8. No-show + forfeiture
- [ ] 9. No-show + explicit fee
- [ ] 10. Go-show + conditional wording
- [ ] 11. Cancellation + explicit fee/percent
- [ ] 12. Tax refund clause

### Batch B: Conditions + Exceptions (10)

- [ ] 13. Contains `if ...`
- [ ] 14. Contains `provided that ...`
- [ ] 15. Contains `subject to ...`
- [ ] 16. Contains `when ...`
- [ ] 17. Contains `except ...`
- [ ] 18. Contains `unless ...`
- [ ] 19. Contains `does not apply to ...`
- [ ] 20. Refund/reissue + condition + penalty
- [ ] 21. Tax clause + exception (e.g., YQ/YR)
- [ ] 22. No-show/go-show + eligibility condition

### Batch C: Coverage + Hard Cases (8)

- [ ] 23. Waiver clause
- [ ] 24. Waiver clause + applicability condition/date window
- [ ] 25. Supplier-offer clause
- [ ] 26. Airline circular/policy bulletin clause
- [ ] 27. Mixed-policy wording (label or skip)
- [ ] 28. Ambiguous/conflicting clause (likely skip)
- [ ] 29. Heuristic weak prediction (`other` / `unknown`) but manually labelable
- [ ] 30. Another weak prediction from a different source

## Source Coverage Tracker

- [ ] Source 1 (`source_id`):
  - [ ] 1-3 clauses labeled
  - [ ] Includes refund/reissue/no-show or tax
- [ ] Source 2 (`source_id`):
  - [ ] 1-3 clauses labeled
  - [ ] Includes different topic mix
- [ ] Source 3 (`source_id`):
  - [ ] 1-3 clauses labeled
  - [ ] Includes conditions/exceptions
- [ ] No single source exceeds 8 clauses in first 30

## Label Quality Rules (Check During Session)

- [ ] Follow the Labeling Conventions above (one topic per record; refund/cancellation collapse to `refund`; tier rows for within-topic variation)
- [ ] Fill `policy_topic` and `action` for each labeled record
- [ ] Fill penalty fields only when explicit in clause text
- [ ] Fill `time_window` / `before_after_departure` when explicit
- [ ] Capture `eligibility_conditions` for `if / subject to / provided that / when`
- [ ] Capture `exceptions` for `except / unless / does not apply`
- [ ] Use `tax_refund_rule` for tax-specific wording
- [ ] Use `Mark Skip` for truly ambiguous or broken clauses

## Session Tally

- [ ] Total processed (labeled + skipped):
- [ ] Labeled:
- [ ] Skipped:
- [ ] Unlabeled remaining in current queue view:

### Topic Counts (Manual)

- [ ] Refund:
- [ ] Reissue/Change:
- [ ] No-show:
- [ ] Go-show:
- [ ] Cancellation: (only when genuinely separate from refund; see Conventions)
- [ ] Waiver:
- [ ] Other:

## Skip Log (Optional but useful)

- [ ] `record_id`:
  - Reason:
- [ ] `record_id`:
  - Reason:
- [ ] `record_id`:
  - Reason:
- [ ] `record_id`:
  - Reason:
- [ ] `record_id`:
  - Reason:

## Post-Session Compare

- [ ] Run compare: `python -m evals.real_pilot_labeling compare`
- [ ] Save/record compare summary below

### Compare Results Snapshot

- [ ] Gold records matched:
- [ ] Overall field accuracy:
- [ ] Topic accuracy:
- [ ] Action accuracy:
- [ ] Penalty type accuracy:
- [ ] Time window accuracy:
- [ ] Eligibility conditions accuracy:
- [ ] Exceptions accuracy:
- [ ] Tax refund rule accuracy:

## Mismatch Review (Top 10)

- [ ] Mismatch 1 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 2 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 3 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 4 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 5 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 6 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 7 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 8 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 9 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

- [ ] Mismatch 10 (`record_id`):
  - Gold:
  - Pred:
  - Root cause:
  - Heuristic fix candidate:

## Next Session Plan

- [ ] Topic focus for next session:
- [ ] Sources to prioritize:
- [ ] Heuristic fixes to implement before next labeling session:
- [ ] Queue regeneration needed after fixes:

