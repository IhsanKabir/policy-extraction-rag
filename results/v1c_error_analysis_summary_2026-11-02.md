# v1c error analysis (2026-11-02)

Gold: `data/labeling_fine.db` on the v1c queue, near-duplicates removed (132
records, the headline view). Script: `scripts/error_analysis.py`; counts in
`results/v1c_error_analysis.json`. Example clauses: `handoff/error_examples.md`
(local only, airline text).

## 1. The citation result splits into two effects

"+ cite" changes two things at once: the prompt asks for a verbatim quote per field,
and a gate nulls any value whose quote is not found in the clause. Scoring each cited
run again with the gate off (the model's raw values from the same run) separates them.

| Model | Citation prompt (cite, gate off - plain) | Gate (gate on - gate off) |
|---|---|---|
| Qwen 7B | -0.9 [-3.7, +1.6] | **-18.6 [-23.8, -13.6]** |
| Sonnet 5.5 | **+4.4 [+1.8, +7.0]** | -0.3 [-1.0, +0.3] |
| Opus 5.5 | **+3.9 [+0.9, +6.6]** | +0.2 [+0.0, +0.7] |
| Opus 5.5 + guide | **+2.8 [+0.4, +5.4]** | +0.1 [+0.0, +0.3] |

(micro F1 x100, paired bootstrap 95% CI, 2000 resamples)

- For strong models, the gain comes from **asking for evidence**, not from the gate.
  The gate is nearly free for them: what it removes is mostly eligibility lists, and
  right and wrong removals roughly cancel (Opus + cite + guide: 33 right values
  removed vs 29 wrong or spurious ones).
- For Qwen 7B the prompt is neutral and **the gate does all the damage**: it removed
  110 right values against 107 wrong or spurious ones, and right values weigh more in F1.
- Why values fail the gate (asserted values on the 132 records; corrected 2026-11-02,
  an earlier version of this note mis-split these counts):

  | Model | Grounded | Field left uncited | Quote as a list | Quote not in clause | Quote found, value not in it |
  |---|---|---|---|---|---|
  | Qwen 7B | 86 | 159 | 10 | 6 | 42 |
  | Sonnet 5.5 | 318 | 7 | 0 | 0 | 70 |
  | Opus 5.5 | 383 | 0 | 0 | 0 | 94 |

  Qwen returns the citations object but leaves over half its values (159 of 303)
  without a quote: a failure to follow the output instructions, not invented quotes
  (only 6 quotes are absent from the clause). Sonnet and Opus quote nearly every
  value; what the gate removes for them are values, mostly paraphrased eligibility
  items, that do not appear word for word in their own quote.

Implication for the paper: the claim should be "asking strong models to cite evidence
improves extraction; a strict verbatim-grounding gate costs them almost nothing but
breaks a 7B model that does not reliably produce quotes". This replaces "citation
grounding helps strong models and hurts weak ones", which mixed the two effects.

## 2. Where the best system (Opus 5.5 + cite + guide) goes wrong

| Field | Correct | Wrong | Missed | Spurious |
|---|---|---|---|---|
| policy_topic | 115 | 16 | 1 | 0 |
| action | 105 | 26 | 1 | 0 |
| penalty_type | 36 | 4 | 4 | 10 |
| before_after_departure | 16 | 5 | 1 | 18 |
| eligibility_conditions | 31 | 16 | 15 | 7 |
| time_window | 18 | 1 | 0 | 9 |
| penalty_currency | 3 | 1 | 0 | 8 |
| other fields | small counts | | | |

Error types, from reading the examples:

1. **Action: `conditional` read as `unknown` (17 of 26 action errors, mostly policy
   text).** The model treats a clause that states a fee or condition without saying
   "allowed" as not stating permission ("Passenger must pay service fee THB 225...").
   The guide counts an attached fee as `conditional`. Action is also the least stable
   field for humans (self-relabel kappa 0.60, A vs B 0.44), so part of this is a
   genuinely unclear boundary in the guide.
2. **Values filled from context the guide says to leave empty.** Before/after
   departure is spurious 18 times (often `unknown` or an inference); currency 8 times,
   mostly fee-table rows where an amount appears but the gold treats it as not a
   penalty (e.g. the Nok Refund add-on price, guide example D); time window 9 times.
   This is the "only what the clause says" rule (guide R1/R2), which the model follows
   less strictly than the annotator.
3. **Topic: `reissue_change` read as `other` (5) or `refund` (2).** Name-change fee
   clauses and upgrade clauses that mention change fees; the topic depends on which
   transaction the guide assigns them to (R17).
4. **Eligibility conditions (16 wrong, 15 missed).** Free-text lists match by token
   F1 >= 0.5, so phrasing differences count as errors; this field is the noisiest to
   score and the one the gate removes most.
5. **Invalid values.** Occasional non-schema output, e.g. currency `$` instead of a
   code.

By genre: fare rules are mostly right on topic/action (77/88) and fail on spurious
fields (currency, before/after, time window); policy text fails mainly on action
(15 wrong of 44) and missed eligibility conditions (11).

## Caveats

- Scored against annotator A's labels; the provenance audit and IAA results apply
  (Claude-assisted fixes, A vs B kappa 0.44-0.51).
- The gate ablation uses each run's own raw values, so it isolates the gate exactly;
  the prompt comparison is between separate runs.

## 3. The prompt-vs-gate split against the other label sets

Same ablation on the 42 IAA records both annotators labelled (`scripts/iaa_agreement.py`,
`cite_ablation` in `results/v1c_iaa.json` and `results/v1c_iaa_adjudicated.json`):

| Cited run | Prompt vs A | Prompt vs B | Prompt vs adjudicated | Gate (all three) |
|---|---|---|---|---|
| Qwen 7B + cite | -0.8 | -2.0 | -0.8 | **-19.1 / -13.0 / -18.3** |
| Sonnet 5.5 + cite | **+4.7** | **+5.3** | **+4.9** | -0.2 / -0.4 / -0.2 |
| Opus 5.5 + cite | **+5.7** | **+5.9** | **+4.5** | 0.0 |
| Opus 5.5 + cite + guide | +2.8 | +0.9 | +2.8 | 0.0 |

Bold = 95% CI excludes 0. The split holds against every label set: the citation prompt
helps Sonnet and Opus, the gate is free for them and costly for Qwen 7B. On top of the
guide prompt the citation prompt adds little on this 42-record subset (it was
+2.8 [+0.4, +5.4] on the full 132).
