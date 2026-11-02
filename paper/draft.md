# Asking for Evidence Helps, Enforcing It Can Hurt: Citation-Grounded Extraction of Airline Refund and Change Conditions (A Pilot Benchmark)

Ihsan Kabir

*Draft v0.1, 2026-11-02. Every number is taken from a committed results file named in brackets; no number is estimated. Items marked TODO need the author's input.*

## Abstract

Airline refund, cancellation and change conditions are published as fare rules and terms pages written in inconsistent semi-structured language, and travel agents still read them by hand. We release a pilot benchmark for extracting these conditions into a structured schema: 250 clauses from 12 public airline documents (11 carriers), of which 166 are labelled with 12 fields (topic, permission, penalty type, amount, currency, percentage, fare difference, time window, before/after departure, tax refund rule, eligibility conditions, exceptions) and 84 are marked as stating no rule. We compare a keyword rule baseline, a local 7B model (Qwen2.5-7B) and two frontier models (Claude Sonnet 5.5 and Opus 5.5), each with and without a *citation* condition in which the model must quote the clause text supporting every value and a verbatim gate removes values whose quote is not found. The best system (Opus + citation + annotation guidelines) reaches 75.4 micro F1 [71.1, 79.1] against 48.6 for rules. An ablation separates the citation condition into its two parts. Asking for quotes improves the frontier models by 3 to 5 F1 points, significant against both annotators and the adjudicated labels and positive in every pairing of three repeated runs; the verbatim gate itself costs them nothing measurable but removes 18.6 points from the 7B model, which leaves 159 of its 303 values without any quote. Inter-annotator agreement is moderate (topic kappa 0.51; intra-annotator 0.82), and we report how much each claim depends on the annotator.

## 1. Introduction

When a passenger asks to change or refund a ticket, an agent must find what the fare allows: whether the change is permitted, what it costs, until when, and whether the fare difference applies. These conditions live in ATPCO fare-rule categories (Cat 16 penalties, Cat 31 voluntary changes, Cat 33 voluntary refunds) and in carriers' public conditions of carriage, written as free text with carrier-specific layouts. Misreading them costs money in both directions: unrecoverable penalties, or refunds that were never due.

Large language models can read this text, but an agent needs to know *where* an answer came from before acting on it. A common answer is to require the model to cite its source. We ask two questions:

1. How well do current models extract refund and change conditions into a fixed schema, compared with hand-written rules?
2. Does requiring citations help, and if so, is it the request for evidence or the enforcement of it that matters?

Contributions:

- **A pilot benchmark** of 250 airline policy clauses with a 12-field schema, a 123-line annotation guide, a second-annotator agreement study, an adjudicated subset and a blind self-relabel (Section 3).
- **A comparison** of rules, a local 7B model and two frontier models under plain, citation, chunk-context and guideline prompts (Section 4).
- **An ablation that separates citation prompting from citation enforcement** using each run's own pre-gate values (Section 5.2). For frontier models the gain comes from asking for evidence; the verbatim gate is nearly free for them and harmful for a small model.
- **An audit of label provenance**: some gold labels were revised in consistency passes drafted with the help of one of the evaluated model families. We report every result on the untouched labels as well (Section 3.4).

This is a pilot: the scored set is small (132 records after near-duplicate removal, from 12 documents), and we treat it as evidence about effect directions and failure modes, not as a leaderboard.

## 2. Related work

**Extraction from contracts and policies.** Legal and policy text has established extraction benchmarks: CUAD labels 41 clause categories in 510 commercial contracts, annotated by trained law students (Hendrycks et al., 2021), and OPP-115 annotates data practices in 115 website privacy policies (Wilson et al., 2016). Both show that expert-defined schemas and careful annotation, not model choice alone, determine what can be measured. Airline conditions are closer to policy text than to contracts, but carry typed values (fees, currencies, percentages, time windows) that span annotation does not capture; we label typed fields instead.

**Structured extraction with LLMs.** Large language models extract schema-shaped records from technical text with little or no task-specific training (Dagdelen et al., 2024, for materials chemistry). We use prompted frontier and open models without fine-tuning and score typed fields against human labels.

**Attribution and citation.** Rashkin et al. (2023) define attribution to identified sources (AIS) as a standard for checking generated statements against evidence, and ALCE benchmarks LLMs that generate text with citations, finding that even strong models leave many statements unsupported (Gao et al., 2023). These works evaluate citations produced alongside free-text answers. We study a narrower setting, verbatim quotes for structured fields checked mechanically against the source clause, and separate the effect of asking for evidence from the effect of enforcing it.

**Language technology for aviation text.** Aviation NLP has concentrated on operational messages: NOTAMs have been processed with self-supervised language models for criticality prediction, entity recognition and structured translation (Arnold et al., 2022), and recent work parses them with LLMs into structured, knowledge-grounded outputs (Liu et al., 2025; Liu et al., 2026). LLM agents have also been used to explain and resolve air traffic conflicts in simulation (Andriuskevicius and Sun, 2024). We did not find a public benchmark for extracting airline commercial conditions such as refund and change rules; this work targets that gap, on the passenger and agent side of aviation rather than the operational side.

## 3. Data

### 3.1 Sources and clauses

Of 27 public documents collected [`data/pilot_source_manifest.csv`], the labelling queue draws on 12 from 11 carriers: nine terms or conditions pages (Nok Air DD (two), Air Arabia G9, Malaysia Airlines MH, Qatar Airways QR, SpiceJet SG, Singapore Airlines SQ, Thai Airways TG, Oman Air WY), one fare-rule page (VietJet VZ), one policy bulletin (AirAsia AK) and one FAQ (Emirates EK). The other 15 are not in the benchmark: 7 travel-trade fare circulars were excluded as non-public trade documents; 7 public pages (six IndiGo fee tabs and one Bangkok Airways page) need a headless browser to render and yielded no parsed text; one Emirates page refused automated access and no saved copy was made. Snapshot dates recorded in the manifest are 2026-02-22/23 (VZ, DD, WY, G9, AK, SG) and 2026-10-28 (SQ, MH, QR, EK, TG); conditions change over time, so every label refers to its snapshot.

Documents were chunked and split into atomic clauses. Fare-rule pages are tables flattened to text ("Changes in Flight: Charge fee of change and difference in fare, if any"), so the splitter first mines recurring field labels from the corpus and splits only at those labels; other text is split at sentence and list boundaries. The labelling queue holds 250 clauses: 177 from terms pages, 53 from fare rules, 20 from policy bulletins. For analysis we group the six carriers whose pages present conditions as fee tables or fare-rule rows (VZ, DD, G9, AK, SG, QR) as *fare rule* and the rest as *policy text*.

### 3.2 Schema and annotation

Each clause gets either *skip* (no rule stated: menus, contact details, fragments) or 12 fields: policy topic (refund, cancellation, reissue/change, no-show, go-show, waiver, other), action (allowed, conditional, not allowed, unknown), penalty type (free, fare difference only, mixed, fixed amount, percentage, forfeiture, tax only, unknown), penalty amount, currency, percentage lost, fare difference required, time window, before/after departure, tax refund rule, eligibility conditions and exceptions. The guide [`docs/LABELING_GUIDE.md`] is built on two rules: label only what the clause states, and an empty field is a correct answer when the clause is silent. Topic follows the passenger's transaction, not the words used, following the ATPCO split between Cat 31 (changes, including residual value) and Cat 33 (refunds).

Annotator A (the author) labelled all 250 clauses in a web tool with guardrails (field locking, format normalisation, pre-save warnings): 166 labelled, 84 skipped. The first 62 queue positions were labelled with the rule baseline's guess pre-filled; the rest were labelled blind. Section 3.3 measures what the pre-fill did. One guide amendment was made during the agreement study (2026-11-01: fixed fee plus percentage is `mixed`); it changed no existing gold label and was given to the second annotator in writing before they started.

### 3.3 Agreement

**Inter-annotator.** A second annotator (B) labelled a stratified sample of 50 clauses (42 labelled by A, 8 skipped; seed fixed) in a spreadsheet, alone, without AI tools and with only the guide; B worked with a colleague's help. B never saw A's labels or any model output. [`results/v1c_iaa_summary_2026-11-01.md`]

| | Agreement | Cohen's kappa |
|---|---|---|
| Label vs skip (n=50) | 90% | 0.50 |
| Topic (n=42) | 64% | 0.51 |
| Action (n=42) | 62% | 0.44 |
| All fields, micro F1 | 0.52 | |

Disagreements were systematic: B labelled fare-rule table cells as `other` rather than `reissue_change`, used `unknown` where the guide leaves the penalty type empty, and labelled 5 clauses A skipped.

**Adjudication.** A and B resolved all 111 field disagreements (39 clauses) together, naming for each the guide rule that decided it [`results/v1c_adjudication.json`]. A's value was kept in 94, B's in 16, and a new value chosen once; no case was marked as a gap in the guide. Because the adjudication mostly kept A's values, we treat it as a consistency check on A, not as independent evidence.

**Intra-annotator.** A relabelled 27 of their own records blind, after the consistency passes [`results/v1c_self_relabel_summary_2026-11-02.md`]. Ten of these had been seen during adjudication the same day; on the other 17, topic kappa is 0.82, action kappa 0.60 and field micro F1 0.80 (all 27: 0.88, 0.65, 0.84). A is therefore much more consistent with themself than with B; the gap is between annotators' readings of the guide, and action is the least stable field for both.

**Pre-fill anchoring.** The 27 relabelled records are exactly the labelled records from the pre-filled part of the queue, so the blind relabel also measures anchoring. The rule baseline scores 47.9 F1 against the original (pre-filled) labels and 41.5 against the blind relabel (51.9 vs 43.1 on the 17 records not seen in adjudication), while the LLM systems move by up to 7 points in either direction with no consistent sign [`results/v1c_self_relabel.json`]. Pre-filling inflated the baseline on these records; it does not favour the LLM systems.

### 3.4 Label provenance

After first entry, the gold went through consistency passes in which fixes were drafted with Claude's help and approved by A. Since Claude models are evaluated, we compared database snapshots taken before each pass [`results/v1c_label_provenance_summary_2026-10-29.md`]. 68 of 250 records changed after first entry; of the 132 scored records, 100 are untouched. Claude models score higher on touched records than untouched ones (Opus + guide 84.5 vs 67.6) while the rule baseline scores lower (43.0 vs 50.7). We therefore report the untouched subset next to the full set. Snapshots record edits, not advice given while a label was first entered, so the untouched subset is a lower bound on contamination, not proof of its absence.

## 4. Methods

**Rule baseline.** Keyword and pattern rules mapping clause text to topic, action, penalty type and numeric fields.

**LLM extraction.** One request per clause with the schema and the rule "never guess; null when the clause does not state it". Qwen2.5-7B-Instruct runs locally through Ollama at temperature 0. Claude Sonnet 5.5 and Opus 5.5 run through the Message Batches API at low effort. Conditions:

- *plain*: clause text only;
- *+ cite*: the model must also return, for every non-null field, a verbatim quote from the clause; a local check normalises whitespace and case and keeps a value only if its quote occurs in the clause and, for numbers and text fields, the value occurs in the quote. Other values are set to null (the *gate*). The model's pre-gate values are stored, which makes the ablation in 5.2 possible;
- *+ ctx*: the parent chunk is shown as context, with values to be taken from the clause only;
- *+ guide*: a 22-line summary of the annotation guide [`docs/prompt_guidelines.txt`] is added to the prompt.

**Scoring.** For each field present in gold, a prediction is correct if it matches (categorical: exact; numbers: equal; time windows: equal durations; text: token F1 >= 0.5). Predictions on fields that are empty in gold count as false positives. We report micro precision, recall and F1 over all fields, on gold with near-duplicate clauses removed (132 records: 88 fare rule, 44 policy text). Confidence intervals are 95% bootstrap intervals over records (1,000 resamples); differences between systems use a paired bootstrap (2,000 resamples), and we call a difference significant when its interval excludes zero.

## 5. Results

### 5.1 Main results

Micro F1 x100 against annotator A, 132 records [`results/v1c_*.json`, view `all_dedup`; untouched column from the provenance audit].

| Method | P | R | F1 [95% CI] | Topic acc | Action acc | F1 untouched (n=100) |
|---|---|---|---|---|---|---|
| Rule baseline | 54.3 | 43.9 | 48.6 [44.0, 52.8] | 73.5 | 51.5 | 50.7 |
| Qwen 7B | 52.1 | 39.9 | 45.2 [39.1, 50.7] | 37.9 | 28.8 | 42.5 |
| Qwen 7B + guide | 62.6 | 45.3 | 52.5 [45.7, 58.0] | 43.9 | 40.2 | 48.8 |
| Qwen 7B + cite | 49.0 | 17.3 | 25.6 [20.4, 30.7] | 9.8 | 6.8 | 25.4 |
| Qwen 7B + cite + guide | 64.3 | 24.8 | 35.8 [28.6, 42.2] | 23.5 | 18.2 | 27.4 |
| Sonnet 5.5 | 57.5 | 70.9 | 63.5 [59.1, 67.8] | 84.8 | 51.5 | 62.0 |
| Sonnet 5.5 + cite | 72.6 | 63.3 | 67.6 [63.0, 71.8] | 80.3 | 45.5 | 65.5 |
| Sonnet 5.5 + cite + ctx | 73.1 | 68.0 | 70.5 [66.2, 74.9] | 82.6 | 56.1 | 69.6 |
| Opus 5.5 | 57.9 | 75.7 | 65.6 [61.4, 69.7] | 87.1 | 64.4 | 62.4 |
| Opus 5.5 + cite | 67.3 | 72.3 | 69.7 [65.3, 74.1] | 86.4 | 59.8 | 66.5 |
| Opus 5.5 + cite + ctx | 68.7 | 72.5 | 70.5 [65.9, 75.0] | 87.9 | 59.8 | 67.7 |
| Opus 5.5 + guide | 67.0 | 78.8 | 72.5 [67.9, 76.8] | 87.9 | 78.0 | 67.6 |
| Opus 5.5 + cite + guide | 72.7 | 78.2 | **75.4** [71.1, 79.1] | 87.1 | 79.5 | 72.6 |

Paired differences (A's labels; full set / untouched): Opus over rules +17.1* / +11.8* [+5.1, +18.4]; Opus over Qwen +20.5* / +19.9*; Opus over Sonnet +2.1 / +0.4 (not significant); guidelines for Opus +6.8* / +5.2*.

Frontier models beat the rule baseline by a wide margin on A's labels, and the margin shrinks but remains significant on untouched labels. The 7B model is no better than rules with a plain prompt; the guide summary raises it to 52.5 on all records but not on untouched ones (48.8, below rules at 50.7), so we make no claim for it. Sonnet and Opus are not distinguishable.

### 5.2 Citation: asking versus enforcing

"+ cite" bundles a prompt change (quote every value) and a filter (the gate). Each cited run stores the model's values before the gate, so we can score the same run with the gate off and split the effect exactly [`results/v1c_error_analysis_summary_2026-11-02.md`].

| Model | Citation prompt (cite, gate off) - plain | Gate (on - off) |
|---|---|---|
| Qwen 7B | -0.9 [-3.7, +1.6] | **-18.6 [-23.8, -13.6]** |
| Sonnet 5.5 | **+4.4 [+1.8, +7.0]** | -0.3 [-1.0, +0.3] |
| Opus 5.5 | **+3.9 [+0.9, +6.6]** | +0.2 [+0.0, +0.7] |
| Opus 5.5 + guide | **+2.8 [+0.4, +5.4]** | +0.1 [+0.0, +0.3] |

For the frontier models, the improvement comes from asking for evidence: precision rises (Opus 57.9 to 67.3) because the model asserts fewer values it cannot point to. The gate then removes little, mostly eligibility list items, and the right and wrong values it removes roughly cancel (Opus + cite + guide: 33 right, 29 wrong or spurious). For the 7B model, the prompt change is neutral and the gate does the damage: Qwen returned a citations object but left 159 of its 303 asserted values without a quote; a further 42 had a quote that did not contain the value, 10 were quoted in the wrong format, and only 6 quotes were absent from the clause. The 7B model fails the gate by not following the output instructions, not by inventing quotes. Sonnet and Opus quote nearly every value; the values the gate removes for them (70 and 94) are mostly paraphrased eligibility items that do not occur word for word in their own quote.

**Run-to-run variance.** The table above uses one run per condition. To check that the prompt effect is larger than run-to-run noise, we repeated the four plain and cited Claude conditions twice more with identical settings, giving three runs each [`results/v1c_variance.json`]. Micro F1 (mean, standard deviation, range over three runs):

| Model | Plain | Cite, gate off | Cite | Prompt effect, all 9 pairings | Gate effect, per run |
|---|---|---|---|---|---|
| Sonnet 5.5 | 64.9, sd 1.4 [63.5, 66.3] | 68.1, sd 0.2 [67.9, 68.4] | 68.2, sd 0.5 [67.6, 68.6] | **+3.3** [+1.7, +4.9] | +0.1 [-0.3, +0.4] |
| Opus 5.5 | 64.7, sd 1.1 [63.5, 65.6] | 69.8, sd 0.4 [69.5, 70.2] | 69.9, sd 0.2 [69.7, 70.2] | **+5.1** [+3.9, +6.7] | +0.2 [+0.0, +0.3] |

Every pairing of a cited run (gate off) with a plain run is positive for both models. The size of the effect varies with the runs paired: the single-run estimates in the table above (+4.4 for Sonnet, +3.9 for Opus) lie within these ranges, and the three-run means are the better estimates. The gate effect stays within half a point in every run. Plain runs vary more than cited runs (sd 1.1 to 1.4 against 0.2 to 0.5).

### 5.3 Dependence on the annotator

We rescored all systems on the 42 clauses both annotators labelled, against A, against B and against the adjudicated labels [`results/v1c_iaa_summary_2026-11-01.md`, `results/v1c_error_analysis_summary_2026-11-02.md` section 3].

| Claim | vs A | vs B | vs adjudicated |
|---|---|---|---|
| Citation prompt, Sonnet | +4.7* | +5.3* | +4.9* |
| Citation prompt, Opus | +5.7* | +5.9* | +4.5* |
| Gate, Qwen 7B | -19.1* | -13.0* | -18.3* |
| Gate, Opus | 0.0 | 0.0 | 0.0 |
| Opus - rules | +18.7* | +0.8 | +21.3* |
| Opus + cite + guide - rules | +25.5* | +7.7 | +27.0* |
| Opus - Qwen 7B | +20.4* | +9.5* | +21.5* |

The citation findings hold against every label set. The advantage of frontier models over rules does not hold against B alone: against B, no LLM condition is significantly better than rules. Models agree with A (0.70 to 0.76 F1) more than B does (0.52). This is consistent with the provenance audit and with B applying some guide rules differently (Section 3.3). We therefore present the LLM-over-rules result as conditional on the annotation and the citation result as the robust finding.

### 5.4 Error analysis

For the best system (Opus + cite + guide) [`results/v1c_error_analysis.json`]:

1. **Permission with a condition read as "unknown"** (17 of 26 action errors, mostly policy text). Clauses that state a fee without saying the change is allowed ("Passenger must pay service fee THB 225 per passenger") are `conditional` in the guide; the model treats them as not stating permission. Action is also the least reliable field for humans (intra kappa 0.60, inter 0.44).
2. **Values the clause does not state.** Before/after departure is filled when gold is empty 18 times, time window 9, currency 8, often from amounts in fee tables that are not penalties (e.g. a paid refund add-on).
3. **Change versus other.** Name-change fees and upgrade terms that mention change fees are labelled `other` (5).
4. **Eligibility conditions.** Free-text lists are the noisiest field to score (16 wrong, 15 missed) and the one the gate removes most.

## 6. Discussion

**Ask for evidence, but check the format before enforcing it.** For capable models, a citation request is a cheap precision gain and a verbatim gate adds a guarantee (every surviving value has a supporting quote) at almost no cost. For a small model the same gate silently deletes most of the output. A deployment that enforces citations should measure quote compliance first; a fallback (re-asking for the missing quote, or flagging rather than deleting) would avoid the 7B collapse. We did not test such fallbacks.

**Agreement is the bottleneck.** Moderate human agreement (topic kappa 0.51) caps what any system score means. The disagreements cluster in a few guide decisions: how to treat fare-table cells, whether an unstated penalty type is `unknown` or empty, and when a stated fee makes an action `conditional`. Those are the guide changes to make before scaling the dataset.

## 7. Limitations

- Small scale: 132 scored records from 12 documents and 11 carriers, English only. Confidence intervals are wide, and several differences (Opus vs Sonnet, ctx and guide combinations on the 42-record subset) are not resolved.
- One primary annotator, who is also the author; the second annotation covers 50 clauses and was produced with a colleague's help; adjudication mostly kept the author's values.
- Some gold labels were revised in passes drafted with help from Claude, one of the evaluated model families. We report untouched-label results, but the audit cannot detect influence at first entry.
- Three runs per condition only for the plain and cited Claude conditions (Section 5.2); all other conditions, including Qwen and the guide and context variants, have one run. No variation of prompt wording was tested.
- Retrieval is not evaluated: every system is given the clause. This is extraction, not retrieval-augmented generation.
- Clause segmentation is fixed and imperfect; some clauses are fragments of tables.

## 8. Conclusion

On a pilot benchmark of airline refund and change conditions, frontier models extract structured conditions far better than a 7B model, and better than rules against the primary annotator's labels. Asking them to quote their evidence improves extraction by three to five F1 points, robustly across annotators and repeated runs, while enforcing verbatim quotes costs them nothing measurable and breaks a small model that does not supply quotes. The data, guide, agreement study and scripts are released to support a larger benchmark.

## Reproducibility

Code and scripts: https://github.com/IhsanKabir/policy-extraction-rag. The `release/` folder holds the clauses, all label sets (including the pre-fix snapshots used by the provenance audit), the samples and every system's predictions. `python scripts/reproduce.py` rebuilds the working files from it alone, reruns the scorer and every analysis script, and checks each reported number against the committed results; bootstrap seeds are fixed, so the check is exact. All Claude runs together cost USD 9.19 through the Message Batches API (5.01 for the main runs, 4.18 for the repeats).

**Data statement.** The clauses come from pages airlines publish so that passengers can read the conditions before buying. Copyright in the text remains with the carriers. We release the labels, source URLs, snapshot dates and clause text for research use, with attribution, and will remove any carrier's text on request. Code is released under MIT and the annotations under CC BY 4.0; the clause text is not covered by either licence.

**Use of AI assistance.** Claude (Anthropic) was used to write analysis code, to draft consistency fixes to the gold labels that the annotator then approved or rejected (Section 3.4), and to draft this paper. All labels were made or approved by human annotators, the second annotation and adjudication used no AI tools, and every reported number comes from committed scripts.

## References

- Andriuskevicius, J., and Sun, J. (2024). Automatic control with human-like reasoning: exploring language model embodied air traffic agents. arXiv:2409.09717.
- Arnold, A., Ernez, F., Kobus, C., and Martin, M.-C. (2022). Knowledge extraction from aeronautical messages (NOTAMs) with self-supervised language models for aircraft pilots. NAACL 2022 Industry Track, 188-196.
- Dagdelen, J., Dunn, A., Lee, S., Walker, N., Rosen, A. S., Ceder, G., Persson, K. A., and Jain, A. (2024). Structured information extraction from scientific text with large language models. Nature Communications, 15, 1418.
- Gao, T., Yen, H., Yu, J., and Chen, D. (2023). Enabling large language models to generate text with citations. EMNLP 2023.
- Hendrycks, D., Burns, C., Chen, A., and Ball, S. (2021). CUAD: an expert-annotated NLP dataset for legal contract review. NeurIPS 2021 Datasets and Benchmarks Track.
- Liu, M., Fang, Q., Yang, Y., Zhao, C., and Cai, K. (2025). Knots: a large-scale multi-agent enhanced expert-annotated dataset and LLM prompt optimization for NOTAM semantic parsing. arXiv:2511.12630.
- Liu, M., Fang, Q., Wu, Y., Zhao, C., Yang, Y., and Cai, K. (2026). NOTAM-Evolve: a knowledge-guided self-evolving optimization framework with LLMs for NOTAM interpretation. AAAI 2026.
- Rashkin, H., et al. (2023). Measuring attribution in natural language generation models. Computational Linguistics, 49(4), 777-840.
- Wilson, S., et al. (2016). The creation and analysis of a website privacy policy corpus. ACL 2016.

