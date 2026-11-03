# Airline refund and change conditions: pilot extraction benchmark (v1c)

Data and system outputs for the paper *Asking for Evidence Helps, Enforcing It Can
Hurt: Citation-Grounded Extraction of Airline Refund and Change Conditions*
(draft: `paper/draft.md`). Regenerate every reported number with:

```
python scripts/reproduce.py
```

It rebuilds the label databases from these files in `build/repro`, reruns the scorer
and every analysis script, and checks each number against `results/*.json`.

## Files

| Path | Contents |
|---|---|
| `clauses.jsonl` | 250 clauses: `record_id`, source and document ids, carrier, document type, `clause_text`, and `chunk_text` (the parent chunk, used only by the `+ ctx` condition) |
| `sources.csv` | the 27 collected sources with URL, snapshot date and status; 12 of them supply the clauses |
| `labels/annotator_a.jsonl` | gold: annotator A's labels for all 250 clauses (166 labelled, 84 `skip`) |
| `labels/annotator_b_iaa.jsonl` | annotator B's blind labels for the 50-clause agreement sample |
| `labels/adjudicated.jsonl` | the 50-clause sample after A and B resolved every disagreement |
| `labels/annotator_a_relabel.jsonl` | A's blind relabel of 27 clauses (intra-annotator agreement) |
| `labels/provenance/*.jsonl` | A's labels before each consistency pass, oldest first (label-provenance audit) |
| `samples/*.jsonl` | the agreement sample, the relabel sample, and the relabel records not seen in adjudication |
| `retrieval/runs.jsonl` | top-50 rankings of each retriever (BM25, dense, hybrid) per query, at clause and chunk level; units are given as keys (a hash of the normalised clause text, or the chunk id), not text |
| `audits/time_window_direction.jsonl` | A's direction (at least before / within / range) and reference event for the 19 scored gold time windows |
| `audits/citation_audit.jsonl` | A's judgement (yes / partly / no) of whether the quote supports the value, for a seeded sample of 46 values the gate accepted |
| `predictions/*.jsonl` | every system's output on these clauses; cited runs keep the model's pre-gate values (`model_raw_fields`) and the per-field grounding check (`grounding`) |
| `SHA256SUMS` | checksums |

Label rows hold the fields of `docs/LABELING_GUIDE.md` plus `label_status` (`labeled` or `skip`), `notes`, `penalty_tiers` and `refund_payout_options`, exactly as the labelling tool saved them. Twelve fields are scored (see the paper, Section 4).

Prediction files: `policy_clauses_fine.jsonl` is the rule baseline; `v1b_llm_<condition>_<model>.jsonl` are the model runs (`cited` = citation prompt and gate, `ctx` = parent chunk shown, `guided` = guideline summary in the prompt; `qwen7b` = Qwen2.5-7B via Ollama at temperature 0, `sonnet55` / `opus55` = Claude Sonnet 5.5 / Opus 5.5 via the Message Batches API at low effort).

## Licence

Annotations, guide and system outputs: CC BY 4.0. Code: MIT (repository `LICENSE`). The airline clause text is not covered by either licence; see `LICENSE-DATA.md`.

## Source text and rights

The clause text comes from pages airlines publish so passengers can read the conditions before buying. Copyright in the text stays with the carriers. It is shared here for research use, with attribution to each carrier through `sources.csv`. Conditions change: every label describes the page as it was on its snapshot date. A carrier's text will be removed on request (open an issue on the repository).

## Known limits

Small (132 scored clauses after near-duplicate removal), English only, one primary annotator, moderate inter-annotator agreement (topic kappa 0.51). Some gold labels were revised in passes drafted with help from Claude, one of the evaluated model families; the provenance snapshots let you score only untouched labels. Details: paper, Sections 3 and 7.
