# Citation-grounded extraction of airline refund and change conditions

Code and data for the paper *Asking for Evidence Helps, Enforcing It Can Hurt:
Citation-Grounded Extraction of Airline Refund and Change Conditions (A Pilot
Benchmark)* ([`paper/main.tex`](paper/main.tex)).

The benchmark has 250 clauses from 12 public airline documents (11 carriers):
166 labelled with a 12-field schema (topic, action, penalty type, amount,
currency, percentage, fare difference, time window, before/after departure,
tax refund rule, eligibility conditions, exceptions) and 84 marked as stating
no rule. It compares a keyword rule baseline, Qwen2.5-7B and Claude Sonnet 5.5
and Opus 5.5, with and without a citation condition (quote every value; a
gate removes values whose quote is not in the clause), and separates the
effect of asking for quotes from the effect of enforcing them.

**Status:** pilot. The gold labels come mainly from one annotator (the
author), with a 50-clause second-annotator study; see the paper's
Limitations section before relying on any absolute score.

## Reproduce the results

```bash
pip install -r requirements-paper.txt     # only needed for extraction runs and tests
python scripts/reproduce.py               # rebuilds everything from release/, checks every results file
python scripts/regate_check.py            # re-applies the citation gate to every stored model reply
python scripts/check_paper_numbers.py     # traces every number in paper/main.tex to a results file
python -m pytest tests
```

`reproduce.py` uses only the standard library and this repository. It
reproduces the metrics from the saved predictions; rerunning the models
(`extractors/claude_batch.py`, `extractors/llm_pipeline.py`) uses the same
prompts and settings, but hosted models can change behind a fixed name.

## Layout

| Path | Contents |
|---|---|
| `release/` | the benchmark: clauses, all label sets, provenance snapshots, samples, every system's predictions, retriever rankings, checksums ([data card](release/README.md)) |
| `paper/` | the paper source and the number allowlist |
| `docs/LABELING_GUIDE.md` | the annotation guide |
| `evals/v1b_eval.py` | the scorer |
| `extractors/` | LLM extraction (Ollama and Claude Message Batches), prompts, citation gate |
| `scripts/` | agreement, adjudication, provenance audit, error analysis, variance, retrieval, rule detection, robustness, release export and reproduction |
| `results/` | committed results; `reproduce.py` checks them |
| `api/`, `web/`, `ingestion/` | the earlier retrieval and labelling prototype the data was collected with |

## Licences

Code: MIT ([`LICENSE`](LICENSE)). Annotations and model outputs: CC BY 4.0
([`release/LICENSE-DATA.md`](release/LICENSE-DATA.md)). The airline clause
text is not covered by either licence; copyright stays with the carriers, and
any carrier's text will be removed on request.
