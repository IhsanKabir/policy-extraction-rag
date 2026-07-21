# Annotation Methodology Research: single-topic vs multi-aspect labeling for fare-rule clause extraction

Generated 2026-06-08. Sources: ~30 across 4 sub-questions. Confidence: High on the
core conclusions, with single-source items flagged inline. This report informs the
A-vs-B labeling decision and the paper's methodology section.

## Executive summary

The literature is consistent on four points:

1. Established information-extraction practice does NOT force one label onto a coarse passage that contains several aspects. The two accepted mechanisms are (a) multi-label the unit, or (b, preferred) segment into finer single-aspect units and label each once. Finer single-topic units measurably lower inter-annotator variance.
2. For clause-level extraction and citation grounding, fine "one atomic rule per record" chunking beats coarse section chunks, provided you compensate for lost context (parent-document retrieval, light overlap, a short document/section summary on each unit, and provenance metadata).
3. Real legal/policy datasets confirm this: CUAD uses span-per-category extraction (multi-label, overlaps allowed); LEDGAR is the one-paragraph-one-label exception; CLAUDETTE/UNFAIR-ToS is sentence-level multi-label; OPP-115/PolicyQA use span-plus-attribute. The recurring fix for a multi-aspect unit is never "pick one category."
4. There is NO public airline fare-rule clause-extraction dataset. That gap is a genuine contribution for the paper.

The practical resolution: the user's pain (a coarse chunk that mixes refund, change, and cancellation) is a chunking problem, not a schema problem. The cleanest fix is finer one-clause-per-record chunking, which keeps the simple single-topic schema AND makes each record complete. Multi-label-per-coarse-chunk is the weaker alternative.

## 1. Single-label vs multi-label, and the inter-annotator-agreement tradeoff

- Multi-label classification (zero to many categories per unit) is the explicit convention when one unit covers several aspects; "aspect category detection is framed as a multi-label text classification problem." ([Multi-label survey, MDPI](https://www.mdpi.com/2076-3417/15/16/8872))
- SemEval-2014 ABSA is the canonical example of both mechanisms on one dataset: each aspect term is its own span, and a sentence can carry multiple aspect-category labels. Multi-aspect sentences are stored as multiple opinion tuples, one per aspect, for example "(pasta, Food, positive)" and "(place, Ambience, negative)" from one sentence. ([SemEval-2014 Task 4, ACL S14-2004](https://aclanthology.org/S14-2004.pdf); [ABSA datasets review, arXiv 2204.05232](https://arxiv.org/html/2204.05232))
- NER/sequence labeling (BIO tagging) routinely puts multiple independently typed spans in one sentence; this has been standard since the CoNLL shared tasks.
- Inter-annotator agreement: LongEval (EACL 2023) experimentally found that finer, clause-level judgment units reduce inter-annotator variance versus coarse whole-passage judgments (faithfulness std-dev dropped from 18.5 to 6.8, and 7.8 vs 14.0 on 100-point scaled ratings), because "summary sentences were overloaded with information," so they segmented into more atomic units. The exact magnitudes are single-source (LongEval) and are for faithfulness scoring, but the direction (smaller single-topic units are easier to agree on) generalizes. ([LongEval, ar5iv 2301.13298](https://ar5iv.labs.arxiv.org/html/2301.13298))
- Best-practice guidance: if the schema is coarse passage classification, the standard fix for a multi-topic passage is multi-label (assign all applicable), never pick one. For cleaner agreement and the ability to localize which text supports which label, segment into finer single-topic units and label those. The two are used together, not as either/or.

## 2. Chunking granularity for clause extraction and citation grounding

- Core tradeoff: smaller chunks raise the chance the exact answer text is retrievable and cut noise, but lose context and add near-miss matches; larger chunks keep context but dilute precision. Extreme sizes (very small or very large) show diminishing returns. ([Chunking strategies, Glukhov](https://www.glukhov.org/rag/retrieval/chunking-strategies-in-rag/))
- Dense X Retrieval introduces the "proposition" as the retrieval unit: a minimal, self-contained atomic fact. Indexing by proposition gave 17 to 25 percent Recall@5 relative improvement on EntityQuestions and consistent downstream QA gains. Caveat from the same paper: the gain is largest for unsupervised retrievers and rare concepts; supervised retrievers trained on passages transfer less well, so validate the embedding model on short units. ([Dense X Retrieval, arXiv 2312.06648](https://arxiv.org/abs/2312.06648); [Weaviate summary](https://weaviate.io/papers/paper10))
- Legal/insurance/financial guidance all says split on the document's own structural units (clause, sub-clause, list item, table row), not arbitrary windows: keep clauses intact, under ~512 tokens, 10 to 15 percent overlap, tables/lists as atomic units, with section/page/document-ID metadata for provenance. ([Milvus legal chunking](https://milvus.io/ai-quick-reference/what-are-best-practices-for-chunking-lengthy-legal-documents-for-vectorization)); financial filings improve with element-type chunking ([Financial Report Chunking, arXiv 2402.05131](https://arxiv.org/abs/2402.05131)).
- Critical for high-boilerplate corpora (fare rules repeat a lot): a legal-RAG study found up to 95 percent document-level retrieval mismatch and only 11 percent character-level precision at baseline, and fixed roughly half of it by prepending a short (~150 char) document-level summary to each chunk. Right clause, wrong document is the failure mode to guard against. ([Reliable Retrieval for Legal Datasets, arXiv 2510.06999](https://arxiv.org/html/2510.06999v1))
- Over-fine chunking downsides (context loss, phrasing sensitivity, index bloat) are mitigated by parent-document / small-to-big retrieval (index the single rule, return the enclosing clause), overlap, summary augmentation, and self-contained units. ([Parent Document Retriever, Full Stack Retrieval](https://community.fullstackretrieval.com/indexing/parent-document-retriever))

## 3. How comparable datasets handle multi-aspect clauses

- CUAD (510 contracts, 13k+ labels, 41 clause categories): unit is the text span; effectively multi-label at the contract level (a span can map to more than one category); framed as extractive QA (one query per category per contract). ([CUAD, arXiv 2103.06268](https://arxiv.org/abs/2103.06268); [HF card](https://huggingface.co/datasets/theatticusproject/cuad-qa))
- LexGLUE: ECtHR A/B, EUR-LEX, UNFAIR-ToS are multi-label; SCOTUS and LEDGAR are single-label. LEDGAR (one contract paragraph to one of 100 provision categories) is the closest precedent for one-clause-one-topic, and it works precisely because the unit is already a single provision. ([LexGLUE, ACL 2022](https://aclanthology.org/2022.acl-long.297.pdf))
- CLAUDETTE / UNFAIR-ToS: sentence-level, multi-label, 8 to 9 unfairness types, with a 1/2/3 severity scale. ([CLAUDETTE, arXiv 1805.01217](https://arxiv.org/pdf/1805.01217))
- PolicyQA / OPP-115: span extraction with attributes; a policy segment can hold multiple data practices, so it is multi-label at the segment level with per-span categories. ([PolicyQA, EMNLP 2020](https://aclanthology.org/2020.findings-emnlp.66/))
- Airline/insurance fare-rule clause datasets: none found. Airline resources (RuleArena, DeonticBench) are rule-reasoning benchmarks with numeric answers, not annotated clause corpora. This is a genuine gap. ([RuleArena, arXiv 2412.08972](https://arxiv.org/pdf/2412.08972))
- Cross-dataset pattern: the universal answer to "one unit, several topics" is never "force one category." It is multi-label the unit, or move to span-level annotation where overlapping/duplicate per-category spans are allowed.

## 4. Evaluation metrics

- Multi-label classification: report macro-F1 (each category counts equally, the usual goal when rare categories matter), micro-F1 (instance-weighted), and per-label precision/recall. Reporting all three is recommended because under class skew they can rank systems differently. ([Multilabel metrics, MDPI 2024](https://www.mdpi.com/2076-3417/14/21/9863))
- Span extraction: Exact Match plus token-level F1 (SQuAD style, which PolicyQA adopts). For sparse, imbalanced field extraction, CUAD deliberately uses AUPR and Precision at fixed Recall (P@80%R, P@90%R) instead, because most candidate fields are absent. ([SQuAD, arXiv 1606.05250](https://arxiv.org/pdf/1606.05250); [CUAD, NeurIPS 2021](https://datasets-benchmarks-proceedings.neurips.cc/paper_files/paper/2021/file/6ea9ab1baa0efb9e19094440c317e21b-Paper-round1.pdf))
- Citation grounding: the AIS framework (Attributable to Identified Sources) defines support as "according to source P, statement s." ALCE operationalizes it automatically with an NLI entailment model into citation recall (cited passages entail the statement) and citation precision (no redundant/irrelevant citations). ([AIS, ACL 2023.cl-4.2](https://aclanthology.org/2023.cl-4.2/); [ALCE, EMNLP 2023](https://aclanthology.org/2023.emnlp-main.398/))
- Faithfulness / groundedness frameworks (RAGAS, TruLens, ARES) all reduce to the same operation: decompose the output into atomic claims, then check each for entailment against the cited/retrieved context. Faithfulness = supported claims / total claims. ([RAGAS](https://docs.ragas.io/en/stable/concepts/metrics/available_metrics/faithfulness/))
- Hallucination rate = the unsupported fraction = 1 minus faithfulness. For field-level extraction, treat each extracted field value as a claim and check entailment against its cited source span; a field with no supporting span is fabricated. (This field-level mapping is a reasonable synthesis of AIS/ALCE; no benchmark publishes a "fabricated-field rate" under that exact name.)

## Recommendation for this project

1. Now (v0): proceed with one topic per record on the current chunks to get the first heuristic baseline number. It is a defensible starting point and unblocks labeling today. Keep the convention already written into the worksheet (refund/cancellation collapse to refund; tier rows for within-topic variation).
2. v1 (the real fix, strongly supported): re-chunk to one atomic clause per record (clause/element-aware or proposition-style splitting), then KEEP the single-topic schema. This is the key insight: finer chunking makes one-topic-per-record both natural and complete, with no multi-label rebuild. It matches LEDGAR (one provision, one label) and lowers annotator variance (LongEval). Add a short document/section summary and provenance (the schema already has citation_span) so each cited clause is traceable to the right source, which matters because fare rules repeat heavily across chunks.
3. Prefer finer chunking over multi-label. Multi-label per coarse chunk is the weaker option here: it blurs which text supports which label and raises annotator variance. Only fall back to multi-label if re-chunking proves impractical.
4. Metrics for the paper: per-field accuracy plus macro and micro F1 for policy_topic and action; for sparse penalty fields, consider CUAD-style AUPR / Precision-at-Recall; grounding accuracy via NLI entailment of each extracted field against its citation_span (ALCE-style); hallucination rate = fraction of extracted fields not entailed by the cited span. This is exactly the grounding-accuracy and hallucination-rate extension planned for extraction_eval.py.
5. Paper framing: position the absence of any public airline fare-rule clause dataset as a contribution. The schema plus gold set is novel; cite CUAD/LEDGAR/CLAUDETTE/OPP-115 as the design lineage.

## Sources

See inline links above. Primary anchors: LongEval (EACL 2023), Dense X Retrieval (arXiv 2312.06648), Reliable Retrieval for Legal Datasets (arXiv 2510.06999), CUAD (NeurIPS 2021), LexGLUE (ACL 2022), CLAUDETTE (arXiv 1805.01217), PolicyQA (EMNLP 2020), AIS (CL 2023), ALCE (EMNLP 2023), RAGAS docs, ARES (NAACL 2024), SQuAD (arXiv 1606.05250).
