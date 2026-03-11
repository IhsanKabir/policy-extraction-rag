# Production-Grade RAG Assistant: Step-by-Step Task List

1. **Choose domain + goal**
   - Domain: Travel fare-rule and penalty policy intelligence across airlines and suppliers.
   - Scope: Extract and answer conditions for refunds, reissues, no-show/go-show, cancellations, change windows, waivers, and related penalties from fare rules, airline offers, and policy circulars with citations.
   - Define success targets: Top-3 retrieval hit rate >= 90%, grounded answer accuracy >= 85%, citation precision >= 95%, no-answer correctness >= 90%.

2. **Define schema + set up repo + stack**
   - Lock a canonical extraction schema early for `refund`, `reissue/change`, `no_show`, `go_show`, `cancellation`, `waiver`, `penalty_amount`, `penalty_type`, `time_window`, `eligibility_conditions`, `exceptions`, `effective_date`, `source_type`, `carrier`, and `supplier`.
   - Create monorepo (`api`, `ingestion`, `extractors`, `web`, `evals`, `data`, `docs`).
   - Add `README.md`, `.env.example`, and `.gitignore`.
   - Add Python environment, package manager, linting, formatting, and pre-commit hooks.
   - Choose stack (recommended): `Python + FastAPI`, `Pydantic`, `PyMuPDF`, `BeautifulSoup`, `Qdrant` (vector DB), `OpenSearch` (BM25), `Next.js` (UI), and custom eval scripts (`CSV/JSONL`).
   - Define source taxonomy: `fare_rule`, `airline_offer`, `supplier_offer`, `circular`, `waiver_notice`, `policy_bulletin`, `terms_page`.
   - Define a metadata contract for every chunk: `source_id`, `document_id`, `document_type`, `carrier`, `supplier`, `offer_or_circular_id`, `market`, `route_scope`, `cabin`, `fare_family`, `effective_from`, `effective_to`, `section_title`, `chunk_id`.
   - Define the first working milestone: ingest documents, extract metadata, chunk and index, then answer one query with correct citations.

3. **Collect and clean documents**
   - Start with a pilot set of 20–30 mixed documents (fare rules, airline offers, circulars, ADM/waiver notices, policy bulletins, terms pages) to validate schema and ingestion.
   - Scale to 200–1000 documents after the pilot pipeline is stable.
   - Remove duplicates, bad OCR, and corrupted files.

4. **Build ingestion pipeline** ✅
   - Parse documents into structured text + metadata (carrier/supplier, document type, offer/circular ID, route scope, fare family, cabin, market, effective date, section).
   - Save normalized outputs to storage.
   - (Implemented `ingestion.parser` for PDF/HTML + metadata and `cli` utility with tests.)

5. **Implement chunking** ✅
   - Start with semantic chunking (fixed token window + overlap).
   - Track chunk IDs and parent document references.
   - (Added `ingestion.chunker` and integrated into CLI with unit tests.)

6. **Create indexes**
   - Vector index (embeddings).
   - Keyword index (BM25/full-text).
   - Store metadata filters (carrier/supplier, document type, offer/circular ID, date, market, route, fare family).

7. **Implement hybrid retrieval**
   - Combine vector + keyword results.
   - Add re-ranking (cross-encoder or LLM reranker).

8. **Build answer generation with citations**
   - Prompt model to answer only from retrieved chunks.
   - Return inline citations linked to source chunk/document.

9. **Create backend API**
   - Endpoints: `/ask`, `/sources`, `/health`.
   - Add auth, rate limits, and request logging.

10. **Build minimal frontend**
   - Chat UI with answer + citations panel.
   - Show latency, token usage, and retrieval sources.

11. **Create evaluation dataset**
   - Write 100+ policy questions with expected answers/sources across refund, reissue, no-show/go-show, cancellation, waiver, and offer/circular-specific scenarios.
   - Include edge cases and no-answer-in-docs queries.

12. **Automate eval pipeline**
   - Metrics: retrieval recall@k, answer correctness, faithfulness, citation precision, and policy-field extraction F1.
   - Add regression checks on every model/prompt/index change.

13. **Add observability**
   - Track latency by stage (retrieve/rerank/generate).
   - Track cost, error rate, and top failure categories.

14. **Add guardrails**
   - Refusal behavior for out-of-scope/sensitive prompts.
   - Hallucination checks and fallback responses when policy is missing or conflicting.

15. **Deploy**
   - Deploy API + web + vector database.
   - Add environment configs for dev/staging/prod.

16. **CI/CD + quality gates**
   - Run unit tests + eval suite in CI.
   - Block deploy if eval scores drop below threshold.

17. **Portfolio packaging**
   - README with architecture diagram and demo GIF/video.
   - Include before-vs-after eval results and tradeoff decisions.
