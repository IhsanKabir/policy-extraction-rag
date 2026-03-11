# Travel Policy Extraction RAG

Production-grade RAG system for travel fare-rule and penalty policy intelligence across airlines and suppliers.

Primary use cases:
- Refund eligibility and penalties
- Reissue/change conditions
- No-show / go-show clauses
- Cancellation windows
- Waivers and exceptions
- Offer/circular-specific conditions

## Initial Scope

- Ingest fare rules, airline offers, supplier offers, circulars, waivers, and policy bulletins
- Extract structured policy fields and metadata
- Index content for hybrid retrieval (vector + BM25)
- Answer questions with source citations

## Repo Structure

- `api/` FastAPI service for ask/search endpoints
- `ingestion/` parsers and normalization pipeline
- `extractors/` structured policy extraction logic
- `web/` minimal UI (chat + citations)
- `evals/` datasets and evaluation scripts
- `data/` manifests and local data folders
- `docs/` schema, plans, and design notes

## First Milestone

1. Ingest 20-30 pilot documents
2. Extract metadata into a canonical schema
3. Chunk and index documents
4. Ask one query and return a grounded answer with correct citations

## Labeling Worksheet (How To Use)

Use `docs/real-pilot-labeling-session-worksheet.md` during real-pilot manual labeling sessions.

Recommended flow:
1. Ingest real pilot docs into `data/chunks.jsonl`
2. Run heuristic extraction into `data/policy_clauses.jsonl`
3. Generate the labeling queue: `python -m evals.real_pilot_labeling queue`
4. Open the labeling UI at `http://127.0.0.1:8000/labeling`
5. Label 30-50 clauses using the worksheet checklist
6. Compare heuristic vs manual gold: `python -m evals.real_pilot_labeling compare`

The worksheet includes a 30-clause target plan, source coverage tracker, skip log, and mismatch review template.

## Next Priorities

- Add real pilot documents to `data/raw/` and complete `data/pilot_source_manifest.csv`
- Generate `data/chunks.jsonl` and `data/policy_clauses.jsonl` from real sources
- Create a real-pilot manual labeling queue (`python -m evals.real_pilot_labeling queue`)
- Label 30-50 real clauses in the labeling UI and run `python -m evals.real_pilot_labeling compare`
- Improve heuristics from mismatch analysis, then add an LLM-assisted extraction pass for hard cases
