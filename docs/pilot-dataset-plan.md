# Pilot Dataset Plan (20-30 Sources)

Purpose: validate schema, parsers, chunking, metadata filters, and citation behavior before scaling to hundreds of documents.

## Pilot Mix Target

Target 20-30 total sources with a balanced spread:

- `fare_rule`: 8-10
- `airline_offer`: 3-5
- `supplier_offer`: 3-5
- `circular`: 3-4
- `waiver_notice` / `policy_bulletin`: 2-4
- `terms_page`: 1-2

## Coverage Goals

- At least 3 airlines
- At least 2 suppliers
- Mix of domestic and international contexts
- Mix of document formats (PDF + HTML, optionally email/text exports)
- At least 10 sources containing explicit refund or reissue penalties
- At least 5 sources containing no-show/go-show clauses

## Collection Rules

- Capture source URL or origin for every file
- Record publication/effective date if present
- Store original files unchanged in `data/raw/` (gitignored)
- Use consistent local filenames (see naming convention below)
- Track duplicates and superseded circulars

## Naming Convention (Recommended)

`{document_type}_{carrier_or_supplier}_{yyyy-mm-dd}_{reference}.{ext}`

Examples:
- `circular_AA_2026-03-10_CIRC-2026-03-001.pdf`
- `supplier_offer_XYZ_2026-02-18_OFFER-042.html`

## Intake Checklist Per Source

- Document opens correctly
- Text is extractable (or OCR is required)
- Document type classified
- Carrier/supplier identified
- Effective date present or marked unknown
- Offer/circular ID captured if present
- Contains at least one target topic (`refund`, `reissue_change`, `no_show`, `go_show`, `cancellation`, `waiver`)

## Output Artifacts for Pilot

- `data/pilot_source_manifest.csv` completed
- `data/raw/` contains collected files
- 5-10 manually reviewed extracted records for schema validation
- 20-30 seed Q/A pairs for early eval smoke tests

## Initial Questions To Test (Examples)

- "Is refund allowed after no-show for supplier offer OFFER-042?"
- "What is the reissue penalty for AA circular CIRC-2026-03-001?"
- "Does this fare rule allow go-show changes for business cabin?"
- "Are taxes refundable if the fare is non-refundable?"

