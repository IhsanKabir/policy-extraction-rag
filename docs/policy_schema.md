# Canonical Policy Schema (v0.1)

This schema standardizes extracted policy information across fare rules, airline offers, supplier offers, circulars, waivers, and policy bulletins.

Use this schema before building extractors. If field names change later, ingestion, retrieval filters, evals, and UI all break together.

## Extraction Unit

One extracted record represents one policy clause or condition statement tied to a source document section.

Examples:
- "Refund allowed before departure with USD 50 fee"
- "No-show: ticket non-refundable"
- "Offer valid for reissue until 31 Mar 2026"

## Record Schema

### Identity and Source

- `record_id` (string, required): unique ID for extracted clause
- `source_id` (string, required): stable source identifier in your system
- `document_id` (string, required): source document identifier
- `document_type` (enum, required): `fare_rule`, `airline_offer`, `supplier_offer`, `circular`, `waiver_notice`, `policy_bulletin`, `terms_page`, `other`
- `offer_or_circular_id` (string, optional): airline/supplier published reference number
- `source_url` (string, optional): original URL if available
- `source_filename` (string, optional): local file name for PDFs/docs

### Issuer and Commercial Context

- `carrier` (string, optional): airline code/name (prefer IATA 2-letter code when known)
- `supplier` (string, optional): OTA/consolidator/GDS-linked supplier name
- `market` (string, optional): POS market or sales country/region
- `route_scope` (string, optional): route or geography applicability (e.g., "US-CA", "Europe", "Domestic")
- `cabin` (enum, optional): `economy`, `premium_economy`, `business`, `first`, `mixed`, `unknown`
- `fare_family` (string, optional): fare brand/family if present
- `pax_type` (enum, optional): `adt`, `chd`, `inf`, `all`, `unknown`

### Policy Classification

- `policy_topic` (enum, required): `refund`, `reissue_change`, `no_show`, `go_show`, `cancellation`, `waiver`, `other`
- `action` (enum, required): `allowed`, `not_allowed`, `conditional`, `unknown`
- `applies_to` (enum, optional): `ticket`, `fare`, `tax`, `segment`, `coupon`, `itinerary`, `passenger`, `unknown`

### Penalty and Financial Terms

- `penalty_type` (enum, optional): `fixed_amount`, `percentage`, `forfeiture`, `free`, `fare_difference_only`, `tax_only`, `mixed`, `unknown`
- `penalty_amount` (number, optional): numeric penalty amount when explicit
- `penalty_currency` (string, optional): ISO currency code (e.g., `USD`, `EUR`)
- `penalty_percent` (number, optional): percent penalty when explicit
- `fare_difference_required` (boolean, optional): whether fare difference applies
- `tax_refund_rule` (string, optional): short normalized text for tax handling

### Timing and Eligibility

- `time_window` (string, optional): normalized timing text (e.g., "before departure", "within 24h", "after no-show")
- `before_after_departure` (enum, optional): `before_departure`, `after_departure`, `both`, `not_applicable`, `unknown`
- `effective_from` (date, optional): policy effective start date
- `effective_to` (date, optional): policy effective end date
- `travel_from` (date, optional): travel validity start date if present
- `travel_to` (date, optional): travel validity end date if present
- `ticketing_from` (date, optional): ticketing validity start date if present
- `ticketing_to` (date, optional): ticketing validity end date if present
- `eligibility_conditions` (array[string], optional): normalized condition list
- `exceptions` (array[string], optional): exclusions/exceptions list

### Clause Quality and Traceability

- `clause_text` (string, required): extracted clause text (normalized but faithful)
- `section_title` (string, optional): section header in source
- `chunk_id` (string, required): chunk identifier used for retrieval
- `citation_span` (string, optional): line/paragraph/span reference if available
- `confidence` (number, optional): extraction confidence score (0-1)
- `needs_review` (boolean, required): true when rule is ambiguous/conflicting
- `notes` (string, optional): parser/extractor notes

## Normalization Rules

- Keep original meaning; do not rewrite commercial conditions beyond normalization.
- Preserve ambiguity. Use `action=conditional` or `unknown` when needed.
- Do not guess missing amounts, dates, or currencies.
- If multiple conditions exist in one paragraph, split into multiple records.
- If a clause conflicts with another clause in the same source, mark `needs_review=true`.

## Required Metadata on Every Retrieval Chunk

These fields must exist for filtering and citation display:

- `source_id`
- `document_id`
- `document_type`
- `carrier`
- `supplier`
- `offer_or_circular_id`
- `market`
- `route_scope`
- `cabin`
- `fare_family`
- `effective_from`
- `effective_to`
- `section_title`
- `chunk_id`

## Example Records (Abbreviated)

```json
{
  "record_id": "rec_001",
  "source_id": "src_aa_circ_2026_03_001",
  "document_id": "doc_001",
  "document_type": "circular",
  "offer_or_circular_id": "CIRC-2026-03-001",
  "carrier": "AA",
  "policy_topic": "refund",
  "action": "conditional",
  "penalty_type": "fixed_amount",
  "penalty_amount": 50,
  "penalty_currency": "USD",
  "before_after_departure": "before_departure",
  "time_window": "before departure",
  "clause_text": "Refunds permitted before departure with USD 50 fee.",
  "chunk_id": "chunk_001",
  "needs_review": false
}
```

```json
{
  "record_id": "rec_002",
  "source_id": "src_supplier_offer_042",
  "document_id": "doc_002",
  "document_type": "supplier_offer",
  "offer_or_circular_id": "OFFER-042",
  "supplier": "ExampleConsolidator",
  "policy_topic": "no_show",
  "action": "not_allowed",
  "penalty_type": "forfeiture",
  "clause_text": "No-show results in full forfeiture of fare value.",
  "chunk_id": "chunk_114",
  "needs_review": false
}
```

## Versioning

- Start with `v0.1`
- Track schema changes in this file
- Add migration notes before changing field names

