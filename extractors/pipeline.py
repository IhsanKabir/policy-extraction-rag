import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

from extractors.schema import (
    BeforeAfterDeparture,
    Metadata,
    PenaltyType,
    PolicyAction,
    PolicyClauseRecord,
    PolicyTopic,
)


def build_stub_clause_record(chunk_record: dict, clause_index: int = 0) -> PolicyClauseRecord:
    """Create a placeholder extraction record from one ingested chunk.

    This is intentionally conservative and marks records as needing review.
    Replace this with rule-based / LLM extraction later.
    """
    metadata = Metadata(**chunk_record["metadata"])
    chunk_id = chunk_record["chunk_id"]

    return PolicyClauseRecord(
        record_id=f"{chunk_id}:{clause_index}",
        source_id=metadata.source_id,
        document_id=metadata.document_id,
        document_type=metadata.document_type,
        offer_or_circular_id=metadata.offer_or_circular_id,
        source_url=metadata.source_url,
        source_filename=metadata.source_filename,
        carrier=metadata.carrier,
        supplier=metadata.supplier,
        market=metadata.market,
        route_scope=metadata.route_scope,
        fare_family=metadata.fare_family,
        clause_text=chunk_record["text"],
        section_title=metadata.section_title,
        chunk_id=chunk_id,
        effective_from=metadata.effective_from,
        effective_to=metadata.effective_to,
        travel_from=metadata.travel_from,
        travel_to=metadata.travel_to,
        ticketing_from=metadata.ticketing_from,
        ticketing_to=metadata.ticketing_to,
        policy_topic=PolicyTopic.other,
        action=PolicyAction.unknown,
        needs_review=True,
        notes="Stub extractor output; classify topic/action and parse structured penalties in next iteration.",
    )


def _lower_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


def _normalize_clause_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip(" \t\r\n-•")


def split_chunk_into_clauses(text: str) -> List[str]:
    """Split a chunk into candidate policy clauses.

    Conservative splitter for early-stage extraction:
    - semicolons and newlines strongly indicate separate clauses
    - sentence boundaries split only when followed by uppercase/digit
    """
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    parts = re.split(r"[;\n]+|(?<=[.!?])\s+(?=[A-Z0-9])", raw)

    clauses: List[str] = []
    for part in parts:
        clause = _normalize_clause_text(part)
        if not clause:
            continue
        # merge tiny fragments caused by aggressive sentence splitting
        if clauses and len(clause.split()) <= 2:
            clauses[-1] = f"{clauses[-1]} {clause}".strip()
            continue
        clauses.append(clause)

    normalized_full = _normalize_clause_text(text)
    if not clauses and normalized_full:
        return [normalized_full]
    return clauses


def classify_policy_topic(text: str) -> PolicyTopic:
    t = _lower_text(text)
    if "no-show" in t or "no show" in t:
        return PolicyTopic.no_show
    if "go-show" in t or "go show" in t:
        return PolicyTopic.go_show
    if re.search(r"\breissue\b|\bre-?book\b|\bchanges?\b", t):
        return PolicyTopic.reissue_change
    if "refund" in t or "refundable" in t:
        return PolicyTopic.refund
    if "cancel" in t or "cancellation" in t:
        return PolicyTopic.cancellation
    if "waiver" in t or "waive" in t:
        return PolicyTopic.waiver
    return PolicyTopic.other


def classify_action(text: str) -> PolicyAction:
    t = _lower_text(text)
    has_fee_marker = bool(re.search(r"\bfee\b", t))

    not_allowed_patterns = [
        "not permitted",
        "not allowed",
        "no refund",
        "no refunds",
        "non-refundable",
        "non refundable",
        "cannot",
        "prohibited",
        "forfeit",
        "forfeiture",
    ]
    if any(p in t for p in not_allowed_patterns):
        return PolicyAction.not_allowed

    conditional_markers = [
        "subject to",
        "provided that",
        "if ",
        "when ",
        "unless ",
        "before departure",
        "after departure",
        "with fee",
        "fee applies",
        "penalty applies",
    ]
    allowed_markers = [
        "allowed",
        "permitted",
        "can be changed",
        "can be refunded",
        "may be changed",
        "may be refunded",
        "refundable",
        "changeable",
    ]

    if any(p in t for p in allowed_markers):
        if any(p in t for p in conditional_markers) or has_fee_marker:
            return PolicyAction.conditional
        return PolicyAction.allowed

    if any(p in t for p in conditional_markers) or has_fee_marker:
        return PolicyAction.conditional

    return PolicyAction.unknown


def extract_time_signals(text: str) -> Dict[str, Optional[object]]:
    t = _lower_text(text)

    phrases = [
        "before departure",
        "after departure",
        "prior to departure",
        "within 24 hours",
        "within 24 hrs",
        "after no-show",
        "before no-show",
    ]
    time_window = next((p for p in phrases if p in t), None)

    before = ("before departure" in t) or ("prior to departure" in t)
    after = ("after departure" in t) or ("after no-show" in t)
    if before and after:
        before_after = BeforeAfterDeparture.both
    elif before:
        before_after = BeforeAfterDeparture.before_departure
    elif after:
        before_after = BeforeAfterDeparture.after_departure
    else:
        before_after = None

    return {
        "time_window": time_window,
        "before_after_departure": before_after,
    }


def _unique_nonempty(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        norm = _normalize_clause_text(item)
        if not norm:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


def _trim_condition_snippet(snippet: str) -> str:
    s = _normalize_clause_text(snippet)
    cut_markers = [
        " without fee",
        " with usd ",
        " with eur ",
        " with gbp ",
        " with aed ",
        " with sar ",
        " with inr ",
        " with pkr ",
        " with cad ",
        " with aud ",
        " with fare difference",
        " and taxes only refundable",
        " with 20% penalty",
        " with penalty",
    ]
    lower = s.lower()
    cut_positions = [lower.find(marker) for marker in cut_markers if lower.find(marker) != -1]
    if cut_positions:
        s = s[: min(cut_positions)].rstrip()
    return s


def extract_eligibility_conditions(text: str) -> List[str]:
    snippets: List[str] = []
    patterns = [
        r"(?i)\bsubject to\b([^.;\n]+)",
        r"(?i)\bprovided that\b([^.;\n]+)",
        r"(?i)\bonly if\b([^.;\n]+)",
        r"(?i)\bif\b([^.;\n]+)",
        r"(?i)\bwhen\b([^.;\n]+)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text or ""):
            snippets.append(_trim_condition_snippet(match.group(0)))
    return _unique_nonempty(snippets)


def extract_exceptions(text: str) -> List[str]:
    snippets: List[str] = []
    patterns = [
        r"(?i)\bexcept(?: for)?\b([^.;\n]+)",
        r"(?i)\bexcluding\b([^.;\n]+)",
        r"(?i)\bdoes not apply to\b([^.;\n]+)",
        r"(?i)\bnot applicable to\b([^.;\n]+)",
        r"(?i)\bunless\b([^.;\n]+)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text or ""):
            snippets.append(match.group(0))
    return _unique_nonempty(snippets)


def extract_tax_refund_rule(text: str) -> Optional[str]:
    t = _lower_text(text)
    if "tax" not in t:
        return None

    if ("only taxes" in t or "taxes only" in t or "tax only" in t or "only tax" in t) and (
        "refund" in t or "refundable" in t
    ):
        return "only taxes refundable"

    if re.search(r"tax(?:es)?[^.:\n]{0,40}(?:non[- ]refundable|not refundable)", t) or re.search(
        r"(?:non[- ]refundable|not refundable)[^.:\n]{0,40}tax(?:es)?", t
    ):
        return "taxes non-refundable"

    if re.search(r"tax(?:es)?[^.:\n]{0,40}refundable", t) or "refundable taxes" in t:
        return "taxes refundable"

    return None


def extract_penalty_terms(text: str) -> Dict[str, Optional[object]]:
    t = _lower_text(text)

    if "full forfeiture" in t or "forfeiture" in t or "forfeit" in t:
        return {"penalty_type": PenaltyType.forfeiture}

    if "no fee" in t or "without fee" in t or "free of charge" in t:
        return {"penalty_type": PenaltyType.free}

    if "fare difference only" in t:
        return {
            "penalty_type": PenaltyType.fare_difference_only,
            "fare_difference_required": True,
        }

    pct = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if pct:
        return {
            "penalty_type": PenaltyType.percentage,
            "penalty_percent": float(pct.group(1)),
        }

    # Examples: USD 50, usd50, $50, EUR 35.00
    amt = re.search(
        r"(?i)\b(USD|EUR|GBP|AED|SAR|INR|PKR|BDT|CAD|AUD)\s*([0-9]+(?:\.[0-9]+)?)\b|\$([0-9]+(?:\.[0-9]+)?)",
        text,
    )
    if amt:
        if amt.group(1) and amt.group(2):
            return {
                "penalty_type": PenaltyType.fixed_amount,
                "penalty_currency": amt.group(1).upper(),
                "penalty_amount": float(amt.group(2)),
            }
        if amt.group(3):
            return {
                "penalty_type": PenaltyType.fixed_amount,
                "penalty_currency": "USD",
                "penalty_amount": float(amt.group(3)),
            }

    if "fare difference" in t:
        return {"fare_difference_required": True}

    return {}


def _chunk_record_with_clause_text(chunk_record: dict, clause_text: str) -> dict:
    cloned = dict(chunk_record)
    cloned["text"] = clause_text
    return cloned


def build_clause_record(chunk_record: dict, clause_index: int = 0) -> PolicyClauseRecord:
    """Heuristic extractor for a single chunk -> one policy clause record.

    This is intentionally conservative and easy to debug. It improves over the
    stub by extracting topic/action and a few common penalty/time signals.
    """
    base = build_stub_clause_record(chunk_record, clause_index=clause_index)
    text = chunk_record.get("text", "") or ""

    topic = classify_policy_topic(text)
    action = classify_action(text)
    penalty = extract_penalty_terms(text)
    timing = extract_time_signals(text)
    eligibility_conditions = extract_eligibility_conditions(text)
    exceptions = extract_exceptions(text)
    tax_refund_rule = extract_tax_refund_rule(text)

    update_values = {
        "policy_topic": topic,
        "action": action,
        "notes": None,
        "needs_review": topic == PolicyTopic.other or action == PolicyAction.unknown,
        "confidence": 0.8 if topic != PolicyTopic.other and action != PolicyAction.unknown else 0.4,
        "eligibility_conditions": eligibility_conditions,
        "exceptions": exceptions,
        "tax_refund_rule": tax_refund_rule,
        **{k: v for k, v in penalty.items() if v is not None},
        **{k: v for k, v in timing.items() if v is not None},
    }
    return base.copy(update=update_values)


def extract_records_from_jsonl(input_path: Path, output_path: Path, mode: str = "heuristic") -> int:
    count = 0
    with open(input_path, "r", encoding="utf-8") as src, open(output_path, "w", encoding="utf-8") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue
            chunk_record = json.loads(line)
            clauses = split_chunk_into_clauses(chunk_record.get("text", ""))
            if not clauses:
                clauses = [chunk_record.get("text", "")]
            for clause_index, clause_text in enumerate(clauses):
                clause_chunk_record = _chunk_record_with_clause_text(chunk_record, clause_text)
                if mode == "stub":
                    record = build_stub_clause_record(clause_chunk_record, clause_index=clause_index)
                else:
                    record = build_clause_record(clause_chunk_record, clause_index=clause_index)
                dst.write(json.dumps(record.dict()) + "\n")
                count += 1
    return count


def extract_stub_records_from_jsonl(input_path: Path, output_path: Path) -> int:
    """Backward-compatible alias used by existing tests/callers."""
    return extract_records_from_jsonl(input_path, output_path, mode="stub")


def main():
    parser = argparse.ArgumentParser(description="Policy clause extractor (heuristic or stub)")
    parser.add_argument("--input", required=True, help="Input chunk JSONL from ingestion")
    parser.add_argument("--output", required=True, help="Output policy-clause JSONL")
    parser.add_argument(
        "--mode",
        choices=["heuristic", "stub"],
        default="heuristic",
        help="Extraction mode (default: heuristic)",
    )
    args = parser.parse_args()

    total = extract_records_from_jsonl(Path(args.input), Path(args.output), mode=args.mode)
    print(f"Wrote {total} extracted policy clause records to {args.output}")


if __name__ == "__main__":
    main()
