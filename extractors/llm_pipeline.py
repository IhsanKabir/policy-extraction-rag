"""LLM-based policy clause extraction (paper methods #2 and #3).

Extracts the same structured fields as the heuristic pipeline, over the SAME
clause units (record_id/clause_text from an existing policy_clauses JSONL), so
results are directly comparable in the existing record_id-keyed eval.

Two modes:
  llm        Ungrounded extraction: the model emits the schema fields directly.
  llm_cited  Citation-constrained extraction: the model must also return, for
             every non-null field, a verbatim quote from the clause text. Quotes
             are verified locally (normalized containment); any field whose
             quote does not appear in the source text is NULLED and counted as
             ungrounded. What survives is grounded by construction; the raw
             ungrounded count is the model's hallucination signal.

Backend: local Ollama (http://127.0.0.1:11434), temperature 0, JSON output.
No new dependencies: stdlib urllib only. Runs are resumable: existing output
records are skipped, so a CPU run can be interrupted and re-launched.

Typical paper runs (restricted to gold-labeled records to bound CPU time):

  python -m extractors.llm_pipeline --model qwen2.5:7b --mode llm \
      --only-gold --output data/policy_clauses_llm_qwen7b.jsonl
  python -m extractors.llm_pipeline --model qwen2.5:7b --mode llm_cited \
      --only-gold --output data/policy_clauses_llmcited_qwen7b.jsonl

Then score with the existing comparer:

  python -m evals.real_pilot_labeling compare --pred data/policy_clauses_llm_qwen7b.jsonl
"""
import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Dict, List, Optional

from extractors.schema import (
    BeforeAfterDeparture,
    PenaltyType,
    PolicyAction,
    PolicyTopic,
)

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"

# The 12 scored fields (mirrors evals.extraction_eval.DEFAULT_FIELDS).
ENUM_FIELDS = {
    "policy_topic": [t.value for t in PolicyTopic],
    "action": [a.value for a in PolicyAction],
    "penalty_type": [p.value for p in PenaltyType],
    "before_after_departure": [b.value for b in BeforeAfterDeparture],
}
NUMBER_FIELDS = ("penalty_amount", "penalty_percent")
BOOL_FIELDS = ("fare_difference_required",)
TEXT_FIELDS = ("penalty_currency", "time_window", "tax_refund_rule")
LIST_FIELDS = ("eligibility_conditions", "exceptions")
ALL_FIELDS = (
    list(ENUM_FIELDS) + list(NUMBER_FIELDS) + list(BOOL_FIELDS)
    + list(TEXT_FIELDS) + list(LIST_FIELDS)
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_prompt(clause_text: str, carrier: Optional[str], document_type: str,
                 cited: bool, chunk_context: Optional[str] = None) -> List[dict]:
    """Build the chat messages for one clause. Kept deterministic (no dates,
    no randomness) so runs are reproducible at temperature 0.

    chunk_context (ablation): the parent chunk's full text, shown to the model
    for disambiguation only - extraction and citations must still come from
    the clause text itself."""
    field_lines = []
    for name, values in ENUM_FIELDS.items():
        field_lines.append(f'- "{name}": one of {values}, or null')
    field_lines += [
        '- "penalty_amount": number (the fee amount only, no currency), or null',
        '- "penalty_percent": number 0-100, or null',
        '- "penalty_currency": ISO/display currency code exactly as written (e.g. "USD", "THB"), or null',
        '- "fare_difference_required": true/false, or null',
        '- "time_window": the deadline/window phrase copied exactly from the text (e.g. "3 hours before departure"), or null',
        '- "tax_refund_rule": the tax-refund statement copied exactly from the text, or null',
        '- "eligibility_conditions": list of short condition strings copied from the text, or []',
        '- "exceptions": list of short exception strings copied from the text, or []',
    ]
    schema_block = "\n".join(field_lines)

    system = (
        "You extract structured airline fare-rule fields from ONE policy clause. "
        "Rules:\n"
        "1. Use ONLY what the clause text states. If the clause does not state a "
        "value, return null (or [] for lists). NEVER guess or invent.\n"
        "2. The clause is ONE rule: pick the single policy_topic it is about. "
        "If refund and cancellation are written together as one rule, use \"refund\".\n"
        "3. Copy textual values (time_window, currency, conditions) verbatim from the clause.\n"
        "4. Return STRICT JSON only, with exactly these fields:\n"
        f"{schema_block}"
    )
    if cited:
        system += (
            '\n5. Also return "citations": an object mapping EVERY non-null field name '
            "to the exact verbatim quote from the clause text that states that value. "
            "A field without a supporting quote must be null."
        )
    user = f"Document type: {document_type}. Carrier: {carrier or 'unknown'}.\n"
    if chunk_context:
        user += (
            "SURROUNDING CONTEXT (for understanding only - do NOT extract "
            f"values that appear only here):\n{chunk_context}\n\n"
        )
    user += f"CLAUSE TEXT (extract from THIS text only):\n{clause_text}"
    return [{"role": "system", "content": system},
            {"role": "user", "content": user}]


def ollama_chat(messages: List[dict], model: str, timeout: float = 600.0) -> str:
    """Single non-streaming Ollama chat call returning the message content.
    Raises urllib.error.URLError on connection problems (caller handles)."""
    payload = {
        "model": model,
        "messages": messages,
        "format": "json",
        "stream": False,
        "options": {"temperature": 0, "num_ctx": 4096},
    }
    req = urllib.request.Request(
        OLLAMA_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return (body.get("message") or {}).get("content") or ""


def parse_model_json(text: str) -> Optional[dict]:
    """Parse the model's JSON reply; tolerate code fences / stray prose."""
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        parts = t.split("```")
        t = parts[1] if len(parts) >= 2 else t
        if t.startswith("json"):
            t = t[4:]
        t = t.strip()
    try:
        out = json.loads(t)
        return out if isinstance(out, dict) else None
    except json.JSONDecodeError:
        start, end = t.find("{"), t.rfind("}")
        if 0 <= start < end:
            try:
                out = json.loads(t[start:end + 1])
                return out if isinstance(out, dict) else None
            except json.JSONDecodeError:
                return None
    return None


def coerce_fields(raw: dict) -> dict:
    """Validate/coerce the model's raw dict into schema-safe field values.
    Anything invalid becomes null (never propagate junk into the eval)."""
    out: Dict[str, object] = {}
    for name, allowed in ENUM_FIELDS.items():
        val = raw.get(name)
        out[name] = val if isinstance(val, str) and val in allowed else None
    for name in NUMBER_FIELDS:
        val = raw.get(name)
        if isinstance(val, bool):  # bool is an int subclass; reject explicitly
            out[name] = None
        elif isinstance(val, (int, float)):
            out[name] = float(val)
        elif isinstance(val, str):
            # Strip thousands separators BEFORE matching so "1,500.50" keeps
            # its decimal part. (Assumes "," is always a thousands separator,
            # never a European decimal comma - fine for this corpus.)
            m = re.search(r"-?\d+(?:\.\d+)?", val.replace(",", ""))
            out[name] = float(m.group(0)) if m else None
        else:
            out[name] = None
    for name in BOOL_FIELDS:
        val = raw.get(name)
        out[name] = val if isinstance(val, bool) else None
    for name in TEXT_FIELDS:
        val = raw.get(name)
        out[name] = val.strip() if isinstance(val, str) and val.strip() else None
    for name in LIST_FIELDS:
        val = raw.get(name)
        if isinstance(val, list):
            out[name] = [str(v).strip() for v in val if str(v).strip()]
        else:
            out[name] = []
    return out


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()


def _strip_digit_commas(s: str) -> str:
    """Remove thousands separators between digits so 1500.0 grounds against
    "THB 1,500" (conservative-direction fix from review follow-up)."""
    return re.sub(r"(?<=\d),(?=\d)", "", s)


def _value_in_text(value, text_norm: str) -> bool:
    """Does the asserted value literally occur in (normalized) text?
    Floats match integer-collapsed and %g renderings (1500.0 -> "1500"),
    with digit-group commas in the text ignored."""
    if isinstance(value, float):
        haystack = _strip_digit_commas(text_norm)
        candidates = {f"{value}", f"{value:g}"}
        if value == int(value):
            candidates.add(str(int(value)))
        return any(c in haystack for c in candidates)
    if isinstance(value, str):
        return _norm(value) in text_norm
    return False


def verify_citations(fields: dict, citations: object, clause_text: str) -> dict:
    """Citation-constrained grounding check.

    A non-null field survives only when BOTH hold:
      1. the model's quote for it occurs verbatim in the clause text, AND
      2. for value-bearing fields (numbers, currency, time_window,
         tax_refund_rule) the asserted value itself occurs in that quote —
         a real-but-irrelevant quote must not launder an invented value.
    Enum/bool fields are categorical (their value is a label, not clause
    text), so for them only check 1 applies.

    List fields are verified PER ITEM: each item is required to occur
    verbatim in the clause text (the prompt instructs verbatim copying);
    ungrounded items are dropped individually rather than nulling the list.

    Failing fields are nulled. Returns a grounding report (kept on the
    record); every list item counts as one assertion.
    """
    normalized_text = _norm(clause_text)
    cit_map = citations if isinstance(citations, dict) else {}
    report = {"asserted": 0, "grounded": 0, "ungrounded_nulled": 0, "fields": {}}

    for name in ALL_FIELDS:
        val = fields.get(name)
        if name in LIST_FIELDS:
            items = val if isinstance(val, list) else []
            if not items:
                continue
            kept, item_reports = [], []
            for item in items:
                report["asserted"] += 1
                ok = _value_in_text(str(item), normalized_text)
                item_reports.append({"item": item, "grounded": ok})
                if ok:
                    report["grounded"] += 1
                    kept.append(item)
                else:
                    report["ungrounded_nulled"] += 1
            fields[name] = kept
            report["fields"][name] = {"items": item_reports}
            continue

        if val is None:
            continue
        report["asserted"] += 1
        quote = cit_map.get(name)
        quote_ok = (isinstance(quote, str) and quote.strip()
                    and _norm(quote) in normalized_text)
        value_ok = True
        if quote_ok and (name in NUMBER_FIELDS or name in TEXT_FIELDS):
            value_ok = _value_in_text(val, _norm(quote))
        ok = quote_ok and value_ok
        report["fields"][name] = {
            "quote": quote if isinstance(quote, str) else None,
            "grounded": bool(ok),
        }
        if ok:
            report["grounded"] += 1
        else:
            report["ungrounded_nulled"] += 1
            fields[name] = None
    return report


def _load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                # Tolerate a truncated final line from an interrupted run -
                # required for the resumability the module promises.
                print(f"warning: skipping malformed JSONL line in {path}",
                      file=sys.stderr)
    return rows


def gold_record_ids(db_path: Path) -> set:
    from labeling_store import sqlite_load_manual_label_map, sqlite_manual_label_count
    if not db_path.exists() or sqlite_manual_label_count(db_path) == 0:
        return set()
    return set(sqlite_load_manual_label_map(db_path).keys())


def extract_records(
    clause_rows: List[dict],
    mode: str,
    model: str,
    output_path: Path,
    chat: Optional[Callable[[List[dict], str], str]] = None,
    max_records: Optional[int] = None,
    progress_every: int = 5,
    chunk_map: Optional[Dict[str, str]] = None,
) -> dict:
    """Run LLM extraction over clause rows, appending JSONL to output_path.
    Resumable: rows whose record_id already exists in the output are skipped.
    `chat` is injectable for tests; defaults to the Ollama backend."""
    chat = chat or (lambda messages, mdl: ollama_chat(messages, mdl))
    cited = mode == "llm_cited"
    # None must never enter the dedup set: a missing record_id on one output
    # row would otherwise silently mark every future id-less row "done".
    done_ids = {rid for r in _load_jsonl(output_path)
                if (rid := r.get("record_id")) is not None}
    missing_id = sum(1 for r in clause_rows if not r.get("record_id"))
    if missing_id:
        print(f"warning: {missing_id} clause rows lack record_id and will be "
              "re-extracted on every run", file=sys.stderr)
    todo = [r for r in clause_rows if r.get("record_id") not in done_ids]
    if max_records is not None:
        todo = todo[:max_records]

    stats = {"requested": len(todo), "extracted": 0, "parse_failures": 0,
             "call_failures": 0, "skipped_existing": len(done_ids)}
    started = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as out:
        for i, row in enumerate(todo):
            clause_text = row.get("clause_text") or row.get("chunk_text") or ""
            if not clause_text.strip():
                stats["parse_failures"] += 1
                continue
            context = None
            if chunk_map is not None:
                context = chunk_map.get(str(row.get("chunk_id")))
                if context == clause_text:
                    context = None  # chunk-granularity: context adds nothing
            messages = build_prompt(clause_text, row.get("carrier"),
                                    str(row.get("document_type")), cited,
                                    chunk_context=context)
            try:
                reply = chat(messages, model)
            except (urllib.error.URLError, OSError, TimeoutError,
                    json.JSONDecodeError, UnicodeDecodeError) as exc:
                stats["call_failures"] += 1
                print(f"[{i+1}/{len(todo)}] call failed for "
                      f"{row.get('record_id')}: {exc}", file=sys.stderr)
                continue
            raw = parse_model_json(reply)
            if raw is None:
                stats["parse_failures"] += 1
                print(f"[{i+1}/{len(todo)}] unparseable JSON for "
                      f"{row.get('record_id')}", file=sys.stderr)
                continue
            fields = coerce_fields(raw)
            record = {
                # identity copied verbatim from the source clause row
                "record_id": row.get("record_id"),
                "source_id": row.get("source_id"),
                "document_id": row.get("document_id"),
                "document_type": row.get("document_type"),
                "carrier": row.get("carrier"),
                "supplier": row.get("supplier"),
                "chunk_id": row.get("chunk_id"),
                "clause_text": clause_text,
                # extraction provenance
                "extractor": f"{mode}:{model}",
                "extraction_mode": mode,
                "with_chunk_context": bool(context),
                # pre-coercion model output, kept for coercion-loss auditing
                "model_raw_fields": raw,
            }
            if cited:
                record["grounding"] = verify_citations(
                    fields, raw.get("citations"), clause_text)
            record.update(fields)
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()
            stats["extracted"] += 1
            if (i + 1) % progress_every == 0:
                elapsed = time.time() - started
                rate = elapsed / (i + 1)
                print(f"[{i+1}/{len(todo)}] ok "
                      f"({rate:.1f}s/record, ~{rate*(len(todo)-i-1)/60:.0f} min left)",
                      file=sys.stderr)
    stats["seconds"] = round(time.time() - started, 1)
    return stats


def main():
    parser = argparse.ArgumentParser(description="LLM policy-clause extraction (Ollama)")
    parser.add_argument("--clauses", default=str(_repo_root() / "data" / "policy_clauses.jsonl"),
                        help="Input clause units (heuristic output) to re-extract")
    parser.add_argument("--output", required=True, help="Output predictions JSONL (appended; resumable)")
    parser.add_argument("--model", default="qwen2.5:7b")
    parser.add_argument("--mode", choices=["llm", "llm_cited"], default="llm")
    parser.add_argument("--only-gold", action="store_true",
                        help="Restrict to record_ids present in the gold labeling DB")
    parser.add_argument("--gold-db", default=str(_repo_root() / "data" / "labeling.db"))
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--with-chunk-context", action="store_true",
                        help="Ablation: include the parent chunk text in the "
                             "prompt for disambiguation")
    parser.add_argument("--chunks", default=str(_repo_root() / "data" / "chunks.jsonl"),
                        help="Chunks file for --with-chunk-context lookup")
    args = parser.parse_args()

    rows = _load_jsonl(Path(args.clauses))
    chunk_map = None
    if args.with_chunk_context:
        chunk_map = {str(c.get("chunk_id")): c.get("text") or ""
                     for c in _load_jsonl(Path(args.chunks))}
    if args.only_gold:
        ids = gold_record_ids(Path(args.gold_db))
        if not ids:
            print(json.dumps({"status": "blocked", "reason": "gold_db_empty",
                              "gold_db": args.gold_db}))
            return
        rows = [r for r in rows if r.get("record_id") in ids]

    stats = extract_records(rows, mode=args.mode, model=args.model,
                            output_path=Path(args.output),
                            max_records=args.max_records,
                            chunk_map=chunk_map)
    stats.update({"status": "ok", "mode": args.mode, "model": args.model,
                  "output": args.output,
                  "with_chunk_context": bool(chunk_map)})
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
