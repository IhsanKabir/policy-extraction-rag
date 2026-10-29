"""Claude backend for the LLM extraction methods, via the Message Batches API.

Uses exactly the same prompts (llm_pipeline.build_prompt) and the same
post-processing (llm_pipeline.build_output_record) as the Ollama runs, so the
only difference between methods is the model. Batches run asynchronously at
50% of standard prices.

Cost guard: before submitting, input tokens are counted exactly with the free
token-counting endpoint, an upper-bound cost is estimated with a conservative
output allowance, and the run refuses to submit if that estimate exceeds
--max-cost. Nothing is sent for generation without --yes.

Resumable: the batch id and the record-id mapping are saved next to the
output file (<output>.batch.json). Re-running the same command polls and
collects that batch instead of submitting a new one.

Examples:
  # estimate only (free)
  python -m extractors.claude_batch --mode llm --clauses data/v1b_gold_clauses.jsonl \
      --output data/v1b_llm_opus55.jsonl --max-records 5
  # submit, wait, collect
  python -m extractors.claude_batch ... --yes
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from extractors.llm_pipeline import _load_jsonl, build_output_record, build_prompt

DEFAULT_MODEL = "claude-opus-5-5"
# First-party prices per million tokens (input, output); batches are billed at 50%.
PRICES = {"claude-opus-5-5": (4.00, 20.00), "claude-sonnet-5-5": (2.00, 10.00), "claude-haiku-5-5": (0.10, 0.50)}
BATCH_DISCOUNT = 0.5
# Conservative per-request output allowance (JSON answer + reasoning at low effort)
# used only for the pre-submit estimate; actual usage is reported after the run.
EST_OUTPUT_TOKENS = 2000
MAX_TOKENS = 8000


def _custom_id(i: int) -> str:
    # record_ids contain ":" which batch custom_ids do not allow
    return f"r{i:05d}"


def build_params(row: dict, mode: str, model: str, effort: str,
                 context: Optional[str]) -> dict:
    clause_text = row.get("clause_text") or row.get("chunk_text") or ""
    messages = build_prompt(clause_text, row.get("carrier"), str(row.get("document_type")),
                            mode == "llm_cited", chunk_context=context)
    system = messages[0]["content"]
    user = messages[1]["content"]
    return {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "system": system,
        "messages": [{"role": "user", "content": user}],
        "output_config": {"effort": effort},
    }


def reply_text(message) -> str:
    return "".join(b.text for b in message.content if getattr(b, "type", None) == "text")


def estimate_cost(client, params_list: List[dict], model: str) -> dict:
    in_price, out_price = PRICES.get(model, PRICES[DEFAULT_MODEL])
    input_tokens = 0
    for p in params_list:
        counted = client.messages.count_tokens(model=p["model"], system=p["system"], messages=p["messages"])
        input_tokens += counted.input_tokens
    out_tokens = EST_OUTPUT_TOKENS * len(params_list)
    usd = (input_tokens * in_price + out_tokens * out_price) / 1e6 * BATCH_DISCOUNT
    return {"requests": len(params_list), "input_tokens": input_tokens,
            "assumed_output_tokens": out_tokens, "estimated_usd_upper": round(usd, 2)}


def actual_cost(usage_in: int, usage_out: int, model: str) -> float:
    in_price, out_price = PRICES.get(model, PRICES[DEFAULT_MODEL])
    return round((usage_in * in_price + usage_out * out_price) / 1e6 * BATCH_DISCOUNT, 4)


def run(args, client_factory=None) -> dict:
    import anthropic
    from dotenv import load_dotenv

    load_dotenv()  # ANTHROPIC_API_KEY from the repo's .env (gitignored)
    client = client_factory() if client_factory else anthropic.Anthropic()
    out_path = Path(args.output)
    state_path = out_path.with_suffix(out_path.suffix + ".batch.json")
    rows = _load_jsonl(Path(args.clauses))
    chunk_map: Optional[Dict[str, str]] = None
    if args.with_chunk_context:
        chunk_map = {str(c.get("chunk_id")): c.get("text") or "" for c in _load_jsonl(Path(args.chunks))}

    state = None
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("collected_at"):
            # finished batch: archive its state and submit whatever is still missing
            archived = state_path.with_name(f"{state_path.name}.{state['batch_id']}.done")
            state_path.rename(archived)
            print(f"Previous batch {state['batch_id']} already collected; archived state to {archived.name}",
                  file=sys.stderr)
            state = None
        else:
            print(f"Resuming batch {state['batch_id']} from {state_path}", file=sys.stderr)
    if state is None:
        done = {r.get("record_id") for r in _load_jsonl(out_path)}
        todo = [r for r in rows if r.get("record_id") not in done]
        if args.max_records is not None:
            todo = todo[: args.max_records]
        if not todo:
            return {"status": "nothing_to_do", "output": str(out_path)}
        id_map, contexts, params_list = {}, {}, []
        for i, row in enumerate(todo):
            ctx = None
            if chunk_map is not None:
                ctx = chunk_map.get(str(row.get("chunk_id")))
                if ctx == row.get("clause_text"):
                    ctx = None
            cid = _custom_id(i)
            id_map[cid] = row["record_id"]
            contexts[cid] = ctx
            params_list.append(build_params(row, args.mode, args.model, args.effort, ctx))
        est = estimate_cost(client, params_list, args.model)
        print(json.dumps({"estimate": est, "max_cost_usd": args.max_cost}, indent=2), file=sys.stderr)
        if est["estimated_usd_upper"] > args.max_cost:
            return {"status": "blocked_over_budget", **est}
        if not args.yes:
            return {"status": "estimate_only (add --yes to submit)", **est}
        batch = client.messages.batches.create(
            requests=[{"custom_id": cid, "params": p} for cid, p in zip(id_map, params_list)])
        state = {"batch_id": batch.id, "model": args.model, "mode": args.mode, "effort": args.effort,
                 "with_chunk_context": bool(chunk_map), "id_map": id_map, "contexts": contexts,
                 "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "estimate": est}
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(f"Submitted batch {batch.id} ({len(id_map)} requests)", file=sys.stderr)

    while True:
        batch = client.messages.batches.retrieve(state["batch_id"])
        if batch.processing_status == "ended":
            break
        c = batch.request_counts
        print(f"waiting: processing={c.processing} succeeded={c.succeeded} errored={c.errored}", file=sys.stderr)
        time.sleep(args.poll_seconds)

    rows_by_id = {r["record_id"]: r for r in rows}
    already_written = {r.get("record_id") for r in _load_jsonl(out_path)}
    stats = {"succeeded": 0, "parse_failures": 0, "refusals": 0, "truncated": 0, "errored": 0,
             "input_tokens": 0, "output_tokens": 0}
    with open(out_path, "a", encoding="utf-8") as out:
        for res in client.messages.batches.results(state["batch_id"]):
            rid = state["id_map"].get(res.custom_id)
            row = rows_by_id.get(rid)
            if row is None or rid in already_written:
                continue
            if res.result.type != "succeeded":
                stats["errored"] += 1
                print(f"{rid}: {res.result.type}", file=sys.stderr)
                continue
            msg = res.result.message
            stats["input_tokens"] += msg.usage.input_tokens
            stats["output_tokens"] += msg.usage.output_tokens
            if msg.stop_reason == "refusal":
                stats["refusals"] += 1
                continue
            if msg.stop_reason == "max_tokens":
                stats["truncated"] += 1
            clause_text = row.get("clause_text") or row.get("chunk_text") or ""
            record = build_output_record(row, clause_text, reply_text(msg), state["mode"], state["model"],
                                         state["contexts"].get(res.custom_id))
            if record is None:
                stats["parse_failures"] += 1
                continue
            record["batch_id"] = state["batch_id"]
            record["usage"] = {"input_tokens": msg.usage.input_tokens, "output_tokens": msg.usage.output_tokens}
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            stats["succeeded"] += 1
    stats["actual_usd"] = actual_cost(stats["input_tokens"], stats["output_tokens"], state["model"])
    state["collected_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    state["result_stats"] = stats
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return {"status": "ok", "batch_id": state["batch_id"], "output": str(out_path), **stats}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--clauses", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--mode", choices=["llm", "llm_cited"], default="llm")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--effort", choices=["low", "medium", "high", "xhigh", "max"], default="low")
    ap.add_argument("--with-chunk-context", action="store_true")
    ap.add_argument("--chunks", default="data/chunks.jsonl")
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--max-cost", type=float, default=3.5, help="refuse to submit above this estimated USD")
    ap.add_argument("--poll-seconds", type=int, default=60)
    ap.add_argument("--yes", action="store_true", help="actually submit the batch (otherwise estimate only)")
    args = ap.parse_args()
    print(json.dumps(run(args), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
