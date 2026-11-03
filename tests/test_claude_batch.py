import json
from types import SimpleNamespace as NS

from extractors import claude_batch


class FakeBatches:
    def __init__(self, replies):
        self.replies = replies
        self.created = None

    def create(self, requests):
        self.created = requests
        return NS(id="batch_test")

    def retrieve(self, batch_id):
        return NS(processing_status="ended", request_counts=NS(processing=0, succeeded=1, errored=0))

    def results(self, batch_id):
        for req in self.created:
            text, stop = self.replies[req["custom_id"]]
            msg = NS(content=[NS(type="thinking", thinking=""), NS(type="text", text=text)],
                     stop_reason=stop, usage=NS(input_tokens=1000, output_tokens=500))
            yield NS(custom_id=req["custom_id"], result=NS(type="succeeded", message=msg))


class FakeClient:
    def __init__(self, replies):
        self.messages = NS(count_tokens=lambda **kw: NS(input_tokens=1000), batches=FakeBatches(replies))


def _args(tmp_path, **kw):
    clauses = tmp_path / "clauses.jsonl"
    rows = [{"record_id": "abc:0", "clause_text": "Changes condition: 3 hours before departure",
             "carrier": "VZ", "document_type": "fare_rule", "chunk_id": "c1"},
            {"record_id": "abc:1", "clause_text": "Refund/Cancellation: Not Available",
             "carrier": "VZ", "document_type": "fare_rule", "chunk_id": "c1"}]
    clauses.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    base = dict(clauses=str(clauses), output=str(tmp_path / "out.jsonl"), mode="llm",
                model="claude-opus-5-5", effort="low", with_chunk_context=False, chunks="",
                max_records=None, max_cost=3.5, poll_seconds=0, yes=True)
    base.update(kw)
    return NS(**base)


def test_estimate_only_without_yes_does_not_submit(tmp_path):
    client = FakeClient({})
    res = claude_batch.run(_args(tmp_path, yes=False), client_factory=lambda: client)
    assert res["status"].startswith("estimate_only")
    assert client.messages.batches.created is None


def test_budget_guard_blocks_submission(tmp_path):
    client = FakeClient({})
    res = claude_batch.run(_args(tmp_path, max_cost=0.0001), client_factory=lambda: client)
    assert res["status"] == "blocked_over_budget"
    assert client.messages.batches.created is None


def test_submit_collect_maps_ids_and_counts_refusals(tmp_path):
    replies = {
        "r00000": ('{"policy_topic": "reissue_change", "action": "conditional", "time_window": "3 hours before departure"}', "end_turn"),
        "r00001": ("", "refusal"),
    }
    client = FakeClient(replies)
    res = claude_batch.run(_args(tmp_path), client_factory=lambda: client)
    assert res["status"] == "ok" and res["succeeded"] == 1 and res["refusals"] == 1
    sent = client.messages.batches.created
    assert [r["custom_id"] for r in sent] == ["r00000", "r00001"]
    assert ":" not in sent[0]["custom_id"]
    assert sent[0]["params"]["output_config"] == {"effort": "low"}
    assert "temperature" not in sent[0]["params"]
    out = [json.loads(l) for l in open(tmp_path / "out.jsonl", encoding="utf-8")]
    assert out[0]["record_id"] == "abc:0" and out[0]["policy_topic"] == "reissue_change"
    assert out[0]["extractor"] == "llm:claude-opus-5-5"
    # state file allows resume without resubmitting
    state = json.loads((tmp_path / "out.jsonl.batch.json").read_text(encoding="utf-8"))
    assert state["batch_id"] == "batch_test" and state["result_stats"]["succeeded"] == 1


def test_second_run_after_collection_submits_only_missing_records(tmp_path):
    replies = {"r00000": ('{"policy_topic": "refund", "action": "not_allowed"}', "end_turn")}
    client = FakeClient(replies)
    first = claude_batch.run(_args(tmp_path, max_records=1), client_factory=lambda: client)
    assert first["succeeded"] == 1
    client2 = FakeClient({"r00000": ('{"policy_topic": "refund", "action": "not_allowed"}', "end_turn")})
    second = claude_batch.run(_args(tmp_path), client_factory=lambda: client2)
    sent = client2.messages.batches.created
    assert len(sent) == 1  # only the record not yet extracted
    out = [json.loads(l)["record_id"] for l in open(tmp_path / "out.jsonl", encoding="utf-8")]
    assert sorted(out) == ["abc:0", "abc:1"] and len(out) == 2
    assert any(p.name.endswith(".done") for p in tmp_path.iterdir())
