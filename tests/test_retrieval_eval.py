from scripts.retrieval_eval import bm25_rank, found, random_recall, recall_stats, rrf


def test_bm25_ranks_matching_text_first():
    texts = ["baggage allowance 7 kg", "refund fee THB 500 before departure", "contact us"]
    assert bm25_rank("refund fee", texts)[0] == 1


def test_rrf_rewards_agreement():
    assert rrf([0, 1, 2], [1, 0, 2])[-1] == 2
    assert set(rrf([0, 1, 2], [1, 0, 2])[:2]) == {0, 1}


def test_found_matches_by_unit_key():
    key = {"a": "chunk1", "b": "chunk1", "c": "chunk2", "chunk1": "chunk1", "chunk2": "chunk2"}
    assert found(["a", "b", "c"], ["chunk1", "chunk2"], 1, key) == {"a", "b"}


def test_recall_success_and_capped():
    key = {r: r for r in "abcdxyz"}
    queries = [{"query_id": "q1", "relevant": ["a", "b", "c", "d"]}, {"query_id": "q2", "relevant": ["z"]}]
    runs = {"q1": {"ranking": ["a", "x", "b"]}, "q2": {"ranking": ["x", "y"]}}
    s = recall_stats(queries, runs, 2, key)
    assert s["recall"] == 1 / 5  # only "a" in the top 2
    assert s["success"] == 0.5
    assert s["capped_recall"] == (1 / 2 + 0) / 2  # q1 can find at most 2 in the top 2


def test_random_recall_is_k_over_pool():
    queries = [{"relevant": ["a"], "pool_size": 10}, {"relevant": ["b", "c"], "pool_size": 4}]
    assert abs(random_recall(queries, 2) - (0.2 + 2 * 0.5) / 3) < 1e-12
