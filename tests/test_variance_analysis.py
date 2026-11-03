from scripts.variance_analysis import run_paths, spread


def test_run_paths_cover_original_and_repeats():
    assert [p.name for p in run_paths("opus55", cited=True)] == [
        "v1b_llm_cited_opus55.jsonl", "v1b_llm_cited_opus55_rep2.jsonl", "v1b_llm_cited_opus55_rep3.jsonl"]
    assert run_paths("sonnet55", cited=False)[0].name == "v1b_llm_sonnet55.jsonl"


def test_spread_summarises_runs():
    s = spread([0.6, 0.7, 0.8])
    assert s["min"] == 0.6 and s["max"] == 0.8
    assert abs(s["mean"] - 0.7) < 1e-12 and abs(s["sd"] - 0.1) < 1e-12
