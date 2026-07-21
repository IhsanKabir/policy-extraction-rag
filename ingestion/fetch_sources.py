"""Fetch the PUBLIC source documents listed in the pilot manifest into data/raw/.

Why this exists: the manifest (`data/pilot_source_manifest.csv`) names public
airline fee / fare-rule / refund / change pages, but the raw files aren't in the
repo (they're gitignored and were never committed). This script downloads only
the rows marked `fetch_status=in_scope` to `data/raw/<source_filename>`, so the
existing ingestion CLI can then parse + chunk them.

HARD scope rule: rows marked `fetch_status=excluded_proprietary` (the trade fare
circulars) are NEVER fetched. They are not public and must not enter the
publishable corpus. This script refuses to touch them.

Access tiers (from a verification pass on each live URL):
  - plain        : a normal GET works.
  - browser_ua   : the site's WAF 403s default bots; a browser User-Agent header
                   gets through (e.g. Nok Air).
  - headless     : content is JavaScript-rendered or behind an aggressive WAF
                   (IndiGo tabs, Bangkok Airways). A simple GET won't capture the
                   real text. These are SKIPPED here and reported as needing a
                   headless-browser capture or a manual snapshot. We do NOT save a
                   misleading empty/landing-page shell for them.

Usage:
    python -m ingestion.fetch_sources                 # fetch all in-scope plain+browser_ua
    python -m ingestion.fetch_sources --report        # just print the plan, fetch nothing
    python -m ingestion.fetch_sources --include-headless-attempt   # try headless rows too (best-effort)
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

import httpx

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _manifest_path() -> Path:
    return _repo_root() / "data" / "pilot_source_manifest.csv"


def _raw_dir() -> Path:
    return _repo_root() / "data" / "raw"


def load_in_scope_rows() -> list[dict]:
    rows = []
    with open(_manifest_path(), encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("fetch_status") or "").strip() == "in_scope":
                rows.append(row)
    return rows


def _fetch_one(url: str, tier: str, timeout: float = 30.0) -> tuple[bytes | None, str]:
    """Return (content_bytes, note). content is None on failure/skip."""
    headers = {"User-Agent": _BROWSER_UA} if tier in ("browser_ua", "headless") else {}
    try:
        r = httpx.get(url, headers=headers, timeout=timeout, follow_redirects=True)
    except Exception as e:  # network error
        return None, f"error: {type(e).__name__}: {e}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    body = r.content
    # Cheap sanity: a JS-shell / block page is usually tiny or has almost no text.
    if len(body) < 1500:
        return body, f"WARNING tiny response ({len(body)} bytes) — likely a block/JS shell, verify"
    return body, f"ok ({len(body)} bytes)"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", action="store_true", help="print the plan, fetch nothing")
    ap.add_argument("--include-headless-attempt", action="store_true",
                    help="best-effort plain GET for headless-tier rows too (may capture only a shell)")
    args = ap.parse_args()

    rows = load_in_scope_rows()
    raw = _raw_dir()
    raw.mkdir(parents=True, exist_ok=True)

    fetch_tiers = {"plain", "browser_ua"}
    if args.include_headless_attempt:
        fetch_tiers.add("headless")

    done, skipped, failed = [], [], []
    # De-dup identical URLs (e.g. the 6 IndiGo tab rows share one URL): fetch the
    # URL once, but still write a copy per distinct source_filename so the CLI +
    # manifest enrichment see each row. (For JS-tab pages the per-tab content is
    # identical until a headless capture splits the tabs — flagged in the report.)
    url_cache: dict[str, bytes] = {}

    for r in rows:
        sid = r["source_id"]; fn = r["source_filename"]; url = (r.get("source_url") or "").strip()
        tier = (r.get("access_tier") or "plain").strip()
        out = raw / fn

        if not url:
            skipped.append((sid, fn, "no URL in manifest"))
            continue
        if tier not in fetch_tiers:
            skipped.append((sid, fn, f"tier={tier} (needs headless capture / manual snapshot)"))
            continue
        if args.report:
            done.append((sid, fn, f"WOULD fetch [{tier}] {url}"))
            continue

        if url in url_cache:
            out.write_bytes(url_cache[url])
            done.append((sid, fn, f"ok (cached copy of {url})"))
            continue

        body, note = _fetch_one(url, tier)
        if body is None:
            failed.append((sid, fn, f"{note} — {url}"))
            continue
        out.write_bytes(body)
        url_cache[url] = body
        done.append((sid, fn, note))
        time.sleep(1.0)  # be polite

    def _print(title, items):
        print(f"\n=== {title} ({len(items)}) ===")
        for sid, fn, note in items:
            print(f"  {sid}  {fn[:55]:55}  {note}")

    _print("FETCHED / WOULD FETCH", done)
    _print("SKIPPED (headless/manual needed)", skipped)
    _print("FAILED", failed)
    print(f"\nRaw dir: {raw}")
    print("Next: python -m ingestion.cli --input data/raw --output data/chunks.jsonl")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
