#!/usr/bin/env python3
"""
fetch_markets_by_tag_id.py
──────────────────────────
Fetches all Polymarket markets for a given tag_id and writes them to JSONL.

Usage:
  python fetch_markets_by_tag_id.py                      # uses defaults
  python fetch_markets_by_tag_id.py --tag-id 339 --max 2000 --out data/markets.jsonl
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

GAMMA_BASE    = "https://gamma-api.polymarket.com"
PAGE_SIZE     = 100
TIMEOUT_SEC   = 30
MAX_RETRIES   = 5
BACKOFF_SEC   = 1.5

DEFAULT_TAG_ID  = 144
DEFAULT_MAX     = 5000
DEFAULT_OUT     = Path("notebooks/timeseries_analysis/data/markets_by_tag.jsonl")


def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)


def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_SEC)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = BACKOFF_SEC * (2 ** (attempt - 1))
                log(f"HTTP {r.status_code} – retrying in {wait:.1f}s ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_err = exc
            time.sleep(BACKOFF_SEC * attempt)
    raise RuntimeError(f"GET failed after {MAX_RETRIES} retries: {url} | {last_err}")


def fetch_markets(tag_id: int, max_markets: int) -> List[Dict[str, Any]]:
    log(f"Fetching markets for tag_id={tag_id} (max={max_markets})")
    markets: List[Dict[str, Any]] = []
    offset = 0

    while len(markets) < max_markets:
        batch = http_get(f"{GAMMA_BASE}/markets", params={
            "limit":       PAGE_SIZE,
            "offset":      offset,
            "tag_id":      tag_id,
            "include_tag": True,
        })
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected response type: {type(batch)}")
        if not batch:
            break
        markets.extend(m for m in batch if isinstance(m, dict))
        offset += len(batch)
        log(f"  fetched {len(markets)} so far …")
        time.sleep(0.2)

    markets = markets[:max_markets]
    log(f"Done — {len(markets)} markets collected")
    return markets


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    log(f"Wrote {len(rows)} records → {path}")


def main(args: argparse.Namespace) -> None:
    markets = fetch_markets(args.tag_id, args.max)
    write_jsonl(Path(args.out), markets)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Polymarket markets by tag ID")
    parser.add_argument("--tag-id", type=int,  default=DEFAULT_TAG_ID, help="Polymarket tag ID")
    parser.add_argument("--max",    type=int,  default=DEFAULT_MAX,    help="Max markets to fetch")
    parser.add_argument("--out",    type=str,  default=str(DEFAULT_OUT), help="Output .jsonl path")
    main(parser.parse_args())