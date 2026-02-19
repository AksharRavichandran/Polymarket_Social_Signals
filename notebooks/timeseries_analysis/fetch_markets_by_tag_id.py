#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

GAMMA = "https://gamma-api.polymarket.com"

TAG_ID = 102599
MAX_MARKETS = 500
OUT_PATH = Path("notebooks/timeseries_analysis/data/markets_by_tag.jsonl")

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_SEC = 1.5


def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)


def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    last_err = None
    for attempt in range(1, DEFAULT_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                log(f"HTTP {r.status_code} on {url}, retry {attempt}/{DEFAULT_RETRIES}")
                time.sleep(DEFAULT_BACKOFF_SEC * attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(DEFAULT_BACKOFF_SEC * attempt)
    raise RuntimeError(f"GET failed after {DEFAULT_RETRIES} retries: {url} params={params} err={last_err}")


def fetch_markets_by_tag_id(tag_id: int, max_markets: Optional[int]) -> List[Dict[str, Any]]:
    log(f"Fetching markets for tag_id={tag_id}")
    out: List[Dict[str, Any]] = []
    offset = 0
    limit = 100
    while True:
        params = {"limit": limit, "offset": offset, "tag_id": int(tag_id), "include_tag": True}
        batch = http_get(f"{GAMMA}/markets", params=params)
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected /markets response type: {type(batch)}")
        if not batch:
            break
        out.extend([m for m in batch if isinstance(m, dict)])
        offset += limit
        log(f"  fetched={len(out)}")
        if max_markets is not None and len(out) >= max_markets:
            break
    if max_markets is not None and len(out) > max_markets:
        out = out[: max_markets]
    return out


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log(f"Writing JSONL -> {path}")
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    markets = fetch_markets_by_tag_id(TAG_ID, MAX_MARKETS)
    write_jsonl(OUT_PATH, markets)
    log(f"Wrote {len(markets)} markets -> {OUT_PATH}")


if __name__ == "__main__":
    main()
