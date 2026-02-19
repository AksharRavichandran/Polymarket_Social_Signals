#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

CLOB = "https://clob.polymarket.com"

MARKETS_PATH = Path("notebooks/timeseries_analysis/data/markets_by_tag.jsonl")
OUT_PATH = Path("notebooks/timeseries_analysis/data/prices_by_tag.jsonl")

FIDELITY_MIN = 60 * 12
INTERVAL = "max"

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


def read_markets(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log(f"Writing JSONL -> {path}")
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def fetch_prices_history(token_id: str) -> Dict[str, Any]:
    params = {"market": token_id, "interval": INTERVAL, "fidelity": int(FIDELITY_MIN)}
    return http_get(f"{CLOB}/prices-history", params=params)


def main() -> None:
    if not MARKETS_PATH.exists():
        raise RuntimeError(f"Missing markets file: {MARKETS_PATH}")

    markets = read_markets(MARKETS_PATH)
    log(f"Loaded {len(markets)} markets")

    price_rows: List[Dict[str, Any]] = []
    for idx, m in enumerate(markets, start=1):
        mid = m.get("id")
        tokens = m.get("clobTokenIds")
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except json.JSONDecodeError:
                tokens = None
        if not isinstance(tokens, list) or not tokens:
            continue
        log(f"[{idx}/{len(markets)}] market_id={mid} tokens={len(tokens)}")
        for token_id in tokens:
            hist = fetch_prices_history(str(token_id))
            price_rows.append(
                {
                    "market_id": mid,
                    "token_id": str(token_id),
                    "interval": INTERVAL,
                    "fidelity_min": FIDELITY_MIN,
                    "history": hist.get("history", []),
                }
            )

    write_jsonl(OUT_PATH, price_rows)
    log(f"Wrote {len(price_rows)} price histories -> {OUT_PATH}")


if __name__ == "__main__":
    main()
