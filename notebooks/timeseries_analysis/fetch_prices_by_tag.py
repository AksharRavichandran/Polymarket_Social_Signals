#!/usr/bin/env python3
"""
fetch_price_history.py
──────────────────────
Reads a markets JSONL file, fetches CLOB price history for every token,
and writes results to a JSONL file.

Features:
  - Filters out markets with no CLOB tokens or insufficient history
  - Resume support: skips tokens already fetched (safe to re-run)
  - Progress logging with ETA
  - Configurable fidelity and interval via CLI

Usage:
  python fetch_price_history.py
  python fetch_price_history.py --markets data/markets_filtered.jsonl --fidelity 720 --min-candles 10
  python fetch_price_history.py --markets data/markets_by_tag.jsonl --out data/prices.jsonl
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

# ── API ───────────────────────────────────────────────────────────────────────
CLOB_BASE = "https://clob.polymarket.com"

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_MARKETS_PATH = Path("notebooks/timeseries_analysis/data/filtered/markets_filtered.jsonl")
DEFAULT_OUT_PATH     = Path("notebooks/timeseries_analysis/data/filtered/filtered_prices_by_tag.jsonl")
DEFAULT_FIDELITY_MIN = 720          # 12 hours in minutes
DEFAULT_INTERVAL     = "max"
DEFAULT_MIN_CANDLES  = 10           # drop tokens with fewer candles than this

# ── HTTP ──────────────────────────────────────────────────────────────────────
TIMEOUT_SEC  = 30
MAX_RETRIES  = 5
BACKOFF_SEC  = 1.5


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    log(f"Loaded {len(rows):,} markets from {path}")
    return rows


def load_already_fetched(path: Path) -> Set[str]:
    """Return set of token_ids already present in the output file (for resume)."""
    if not path.exists():
        return set()
    seen = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row = json.loads(line)
                seen.add(row["token_id"])
    log(f"Resume: {len(seen):,} tokens already fetched – skipping")
    return seen


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Token extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_tokens(m: Dict[str, Any]) -> List[str]:
    """
    Polymarket markets can store token IDs in a few different shapes.
    Returns a flat list of token ID strings.
    """
    raw = m.get("clobTokenIds") or m.get("tokens") or []

    # Sometimes it arrives as a JSON string
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []

    if not isinstance(raw, list) or not raw:
        return []

    # tokens can be dicts {"token_id": "..."} or plain strings
    ids = []
    for t in raw:
        if isinstance(t, dict):
            tid = t.get("token_id") or t.get("id")
            if tid:
                ids.append(str(tid))
        elif isinstance(t, str) and t:
            ids.append(t)

    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Price fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_price_history(token_id: str, interval: str, fidelity_min: int) -> List[Dict]:
    params = {
        "market":   token_id,
        "interval": interval,
        "fidelity": fidelity_min,
    }
    resp = http_get(f"{CLOB_BASE}/prices-history", params=params)
    return resp.get("history") or []


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(args: argparse.Namespace) -> None:
    markets_path = Path(args.markets)
    out_path     = Path(args.out)

    if not markets_path.exists():
        raise FileNotFoundError(f"Markets file not found: {markets_path}")

    markets        = read_jsonl(markets_path)
    already_done   = load_already_fetched(out_path)

    # Build flat list of (market_id, token_id) pairs to process
    work: List[tuple[str, str]] = []
    for m in markets:
        mid    = str(m.get("id") or m.get("conditionId") or "")
        tokens = extract_tokens(m)
        for tid in tokens:
            if tid not in already_done:
                work.append((mid, tid))

    total     = len(work)
    skipped   = len(already_done)
    written   = 0
    dropped   = 0
    start_t   = time.time()

    log(f"Tokens to fetch: {total:,}  |  already done: {skipped:,}")

    for i, (market_id, token_id) in enumerate(work, start=1):
        history = fetch_price_history(token_id, args.interval, args.fidelity)

        # Drop tokens with too few candles – not enough history for time-series
        if len(history) < args.min_candles:
            dropped += 1
            log(f"[{i}/{total}] DROP token={token_id} – only {len(history)} candles")
            continue

        row = {
            "market_id":   market_id,
            "token_id":    token_id,
            "interval":    args.interval,
            "fidelity_min": args.fidelity,
            "n_candles":   len(history),
            "fetched_at":  datetime.now(tz=timezone.utc).isoformat(),
            "history":     history,
        }
        append_jsonl(out_path, row)
        written += 1

        # Progress + ETA every 25 tokens
        if i % 25 == 0 or i == total:
            elapsed  = time.time() - start_t
            rate     = i / elapsed if elapsed > 0 else 0
            eta_sec  = (total - i) / rate if rate > 0 else 0
            eta_str  = f"{eta_sec/60:.1f}m" if eta_sec > 60 else f"{eta_sec:.0f}s"
            log(f"[{i}/{total}] written={written} dropped={dropped} | rate={rate:.1f} tok/s | ETA {eta_str}")

        time.sleep(0.1)   # be polite to the CLOB API

    log("=" * 55)
    log(f"Done. Written: {written:,}  |  Dropped (<{args.min_candles} candles): {dropped:,}  |  Skipped (resume): {skipped:,}")
    log(f"Output → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch CLOB price history for Polymarket tokens")
    parser.add_argument("--markets",     type=str, default=str(DEFAULT_MARKETS_PATH), help="Input markets JSONL")
    parser.add_argument("--out",         type=str, default=str(DEFAULT_OUT_PATH),     help="Output prices JSONL")
    parser.add_argument("--fidelity",    type=int, default=DEFAULT_FIDELITY_MIN,      help="Candle size in minutes (default 720 = 12h)")
    parser.add_argument("--interval",    type=str, default=DEFAULT_INTERVAL,          help="History interval (default 'max')")
    parser.add_argument("--min-candles", type=int, default=DEFAULT_MIN_CANDLES,       help="Drop tokens with fewer candles than this")
    main(parser.parse_args())