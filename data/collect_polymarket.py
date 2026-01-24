#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_SEC = 1.5


def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)


def http_get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = DEFAULT_TIMEOUT) -> Any:
    last_err = None
    for attempt in range(1, DEFAULT_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
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


def parse_bool(x: str) -> bool:
    if x.lower() in ("true", "1", "yes", "y", "t"):
        return True
    if x.lower() in ("false", "0", "no", "n", "f"):
        return False
    raise argparse.ArgumentTypeError("Expected boolean (true/false)")


def write_jsonl(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    outdir = os.path.dirname(path)
    if outdir:
        os.makedirs(outdir, exist_ok=True)

    log(f"Writing JSONL -> {path}")
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def write_csv(path: str, rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> None:
    outdir = os.path.dirname(path)
    if outdir:
        os.makedirs(outdir, exist_ok=True)

    log(f"Writing CSV -> {path}")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())


# ----------------------------
# Gamma API
# ----------------------------

def get_all_tags() -> List[Dict[str, Any]]:
    log("Fetching tags from Gamma API")
    return http_get(f"{GAMMA}/tags")


def fetch_markets_by_tag(tag_slug: str, tag_id: str, closed: Optional[bool]) -> List[Dict[str, Any]]:
    log(f"Fetching markets for tag: {tag_slug} (id={tag_id})")
    out: List[Dict[str, Any]] = []
    offset = 0
    limit = 100
    batch_num = 0

    while True:
        params: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "tag_id": tag_id,
            "include_tag": True,
        }
        if closed is not None:
            params["closed"] = str(closed).lower()

        batch = http_get(f"{GAMMA}/markets", params=params)
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected Gamma /markets response type: {type(batch)}")

        if not batch:
            break

        batch_num += 1
        out.extend(batch)
        offset += limit
        log(f"  Tag {tag_slug}: batch {batch_num} fetched, total={len(out)}")
        time.sleep(0.05)

    log(f"  Finished tag {tag_slug}: {len(out)} markets")
    return out


def fetch_market_details(market_id: str) -> Optional[Dict[str, Any]]:
    try:
        data = http_get(f"{GAMMA}/markets/{market_id}")
    except Exception as e:
        log(f"Gamma market details failed for id={market_id}: {e}")
        return None

    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data:
        return data[0] if isinstance(data[0], dict) else None
    return None


def extract_condition_id(m: Dict[str, Any]) -> Optional[str]:
    return m.get("conditionId") or m.get("condition_id")


def _is_yes_label(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"yes", "y", "true", "1"}


def _token_label(t: Dict[str, Any]) -> Optional[str]:
    for key in ("outcome", "outcomeName", "name", "title", "label"):
        if t.get(key) is not None:
            return str(t.get(key))
    return None


def extract_clob_token_ids(m: Dict[str, Any], yes_only: bool = False) -> List[str]:
    # 1) Direct fields
    for key in ("clobTokenIds", "clob_token_ids", "outcomeTokenIds", "outcome_token_ids"):
        v = m.get(key)
        if isinstance(v, list) and v:
            if yes_only:
                # No labels here, so don't guess; allow labeled paths below.
                return []
            return [str(x) for x in v]

    # 2) tokens: [{"token_id": "..."}] or [{"tokenId": "..."}] or [{"id": "..."}]
    tokens = m.get("tokens")
    if isinstance(tokens, list) and tokens:
        ids = []
        for t in tokens:
            if not isinstance(t, dict):
                continue
            if yes_only and not _is_yes_label(_token_label(t)):
                continue
            for k in ("token_id", "tokenId", "id"):
                if t.get(k) is not None:
                    ids.append(str(t.get(k)))
                    break
        if ids:
            return ids

    # 3) outcomes: [{"tokenId": "..."}] or [{"clobTokenId": "..."}]
    outcomes = m.get("outcomes")
    if isinstance(outcomes, list) and outcomes:
        ids = []
        for o in outcomes:
            if not isinstance(o, dict):
                continue
            if yes_only and not _is_yes_label(_token_label(o)):
                continue
            for k in ("tokenId", "token_id", "clobTokenId", "clob_token_id", "id"):
                if o.get(k) is not None:
                    ids.append(str(o.get(k)))
                    break
        if ids:
            return ids

    # 4) nested outcome token ids in "market" objects (rare)
    market = m.get("market")
    if isinstance(market, dict):
        for key in ("clobTokenIds", "outcomeTokenIds"):
            v = market.get(key)
            if isinstance(v, list) and v:
                return [str(x) for x in v]

    return []


def fetch_clob_token_ids_by_condition(condition_id: str, yes_only: bool) -> List[str]:
    params_list = [
        {"condition_id": condition_id},
        {"conditionId": condition_id},
        {"market": condition_id},
    ]
    for params in params_list:
        try:
            data = http_get(f"{CLOB}/markets", params=params)
        except Exception as e:
            log(f"CLOB /markets lookup failed for {condition_id} params={params}: {e}")
            continue

        markets: List[Dict[str, Any]] = []
        if isinstance(data, list):
            markets = [m for m in data if isinstance(m, dict)]
        elif isinstance(data, dict):
            if isinstance(data.get("data"), list):
                markets = [m for m in data["data"] if isinstance(m, dict)]
            else:
                markets = [data]

        token_ids: List[str] = []
        for m in markets:
            token_ids.extend(extract_clob_token_ids(m, yes_only=yes_only))

        if token_ids:
            unique = list(dict.fromkeys(token_ids))
            log(f"Resolved {len(unique)} token ids from CLOB for conditionId={condition_id}")
            return unique

    return []


def resolve_token_ids(market: Dict[str, Any], yes_only: bool) -> List[str]:
    token_ids = extract_clob_token_ids(market, yes_only=yes_only)
    if token_ids:
        return token_ids

    market_id = None
    for key in ("id", "marketId", "market_id"):
        if market.get(key) is not None:
            market_id = str(market.get(key))
            break

    if market_id:
        detail = fetch_market_details(market_id)
        if isinstance(detail, dict):
            token_ids = extract_clob_token_ids(detail, yes_only=yes_only)
            if token_ids:
                return token_ids

    condition_id = extract_condition_id(market)
    if condition_id:
        return fetch_clob_token_ids_by_condition(condition_id, yes_only=yes_only)

    return []


def fetch_prices_history(token_id: str, interval: str, fidelity_min: int) -> Dict[str, Any]:
    return http_get(
        f"{CLOB}/prices-history",
        params={"market": token_id, "interval": interval, "fidelity": int(fidelity_min)},
    )


def _select_single_token(token_ids: List[str], condition_id: str) -> List[str]:
    if not token_ids:
        return []
    if len(token_ids) > 1:
        log(f"Multiple YES tokens for conditionId={condition_id}; using first of {len(token_ids)}")
    return [token_ids[0]]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=os.path.join("data", "polymarket"))
    ap.add_argument("--all-tags", action="store_true")
    ap.add_argument("--tag", action="append", default=[])
    ap.add_argument("--closed", type=parse_bool, default=True)
    ap.add_argument("--fidelity-min", type=int, default=1440)
    ap.add_argument("--interval", default="max")
    ap.add_argument("--yes-only", action="store_true")
    ap.add_argument("--prices", type=parse_bool, default=True)
    ap.add_argument("--format", choices=["json", "csv"], default="json")
    ap.add_argument("--max-markets", type=int, default=None)
    ap.add_argument("--sample", type=parse_bool, default=False)
    ap.add_argument("--seed", type=int, default=1337)

    args = ap.parse_args()

    abs_outdir = os.path.abspath(args.outdir)
    os.makedirs(abs_outdir, exist_ok=True)

    fmt = args.format
    ext = "jsonl" if fmt == "json" else "csv"
    markets_path = os.path.join(abs_outdir, f"markets.{ext}")
    prices_path = os.path.join(abs_outdir, f"prices_history.{ext}")

    log(f"Current working dir: {os.path.abspath(os.getcwd())}")
    log(f"Output directory: {abs_outdir}")

    # -------- Tags --------
    tags = get_all_tags()
    tag_slug_to_id = {
        str(t.get("slug")).lower(): str(t.get("id"))
        for t in tags if t.get("slug") and t.get("id")
    }

    if not args.all_tags and not args.tag:
        log("No tags specified; defaulting to --all-tags")
        args.all_tags = True

    if args.tag:
        requested = [t.lower() for t in args.tag]
        tag_slug_to_id = {k: v for k, v in tag_slug_to_id.items() if k in requested}
        log(f"Using explicit tags: {len(tag_slug_to_id)} tags")
    else:
        log(f"Using ALL tags: {len(tag_slug_to_id)} tags")

    # -------- Markets --------
    all_markets: List[Dict[str, Any]] = []
    for idx, (slug, tag_id) in enumerate(tag_slug_to_id.items(), start=1):
        log(f"[{idx}/{len(tag_slug_to_id)}] Tag loop: {slug}")
        ms = fetch_markets_by_tag(slug, tag_id, args.closed)
        for m in ms:
            m["_fetched_tag_slug"] = slug
        all_markets.extend(ms)

    log(f"Total markets fetched (pre-dedup): {len(all_markets)}")

    # Dedup
    dedup: Dict[str, Dict[str, Any]] = {}
    for m in all_markets:
        cid = extract_condition_id(m)
        if cid:
            dedup[str(cid)] = m

    condition_ids = list(dedup.keys())
    log(f"Deduplicated markets: {len(condition_ids)} unique conditionIds")

    # Cap/sample
    if args.max_markets is not None and len(condition_ids) > args.max_markets:
        log(f"Applying market cap: {args.max_markets} (sample={args.sample})")
        if args.sample:
            rnd = random.Random(args.seed)
            condition_ids = rnd.sample(condition_ids, args.max_markets)
        else:
            condition_ids = condition_ids[: args.max_markets]

    selected = {cid: dedup[cid] for cid in condition_ids}
    log(f"Final markets selected: {len(selected)}")

    token_ids_by_cid: Dict[str, List[str]] = {}
    filtered_selected: Dict[str, Dict[str, Any]] = {}
    total = len(selected)
    for i, (cid, m) in enumerate(selected.items(), start=1):
        token_ids = resolve_token_ids(m, yes_only=args.yes_only)
        if args.yes_only:
            token_ids = _select_single_token(token_ids, cid)
        token_ids_by_cid[cid] = token_ids
        if not token_ids and args.yes_only:
            log(f"Skipping conditionId={cid}; no YES token found")
            continue
        filtered_selected[cid] = m
        log(f"Resolved tokens {i}/{total} | conditionId={cid} | tokens={len(token_ids)}")

    # Write markets
    market_rows = [
        {
            "conditionId": cid,
            "question": m.get("question"),
            "tags": m.get("tags"),
            "clobTokenIds": token_ids_by_cid.get(cid, []),
            "_fetched_tag_slug": m.get("_fetched_tag_slug"),
            "closed": m.get("closed"),
        }
        for cid, m in filtered_selected.items()
    ]

    if fmt == "json":
        write_jsonl(markets_path, market_rows)
    else:
        rows = []
        for row in market_rows:
            rows.append(
                {
                    "conditionId": row.get("conditionId"),
                    "question": row.get("question"),
                    "tags": json.dumps(row.get("tags"), ensure_ascii=False),
                    "clobTokenIds": ";".join(row.get("clobTokenIds") or []),
                    "_fetched_tag_slug": row.get("_fetched_tag_slug"),
                    "closed": row.get("closed"),
                }
            )
        write_csv(
            markets_path,
            rows,
            ["conditionId", "question", "tags", "clobTokenIds", "_fetched_tag_slug", "closed"],
        )

    # Prices
    if args.prices:
        log("Fetching price history")
        price_rows: List[Dict[str, Any]] = []
        total_tokens = sum(len(token_ids_by_cid.get(cid, [])) for cid in filtered_selected.keys())
        token_counter = 0
        for i, (cid, m) in enumerate(filtered_selected.items(), start=1):
            token_ids = token_ids_by_cid.get(cid, [])
            log(f"Market {i}/{total} | conditionId={cid} | tokens={len(token_ids)}")
            for token_id in token_ids:
                token_counter += 1
                log(f"  Token {token_counter}/{total_tokens} | token_id={token_id}")
                hist = fetch_prices_history(token_id, args.interval, args.fidelity_min)
                if fmt == "json":
                    price_rows.append(
                        {
                            "conditionId": cid,
                            "token_id": token_id,
                            "interval": args.interval,
                            "fidelity_min": args.fidelity_min,
                            "history": hist.get("history", []),
                        }
                    )
                else:
                    for point in hist.get("history", []):
                        price_rows.append(
                            {
                                "conditionId": cid,
                                "token_id": token_id,
                                "interval": args.interval,
                                "fidelity_min": args.fidelity_min,
                                "timestamp": point.get("t"),
                                "price": point.get("p"),
                            }
                        )
                time.sleep(0.03)

        if fmt == "json":
            write_jsonl(prices_path, price_rows)
        else:
            write_csv(
                prices_path,
                price_rows,
                ["conditionId", "token_id", "interval", "fidelity_min", "timestamp", "price"],
            )

    log("Done.")


if __name__ == "__main__":
    main()
