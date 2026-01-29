#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 5
DEFAULT_BACKOFF_SEC = 1.5

PARAMS = {
    "outdir": os.path.join("data", "polymarket"),
    "markets_csv": os.path.join("data", "polymarket_data", "markets.csv"),
    "min_year": 2024,
    "fidelity_min": 60*12,
    "interval": "max",
    "yes_only": False,
    "single_token": False,
    "prices": True,
    "format": "json",
    "max_markets": 500,
    "sample": False,
    "seed": 1337,
}


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


def _maybe_parse_json_list(value: Any) -> Optional[List[Any]]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, list) else None
    return None


def extract_clob_token_ids(m: Dict[str, Any], yes_only: bool = False) -> List[str]:
    # 1) Direct fields
    for key in ("clobTokenIds", "clob_token_ids", "outcomeTokenIds", "outcome_token_ids"):
        v = _maybe_parse_json_list(m.get(key))
        if v:
            if yes_only:
                # No labels here, so don't guess; allow labeled paths below.
                return []
            return [str(x) for x in v]

    # 2) outcomes: [{"tokenId": "..."}] or [{"clobTokenId": "..."}]
    outcomes = _maybe_parse_json_list(m.get("outcomes"))
    if outcomes:
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
            v = _maybe_parse_json_list(market.get(key))
            if v:
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
            limited = unique[:1]
            log(f"Resolved {len(unique)} token ids from CLOB for conditionId={condition_id} (using {len(limited)})")
            return limited

    return []


def fetch_prices_history(token_id: str, interval: str, fidelity_min: int) -> Dict[str, Any]:
    params = {"market": token_id, "interval": interval, "fidelity": int(fidelity_min)}
    req = requests.Request("GET", f"{CLOB}/prices-history", params=params).prepare()
    log(f"Request URL: {req.url}")
    data = http_get(f"{CLOB}/prices-history", params=params)
    if isinstance(data, dict) and "history" in data:
        log(f"History points: {len(data.get('history', []))}")
        return data
    log(f"Unexpected prices-history response type={type(data)}")
    if isinstance(data, list):
        return {"history": data}
    return {"history": []}


def _select_single_token(token_ids: List[str], condition_id: str) -> List[str]:
    if not token_ids:
        return []
    if len(token_ids) > 1:
        log(f"Multiple YES tokens for conditionId={condition_id}; using first of {len(token_ids)}")
    return [token_ids[0]]


def read_markets_csv(path: str) -> List[Dict[str, Any]]:
    log(f"Reading markets CSV: {path}")
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]
    log(f"Loaded {len(rows)} rows from markets CSV")
    return rows


def _parse_created_at(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_market_row(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    market_id = row.get("id") or row.get("market_id") or row.get("marketId")
    condition_id = row.get("condition_id") or row.get("conditionId")
    if market_id is not None:
        market_id = str(market_id)
    if condition_id is not None:
        condition_id = str(condition_id)
    return market_id, condition_id


def resolve_token_ids_from_csv_row(
    row: Dict[str, Any],
    yes_only: bool,
) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    market_id, condition_id = parse_market_row(row)
    if market_id is None and condition_id is None:
        log("Skipping row; missing both market id and condition id")
        return [], None

    detail: Optional[Dict[str, Any]] = None
    if market_id is not None:
        log(f"Fetching Gamma market details for id={market_id}")
        detail = fetch_market_details(market_id)
        if detail is None:
            log(f"No Gamma details for id={market_id}")

    token_ids: List[str] = []
    if detail:
        token_ids = extract_clob_token_ids(detail, yes_only=yes_only)
        if token_ids:
            return token_ids, detail

    # Fall back to condition id lookup if we have it.
    if condition_id:
        token_ids = fetch_clob_token_ids_by_condition(condition_id, yes_only=yes_only)
        if token_ids:
            return token_ids, detail

    # Final fallback: try resolving from the row itself (in case tokens exist).
    token_ids = extract_clob_token_ids(row, yes_only=yes_only)
    return token_ids, detail


def main() -> None:
    params = PARAMS

    abs_outdir = os.path.abspath(params["outdir"])
    os.makedirs(abs_outdir, exist_ok=True)

    fmt = params["format"]
    ext = "jsonl" if fmt == "json" else "csv"
    markets_path = os.path.join(abs_outdir, f"markets.{ext}")
    prices_path = os.path.join(abs_outdir, f"prices_history.{ext}")

    log(f"Current working dir: {os.path.abspath(os.getcwd())}")
    log(f"Output directory: {abs_outdir}")

    # -------- Markets from CSV --------
    all_rows = read_markets_csv(params["markets_csv"])
    total_rows = len(all_rows)
    if total_rows == 0:
        raise RuntimeError(f"No rows in markets CSV: {params['markets_csv']}")

    if params["min_year"] is not None:
        min_dt = datetime(params["min_year"], 1, 1, tzinfo=timezone.utc)
        filtered_rows = []
        skipped = 0
        for row in all_rows:
            created_at = _parse_created_at(row.get("createdAt"))
            if created_at is None or created_at < min_dt:
                skipped += 1
                continue
            filtered_rows.append(row)
        log(f"Filtered by min-year={params['min_year']}: kept={len(filtered_rows)} skipped={skipped}")
        all_rows = filtered_rows

    total_rows = len(all_rows)

    # Cap/sample
    if params["max_markets"] is not None and total_rows > params["max_markets"]:
        log(f"Applying market cap: {params['max_markets']} (sample={params['sample']})")
        if params["sample"]:
            rnd = random.Random(params["seed"])
            all_rows = rnd.sample(all_rows, params["max_markets"])
        else:
            all_rows = all_rows[: params["max_markets"]]

    log(f"Final markets selected from CSV: {len(all_rows)}")

    token_ids_by_cid: Dict[str, List[str]] = {}
    filtered_selected: Dict[str, Dict[str, Any]] = {}
    total = len(all_rows)
    for i, row in enumerate(all_rows, start=1):
        market_id, condition_id = parse_market_row(row)
        log(f"[{i}/{total}] Resolving tokens for market_id={market_id} condition_id={condition_id}")
        token_ids, detail = resolve_token_ids_from_csv_row(row, yes_only=params["yes_only"])
        if detail and condition_id is None:
            condition_id = extract_condition_id(detail)
        if params["yes_only"] or params["single_token"]:
            token_ids = _select_single_token(token_ids, condition_id or "unknown")
        row_key = condition_id or f"row_{i}"
        token_ids_by_cid[row_key] = token_ids
        if not token_ids and params["yes_only"]:
            log(f"Skipping conditionId={condition_id}; no YES token found")
            continue
        selected_market: Dict[str, Any] = {}
        if detail:
            selected_market.update(detail)
        selected_market.update(row)
        filtered_selected[row_key] = selected_market
        log(f"Resolved tokens {i}/{total} | conditionId={condition_id} | tokens={len(token_ids)}")

    # Write markets
    market_rows = [
        {
            "conditionId": cid,
            "question": m.get("question"),
            "tags": m.get("tags"),
            "clobTokenIds": token_ids_by_cid.get(cid, []),
            "closed": m.get("closed"),
            "market_id": m.get("id") or m.get("market_id") or m.get("marketId"),
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
                    "closed": row.get("closed"),
                }
            )
        write_csv(
            markets_path,
            rows,
            ["conditionId", "question", "tags", "clobTokenIds", "closed"],
        )

    # Prices
    if params["prices"]:
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
                hist = fetch_prices_history(token_id, params["interval"], params["fidelity_min"])
                if fmt == "json":
                    price_rows.append(
                        {
                            "conditionId": cid,
                            "market_id": m.get("id") or m.get("market_id") or m.get("marketId"),
                            "token_id": token_id,
                            "interval": params["interval"],
                            "fidelity_min": params["fidelity_min"],
                            "history": hist.get("history", []),
                        }
                    )
                else:
                    for point in hist.get("history", []):
                        price_rows.append(
                            {
                                "conditionId": cid,
                                "token_id": token_id,
                                "interval": params["interval"],
                                "fidelity_min": params["fidelity_min"],
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
