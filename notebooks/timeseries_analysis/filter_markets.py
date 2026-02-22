#!/usr/bin/env python3
"""
filter_markets.py
─────────────────
Reads a raw markets JSONL file, applies quality filters, and writes a
filtered JSONL + a summary JSON.

Usage:
  python filter_markets.py
  python filter_markets.py --input data/markets_by_tag.jsonl --min-volume 25000
  python filter_markets.py --input data/markets_by_tag.jsonl --min-volume 10000 --min-active-days 7

Outputs:
  <out-dir>/markets_filtered.jsonl
  <out-dir>/filter_summary.json
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_IN              = Path("notebooks/timeseries_analysis/data/markets_by_tag.jsonl")
DEFAULT_OUT_DIR         = Path("notebooks/timeseries_analysis/data/filtered")
DEFAULT_MIN_VOLUME_USD  = 10_000
DEFAULT_MIN_ACTIVE_DAYS = 7


def log(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)


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


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    log(f"Wrote {len(rows):,} records → {path}")


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
    log(f"Wrote summary → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _get_volume(m: Dict[str, Any]) -> float:
    """Try every known Polymarket volume field name."""
    for field in ("volume", "volumeNum", "volumeClob", "usdcSize", "totalVolume"):
        v = _safe_float(m.get(field))
        if v > 0:
            return v
    return 0.0


def _get_active_days(m: Dict[str, Any]) -> Optional[float]:
    now   = datetime.now(tz=timezone.utc)
    start = _parse_iso(m.get("createdAt") or m.get("created_at") or m.get("startDate"))
    end   = _parse_iso(m.get("endDate")   or m.get("end_date"))
    if not start:
        return None
    ref = min(now, end) if end else now
    return max(0.0, (ref - start).total_seconds() / 86_400)


def _get_n_outcomes(m: Dict[str, Any]) -> int:
    tokens   = m.get("tokens") or m.get("clobTokenIds") or []
    outcomes = m.get("outcomes") or []
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = []
    return max(len(tokens), len(outcomes), 0)


def _has_clob_tokens(m: Dict[str, Any]) -> bool:
    tokens = m.get("tokens") or m.get("clobTokenIds") or []
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            return False
    return len(tokens) > 0


# ─────────────────────────────────────────────────────────────────────────────
# Filter
# ─────────────────────────────────────────────────────────────────────────────

FilterResult = Tuple[bool, str]


def apply_filter(
    m: Dict[str, Any],
    min_volume: float,
    min_active_days: float,
) -> FilterResult:
    if not _has_clob_tokens(m):
        return False, "no_clob_tokens"

    if not (m.get("question") or m.get("description")):
        return False, "no_question_or_description"
    
    n_out = _get_n_outcomes(m)

    vol = _get_volume(m)
    if vol < min_volume:
        return False, f"volume_too_low:{vol:.0f}"

    active = _get_active_days(m)
    if active is not None and active < min_active_days:
        return False, f"too_new:{active:.1f}_days"

    return True, ""


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

def build_summary(
    total_raw: int,
    filtered: List[Dict],
    rejection_counts: Dict[str, int],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    resolved   = [m for m in filtered if m.get("resolved") or m.get("isResolved")]
    unresolved = [m for m in filtered if not (m.get("resolved") or m.get("isResolved"))]
    volumes    = [v for m in filtered if (v := _get_volume(m)) > 0]

    return {
        "filtered_at": datetime.now(tz=timezone.utc).isoformat(),
        "filter_params": {
            "min_volume_usd":  args.min_volume,
            "min_active_days": args.min_active_days
        },
        "raw_count":        total_raw,
        "filtered_count":   len(filtered),
        "resolved_count":   len(resolved),
        "unresolved_count": len(unresolved),
        "volume_stats": {
            "min":    min(volumes) if volumes else None,
            "max":    max(volumes) if volumes else None,
            "median": sorted(volumes)[len(volumes) // 2] if volumes else None,
            "total":  sum(volumes) if volumes else None,
        },
        "rejection_reasons": dict(sorted(rejection_counts.items(), key=lambda x: -x[1])),
    }


def print_summary(summary: Dict[str, Any]) -> None:
    vs = summary["volume_stats"]
    print("\n" + "=" * 60)
    print("  FILTER COMPLETE")
    print("=" * 60)
    print(f"  Raw markets          : {summary['raw_count']:,}")
    print(f"  After filter         : {summary['filtered_count']:,}")
    print(f"  ├─ Resolved          : {summary['resolved_count']:,}  ← backtest set")
    print(f"  └─ Unresolved        : {summary['unresolved_count']:,}  ← paper trading set")
    print(f"\n  Filter params:")
    for k, v in summary["filter_params"].items():
        print(f"    {k:<25} {v}")
    print(f"\n  Rejection reasons:")
    for reason, count in summary["rejection_reasons"].items():
        print(f"    {reason:<35} {count:>5}")
    if vs["min"] is not None:
        print(f"\n  Volume range  : ${vs['min']:>15,.0f} – ${vs['max']:,.0f}")
        print(f"  Median volume : ${vs['median']:>15,.0f}")
        print(f"  Total volume  : ${vs['total']:>15,.0f}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main(args: argparse.Namespace) -> None:
    markets = read_jsonl(Path(args.input))

    filtered: List[Dict] = []
    rejection_counts: Dict[str, int] = {}

    for m in markets:
        passes, reason = apply_filter(m, args.min_volume, args.min_active_days)
        if passes:
            filtered.append(m)
        else:
            bucket = reason.split(":")[0]
            rejection_counts[bucket] = rejection_counts.get(bucket, 0) + 1

    # Sort by volume descending so highest-signal markets come first
    filtered.sort(key=lambda m: _get_volume(m), reverse=True)

    out_dir = Path(args.out_dir)
    write_jsonl(out_dir / "markets_filtered.jsonl", filtered)

    summary = build_summary(len(markets), filtered, rejection_counts, args)
    write_json(out_dir / "filter_summary.json", summary)
    print_summary(summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter raw Polymarket markets JSONL")
    parser.add_argument("--input",           type=str,   default=str(DEFAULT_IN),         help="Path to raw markets JSONL")
    parser.add_argument("--out-dir",         type=str,   default=str(DEFAULT_OUT_DIR),     help="Output directory")
    parser.add_argument("--min-volume",      type=float, default=DEFAULT_MIN_VOLUME_USD,   help="Min USD volume")
    parser.add_argument("--min-active-days", type=float, default=DEFAULT_MIN_ACTIVE_DAYS,  help="Min days market was active")
    main(parser.parse_args())
