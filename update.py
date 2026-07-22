#!/usr/bin/env uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Refresh market data and rebuild viz_data.json + the inline DATA in index.html.

Data sources:
- OHLCV: https://kmikeym.com/trades/history.json (public JSONP) — fetched live here.
- Exact-timestamped trades, leaderboard, order book: the kmikeym MCP (Claude-only).
  Save a fresh MCP get_latest_trades dump as JSON and merge it with
  `./update.py --merge-trades <dump.json>`; refresh data/leaderboard.json and
  data/orderbook.json by hand from MCP output (they carry an as_of date).

The pre-2026-04 monthly activity rows come from the full scraped ledger
(data/trades.json, estimated dates); later months aggregate from
data/trades_recent.json (exact timestamps).
"""

import argparse
import json
import re
import statistics
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
INDEX = Path(__file__).parent / "index.html"
HISTORY_URL = "https://kmikeym.com/trades/history.json"
# Months up to and including this one come from the scraped ledger's aggregates.
LEDGER_LAST_MONTH = "2026-03"


def merge_trades(dump_path: Path) -> None:
    recent_path = DATA_DIR / "trades_recent.json"
    recent = json.loads(recent_path.read_text())
    dump = json.loads(dump_path.read_text())
    seen = {
        (t["date"], t["from"], t["to"], t["shares"], t["price"])
        for t in recent["trades"]
    }
    added = 0
    for t in dump["trades"]:
        key = (t["date"], t["from"], t["to"], t["shares"], t["price"])
        if key not in seen:
            recent["trades"].append(t)
            seen.add(key)
            added += 1
    recent["trades"].sort(key=lambda t: t["date"], reverse=True)
    recent["as_of"] = datetime.now(UTC).strftime("%Y-%m-%d")
    recent_path.write_text(json.dumps(recent, indent=2) + "\n")
    print(f"merged {added} new trades into {recent_path}")


def fetch_ohlc() -> list[list[float]]:
    with urllib.request.urlopen(HISTORY_URL, timeout=30) as resp:
        raw = resp.read().decode()
    (DATA_DIR / "ohlc_raw.txt").write_text(raw)
    ohlc = json.loads(raw.strip().removeprefix("(").removesuffix(")"))
    (DATA_DIR / "ohlc.json").write_text(json.dumps(ohlc) + "\n")
    print(f"fetched {len(ohlc)} OHLCV days through {datetime.fromtimestamp(ohlc[-1][0] / 1000, tz=UTC).date()}")
    return ohlc


def recent_monthly_rows() -> list[dict]:
    recent = json.loads((DATA_DIR / "trades_recent.json").read_text())
    months = defaultdict(list)
    for t in recent["trades"]:
        month = t["date"][:7]
        if month > LEDGER_LAST_MONTH:
            months[month].append(t)
    rows = []
    for month in sorted(months):
        ts = months[month]
        traders = {t["from"] for t in ts} | {t["to"] for t in ts}
        prices = [t["price"] for t in ts]
        rows.append({
            "month": month,
            "trades": len(ts),
            "volume": round(sum(t["total"] for t in ts), 2),
            "shares": round(sum(t["shares"] for t in ts), 2),
            "unique_traders": len(traders),
            "avg_price": round(statistics.mean(prices), 2),
            "min_price": min(prices),
            "max_price": max(prices),
        })
    return rows


def rebuild_viz(ohlc_raw: list[list[float]]) -> dict:
    viz = json.loads((DATA_DIR / "viz_data.json").read_text())
    viz["ohlc"] = [
        {
            "date": datetime.fromtimestamp(ts / 1000, tz=UTC).strftime("%Y-%m-%d"),
            "open": o, "high": h, "low": lo, "close": c, "volume": v,
        }
        for ts, o, h, lo, c, v in ohlc_raw
    ]
    ledger_rows = [r for r in viz["trades"] if r["month"] <= LEDGER_LAST_MONTH]
    viz["trades"] = ledger_rows + recent_monthly_rows()

    leaderboard = json.loads((DATA_DIR / "leaderboard.json").read_text())
    orderbook = json.loads((DATA_DIR / "orderbook.json").read_text())
    viz["leaderboard"] = leaderboard["leaderboard"]
    viz["orderbook"] = {"buy": orderbook["buy"], "sell": orderbook["sell"]}
    viz["market_as_of"] = min(leaderboard["as_of"], orderbook["as_of"])

    (DATA_DIR / "viz_data.json").write_text(json.dumps(viz) + "\n")
    print(f"rebuilt viz_data.json: {len(viz['ohlc'])} ohlc days, {len(viz['trades'])} months, market_as_of {viz['market_as_of']}")
    return viz


def inline_data(viz: dict) -> None:
    html = INDEX.read_text()
    new_line = "const DATA = " + json.dumps(viz, separators=(",", ":")) + ";"
    html, n = re.subn(r"^const DATA = .*$", lambda _: new_line, html, count=1, flags=re.M)
    if n != 1:
        raise SystemExit("const DATA line not found in index.html")
    INDEX.write_text(html)
    print("inlined DATA into index.html")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--merge-trades", type=Path, help="JSON dump from MCP get_latest_trades to merge into trades_recent.json")
    args = parser.parse_args()
    if args.merge_trades:
        merge_trades(args.merge_trades)
    viz = rebuild_viz(fetch_ohlc())
    inline_data(viz)


if __name__ == "__main__":
    main()
