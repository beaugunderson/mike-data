# mike-data

A data story about [KmikeyM](https://kmikeym.com) — Mike Merrill, the publicly traded person. `index.html` is a fully self-contained D3 page (data inlined as `const DATA`) covering 18 years of share trading and 300+ shareholder votes.

## Data sources

- `scrape.py` — full trade ledger + user directory from kmikeym.com user pages (dates estimated from Rails relative times). One-time historical scrape.
- `scrape_votes.py` — all votes, options, and comments from vote.kmikeym.com (needs `KMIKEYM_SESSION` in `.env`).
- `update.py` — refresh: fetches daily OHLCV from the public `https://kmikeym.com/trades/history.json`, rebuilds `data/viz_data.json`, and re-inlines `DATA` into `index.html`. `--merge-trades <dump.json>` merges a kmikeym MCP `get_latest_trades` dump (exact timestamps) into `data/trades_recent.json`.

The kmikeym MCP (Claude-only) supplies what the site doesn't expose publicly: exact-timestamped recent trades (latest 100, no pagination), the top-50 leaderboard, and the open order book — snapshotted into `data/leaderboard.json` and `data/orderbook.json` with `as_of` dates.

`data/` is gitignored; the page carries everything it needs.

## Viewing

Open `index.html` in a browser (or `python3 -m http.server`).
