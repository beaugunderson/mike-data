# mike-data

- `index.html` is self-contained: all chart data lives in the single-line `const DATA = {...};` on one line. Never hand-edit that line — `update.py` regenerates it from `data/viz_data.json`.
- `data/` is gitignored on purpose; only the inlined DATA ships.
- Monthly trade aggregates before `LEDGER_LAST_MONTH` (2026-03) come from the scraped ledger (`data/trades.json`, estimated dates); later months aggregate from `data/trades_recent.json` (exact MCP timestamps). Don't recompute the old months — the ledger's dates are estimates reverse-engineered from Rails relative-time strings.
- Refresh flow: in a Claude session, call the kmikeym MCP `get_latest_trades`, save the dump, run `./update.py --merge-trades <dump.json>`; also refresh `data/leaderboard.json` / `data/orderbook.json` from MCP output (update their `as_of`). Plain `./update.py` refreshes OHLCV only (public endpoint, no auth).
- The MCP has no pagination (`get_latest_trades` caps at 100); the site's `/trades` page always shows the same latest 50 regardless of query params. The full 6,477-trade ledger exists only in `data/trades.json`.
- The `KMIKEYM_SESSION` cookie in `.env` works on both vote.kmikeym.com and kmikeym.com.
- Vote data (`scrape_votes.py`) has no MCP equivalent — the scrape is the only source.
