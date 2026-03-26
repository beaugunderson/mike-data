#!/usr/bin/env uv run
"""Scrape all trade history from kmikeym.com user pages."""

import asyncio
import csv
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://kmikeym.com"
CONCURRENCY = 10
USER_DIR_PAGES = 71
OUTPUT_DIR = Path("data")


def parse_relative_time(text: str, now: datetime) -> str:
    """Convert Rails distance_of_time_in_words back to an estimated ISO date.

    Rails breakpoints (from actionview source):
      0-29s        -> "less than a minute"
      30-89s       -> "1 minute"
      90s-44min    -> "N minutes"
      45-89min     -> "about 1 hour"
      90min-24h    -> "about N hours"
      24h-42h      -> "1 day"
      42h-30d      -> "N days"
      30-45d       -> "about 1 month"
      45-60d       -> "about 2 months"
      60d-365d     -> "N months"
      365d-548d    -> "about 1 year"    (midpoint ~1.25yr)
      548d-730d    -> "over 1 year"     (midpoint ~1.75yr)
      730d-912d    -> "almost 2 years"  (midpoint ~2.25yr)
      Then per-year quarters:
        about N years  -> N + 0.125
        over N years   -> N + 0.375
        almost N years -> N - 0.125
    """
    t = text.strip().lower()

    if t in ("less than a minute", "1 minute"):
        return now.strftime("%Y-%m-%d")

    # "N minutes"
    if m := re.match(r"(\d+) minutes?", t):
        delta = timedelta(minutes=int(m[1]))
    # "about N hours"
    elif m := re.match(r"about (\d+) hours?", t):
        delta = timedelta(hours=int(m[1]))
    # "1 day"
    elif t == "1 day":
        delta = timedelta(days=1)
    # "N days"
    elif m := re.match(r"(\d+) days?", t):
        delta = timedelta(days=int(m[1]))
    # "about 1 month" / "about N months"
    elif m := re.match(r"about (\d+) months?", t):
        delta = timedelta(days=int(m[1]) * 30.44)
    # "N months"
    elif m := re.match(r"(\d+) months?", t):
        delta = timedelta(days=int(m[1]) * 30.44)
    # "about N year(s)"
    elif m := re.match(r"about (\d+) years?", t):
        delta = timedelta(days=(int(m[1]) + 0.125) * 365.25)
    # "over N year(s)"
    elif m := re.match(r"over (\d+) years?", t):
        delta = timedelta(days=(int(m[1]) + 0.375) * 365.25)
    # "almost N years"
    elif m := re.match(r"almost (\d+) years?", t):
        delta = timedelta(days=(int(m[1]) - 0.125) * 365.25)
    else:
        return ""

    return (now - delta).strftime("%Y-%m-%d")


def get_cookie():
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("KMIKEYM_SESSION="):
                return line.split("=", 1)[1].strip()
    print("Set KMIKEYM_SESSION in .env", file=sys.stderr)
    sys.exit(1)


async def fetch(client: httpx.AsyncClient, url: str) -> str:
    for attempt in range(3):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            if attempt == 2:
                print(f"  FAILED {url}: {e}", file=sys.stderr)
                return ""
            await asyncio.sleep(2**attempt)
    return ""


def parse_user_ids(html: str) -> list[dict]:
    """Extract user IDs, names, and metadata from a user directory page."""
    soup = BeautifulSoup(html, "lxml")
    users = []
    for row in soup.select("#userList table tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        link = cells[0].find("a")
        if not link:
            continue
        href = link.get("href", "")
        # href like https://kmikeym.com/users/6903 or /users/6903
        user_id = href.rstrip("/").split("/")[-1]
        if not user_id.isdigit():
            continue
        name = link.get_text(strip=True)
        users.append({"id": int(user_id), "name": name})
    return users


def parse_trades(html: str, source_user_id: int, now: datetime) -> list[dict]:
    """Extract trades from a user profile page's Trade History section."""
    soup = BeautifulSoup(html, "lxml")

    # Find the Trade History heading and its table
    trade_heading = soup.find("h3", string="Trade History")
    if not trade_heading:
        return []

    table = trade_heading.find_next("table")
    if not table:
        return []

    trades = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        # Cell 0: User (with link)
        user_link = cells[0].find("a")
        user_name = user_link.get_text(strip=True) if user_link else ""
        user_href = user_link.get("href", "") if user_link else ""
        user_id = user_href.rstrip("/").split("/")[-1]

        # Cell 1: Bought/Sold
        direction = cells[1].get_text(strip=True)

        # Cell 2: Quantity
        quantity = cells[2].get_text(strip=True)

        # Cell 3: From/To (counterparty)
        counter_link = cells[3].find("a")
        counter_name = counter_link.get_text(strip=True) if counter_link else ""
        counter_href = counter_link.get("href", "") if counter_link else ""
        counter_id = counter_href.rstrip("/").split("/")[-1]

        # Cell 4: $/Share
        price = cells[4].get_text(strip=True).replace("$", "").replace(",", "")

        # Cell 5: Value
        value = cells[5].get_text(strip=True).replace("$", "").replace(",", "")

        # Cell 6: Closed (relative time)
        closed = cells[6].get_text(strip=True)

        # Normalize to seller/buyer regardless of perspective
        if direction == "Bought":
            buyer_id, buyer_name = user_id, user_name
            seller_id, seller_name = counter_id, counter_name
        else:
            seller_id, seller_name = user_id, user_name
            buyer_id, buyer_name = counter_id, counter_name

        trades.append({
            "seller_id": seller_id,
            "seller_name": seller_name,
            "buyer_id": buyer_id,
            "buyer_name": buyer_name,
            "quantity": quantity,
            "price_per_share": price,
            "value": value,
            "closed_relative": closed,
            "closed_date": parse_relative_time(closed, now),
            "source_user_id": source_user_id,
        })

    return trades


async def scrape_user_ids(client: httpx.AsyncClient) -> list[dict]:
    """Scrape all user IDs from the paginated user directory."""
    print(f"Scraping user directory ({USER_DIR_PAGES} pages)...")
    sem = asyncio.Semaphore(CONCURRENCY)

    async def fetch_page(page: int):
        async with sem:
            html = await fetch(client, f"{BASE_URL}/users?page={page}")
            return parse_user_ids(html)

    tasks = [fetch_page(p) for p in range(1, USER_DIR_PAGES + 1)]
    results = await asyncio.gather(*tasks)

    all_users = []
    seen = set()
    for users in results:
        for u in users:
            if u["id"] not in seen:
                seen.add(u["id"])
                all_users.append(u)

    all_users.sort(key=lambda u: u["id"])
    print(f"  Found {len(all_users)} unique users")
    return all_users


async def scrape_trades(
    client: httpx.AsyncClient, users: list[dict]
) -> list[dict]:
    """Scrape trade history from each user's profile page."""
    print(f"Scraping trade history for {len(users)} users...")
    sem = asyncio.Semaphore(CONCURRENCY)
    all_trades = []
    completed = 0
    start = time.monotonic()
    now = datetime.now()

    async def fetch_user_trades(user: dict):
        nonlocal completed
        async with sem:
            html = await fetch(client, f"{BASE_URL}/users/{user['id']}")
            trades = parse_trades(html, user["id"], now)
            completed += 1
            if completed % 100 == 0 or completed == len(users):
                elapsed = time.monotonic() - start
                rate = completed / elapsed
                remaining = (len(users) - completed) / rate if rate > 0 else 0
                print(
                    f"  {completed}/{len(users)} users scraped "
                    f"({elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)"
                )
            return trades

    tasks = [fetch_user_trades(u) for u in users]
    results = await asyncio.gather(*tasks)

    for trades in results:
        all_trades.extend(trades)

    print(f"  Found {len(all_trades)} total trade entries (before dedup)")
    return all_trades


def dedup_trades(trades: list[dict]) -> list[dict]:
    """Deduplicate trades that appear on both parties' pages.

    Each trade appears once on the buyer's page and once on the seller's page.
    Dedup on (seller_id, buyer_id, quantity, price_per_share, value).
    """
    seen = {}
    for t in trades:
        key = (t["seller_id"], t["buyer_id"], t["quantity"], t["price_per_share"], t["value"])
        if key not in seen:
            seen[key] = {"trade": t, "count": 1}
        else:
            seen[key]["count"] += 1

    # For trades that appear exactly twice, that's the expected case (both sides).
    # For trades appearing more than twice, these are genuinely repeated identical trades
    # (same parties, same amount, same price but at different times). We keep N//2
    # (rounded up) of them since each real trade appears on two user pages.
    unique = []
    for key, entry in seen.items():
        count = (entry["count"] + 1) // 2  # ceil division
        for _ in range(count):
            unique.append(entry["trade"])

    return unique


async def main():
    cookie = get_cookie()
    OUTPUT_DIR.mkdir(exist_ok=True)

    async with httpx.AsyncClient(
        cookies={"_kmikeym_session": cookie},
        timeout=30,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=CONCURRENCY + 5),
    ) as client:
        # Step 1: Get all user IDs
        users = await scrape_user_ids(client)
        users_path = OUTPUT_DIR / "users.json"
        users_path.write_text(json.dumps(users, indent=2))
        print(f"  Saved to {users_path}")

        # Step 2: Scrape trades from each user page
        raw_trades = await scrape_trades(client, users)

        # Step 3: Deduplicate
        trades = dedup_trades(raw_trades)
        print(f"  {len(trades)} unique trades after dedup")

        # Save as JSON
        json_path = OUTPUT_DIR / "trades.json"
        json_path.write_text(json.dumps(trades, indent=2))
        print(f"  Saved to {json_path}")

        # Save as CSV
        csv_path = OUTPUT_DIR / "trades.csv"
        if trades:
            fieldnames = [
                "seller_id", "seller_name", "buyer_id", "buyer_name",
                "quantity", "price_per_share", "value",
                "closed_date", "closed_relative",
            ]
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(trades)
            print(f"  Saved to {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
