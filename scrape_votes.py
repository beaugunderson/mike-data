#!/usr/bin/env uv run
"""Scrape all vote data from vote.kmikeym.com."""

import asyncio
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://vote.kmikeym.com"
CONCURRENCY = 5
OUTPUT_DIR = Path("data")


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
            if resp.status_code == 404:
                return ""
            resp.raise_for_status()
            return resp.text
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            if attempt == 2:
                print(f"  FAILED {url}: {e}", file=sys.stderr)
                return ""
            await asyncio.sleep(2**attempt)
    return ""


def parse_question_ids(html: str) -> list[int]:
    """Extract all question IDs from the /k5m main page."""
    soup = BeautifulSoup(html, "lxml")
    ids = []
    for link in soup.find_all("a", href=True):
        m = re.match(r".*/k5m/questions/(\d+)$", link["href"])
        if m:
            ids.append(int(m[1]))
    return sorted(set(ids))


def parse_vote_page(html: str, question_id: int) -> dict | None:
    """Parse a single vote question page into structured data."""
    soup = BeautifulSoup(html, "lxml")

    question_div = soup.find("div", class_="h-entry")
    if not question_div:
        return None

    # Title
    title_span = question_div.find("span", class_="p-name")
    title = title_span.get_text(strip=True) if title_span else ""

    # Date
    time_el = question_div.find("time")
    date = time_el.get("datetime", "") if time_el else ""

    # Deadline (from countdown component)
    countdown = question_div.find("question-countdown")
    deadline = countdown.get("targetdate", "") if countdown else ""

    # Vote options
    options = []
    options_ul = question_div.find("ul", class_="options")
    if options_ul:
        for li in options_ul.find_all("li"):
            percent = int(li.get("data-percent", 0))
            label = li.get_text(strip=True)
            # Strip the leading percentage from the label text
            label = re.sub(r"^\d+%\s*", "", label)
            options.append({"label": label, "percent": percent})

    # Voted summary ("56 users voted with 9924 shares")
    voters = 0
    shares_voted = 0
    summary_p = question_div.find("p", class_="voted-summary")
    if summary_p:
        text = summary_p.get_text(strip=True)
        m = re.match(r"(\d+)\s+users?\s+voted\s+with\s+(\d+)\s+shares?", text)
        if m:
            voters = int(m[1])
            shares_voted = int(m[2])

    # Comments
    comments = []
    comments_ul = question_div.find("ul", class_="comments")
    if comments_ul:
        for li in comments_ul.find_all("li", class_="p-comment"):
            comment_id = li.get("id", "").replace("comment-", "")

            # Author info
            author_link = li.select_one(".p-author a.p-name")
            author_name = author_link.get_text(strip=True) if author_link else ""
            author_href = author_link.get("href", "") if author_link else ""
            author_id = author_href.rstrip("/").split("/")[-1] if author_href else ""

            # Share count from "[ N ]"
            share_span = li.select_one(".p-author span")
            share_count = 0
            if share_span:
                share_text = share_span.get_text(strip=True)
                m = re.search(r"\[\s*(\d+)\s*\]", share_text)
                if m:
                    share_count = int(m[1])

            # Comment text
            content_div = li.find("div", class_="e-content")
            comment_text = content_div.get_text(strip=True) if content_div else ""

            # Timestamp
            comment_time = li.find("time")
            comment_date = comment_time.get("datetime", "") if comment_time else ""

            comments.append({
                "comment_id": comment_id,
                "author_id": author_id,
                "author_name": author_name,
                "shares": share_count,
                "text": comment_text,
                "date": comment_date,
            })

    unique_commenters = list({c["author_id"] for c in comments if c["author_id"]})

    return {
        "question_id": question_id,
        "title": title,
        "date": date,
        "deadline": deadline,
        "options": options,
        "voters": voters,
        "shares_voted": shares_voted,
        "comment_count": len(comments),
        "unique_commenters": len(unique_commenters),
        "comments": comments,
    }


async def main():
    cookie = get_cookie()
    OUTPUT_DIR.mkdir(exist_ok=True)

    async with httpx.AsyncClient(
        cookies={"_kmikeym_session": cookie},
        timeout=30,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=CONCURRENCY + 5),
    ) as client:
        # Step 1: Get all question IDs by probing every ID 1–MAX
        # The /k5m index page only lists ~half the questions, so we check all IDs.
        MAX_ID = 200
        print(f"Probing question IDs 1–{MAX_ID}...")
        sem_probe = asyncio.Semaphore(CONCURRENCY)

        async def probe(qid: int) -> int | None:
            async with sem_probe:
                try:
                    resp = await client.get(f"{BASE_URL}/k5m/questions/{qid}")
                    return qid if resp.status_code == 200 else None
                except (httpx.HTTPError, httpx.TimeoutException):
                    return None

        probe_results = await asyncio.gather(*[probe(i) for i in range(1, MAX_ID + 1)])
        question_ids = sorted(qid for qid in probe_results if qid is not None)
        print(f"  Found {len(question_ids)} questions (IDs {question_ids[0]}–{question_ids[-1]})")

        # Step 2: Scrape each question page
        print(f"Scraping {len(question_ids)} vote pages...")
        sem = asyncio.Semaphore(CONCURRENCY)
        votes = []
        completed = 0
        start = time.monotonic()

        async def fetch_question(qid: int):
            nonlocal completed
            async with sem:
                html = await fetch(client, f"{BASE_URL}/k5m/questions/{qid}")
                result = parse_vote_page(html, qid) if html else None
                completed += 1
                if completed % 20 == 0 or completed == len(question_ids):
                    elapsed = time.monotonic() - start
                    print(f"  {completed}/{len(question_ids)} scraped ({elapsed:.0f}s)")
                return result

        tasks = [fetch_question(qid) for qid in question_ids]
        results = await asyncio.gather(*tasks)
        votes = [v for v in results if v]
        votes.sort(key=lambda v: v["question_id"])

        print(f"  Successfully parsed {len(votes)} votes")

        # Compute days_since_last_vote for each vote (sorted chronologically)
        for i, v in enumerate(votes):
            if i == 0 or not v["date"] or not votes[i - 1]["date"]:
                v["days_since_last_vote"] = None
            else:
                try:
                    cur = datetime.fromisoformat(v["date"])
                    prev = datetime.fromisoformat(votes[i - 1]["date"])
                    v["days_since_last_vote"] = (cur - prev).days
                except ValueError:
                    v["days_since_last_vote"] = None

        # Step 3: Save full data as JSON
        json_path = OUTPUT_DIR / "votes.json"
        json_path.write_text(json.dumps(votes, indent=2))
        print(f"  Saved to {json_path}")

        # Step 4: Save summary CSV (one row per vote)
        csv_path = OUTPUT_DIR / "votes.csv"
        fieldnames = [
            "question_id",
            "title",
            "date",
            "deadline",
            "voters",
            "shares_voted",
            "comment_count",
            "unique_commenters",
            "days_since_last_vote",
            "options_summary",
        ]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for v in votes:
                options_str = " | ".join(
                    f"{o['label']}: {o['percent']}%" for o in v["options"]
                )
                writer.writerow({
                    "question_id": v["question_id"],
                    "title": v["title"],
                    "date": v["date"],
                    "deadline": v["deadline"],
                    "voters": v["voters"],
                    "shares_voted": v["shares_voted"],
                    "comment_count": v["comment_count"],
                    "unique_commenters": v["unique_commenters"],
                    "days_since_last_vote": v["days_since_last_vote"] if v["days_since_last_vote"] is not None else "",
                    "options_summary": options_str,
                })
        print(f"  Saved to {csv_path}")

        # Step 5: Save comments CSV (one row per comment)
        comments_csv_path = OUTPUT_DIR / "vote_comments.csv"
        comment_fields = [
            "question_id",
            "question_title",
            "comment_id",
            "author_id",
            "author_name",
            "shares",
            "text",
            "date",
        ]
        with open(comments_csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=comment_fields)
            writer.writeheader()
            for v in votes:
                for c in v["comments"]:
                    writer.writerow({
                        "question_id": v["question_id"],
                        "question_title": v["title"],
                        **c,
                    })
        print(f"  Saved to {comments_csv_path}")

        # Print summary stats
        total_comments = sum(v["comment_count"] for v in votes)
        total_voters = sum(v["voters"] for v in votes)
        print(f"\nSummary:")
        print(f"  {len(votes)} votes scraped")
        print(f"  {total_voters} total vote participations")
        print(f"  {total_comments} total comments")


if __name__ == "__main__":
    asyncio.run(main())
