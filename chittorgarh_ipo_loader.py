#!/usr/bin/env python3
"""
Load mainline IPO listing data from chittorgarh.com into bhav.ipobhav.

Page columns  : Company Name | Listed On | Issue Price | Listing Day Close | Listing Day Gain
ipobhav cols  : SYMBOL       | LISTING_DATE | LISTING_OPEN | LISTING_CLOSE | ISSUE_PRICE
                                             (always 0)

Example:
  python chittorgarh_ipo_loader.py
  python chittorgarh_ipo_loader.py --start-year 2022 --end-year 2026
"""

from __future__ import annotations

import argparse
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

import mysql.connector
import requests
from bs4 import BeautifulSoup

# ── DB ────────────────────────────────────────────────────────────────────────
DB = dict(host="localhost", port=3306, user="root", password="root", database="bhav")

# ── Scraping ──────────────────────────────────────────────────────────────────
BASE_URL = "https://www.chittorgarh.com/ipo/ipo_perf_tracker.asp?exchange=mainline&year={year}"
DELAY    = 1.5
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Referer":    "https://www.chittorgarh.com/",
}

CURRENT_YEAR = datetime.now().year

# ── Helpers ───────────────────────────────────────────────────────────────────

def to_symbol(name: str) -> str:
    """Clean company name → SYMBOL (uppercase alphanum, max 32 chars)."""
    s = re.sub(r"\b(limited|ltd\.?|private|pvt\.?|inc\.?|llp|co\.?)\b", "", name, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9]+", "", s).upper()
    return (s or name.upper().replace(" ", ""))[:32]

def to_date(text: str):
    text = text.strip()
    for fmt in ("%a, %b %d, %Y", "%d %b %Y", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None

def to_decimal(text: str, default=None):
    t = re.sub(r"[₹,\s%]", "", text).strip()
    if not t or t in ("-", "N.A.", "NA", "N/A", "--"):
        return default
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return default

def cell(td) -> str:
    return re.sub(r"\s+", " ", td.get_text(" ")).strip()

# ── Scraper ───────────────────────────────────────────────────────────────────

def classify_header(h: str) -> str | None:
    """
    Map any header text → role using substring rules.
    Returns None if the column is not needed.
    """
    h = h.lower().strip()
    # company
    if "company" in h or h in ("ipo", "ipo name", "name"):
        return "company"
    # listing date  — check before plain "listing" to avoid misfire
    if ("list" in h and "date" in h) or h == "listed on":
        return "listing_date"
    # issue price
    if "issue price" in h or "issue pric" in h:
        return "issue_price"
    # listing close — "listing day close", "listing close", "close price" etc.
    if ("listing" in h or "listing day" in h) and "close" in h:
        return "listing_close"
    # listing open
    if ("listing" in h or "listing day" in h) and "open" in h:
        return "listing_open"
    return None   # gain, high, low, current — not stored


def fetch_year(session: requests.Session, year: int) -> list[dict]:
    url = BASE_URL.format(year=year)
    print(f"  {year}: GET {url}", end=" ... ", flush=True)

    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"FAILED ({e})")
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # Find best table: most data rows that mention IPO-ish words anywhere
    keywords = {"company", "issue price", "listing", "ipo"}
    best, best_tbl = 0, None
    for tbl in soup.find_all("table"):
        text = tbl.get_text(" ").lower()
        hits = sum(1 for k in keywords if k in text)
        rows = len(tbl.find_all("tr"))
        if hits * rows > best:
            best, best_tbl = hits * rows, tbl

    if not best_tbl:
        print("no table found")
        return []

    all_rows = best_tbl.find_all("tr")

    # Try first 6 rows as candidate headers; pick the one with most role hits
    header_idx, col_map = 0, {}
    best_hits = 0
    for i, tr in enumerate(all_rows[:6]):
        cells = tr.find_all(["th", "td"])
        raw_texts = [re.sub(r"\s+", " ", c.get_text(" ")).strip() for c in cells]
        print(f"\n    row[{i}] : {raw_texts}")
        mapping = {}
        seen_roles: set[str] = set()
        for j, txt in enumerate(raw_texts):
            role = classify_header(txt)
            if role and role not in seen_roles:
                mapping[j] = role
                seen_roles.add(role)
        print(f"           → {mapping}")
        if len(mapping) > best_hits:
            best_hits, col_map, header_idx = len(mapping), mapping, i

    print(f"\n    chosen header row[{header_idx}], mapped={col_map}")

    roles = set(col_map.values())
    if "company" not in roles or "listing_date" not in roles:
        print(f"    WARNING: essential columns missing — skipping {year}")
        return []

    records = []
    for tr in all_rows[header_idx + 1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        row: dict = {}
        for col_idx, role in col_map.items():
            if col_idx >= len(cells):
                continue
            val = cell(cells[col_idx])
            if role == "company":
                # first line of cell is the company name; strip sub-links
                first_line = val.split("\n")[0].split("IPO Detail")[0].strip()
                row["company"] = first_line
            elif role == "listing_date":
                row["listing_date"] = to_date(val)
            elif role == "issue_price":
                row["issue_price"] = to_decimal(val)
            elif role == "listing_close":
                row["listing_close"] = to_decimal(val)

        company = row.get("company", "").strip()
        listing_date = row.get("listing_date")

        if not company or re.fullmatch(r"[\d\s]+", company):
            continue
        if not listing_date:
            continue

        records.append({
            "symbol":       to_symbol(company),
            "listing_date": listing_date,
            "listing_open": Decimal("0"),
            "listing_close": row.get("listing_close"),
            "issue_price":   row.get("issue_price"),
        })

    print(f"  → {len(records)} rows")
    return records

# ── DB ────────────────────────────────────────────────────────────────────────

UPSERT = """
INSERT INTO ipobhav (SYMBOL, LISTING_DATE, LISTING_OPEN, LISTING_CLOSE, ISSUE_PRICE)
VALUES (%(symbol)s, %(listing_date)s, %(listing_open)s, %(listing_close)s, %(issue_price)s)
ON DUPLICATE KEY UPDATE
    LISTING_OPEN  = VALUES(LISTING_OPEN),
    LISTING_CLOSE = VALUES(LISTING_CLOSE),
    ISSUE_PRICE   = VALUES(ISSUE_PRICE);
"""

def upsert(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    cur = conn.cursor()
    cur.executemany(UPSERT, rows)
    conn.commit()
    n = cur.rowcount
    cur.close()
    return n

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-year", type=int, default=CURRENT_YEAR - 9)
    ap.add_argument("--end-year",   type=int, default=CURRENT_YEAR)
    ap.add_argument("--delay",      type=float, default=DELAY)
    args = ap.parse_args()

    years = list(range(args.start_year, args.end_year + 1))
    print(f"Chittorgarh IPO loader  |  years {years[0]}–{years[-1]}  |  table: bhav.ipobhav\n")

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.chittorgarh.com/", timeout=10)
    except Exception:
        pass

    conn = mysql.connector.connect(**DB)
    total = 0
    try:
        for i, year in enumerate(years):
            print(f"[{i+1}/{len(years)}]", end=" ")
            rows = fetch_year(session, year)
            affected = upsert(conn, rows)
            print(f"  DB: {affected} rows affected")
            total += len(rows)
            if i < len(years) - 1:
                time.sleep(args.delay)
    finally:
        conn.close()

    print(f"\nDone — {total} rows across {len(years)} years.")

if __name__ == "__main__":
    main()
