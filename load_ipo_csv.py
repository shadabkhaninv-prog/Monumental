#!/usr/bin/env python3
"""
Load chittorgarh IPO performance CSVs into bhav.ipobhav.

Reads all ipo-performance-mainline-*.csv from C:\\Users\\shada\\Monumental\\feeds

SYMBOL is resolved by fuzzy-matching company name against bhav.nse_symbols.
Rows with no symbol match are inserted with a derived symbol (cleaned company name).
Decimals are rounded to 2 places.

Usage:
  python load_ipo_csv.py
  python load_ipo_csv.py --file feeds\\ipo-performance-mainline-2025.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import mysql.connector

# ── Config ────────────────────────────────────────────────────────────────────
DB       = dict(host="localhost", port=3306, user="root", password="root", database="bhav")
FEEDS    = Path(r"C:\Users\shada\Monumental\feeds")

UPSERT = """
INSERT INTO ipobhav (SYMBOL, LISTING_DATE, LISTING_OPEN, LISTING_CLOSE, ISSUE_PRICE)
VALUES (%(symbol)s, %(listing_date)s, 0, %(listing_close)s, %(issue_price)s)
ON DUPLICATE KEY UPDATE
    LISTING_OPEN  = 0,
    LISTING_CLOSE = VALUES(LISTING_CLOSE),
    ISSUE_PRICE   = VALUES(ISSUE_PRICE);
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalise(name: str) -> str:
    """Lowercase, strip suffixes, keep alphanum only — for fuzzy matching."""
    s = name.lower()
    s = re.sub(r"\b(limited|ltd\.?|private|pvt\.?|public|co\.?|inc\.?|llp|services|"
               r"and|&)\b", " ", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.strip()

def to_date(text: str):
    text = text.strip()
    for fmt in ("%a, %b %d, %Y", "%d %b %Y", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None

def to_dec(text: str):
    t = re.sub(r"[₹,%\s]", "", str(text)).strip()
    if not t or t in ("-", "N.A.", "NA", "--"):
        return None
    try:
        return round(Decimal(t), 2)
    except (InvalidOperation, ValueError):
        return None

def derived_symbol(name: str) -> str:
    s = re.sub(r"\b(limited|ltd\.?|private|pvt\.?|inc\.?|llp|co\.?)\b", "", name, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9]+", "", s).upper()
    return (s or re.sub(r"\s+", "", name).upper())[:32]

# ── Symbol lookup ─────────────────────────────────────────────────────────────

def build_symbol_index(conn) -> dict[str, str]:
    """
    Return {normalised_company_name: SYMBOL} from bhav.nse_symbols.
    Falls back gracefully if the table doesn't exist.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT SYMBOL, COMPANY_NAME FROM nse_symbols")
        index = {normalise(name): sym for sym, name in cur.fetchall()}
        cur.close()
        print(f"  Loaded {len(index):,} entries from nse_symbols for symbol lookup")
        return index
    except Exception as e:
        print(f"  WARNING: nse_symbols not available ({e}) — will derive symbols from name")
        return {}

def resolve_symbol(company: str, index: dict[str, str]) -> str:
    key = normalise(company)
    if key in index:
        return index[key]
    # Partial match: find index entry whose key contains or is contained in key
    for idx_key, sym in index.items():
        if key and idx_key and (key in idx_key or idx_key in key):
            return sym
    return derived_symbol(company)

# ── CSV loading ───────────────────────────────────────────────────────────────

def load_csv(path: Path, sym_index: dict[str, str]) -> list[dict]:
    rows = []
    unmatched = []

    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # Normalise header keys (strip BOM / extra spaces)
        for raw in reader:
            row = {k.strip(): v.strip() for k, v in raw.items()}

            company      = row.get("Company Name", "").strip()
            listing_date = to_date(row.get("Listed On", ""))
            issue_price  = to_dec(row.get("Issue Price", ""))
            listing_close= to_dec(row.get("Listing Day Close", ""))

            if not company or not listing_date:
                continue

            symbol = resolve_symbol(company, sym_index)
            if symbol == derived_symbol(company):
                unmatched.append(company)

            rows.append({
                "symbol":        symbol,
                "listing_date":  listing_date,
                "listing_close": listing_close,
                "issue_price":   issue_price,
            })

    if unmatched:
        print(f"    {len(unmatched)} companies used derived symbol (no nse_symbols match):")
        for c in unmatched[:10]:
            print(f"      {c:50s} → {derived_symbol(c)}")
        if len(unmatched) > 10:
            print(f"      … and {len(unmatched)-10} more")

    return rows

# ── DB ────────────────────────────────────────────────────────────────────────

def upsert_rows(conn, rows: list[dict]) -> int:
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
    ap.add_argument("--file", default=None,
                    help="Load a single CSV file instead of the feeds folder")
    args = ap.parse_args()

    if args.file:
        files = [Path(args.file)]
    else:
        files = sorted(FEEDS.glob("ipo-performance-mainline-*.csv"))

    if not files:
        print(f"No ipo-performance-mainline-*.csv files found in {FEEDS}")
        return

    print(f"Feeds folder : {FEEDS}")
    print(f"Files found  : {len(files)}\n")

    conn = mysql.connector.connect(**DB)
    try:
        print("Building symbol lookup from nse_symbols …")
        sym_index = build_symbol_index(conn)

        total_rows = total_affected = 0
        for f in files:
            print(f"\nLoading {f.name} …")
            rows = load_csv(f, sym_index)
            affected = upsert_rows(conn, rows)
            print(f"  → {len(rows)} rows parsed, {affected} DB rows affected")
            total_rows += len(rows)
            total_affected += affected

        print(f"\nDone — {total_rows} rows loaded, {total_affected} DB rows affected "
              f"across {len(files)} file(s).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
