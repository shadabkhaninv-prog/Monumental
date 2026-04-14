#!/usr/bin/env python3
"""
Update NSE Smallcap index EOD rows in bhav.indexbhav.

Source:
  Official NSE daily index archive CSV:
  https://nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv

Updates:
  - NIFTY SMALLCAP 100
  - NIFTY SMALLCAP 250

Behavior:
  - Defaults to filling from the last date present in indexbhav + 1 day
    through today.
  - Uses INSERT ... ON DUPLICATE KEY UPDATE on (symbol, mktdate).
  - Skips dates whose archive file is unavailable, which naturally handles
    weekends and exchange holidays.

Examples:
  python update_indexbhav_smallcaps.py
  python update_indexbhav_smallcaps.py --from-date 2026-04-01 --to-date 2026-04-13
  python update_indexbhav_smallcaps.py --date 2026-04-10 --dry-run
"""

from __future__ import annotations

import argparse
import io
from datetime import date, datetime, timedelta

import mysql.connector
import pandas as pd
import requests


DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "bhav"

TARGET_INDICES = {
    "NIFTY SMALLCAP 100": "NIFTY SMALLCAP 100",
    "NIFTY SMALLCAP 250": "NIFTY SMALLCAP 250",
}

NSE_ARCHIVE_URL = "https://nsearchives.nseindia.com/content/indices/ind_close_all_{ddmmyyyy}.csv"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/reports-indices-historical-index-data",
}


def parse_iso_date(text: str) -> date:
    return datetime.strptime(text.strip(), "%Y-%m-%d").date()


def daterange(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def get_conn():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )


def normalize_index_name(value: str) -> str:
    return " ".join(str(value).strip().upper().split())


def to_float_or_none(value):
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return float(parsed)


def get_default_start_date(conn) -> date:
    sql = """
        SELECT MAX(mktdate)
        FROM indexbhav
        WHERE UPPER(symbol) IN (%s, %s)
    """
    cur = conn.cursor()
    cur.execute(sql, tuple(TARGET_INDICES.keys()))
    last_date = cur.fetchone()[0]
    cur.close()
    if last_date:
        return last_date + timedelta(days=1)
    return date.today() - timedelta(days=7)


def fetch_nse_index_csv(run_date: date, session: requests.Session) -> pd.DataFrame:
    url = NSE_ARCHIVE_URL.format(ddmmyyyy=run_date.strftime("%d%m%Y"))
    response = session.get(url, headers=REQUEST_HEADERS, timeout=25)
    if response.status_code == 404:
        return pd.DataFrame()
    response.raise_for_status()
    text = response.text.strip()
    if not text:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(text))


def extract_target_rows(raw_df: pd.DataFrame, run_date: date) -> list[dict]:
    if raw_df.empty:
        return []

    work = raw_df.copy()
    work["normalized_name"] = work["Index Name"].map(normalize_index_name)
    work = work[work["normalized_name"].isin(TARGET_INDICES)]
    if work.empty:
        return []

    rows: list[dict] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "symbol": TARGET_INDICES[row["normalized_name"]],
                "mktdate": run_date,
                "open": to_float_or_none(row.get("Open Index Value")),
                "high": to_float_or_none(row.get("High Index Value")),
                "low": to_float_or_none(row.get("Low Index Value")),
                "close": to_float_or_none(row.get("Closing Index Value")),
                "diff": to_float_or_none(row.get("Change(%)")),
            }
        )
    return rows


def upsert_rows(conn, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = """
        INSERT INTO indexbhav (symbol, mktdate, open, high, low, close, diff)
        VALUES (%(symbol)s, %(mktdate)s, %(open)s, %(high)s, %(low)s, %(close)s, %(diff)s)
        ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            diff = VALUES(diff)
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    affected = cur.rowcount
    conn.commit()
    cur.close()
    return affected


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Update NSE Smallcap 100 and Smallcap 250 rows in bhav.indexbhav"
    )
    parser.add_argument("--date", help="Single date in YYYY-MM-DD format.")
    parser.add_argument("--from-date", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--to-date", help="End date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--host", default=DB_HOST)
    parser.add_argument("--port", type=int, default=DB_PORT)
    parser.add_argument("--user", default=DB_USER)
    parser.add_argument("--password", default=DB_PASS)
    parser.add_argument("--database", default=DB_NAME)
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print rows without writing to MySQL.")
    return parser


def resolve_dates(args, conn) -> tuple[date, date]:
    if args.date:
        single = parse_iso_date(args.date)
        return single, single

    start_date = parse_iso_date(args.from_date) if args.from_date else get_default_start_date(conn)
    end_date = parse_iso_date(args.to_date) if args.to_date else date.today()
    if start_date > end_date:
        raise ValueError("from-date cannot be later than to-date")
    return start_date, end_date


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    global DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
    DB_HOST = args.host
    DB_PORT = args.port
    DB_USER = args.user
    DB_PASS = args.password
    DB_NAME = args.database

    conn = get_conn()
    try:
        start_date, end_date = resolve_dates(args, conn)
        print(f"Updating indexbhav from {start_date} to {end_date}")

        session = requests.Session()
        total_rows = 0
        touched_dates = 0
        missing_dates: list[str] = []

        for run_date in daterange(start_date, end_date):
            print(f"[{run_date}] Fetching NSE index archive...")
            raw_df = fetch_nse_index_csv(run_date, session)
            rows = extract_target_rows(raw_df, run_date)

            if not rows:
                missing_dates.append(run_date.isoformat())
                print("  No Smallcap 100/250 data found for this date.")
                continue

            touched_dates += 1
            if args.dry_run:
                print("  Dry run rows:")
                for row in rows:
                    print(f"    {row}")
                total_rows += len(rows)
                continue

            affected = upsert_rows(conn, rows)
            total_rows += len(rows)
            print(f"  Upserted {len(rows)} rows ({affected} MySQL affected-row count).")

        print("")
        print(f"Dates processed with data: {touched_dates}")
        print(f"Index rows prepared/upserted: {total_rows}")
        if missing_dates:
            print(f"Dates skipped (no NSE archive or no target rows): {', '.join(missing_dates)}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
