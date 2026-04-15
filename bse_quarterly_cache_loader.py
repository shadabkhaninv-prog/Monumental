#!/usr/bin/env python3
"""
Populate bhav.quarterly_fundamentals from BSE XBRL for the top-turnover universe.

Example:
    python bse_quarterly_cache_loader.py 2024-08-05 --top-turnover 100 --quarters 10
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime

import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

import quarterly_fundamentals_report as qfr
import screener_top_sales_yoy as cache_mod


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load BSE XBRL quarterly data into bhav.quarterly_fundamentals."
    )
    parser.add_argument("cutoff_date", help="As-of date in YYYY-MM-DD format.")
    parser.add_argument(
        "--top-turnover",
        type=int,
        default=100,
        help="Number of stocks to take by avg 42D turnover (default: 100).",
    )
    parser.add_argument(
        "--turnover-days",
        type=int,
        default=42,
        help="Trading-day lookback for avg turnover (default: 42).",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        default=10,
        help="How many quarters to retain from BSE XBRL per symbol (default: 10).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.75,
        help="Delay between network requests (default: 0.75).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retries (default: 3).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional cap for quick testing.",
    )
    args = parser.parse_args()
    args.cutoff_date = datetime.strptime(args.cutoff_date, "%Y-%m-%d").date()
    return args


def build_session(retries: int) -> requests.Session:
    session = requests.Session()
    session.headers.update(qfr.DEFAULT_HEADERS)
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=0.8,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def main() -> int:
    args = parse_args()
    print("=" * 72)
    print("BSE QUARTERLY CACHE LOADER")
    print("=" * 72)
    print(f"Cutoff date        : {args.cutoff_date}")
    print(f"Top turnover count : {args.top_turnover}")
    print(f"Turnover lookback  : {args.turnover_days} trading days")
    print(f"Quarter count      : {args.quarters}")
    print(f"Cache table        : {cache_mod.CACHE_TABLE}")
    print("=" * 72)

    conn = cache_mod.get_db_connection()
    try:
        cache_mod.ensure_cache_table(conn)
        turnover_df = cache_mod.fetch_turnover_universe(conn, args.cutoff_date, args.turnover_days)
        replacements = cache_mod.load_symbol_replacements(conn)

        turnover_df["screener_symbol"] = turnover_df["symbol"].map(
            lambda value: replacements.get(str(value).upper(), str(value).upper())
        )
        turnover_df["replacement_applied"] = turnover_df["symbol"] != turnover_df["screener_symbol"]
        turnover_df = turnover_df.drop_duplicates(subset=["symbol"], keep="first")
        turnover_df = turnover_df.head(args.top_turnover).reset_index(drop=True)
        if args.limit is not None:
            turnover_df = turnover_df.head(args.limit).reset_index(drop=True)

        session = build_session(args.retries)
        loaded = 0
        failed_rows: list[dict[str, object]] = []

        total = len(turnover_df)
        for index, row in turnover_df.iterrows():
            symbol = str(row["symbol"]).upper()
            cache_symbol = str(row["screener_symbol"]).upper()
            print(f"[{index + 1}/{total}] {symbol} -> BSE load")
            try:
                bse_df, note = qfr.fetch_bse_quarterly_long(
                    session=session,
                    symbol=symbol,
                    cutoff=args.cutoff_date,
                    quarter_count=args.quarters,
                    delay=args.delay,
                    timeout=args.timeout,
                )
                if not bse_df.empty:
                    cache_mod.cache_quarterly_rows(
                        conn=conn,
                        symbol=symbol,
                        screener_symbol=cache_symbol,
                        long_df=bse_df,
                        load_source="bse_xbrl",
                    )
                    loaded += 1
                    print(
                        f"  OK ({len(bse_df)} quarters, latest {bse_df['quarter_label'].iloc[-1]}, "
                        f"BSE {bse_df['bse_code'].iloc[-1]})"
                    )
                else:
                    failed_rows.append({"symbol": symbol, "error": "No BSE rows returned"})
            except Exception as exc:  # noqa: BLE001
                failed_rows.append({"symbol": symbol, "error": str(exc)})
                print(f"  FAIL: {exc}")
            if index < total - 1 and args.delay > 0:
                time.sleep(args.delay)

        print("=" * 72)
        print(f"Loaded symbols     : {loaded}")
        print(f"Failed symbols     : {len(failed_rows)}")
        if failed_rows:
            sample = pd.DataFrame(failed_rows).head(20)
            print(sample.to_string(index=False))
        print("=" * 72)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
