#!/usr/bin/env python3
"""
Get the top YoY sales-growth stocks from Screener among the top-turnover universe.

Example:
    python screener_top_sales_yoy.py 2024-08-05
    python screener_top_sales_yoy.py 2024-08-05 --top-turnover 200 --top-sales 20

Outputs:
    reports/screener_top_sales_yoy_<ddmmmyyyy>.xlsx
    reports/screener_top_sales_yoy_<ddmmmyyyy>.csv
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pymysql
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import quarterly_fundamentals_report as qfr


DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "bhav",
    "charset": "utf8mb4",
    "autocommit": True,
}

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
TURNOVER_CRORE_DIVISOR = 10_000_000.0
CACHE_TABLE = "quarterly_fundamentals"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch the latest-quarter sales YoY growth from Screener for the "
            "top-turnover NSE stock universe."
        )
    )
    parser.add_argument("cutoff_date", help="As-of date in YYYY-MM-DD format.")
    parser.add_argument(
        "--top-turnover",
        type=int,
        default=200,
        help="Number of stocks to keep by average 42D turnover (default: 200).",
    )
    parser.add_argument(
        "--top-sales",
        type=int,
        default=20,
        help="Number of final names to keep by sales YoY growth (default: 20).",
    )
    parser.add_argument(
        "--turnover-days",
        type=int,
        default=42,
        help="Trading-day lookback for average turnover ranking (default: 42).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay in seconds between Screener requests (default: 0.6).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=45.0,
        help="HTTP timeout in seconds (default: 45).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retry attempts (default: 3).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPORTS_DIR),
        help=f"Directory for output files (default: {REPORTS_DIR}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional cap on Screener fetches, useful for quick testing.",
    )
    args = parser.parse_args()

    try:
        args.cutoff_date = datetime.strptime(args.cutoff_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(
            f"Invalid cutoff_date: {args.cutoff_date}. Expected YYYY-MM-DD."
        ) from exc

    if args.top_turnover < 1:
        raise SystemExit("--top-turnover must be at least 1.")
    if args.top_sales < 1:
        raise SystemExit("--top-sales must be at least 1.")
    if args.turnover_days < 5:
        raise SystemExit("--turnover-days must be at least 5.")

    args.output_dir = Path(args.output_dir).expanduser().resolve()
    return args


def get_db_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as exc:
        raise SystemExit(f"Failed to connect to MySQL bhav database: {exc}") from exc


def query_df(conn, sql: str, params: list[object] | None = None) -> pd.DataFrame:
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or [])
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
    finally:
        cursor.close()
    return pd.DataFrame(rows, columns=columns)


def ensure_cache_table(conn) -> None:
    sql = f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
          idquarterly_fundamentals INT(11) NOT NULL AUTO_INCREMENT,
          SYMBOL VARCHAR(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          SCREENER_SYMBOL VARCHAR(256) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          QUARTER_LABEL VARCHAR(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          QUARTER_END DATE DEFAULT NULL,
          SALES DOUBLE DEFAULT NULL,
          SALES_YOY_PCT DOUBLE DEFAULT NULL,
          PROFIT DOUBLE DEFAULT NULL,
          PROFIT_YOY_PCT DOUBLE DEFAULT NULL,
          STATEMENT_USED VARCHAR(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          SOURCE_URL VARCHAR(1024) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          LOAD_SOURCE VARCHAR(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          LAST_REFRESHED_AT DATETIME DEFAULT NULL,
          PRIMARY KEY (idquarterly_fundamentals),
          UNIQUE KEY SYMBOL_QUARTER (SYMBOL, QUARTER_END),
          KEY SCREENER_SYMBOL_QUARTER (SCREENER_SYMBOL, QUARTER_END),
          KEY QUARTER_END (QUARTER_END)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()


def normalize_date_value(value) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def normalize_datetime_value(value) -> datetime | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def get_cached_quarter_row(conn, symbol: str, target_quarter_end: date) -> dict[str, object] | None:
    sql = f"""
        SELECT
            SYMBOL,
            SCREENER_SYMBOL,
            QUARTER_LABEL,
            QUARTER_END,
            SALES,
            SALES_YOY_PCT,
            PROFIT,
            PROFIT_YOY_PCT,
            STATEMENT_USED,
            SOURCE_URL,
            LOAD_SOURCE,
            LAST_REFRESHED_AT
        FROM {CACHE_TABLE}
        WHERE SYMBOL = %s
          AND QUARTER_END = %s
        LIMIT 1
    """
    df = query_df(conn, sql, [symbol.upper(), target_quarter_end])
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    row["QUARTER_END"] = normalize_date_value(row.get("QUARTER_END"))
    row["LAST_REFRESHED_AT"] = normalize_datetime_value(row.get("LAST_REFRESHED_AT"))
    return row


def cache_quarterly_rows(
    conn,
    symbol: str,
    screener_symbol: str,
    long_df: pd.DataFrame,
    load_source: str = "screener",
) -> None:
    if long_df.empty:
        return
    sql = f"""
        INSERT INTO {CACHE_TABLE} (
            SYMBOL, SCREENER_SYMBOL, QUARTER_LABEL, QUARTER_END,
            SALES, SALES_YOY_PCT, PROFIT, PROFIT_YOY_PCT,
            STATEMENT_USED, SOURCE_URL, LOAD_SOURCE, LAST_REFRESHED_AT
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            SCREENER_SYMBOL = VALUES(SCREENER_SYMBOL),
            QUARTER_LABEL = VALUES(QUARTER_LABEL),
            SALES = VALUES(SALES),
            SALES_YOY_PCT = VALUES(SALES_YOY_PCT),
            PROFIT = VALUES(PROFIT),
            PROFIT_YOY_PCT = VALUES(PROFIT_YOY_PCT),
            STATEMENT_USED = VALUES(STATEMENT_USED),
            SOURCE_URL = VALUES(SOURCE_URL),
            LOAD_SOURCE = VALUES(LOAD_SOURCE),
            LAST_REFRESHED_AT = VALUES(LAST_REFRESHED_AT)
    """
    now_ts = datetime.now()
    payload = []
    for _, row in long_df.iterrows():
        payload.append(
            (
                symbol.upper(),
                screener_symbol.upper(),
                str(row.get("quarter_label") or ""),
                normalize_date_value(row.get("quarter_end")),
                None if pd.isna(row.get("sales")) else float(row.get("sales")),
                None if pd.isna(row.get("sales_yoy_pct")) else float(row.get("sales_yoy_pct")),
                None if pd.isna(row.get("net_profit", row.get("profit"))) else float(row.get("net_profit", row.get("profit"))),
                None if pd.isna(row.get("profit_yoy_pct")) else float(row.get("profit_yoy_pct")),
                str(row.get("statement_used") or ""),
                str(row.get("source_url") or ""),
                load_source,
                now_ts,
            )
        )
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, payload)
    finally:
        cursor.close()


def get_existing_bhav_tables(conn, start_date: date, cutoff_date: date) -> list[str]:
    wanted = [f"bhav{year}" for year in range(start_date.year, cutoff_date.year + 1)]
    existing: list[str] = []
    cursor = conn.cursor()
    try:
        for table_name in wanted:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if cursor.fetchone():
                existing.append(table_name)
    finally:
        cursor.close()
    return existing


def fetch_turnover_universe(conn, cutoff_date: date, turnover_days: int) -> pd.DataFrame:
    start_date = cutoff_date - timedelta(days=120)
    tables = get_existing_bhav_tables(conn, start_date, cutoff_date)
    if not tables:
        raise SystemExit("No bhav yearly tables found for the requested period.")

    parts: list[str] = []
    params: list[object] = []
    for table_name in tables:
        parts.append(
            f"""
            SELECT
                UPPER(SYMBOL) AS symbol,
                MKTDATE AS trade_date,
                CLOSE,
                VOLUME
            FROM {table_name}
            WHERE MKTDATE BETWEEN %s AND %s
            """
        )
        params.extend([start_date, cutoff_date])

    sql = f"""
        SELECT symbol, trade_date, CLOSE, VOLUME
        FROM (
            {" UNION ALL ".join(parts)}
        ) turnover_rows
        ORDER BY symbol, trade_date
    """
    df = query_df(conn, sql, params)
    if df.empty:
        raise SystemExit(
            f"No bhav turnover data found between {start_date} and {cutoff_date}."
        )

    df["turnover"] = (
        pd.to_numeric(df["CLOSE"], errors="coerce")
        * pd.to_numeric(df["VOLUME"], errors="coerce")
    )
    df = df.dropna(subset=["turnover"])
    df = df.groupby("symbol", group_keys=False).tail(turnover_days)

    summary = (
        df.groupby("symbol", as_index=False)
        .agg(
            avg_turnover=("turnover", "mean"),
            median_turnover=("turnover", "median"),
            latest_close=("CLOSE", "last"),
            traded_days=("trade_date", "count"),
            last_trade_date=("trade_date", "max"),
        )
        .sort_values(["avg_turnover", "symbol"], ascending=[False, True])
        .reset_index(drop=True)
    )
    summary["avg_turnover_cr"] = summary["avg_turnover"] / TURNOVER_CRORE_DIVISOR
    summary["median_turnover_cr"] = summary["median_turnover"] / TURNOVER_CRORE_DIVISOR
    return summary


def load_symbol_replacements(conn) -> dict[str, str]:
    sql = """
        SELECT UPPER(symbol) AS symbol, UPPER(TRIM(new_symbol)) AS new_symbol
        FROM inactive_symbols
        WHERE new_symbol IS NOT NULL
          AND TRIM(new_symbol) <> ''
    """
    try:
        df = query_df(conn, sql)
    except Exception:
        return {}
    if df.empty:
        return {}
    return dict(zip(df["symbol"], df["new_symbol"]))


def build_screener_session(retries: int) -> requests.Session:
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


def force_delete_if_open(path: Path) -> None:
    """Delete an output file, trying to close it first if Excel has it open."""
    if not path.exists():
        return
    try:
        path.unlink()
        print(f"Deleted old file: {path.name}")
        return
    except PermissionError:
        pass

    print(f"File is open in Excel - attempting to close it: {path.name}")

    closed = False
    try:
        import win32com.client

        xl = win32com.client.GetActiveObject("Excel.Application")
        for wb in list(xl.Workbooks):
            if os.path.normcase(wb.FullName) == os.path.normcase(str(path.resolve())):
                wb.Close(SaveChanges=False)
                closed = True
                print("Closed workbook via Excel COM.")
                break
    except Exception:
        pass

    if not closed:
        try:
            ps = (
                "$xl = [Runtime.InteropServices.Marshal]"
                "::GetActiveObject('Excel.Application'); "
                f"$xl.Workbooks | Where-Object {{ $_.FullName -eq '{str(path.resolve())}' }}"
                " | ForEach-Object { $_.Close($false) }"
            )
            subprocess.run(["powershell", "-Command", ps], capture_output=True, timeout=10)
            closed = True
            print("Closed workbook via PowerShell.")
        except Exception:
            pass

    time.sleep(0.5)
    try:
        path.unlink()
        print(f"Deleted old file: {path.name}")
    except PermissionError as exc:
        raise SystemExit(
            f"Still cannot delete {path}. Please close Excel completely and re-run."
        ) from exc


def resolve_target_quarter_from_bse(
    session: requests.Session,
    symbol: str,
    cutoff_date: date,
    delay: float,
    timeout: float,
) -> dict[str, object]:
    bse_code, note = qfr.resolve_bse_code(session, symbol, timeout)
    if not bse_code:
        raise ValueError(note)

    rows_df = qfr.fetch_bse_quarter_rows(
        session=session,
        security_code=bse_code,
        cutoff=cutoff_date,
        delay=delay,
        timeout=timeout,
    )
    if rows_df.empty:
        raise ValueError(f"No BSE consolidated quarterly rows found on or before {cutoff_date}.")

    cutoff_ts = pd.Timestamp(datetime.combine(cutoff_date, datetime.max.time()))
    eligible = rows_df[rows_df["effective_dt"].notna() & (rows_df["effective_dt"] <= cutoff_ts)].copy()
    if eligible.empty:
        raise ValueError(f"No BSE consolidated quarter was reported by cutoff date {cutoff_date}.")

    chosen = eligible.sort_values(["quarter_end", "effective_dt"]).iloc[-1]
    return {
        "bse_code": bse_code,
        "bse_note": note,
        "target_quarter_end": chosen["quarter_end"],
        "target_quarter_label": chosen["quarter_label"],
        "filed_at": chosen["filed_at"],
        "revised_at": chosen["revised_at"],
        "effective_dt": chosen["effective_dt"],
    }


def extract_latest_sales_yoy(
    conn,
    session: requests.Session,
    screener_symbol: str,
    bse_symbol: str,
    cutoff_date: date,
    delay: float,
    timeout: float,
) -> dict[str, object]:
    target = resolve_target_quarter_from_bse(
        session=session,
        symbol=bse_symbol,
        cutoff_date=cutoff_date,
        delay=delay,
        timeout=timeout,
    )
    cached = get_cached_quarter_row(conn, bse_symbol, target["target_quarter_end"])
    if cached is not None:
        return {
            "quarter_label": cached["QUARTER_LABEL"],
            "quarter_end": cached["QUARTER_END"],
            "sales": cached["SALES"],
            "sales_yoy_pct": cached["SALES_YOY_PCT"],
            "profit": cached["PROFIT"],
            "profit_yoy_pct": cached["PROFIT_YOY_PCT"],
            "statement_used": cached["STATEMENT_USED"],
            "source_url": cached["SOURCE_URL"],
            "bse_code": target["bse_code"],
            "bse_filed_at": target["filed_at"],
            "bse_revised_at": target["revised_at"],
            "bse_effective_dt": target["effective_dt"],
            "cache_status": "hit",
            "cache_refreshed_at": cached["LAST_REFRESHED_AT"],
        }
    table, source_url, statement_used = qfr.fetch_quarterly_table(
        session=session,
        symbol=screener_symbol,
        statement_mode="auto",
        timeout=timeout,
    )
    long_df = qfr.quarterly_table_to_long(
        symbol=screener_symbol,
        table=table,
        cutoff=cutoff_date,
        quarter_count=8,
        source_url=source_url,
        statement_used=statement_used,
    )
    cache_quarterly_rows(conn, bse_symbol, screener_symbol, long_df)
    matched = long_df[long_df["quarter_end"] == target["target_quarter_end"]].copy()
    if matched.empty:
        raise ValueError(
            f"Screener does not have quarter {target['target_quarter_label']} selected by BSE cutoff gate."
        )
    latest = matched.sort_values("quarter_end").iloc[-1]
    return {
        "quarter_label": latest["quarter_label"],
        "quarter_end": latest["quarter_end"],
        "sales": latest["sales"],
        "sales_yoy_pct": latest["sales_yoy_pct"],
        "profit": latest["net_profit"],
        "profit_yoy_pct": latest["profit_yoy_pct"],
        "statement_used": latest["statement_used"],
        "source_url": latest["source_url"],
        "bse_code": target["bse_code"],
        "bse_filed_at": target["filed_at"],
        "bse_revised_at": target["revised_at"],
        "bse_effective_dt": target["effective_dt"],
        "cache_status": "miss_loaded",
        "cache_refreshed_at": datetime.now(),
    }


def write_outputs(
    output_dir: Path,
    cutoff_date: date,
    top20_df: pd.DataFrame,
    universe_df: pd.DataFrame,
    failures_df: pd.DataFrame,
    summary_df: pd.DataFrame,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = cutoff_date.strftime("%d%b%Y").lower()
    excel_path = output_dir / f"screener_top_sales_yoy_{stamp}.xlsx"
    csv_path = output_dir / f"screener_top_sales_yoy_{stamp}.csv"

    force_delete_if_open(excel_path)
    force_delete_if_open(csv_path)
    top20_df.to_csv(csv_path, index=False)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        top20_df.to_excel(writer, sheet_name="Top20", index=False)
        universe_df.to_excel(writer, sheet_name="TopTurnoverUniverse", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        if not failures_df.empty:
            failures_df.to_excel(writer, sheet_name="Failures", index=False)

        for sheet_name, df in (
            ("Top20", top20_df),
            ("TopTurnoverUniverse", universe_df),
            ("Summary", summary_df),
        ):
            qfr.autofit_excel_columns(writer, sheet_name, df)
        if not failures_df.empty:
            qfr.autofit_excel_columns(writer, "Failures", failures_df)

    return excel_path, csv_path


def main() -> int:
    args = parse_args()
    print("=" * 72)
    print("TOP SALES YOY FROM SCREENER")
    print("=" * 72)
    print(f"Cutoff date        : {args.cutoff_date}")
    print(f"Top turnover count : {args.top_turnover}")
    print(f"Top sales count    : {args.top_sales}")
    print(f"Turnover lookback  : {args.turnover_days} trading days")
    print(f"Output dir         : {args.output_dir}")
    print("=" * 72)

    conn = get_db_connection()
    try:
        ensure_cache_table(conn)
        turnover_df = fetch_turnover_universe(conn, args.cutoff_date, args.turnover_days)
        replacements = load_symbol_replacements(conn)
        turnover_df["screener_symbol"] = turnover_df["symbol"].map(
            lambda value: replacements.get(str(value).upper(), str(value).upper())
        )
        turnover_df["replacement_applied"] = turnover_df["symbol"] != turnover_df["screener_symbol"]
        turnover_df = turnover_df.drop_duplicates(subset=["screener_symbol"], keep="first")
        turnover_df = turnover_df.head(args.top_turnover).reset_index(drop=True)

        if args.limit is not None:
            turnover_df = turnover_df.head(args.limit).reset_index(drop=True)

        session = build_screener_session(args.retries)
        results: list[dict[str, object]] = []
        failures: list[dict[str, object]] = []
        cache_hits = 0
        cache_misses = 0

        total = len(turnover_df)
        for index, row in turnover_df.iterrows():
            symbol = str(row["symbol"]).upper()
            screener_symbol = str(row["screener_symbol"]).upper()
            print(f"[{index + 1}/{total}] {symbol} -> {screener_symbol}")
            try:
                latest = extract_latest_sales_yoy(
                    conn=conn,
                    session=session,
                    screener_symbol=screener_symbol,
                    bse_symbol=symbol,
                    cutoff_date=args.cutoff_date,
                    delay=args.delay,
                    timeout=args.timeout,
                )
                if latest["cache_status"] == "hit":
                    cache_hits += 1
                else:
                    cache_misses += 1
                results.append(
                    {
                        "symbol": symbol,
                        "screener_symbol": screener_symbol,
                        "quarter_label": latest["quarter_label"],
                        "quarter_end": latest["quarter_end"],
                        "sales": latest["sales"],
                        "sales_yoy_pct": latest["sales_yoy_pct"],
                        "profit": latest["profit"],
                        "profit_yoy_pct": latest["profit_yoy_pct"],
                        "avg_turnover_cr": row["avg_turnover_cr"],
                        "median_turnover_cr": row["median_turnover_cr"],
                        "latest_close": row["latest_close"],
                        "traded_days": row["traded_days"],
                        "last_trade_date": row["last_trade_date"],
                        "statement_used": latest["statement_used"],
                        "source_url": latest["source_url"],
                        "bse_code": latest["bse_code"],
                        "bse_filed_at": latest["bse_filed_at"],
                        "bse_revised_at": latest["bse_revised_at"],
                        "bse_effective_dt": latest["bse_effective_dt"],
                        "cache_status": latest["cache_status"],
                        "cache_refreshed_at": latest["cache_refreshed_at"],
                        "replacement_applied": bool(row["replacement_applied"]),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "symbol": symbol,
                        "screener_symbol": screener_symbol,
                        "avg_turnover_cr": row["avg_turnover_cr"],
                        "error": str(exc),
                    }
                )
            if index < total - 1 and args.delay > 0:
                time.sleep(args.delay)
    finally:
        conn.close()

    universe_df = pd.DataFrame(results)
    failures_df = pd.DataFrame(failures)
    if universe_df.empty:
        print("No Screener results were fetched successfully.", file=sys.stderr)
        return 1

    universe_df = universe_df.sort_values(
        ["sales_yoy_pct", "avg_turnover_cr", "symbol"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    universe_df.insert(0, "rank_by_sales_yoy", range(1, len(universe_df) + 1))
    top20_df = universe_df.head(args.top_sales).copy()

    summary_df = pd.DataFrame(
        [
            ("Cutoff Date", args.cutoff_date.isoformat()),
            ("Turnover Lookback Days", args.turnover_days),
            ("Top Turnover Universe Requested", args.top_turnover),
            ("Screener Fetch Limit Used", args.limit if args.limit is not None else ""),
            ("Cache Table", CACHE_TABLE),
            ("Unique Screener Symbols Considered", len(turnover_df)),
            ("Successful Screener Fetches", len(universe_df)),
            ("Cache Hits", cache_hits),
            ("Cache Misses Loaded", cache_misses),
            ("Failures", len(failures_df)),
            ("Top Sales Rows Exported", len(top20_df)),
            ("Quarter Selection Rule", "Latest quarter reported on BSE by cutoff date; values read from Screener for that exact quarter"),
            ("Sort Order", "Selected-quarter sales YoY %, then avg turnover"),
        ],
        columns=["Field", "Value"],
    )

    excel_path, csv_path = write_outputs(
        output_dir=args.output_dir,
        cutoff_date=args.cutoff_date,
        top20_df=top20_df,
        universe_df=universe_df,
        failures_df=failures_df,
        summary_df=summary_df,
    )

    print(f"Excel written to: {excel_path}")
    print(f"CSV written to  : {csv_path}")
    if not top20_df.empty:
        print("\nTop names:")
        preview = top20_df[["rank_by_sales_yoy", "symbol", "quarter_label", "sales_yoy_pct"]]
        print(preview.head(min(10, len(preview))).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
