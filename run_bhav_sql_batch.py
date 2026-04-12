#!/usr/bin/env python3
"""
Run the bhav SQL batch for a passed date against local MySQL.

The runner keeps all scripts in one MySQL session so session variables like
@yesterday, @bhavyear, and @oldbhav remain available across files.

Default connection:
  host=localhost
  user=root
  password=root
  database=bhav

Example:
  python run_bhav_sql_batch.py 2024-08-05
  python run_bhav_sql_batch.py 05-08-2024
  python run_bhav_sql_batch.py 2024-08-05 --gm
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Iterable

import pymysql
from pymysql.constants import CLIENT
from pymysql.cursors import DictCursor


SQL_FILES = [
    Path(r"C:\Users\shada\workspace\sql\3MONTHMOMOSCREENER.sql"),
    Path(r"C:\Users\shada\workspace\sql\6monthmomoscreener.sql"),
    Path(r"C:\Users\shada\workspace\sql\12monthscreener.sql"),
    Path(r"C:\Users\shada\workspace\sql\masterlist.sql"),
    Path(r"C:\Users\shada\workspace\sql\archiveinsideday.sql"),
    Path(r"C:\Users\shada\workspace\sql\archive_gmlistcreation.sql"),    
]

GM_SQL_FILES = [
    Path(r"C:\Users\shada\workspace\sql\archiveinsideday.sql"),
    Path(r"C:\Users\shada\workspace\sql\archive_gmlistcreation.sql"),
]

OUTPUT_DIR = Path(__file__).parent / "reports"


def parse_date_arg(text: str) -> str:
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date '{text}'. Use YYYY-MM-DD or DD-MM-YYYY.")


def sanitize_sql(raw_sql: str) -> str:
    """
    Keep the original script intact as much as possible, but prevent the script
    from overriding the runtime @yesterday value.
    """
    sql = raw_sql.replace("\ufeff", "")

    patterns = [
        r"(?im)^\s*select\s+max\s*\(\s*mktdate\s*\)\s+into\s+@yesterday\s+from\s+mktdatecalendar\s*;\s*",
        r"(?im)^\s*set\s+@yesterday\s*=\s*'[^']*'\s*(?:collate\s+\w+)?\s*;\s*",
    ]
    for pattern in patterns:
        sql = re.sub(pattern, "", sql)

    # Strip legacy block comments used heavily in these SQL files.
    # The MySQL CLI tolerates them more leniently than PyMySQL does when the
    # whole script is sent as a multi-statement blob.
    sql = re.sub(r"/\*\*.*?\*\*/", "", sql, flags=re.DOTALL)

    return sql.strip() + "\n"


def build_session_prefix(run_date: str) -> str:
    return (
        "SET SQL_SAFE_UPDATES = 0;\n"
        "USE bhav;\n"
        "SET NAMES utf8mb4;\n"
        f"SET @yesterday='{run_date}' COLLATE utf8mb4_unicode_ci;\n"
        "SELECT YEAR(@yesterday) INTO @bhavyear;\n"
        "SET @oldbhav=@bhavyear-1;\n"
    )


def execute_script(cursor, sql_text: str) -> None:
    cursor.execute(sql_text)
    while cursor.nextset():
        pass


def fetch_all_dicts(cursor, sql_text: str) -> list[dict]:
    cursor.execute(sql_text)
    rows = list(cursor.fetchall())
    while cursor.nextset():
        pass
    return rows


def print_result_table(rows: list[dict]) -> None:
    if not rows:
        print("No rows returned.")
        return

    headers = list(rows[0].keys())
    widths = {}
    for header in headers:
        widths[header] = max(len(str(header)), max(len(str(row.get(header, ""))) for row in rows))

    header_line = " | ".join(str(h).ljust(widths[h]) for h in headers)
    sep_line = "-+-".join("-" * widths[h] for h in headers)
    print(header_line)
    print(sep_line)
    for row in rows:
        print(" | ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers))


def write_symbol_txt(run_date: str, symbols: Iterable[str]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"gmlist_{datetime.strptime(run_date, '%Y-%m-%d').strftime('%d%b%Y')}.txt"
    cleaned = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
    out_path.write_text("\n".join(cleaned) + ("\n" if cleaned else ""), encoding="utf-8")
    return out_path


def run_sql_files(run_date: str, host: str, user: str, password: str, database: str, gm_only: bool = False) -> None:
    files_to_run = GM_SQL_FILES if gm_only else SQL_FILES

    missing = [str(path) for path in files_to_run if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing SQL file(s): {missing}")

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS,
        cursorclass=DictCursor,
    )

    try:
        with conn.cursor() as cursor:
            session_prefix = build_session_prefix(run_date)
            mode_label = "GM-only" if gm_only else "full"
            print(f"Connected to MySQL {host}/{database} as {user}")
            print(f"Running {mode_label} batch for date: {run_date}\n")

            execute_script(cursor, session_prefix)

            for index, sql_file in enumerate(files_to_run, start=1):
                started = time.time()
                print(f"[{index}/{len(files_to_run)}] Running {sql_file.name} ...")
                raw_sql = sql_file.read_text(encoding="utf-8", errors="ignore")
                sql_text = session_prefix + sanitize_sql(raw_sql)
                try:
                    execute_script(cursor, sql_text)
                except Exception as exc:
                    raise RuntimeError(f"{sql_file.name} failed: {exc}") from exc
                elapsed = time.time() - started
                print(f"  Completed in {elapsed:.1f}s")

            print("\nQuery: select * from gmlistarchive where cutoff=@yesterday;")
            archive_rows = fetch_all_dicts(
                cursor,
                "select * from gmlistarchive where cutoff=@yesterday;"
            )
            print_result_table(archive_rows)

            symbol_rows = fetch_all_dicts(
                cursor,
                "select distinct CONCAT('NSE:',symbol) as ticker from gmlistarchive where cutoff=@yesterday order by ticker;"
            )
            txt_path = write_symbol_txt(run_date, [row["ticker"] for row in symbol_rows])
            print(f"\nTXT saved -> {txt_path}")
            print("\nAll SQL scripts completed successfully.")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bhav SQL scripts for a passed date.")
    parser.add_argument("date", help="Run date in YYYY-MM-DD or DD-MM-YYYY format.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="root")
    parser.add_argument("--database", default="bhav")
    parser.add_argument(
        "--gm",
        action="store_true",
        help="Run only archiveinsideday.sql and archive_gmlistcreation.sql.",
    )
    args = parser.parse_args()

    try:
        run_date = parse_date_arg(args.date)
        run_sql_files(run_date, args.host, args.user, args.password, args.database, gm_only=args.gm)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
