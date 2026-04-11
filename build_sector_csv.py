# -*- coding: utf-8 -*-
"""
build_sector_csv.py
─────────────────────────────────────────────────────────────────────
Exports sector data from the local MySQL `bhav` database to a CSV file
(bse_master.csv) that stock_rating_v7.py reads for sector classification.

SOURCE TABLE:
    Database : bhav
    Table    : sectors
    Columns  : symbol  (NSE trading symbol)
               sector1 (sector / industry name)

OUTPUT FILE:
    bse_master.csv  — saved in the same directory as this script
    Columns: symbol, sector

USAGE:
    python build_sector_csv.py
    python build_sector_csv.py --user root --host localhost --port 3306
    python build_sector_csv.py --out C:\\path\\to\\bse_master.csv

REQUIREMENTS:
    pip install pymysql
─────────────────────────────────────────────────────────────────────
"""

import os
import sys
import argparse
import getpass
import csv

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT  = os.path.join(SCRIPT_DIR, "bse_master.csv")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export bhav.sectors → bse_master.csv for stock_rating_v7.py"
    )
    parser.add_argument("--host",  default="localhost", help="MySQL host (default: localhost)")
    parser.add_argument("--port",  default=3306, type=int, help="MySQL port (default: 3306)")
    parser.add_argument("--user",  default="root",      help="MySQL user (default: root)")
    parser.add_argument("--db",    default="bhav",      help="Database name (default: bhav)")
    parser.add_argument("--table", default="sectors",   help="Table name (default: sectors)")
    parser.add_argument("--sym-col",    default="symbol",  help="Symbol column name (default: symbol)")
    parser.add_argument("--sector-col", default="sector1", help="Sector column name (default: sector1)")
    parser.add_argument("--out",   default=DEFAULT_OUT, help=f"Output CSV path (default: {DEFAULT_OUT})")
    return parser.parse_args()


def main():
    args = parse_args()

    # Prompt for password securely (not echoed to terminal)
    password = getpass.getpass(
        f"  MySQL password for {args.user}@{args.host}:{args.port}: "
    )

    # Connect
    try:
        import pymysql
    except ImportError:
        print("ERROR: pymysql not installed.  Run:  pip install pymysql")
        sys.exit(1)

    print(f"\n  Connecting to {args.user}@{args.host}:{args.port}/{args.db} …")
    try:
        conn = pymysql.connect(
            host     = args.host,
            port     = args.port,
            user     = args.user,
            password = password,
            database = args.db,
            charset  = "utf8mb4",
        )
    except Exception as e:
        print(f"  ERROR: Could not connect — {e}")
        sys.exit(1)

    # Query
    query = (
        f"SELECT `{args.sym_col}`, `{args.sector_col}` "
        f"FROM `{args.table}` "
        f"WHERE `{args.sym_col}` IS NOT NULL "
        f"  AND `{args.sector_col}` IS NOT NULL "
        f"  AND `{args.sector_col}` != '' "
        f"ORDER BY `{args.sym_col}`"
    )
    print(f"  Query: {query}")

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    except Exception as e:
        print(f"  ERROR: Query failed — {e}")
        conn.close()
        sys.exit(1)

    conn.close()
    print(f"  Rows fetched: {len(rows):,}")

    if not rows:
        print("  WARNING: No rows returned.  CSV not written.")
        sys.exit(1)

    # Write CSV
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "sector"])          # header
        for sym, sector in rows:
            sym    = str(sym).strip().upper()
            sector = str(sector).strip().title()       # "AUTO ANCILLARIES" → "Auto Ancillaries"
            if sym and sector and sector.lower() not in ("none", "nan", ""):
                writer.writerow([sym, sector])

    print(f"  CSV written → {args.out}")

    # Quick preview
    print("\n  Sample rows:")
    print(f"  {'SYMBOL':<20}  SECTOR")
    print(f"  {'─'*20}  {'─'*30}")
    with open(args.out, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader):
            if i >= 8:
                break
            print(f"  {row['symbol']:<20}  {row['sector']}")

    print(f"\n  Done.  {args.out} is ready for stock_rating_v7.py")


if __name__ == "__main__":
    main()