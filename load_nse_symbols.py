#!/usr/bin/env python3
"""
Create bhav.nse_symbols and load SYMBOL + COMPANY_NAME from NSE equity listing CSV.

Usage:
  python load_nse_symbols.py
  python load_nse_symbols.py --csv "C:\path\to\EQUITY_L.csv"
"""

import argparse
import csv
import os
from pathlib import Path
import mysql.connector

DB  = dict(host="localhost", port=3306, user="root", password="root", database="bhav")
DEFAULT_CSV = Path(__file__).parent / "EQUITY_L (1).csv"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(DEFAULT_CSV), help="Path to EQUITY_L.csv")
    ap.add_argument("--host", default=DB["host"], help="MySQL host")
    ap.add_argument("--port", default=DB["port"], type=int, help="MySQL port")
    ap.add_argument("--user", default=DB["user"], help="MySQL user")
    ap.add_argument("--db", default=DB["database"], help="MySQL database")
    ap.add_argument("--password", default=os.environ.get("MYSQL_PASSWORD", DB["password"]), help="MySQL password")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    conn = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
    )
    cur  = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS nse_symbols")
    cur.execute("""
        CREATE TABLE nse_symbols (
            SYMBOL       VARCHAR(32)  NOT NULL,
            COMPANY_NAME VARCHAR(255) NOT NULL,
            PRIMARY KEY (SYMBOL)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    print("Created table bhav.nse_symbols")

    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sym  = row["SYMBOL"].strip()
            name = row["NAME OF COMPANY"].strip()
            if sym and name:
                rows.append((sym, name))

    cur.executemany(
        "INSERT INTO nse_symbols (SYMBOL, COMPANY_NAME) VALUES (%s, %s)", rows
    )
    conn.commit()
    print(f"Loaded {len(rows)} rows")

    cur.execute("SELECT * FROM nse_symbols LIMIT 5")
    print("\nSample rows:")
    for r in cur.fetchall():
        print(f"  {r[0]:<20} {r[1]}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
