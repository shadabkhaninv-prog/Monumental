#!/usr/bin/env python3
"""
Schema probe — dumps full table definitions + sample rows + date range
for bhav2025 and bhav2026.  Run this first, paste output back to Claude.
"""
import pymysql, json

conn = pymysql.connect(host="localhost", user="root", password="root",
                       database="bhav", cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()

for tbl in ("bhav2025", "bhav2026"):
    print(f"\n{'='*60}")
    print(f"  TABLE: {tbl}")
    print('='*60)

    # Full column definitions
    cur.execute(f"DESCRIBE {tbl}")
    cols = cur.fetchall()
    print("\n  COLUMNS:")
    for c in cols:
        print(f"    {c['Field']:25s}  {c['Type']:20s}  {c.get('Key',''):5s}  {c.get('Default','')}")

    # Row count
    cur.execute(f"SELECT COUNT(*) AS n FROM {tbl}")
    print(f"\n  ROW COUNT: {cur.fetchone()['n']:,}")

    # Date range (try MKTDATE)
    try:
        cur.execute(f"SELECT MIN(MKTDATE), MAX(MKTDATE) FROM {tbl}")
        r = cur.fetchone()
        print(f"  DATE RANGE (MKTDATE): {list(r.values())}")
    except Exception as e:
        print(f"  DATE RANGE ERROR: {e}")

    # Sample rows for PFOCUS
    cur.execute(f"SELECT * FROM {tbl} WHERE SYMBOL='PFOCUS' ORDER BY MKTDATE DESC LIMIT 3")
    rows = cur.fetchall()
    print(f"\n  SAMPLE (PFOCUS, last 3 rows):")
    for r in rows:
        print(f"    {json.dumps({k: str(v) for k,v in r.items()}, indent=None)}")

    # Distinct symbols count
    cur.execute(f"SELECT COUNT(DISTINCT SYMBOL) AS n FROM {tbl}")
    print(f"\n  DISTINCT SYMBOLS: {cur.fetchone()['n']}")

conn.close()
print("\nDone.")
