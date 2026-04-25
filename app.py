"""
NSE BHAV Stock Viewer — standalone Flask app
Queries: bhav2024, bhav2025, bhav2026, mktdatecalendar  in localhost/bhav

Run:  python app.py
Open: http://localhost:5000
"""

from __future__ import annotations
import json
import math
import re
import subprocess
import time
import sys
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from flask import Flask, request, jsonify, render_template_string

import mysql.connector

# ── DB config (mirrors stock_rating.py) ─────────────────────────────────────
DB_CONFIG = dict(host="localhost", port=3306, user="root",
                 password="root", database="bhav")

app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
GMLIST_FILE = Path(__file__).resolve().parent / "gmlist" / "updated_gmlist.txt"
KITE_TOKEN_FILE = Path(__file__).resolve().parent / "kite_token.txt"
SQL_BATCH_DIR = Path(r"C:\Users\shada\workspace\sql")
MAINTENANCE_DB = dict(host="localhost", port=3306, user="root", password="root", database="bhav")


# ── helpers ──────────────────────────────────────────────────────────────────

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def to_date(v):
    """Coerce a value to datetime.date — handles both date objects and ISO strings."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return date.fromisoformat(v[:10])   # safe even with datetime strings
    return v


def safe(v):
    """Make a value JSON-serialisable (handle NaN / None / date)."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (date,)):
        return v.isoformat()
    return v


def get_latest_bhav_date(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(mktdate) AS d FROM mktdatecalendar")
    row = cursor.fetchone()
    cursor.close()
    return to_date(row[0]) if row else None


def fetch_bhav_archive_rows(conn, symbol, from_date, to_date):
    """Fetch BHAV rows for one symbol across the yearly bhav tables."""
    select_block = """
        SELECT
            mktdate, symbol, VOLATILITY,
            open, high, low, close, prevclose,
            volume, deliveryvolume, closeindictor,
            ROUND(((close - prevclose) / prevclose) * 100, 2) AS diff,
            ROUND(((high  - close)    / close)     * 100, 2) AS jag,
            ROUND((deliveryvolume     / volume)    * 100, 2) AS delper,
            `20DMA`, `10dma`, `50dma`, `5dma`
        FROM {table}
        WHERE UPPER(symbol) = %s
          AND mktdate >= %s
          AND mktdate <= %s
    """

    years_needed = list(range(from_date.year, to_date.year + 1))
    union_parts = [select_block.format(table=f"bhav{y}") for y in years_needed]
    archive_sql = "\n\nUNION ALL\n\n".join(union_parts) + "\n\nORDER BY mktdate DESC"
    params = []
    for _ in years_needed:
        params += [symbol, from_date, to_date]

    cursor = conn.cursor(dictionary=True)
    cursor.execute(archive_sql, params)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def avg_turnover_21d(rows):
    """Average 21-trading-day turnover using volume * close."""
    window = rows[-21:] if len(rows) >= 21 else rows
    turnovers = []
    for r in window:
        volume = r.get("volume")
        close = r.get("close")
        if volume is None or close is None:
            continue
        try:
            turnovers.append(float(volume) * float(close))
        except Exception:
            continue
    if not turnovers:
        return None
    return sum(turnovers) / len(turnovers)


def trading_days_ending(conn, end_date, n):
    """Return the last n trading dates ending on end_date (inclusive)."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT mktdate FROM (
            SELECT DISTINCT mktdate FROM mktdatecalendar
             WHERE mktdate <= %s
             ORDER BY mktdate DESC LIMIT %s
        ) m ORDER BY mktdate ASC
        """,
        (end_date, int(n)),
    )
    rows = [to_date(r[0]) for r in cursor.fetchall()]
    cursor.close()
    return [d for d in rows if d is not None]


@lru_cache(maxsize=1)
def load_gmlist_symbols():
    """Load the curated GMList universe from gmlist/updated_gmlist.txt."""
    symbols = []
    try:
        with GMLIST_FILE.open("r", encoding="utf-8") as handle:
            for line in handle:
                sym = line.strip().upper()
                if not sym:
                    continue
                if ":" in sym:
                    sym = sym.split(":", 1)[1].strip()
                sym = sym.strip().upper()
                if sym:
                    symbols.append(sym)
    except FileNotFoundError:
        return []

    seen = set()
    unique = []
    for sym in symbols:
        if sym in seen:
            continue
        seen.add(sym)
        unique.append(sym)
    return unique


MAINTENANCE_JOBS = {
    "bhav_sql_batch": {
        "label": "Bhav SQL batch",
        "script": BASE_DIR / "run_bhav_sql_batch.py",
        "description": "Runs the date-based SQL batch and can optionally switch to GM-only mode.",
        "needs": ["date"],
    },
    "index_smallcaps": {
        "label": "Index BHAV smallcaps",
        "script": BASE_DIR / "update_indexbhav_smallcaps.py",
        "description": "Refreshes NIFTY Smallcap 100 / 250 rows in indexbhav.",
        "needs": ["date_mode"],
    },
    "sector_csv": {
        "label": "Sector CSV export",
        "script": BASE_DIR / "build_sector_csv.py",
        "description": "Exports bhav.sectors to bse_master.csv for downstream rating jobs.",
        "needs": [],
    },
    "nse_symbols": {
        "label": "NSE symbols load",
        "script": BASE_DIR / "load_nse_symbols.py",
        "description": "Reloads bhav.nse_symbols from the NSE equity listing CSV.",
        "needs": [],
    },
    "ipo_csv": {
        "label": "IPO CSV load",
        "script": BASE_DIR / "load_ipo_csv.py",
        "description": "Loads local IPO performance CSVs into bhav.ipobhav.",
        "needs": [],
    },
    "chittorgarh_ipo": {
        "label": "Chittorgarh IPO load",
        "script": BASE_DIR / "chittorgarh_ipo_loader.py",
        "description": "Scrapes and loads listed mainline IPO rows into bhav.ipobhav.",
        "needs": ["year_range"],
    },
    "moneycontrol_ipo": {
        "label": "Moneycontrol IPO load",
        "script": BASE_DIR / "moneycontrol_mainline_ipo_loader.py",
        "description": "Loads listed mainline IPO reference data into bhav.ipobhav.",
        "needs": [],
    },
}


def run_maintenance_job(job_key: str, payload: dict) -> dict:
    job = MAINTENANCE_JOBS.get(job_key)
    if not job:
        raise ValueError("Unknown maintenance job.")

    script = Path(job["script"])
    if not script.exists():
        raise FileNotFoundError(f"Script not found: {script}")

    cmd = [sys.executable, str(script)]

    if job_key == "bhav_sql_batch":
        run_date = str(payload.get("date") or "").strip()
        if not run_date:
            raise ValueError("Missing date.")
        cmd.append(run_date)
        if payload.get("gm"):
            cmd.append("--gm")
    elif job_key == "index_smallcaps":
        mode = str(payload.get("mode") or "date").strip()
        if mode == "single":
            single_date = str(payload.get("date") or "").strip()
            if not single_date:
                raise ValueError("Missing date.")
            cmd.extend(["--date", single_date])
        else:
            from_date = str(payload.get("from_date") or "").strip()
            to_date = str(payload.get("to_date") or "").strip()
            if from_date:
                cmd.extend(["--from-date", from_date])
            if to_date:
                cmd.extend(["--to-date", to_date])
        if payload.get("dry_run"):
            cmd.append("--dry-run")
    elif job_key == "sector_csv":
        cmd.extend(["--password", str(MAINTENANCE_DB["password"])])
    elif job_key == "nse_symbols":
        cmd.extend(["--password", str(MAINTENANCE_DB["password"])])
    elif job_key == "ipo_csv":
        csv_file = str(payload.get("file") or "").strip()
        if csv_file:
            cmd.extend(["--file", csv_file])
    elif job_key == "chittorgarh_ipo":
        start_year = str(payload.get("start_year") or "").strip()
        end_year = str(payload.get("end_year") or "").strip()
        if start_year:
            cmd.extend(["--start-year", start_year])
        if end_year:
            cmd.extend(["--end-year", end_year])
    elif job_key == "moneycontrol_ipo":
        min_year = str(payload.get("min_year") or "").strip()
        table = str(payload.get("table") or "").strip()
        limit = str(payload.get("limit") or "").strip()
        if table:
            cmd.extend(["--table", table])
        if limit:
            cmd.extend(["--limit", limit])
        if min_year:
            cmd.extend(["--min-year", min_year])

    proc = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "command": cmd,
        "stdout": proc.stdout[-20000:],
        "stderr": proc.stderr[-20000:],
    }


def read_kite_token_file(token_file=KITE_TOKEN_FILE):
    """Read API_KEY and ACCESS_TOKEN from kite_token.txt."""
    token_path = Path(token_file)
    if not token_path.exists():
        return None

    values = {}
    for raw_line in token_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip().upper()] = value.strip()

    if "API_KEY" not in values or "ACCESS_TOKEN" not in values:
        return None
    return values


def get_kite_client():
    """Return an authenticated KiteConnect client, or None if unavailable."""
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        return None

    creds = read_kite_token_file()
    if not creds:
        return None

    kite = KiteConnect(api_key=creds["API_KEY"])
    kite.set_access_token(creds["ACCESS_TOKEN"])
    return kite


def normalize_kite_symbol(symbol: str) -> str:
    s = (symbol or "").upper().strip()
    s = s.replace("&", "AND")
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


@lru_cache(maxsize=1)
def build_kite_nse_token_lookup():
    """Build a best-effort NSE EQ instrument token lookup from Kite."""
    kite = get_kite_client()
    if kite is None:
        return {}

    try:
        instruments = kite.instruments("NSE")
    except Exception:
        return {}

    lookup = {}
    for inst in instruments:
        if str(inst.get("exchange") or "").upper() != "NSE":
            continue
        if str(inst.get("instrument_type") or "").upper() != "EQ":
            continue
        if str(inst.get("segment") or "").upper() == "INDICES":
            continue

        tradingsymbol = str(inst.get("tradingsymbol") or "").strip().upper()
        name = str(inst.get("name") or "").strip().upper()
        if not tradingsymbol:
            continue
        if any(marker in name for marker in ("ETF", "INDEX ETF", "EXCHANGE TRADED FUND")):
            continue
        if tradingsymbol.endswith("BEES") or tradingsymbol.endswith("ETF"):
            continue
        if "ETF" in tradingsymbol:
            continue

        token = inst.get("instrument_token")
        if token is None:
            continue
        token = int(token)
        lookup.setdefault(tradingsymbol, token)
        lookup.setdefault(normalize_kite_symbol(tradingsymbol), token)
    return lookup


def chart_time_to_lightweight(value):
    """Convert Kite timestamps into a Lightweight Charts-friendly time value."""
    if value is None:
        return None
    if isinstance(value, datetime):
        try:
            return int(value.timestamp())
        except Exception:
            return safe(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    try:
        parsed = to_datetime(value)
        if parsed is not None:
            return int(parsed.timestamp())
    except Exception:
        pass
    return safe(value)


def resolve_kite_token(symbol: str):
    """Resolve a gmlist symbol to a Kite instrument token."""
    lookup = build_kite_nse_token_lookup()
    sym = (symbol or "").strip().upper()
    if sym in lookup:
        return lookup[sym]
    norm = normalize_kite_symbol(sym)
    return lookup.get(norm)


def fetch_previous_day_highs(conn, symbols, as_of):
    """Fetch previous trading day's highs for the given symbols."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT MAX(mktdate) FROM mktdatecalendar
        WHERE mktdate < %s
        """,
        (as_of,),
    )
    row = cursor.fetchone()
    prev_day = to_date(row[0]) if row and row[0] else None
    cursor.close()
    if prev_day is None:
        return {}

    year = prev_day.year
    symbol_list = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not symbol_list:
        return {}

    placeholders = ",".join(["%s"] * len(symbol_list))
    sql = f"""
        SELECT UPPER(symbol) AS symbol, high
        FROM bhav{year}
        WHERE mktdate = %s
          AND UPPER(symbol) IN ({placeholders})
    """
    cursor = conn.cursor()
    cursor.execute(sql, [prev_day] + symbol_list)
    highs = {}
    for sym, high in cursor.fetchall():
        if sym is None or high is None:
            continue
        try:
            highs[str(sym).strip().upper()] = float(high)
        except Exception:
            continue
    cursor.close()
    return highs


def fetch_kite_5m_opening_bar(kite, instrument_token, as_of):
    """Return the first 5-minute candle for the selected day."""
    try:
        rows = kite.historical_data(
            instrument_token=instrument_token,
            from_date=datetime.combine(as_of, datetime.min.time()),
            to_date=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
            interval="5minute",
            continuous=False,
            oi=False,
        )
    except Exception:
        return None

    if not rows:
        return None

    for row in rows:
        ts = row.get("date")
        if not ts:
            continue
        row_date = to_date(ts)
        if row_date != as_of:
            continue
        return row
    return None


def union_bhav_select(years):
    """Build a UNION ALL across yearly bhav tables for screener scans."""
    block = (
        "SELECT UPPER(symbol) AS symbol, mktdate, open, high, low, volume, VOLATILITY, close "
        "FROM bhav{year} WHERE mktdate BETWEEN %s AND %s"
    )
    return "\nUNION ALL\n".join(block.format(year=y) for y in years)


def get_excluded_symbols(conn):
    """Return ETF/index symbols that should not appear in the screener."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT UPPER(symbol)
        FROM sectors
        WHERE UPPER(COALESCE(sector1, '')) = 'ETF'
    """)
    excluded = {str(row[0]).strip().upper() for row in cursor.fetchall() if row and row[0]}
    cursor.close()
    return excluded


def build_gmlist_screen_card(symbol, as_of, start_21d, start_22d, start_3d):
    """Build GMList screen metrics for one symbol."""
    conn = None
    try:
        conn = get_conn()
        rows = fetch_bhav_archive_rows(conn, symbol, start_22d, as_of)
        if not rows:
            return None

        ordered = sorted(rows, key=lambda r: r["mktdate"] or date.min)
        current = None
        previous = None
        for idx in range(len(ordered) - 1, -1, -1):
            row = ordered[idx]
            row_date = to_date(row.get("mktdate"))
            if row_date is None or row_date > as_of:
                continue
            current = row
            if idx > 0:
                previous = ordered[idx - 1]
            break

        if current is None:
            return None

        window_rows = [r for r in ordered if to_date(r.get("mktdate")) is not None and to_date(r.get("mktdate")) >= start_21d and to_date(r.get("mktdate")) <= as_of]
        if len(window_rows) < 21:
            return None

        current_vol = current.get("volume")
        current_vlt = current.get("VOLATILITY")
        current_close = current.get("close")
        current_high = current.get("high")
        current_low = current.get("low")

        vols = []
        vlts = []
        for row in window_rows:
            if row.get("volume") is not None:
                try:
                    vols.append(float(row.get("volume")))
                except Exception:
                    pass
            if row.get("VOLATILITY") is not None:
                try:
                    vlts.append(float(row.get("VOLATILITY")))
                except Exception:
                    pass

        min_vol_21d = min(vols) if vols else None
        min_vlt_21d = min(vlts) if vlts else None
        avg_turnover = avg_turnover_21d(window_rows)

        delivery_days = []
        delivery_max = None
        delivery_latest = current.get("delper")
        for row in ordered:
            row_date = to_date(row.get("mktdate"))
            if row_date is None or row_date < start_3d or row_date > as_of:
                continue
            delper = row.get("delper")
            if delper is None:
                continue
            try:
                delper_f = float(delper)
            except Exception:
                continue
            delivery_max = delper_f if delivery_max is None else max(delivery_max, delper_f)
            if delper_f >= 60.0:
                delivery_days.append({
                    "date": safe(row_date),
                    "delper": safe(delper_f),
                })

        prev_high = previous.get("high") if previous else None
        prev_low = previous.get("low") if previous else None
        inside_day = bool(
            previous
            and current_high is not None
            and current_low is not None
            and prev_high is not None
            and prev_low is not None
            and float(current_high) < float(prev_high)
            and float(current_low) > float(prev_low)
        )

        return {
            "symbol": symbol,
            "as_of": safe(as_of),
            "close": safe(current_close),
            "volume": safe(current_vol),
            "volatility": safe(current_vlt),
            "high": safe(current_high),
            "low": safe(current_low),
            "prev_high": safe(prev_high),
            "prev_low": safe(prev_low),
            "min_vol_21d": safe(min_vol_21d),
            "min_vlt_21d": safe(min_vlt_21d),
            "avg_turnover_21d": safe(round(avg_turnover, 2)) if avg_turnover is not None else None,
            "delivery_days_3d": len(delivery_days),
            "delivery_max_3d": safe(delivery_max),
            "delivery_latest": safe(delivery_latest),
            "delivery_hit": len(delivery_days) > 0,
            "delivery_hits": delivery_days,
            "lv21": bool(
                current_vlt is not None and min_vlt_21d is not None and float(current_vlt) <= float(min_vlt_21d)
            ),
            "lowvol21": bool(
                current_vol is not None and min_vol_21d is not None and float(current_vol) <= float(min_vol_21d)
            ),
            "inside_day": inside_day,
        }
    finally:
        if conn is not None:
            conn.close()


def build_gmlist_live_card(symbol, as_of, start_21d):
    """Build a live Kite daily-chart scan card for one symbol."""
    try:
        kite = get_kite_client()
        if kite is None:
            return None
        token = resolve_kite_token(symbol)
        if token is None:
            return None
        daily_rows = kite.historical_data(
            instrument_token=token,
            from_date=datetime.combine(start_21d - timedelta(days=30), datetime.min.time()),
            to_date=datetime.combine(as_of, datetime.min.time()),
            interval="day",
            continuous=False,
            oi=False,
        )
    except Exception:
        return None

    ordered = []
    for row in daily_rows or []:
        row_date = to_date(row.get("date"))
        if row_date is None or row_date < start_21d or row_date > as_of:
            continue
        clean_row = dict(row)
        clean_row["date"] = row_date
        ordered.append(clean_row)
    if len(ordered) < 20:
        return None

    current = ordered[-1]
    current_date = to_date(current.get("date"))
    current_intraday = None
    try:
        kite = get_kite_client()
        token = resolve_kite_token(symbol)
        if kite is not None and token is not None:
            intraday_rows = kite.historical_data(
                instrument_token=token,
                from_date=datetime.combine(as_of, datetime.min.time()),
                to_date=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
                interval="5minute",
                continuous=False,
                oi=False,
            )
            day_rows = []
            for row in intraday_rows or []:
                row_date = to_date(row.get("date"))
                if row_date == as_of:
                    day_rows.append(row)
            if day_rows:
                opens = [r.get("open") for r in day_rows if r.get("open") is not None]
                highs = [r.get("high") for r in day_rows if r.get("high") is not None]
                lows = [r.get("low") for r in day_rows if r.get("low") is not None]
                closes = [r.get("close") for r in day_rows if r.get("close") is not None]
                volumes = [r.get("volume") for r in day_rows if r.get("volume") is not None]
                if opens and highs and lows and closes:
                    current_intraday = {
                        "date": as_of,
                        "open": opens[0],
                        "high": max(float(v) for v in highs),
                        "low": min(float(v) for v in lows),
                        "close": closes[-1],
                        "volume": sum(float(v) for v in volumes) if volumes else None,
                    }
    except Exception:
        current_intraday = None

    if current_intraday is not None:
        current = current_intraday
    else:
        current = ordered[-1]

    previous = ordered[-2] if len(ordered) > 1 else None
    window_rows = ordered[-20:] + ([current] if current is not None else [])

    current_vol = current.get("volume")
    current_close = current.get("close")
    current_high = current.get("high")
    current_low = current.get("low")
    try:
        current_vlt = round(((float(current_high) - float(current_low)) / float(current_close)) * 100.0, 2) if current_high is not None and current_low is not None and current_close not in (None, 0) else None
    except Exception:
        current_vlt = None

    vols = []
    vlts = []
    for row in window_rows:
        if row.get("volume") is not None:
            try:
                vols.append(float(row.get("volume")))
            except Exception:
                pass
        try:
            hi = float(row.get("high")) if row.get("high") is not None else None
            lo = float(row.get("low")) if row.get("low") is not None else None
            cl = float(row.get("close")) if row.get("close") is not None else None
            if hi is not None and lo is not None and cl not in (None, 0):
                vlts.append(round(((hi - lo) / cl) * 100.0, 2))
        except Exception:
            pass

    min_vol_21d = min(vols) if vols else None
    min_vlt_21d = min(vlts) if vlts else None
    prev_high = previous.get("high") if previous else None
    prev_low = previous.get("low") if previous else None
    inside_day = bool(
        previous
        and current_high is not None
        and current_low is not None
        and prev_high is not None
        and prev_low is not None
        and float(current_high) < float(prev_high)
        and float(current_low) > float(prev_low)
    )

    return {
        "symbol": symbol,
        "as_of": safe(as_of),
        "close": safe(current_close),
        "volume": safe(current_vol),
        "volatility": safe(current_vlt),
        "high": safe(current_high),
        "low": safe(current_low),
        "prev_high": safe(prev_high),
        "prev_low": safe(prev_low),
        "min_vol_21d": safe(min_vol_21d),
        "min_vlt_21d": safe(min_vlt_21d),
        "avg_turnover_21d": None,
        "lv21": bool(
            current_vlt is not None and min_vlt_21d is not None and float(current_vlt) <= float(min_vlt_21d)
        ),
        "lowvol21": bool(
            current_vol is not None and min_vol_21d is not None and float(current_vol) <= float(min_vol_21d)
        ),
        "inside_day": inside_day,
    }


def build_gmlist_screen(as_of, conn=None):
    """Build the GMList screen response for the selected date."""
    own_conn = conn is None
    if conn is None:
        conn = get_conn()
    try:
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            return {"error": "mktdatecalendar is empty"}, 500

        cursor = conn.cursor()
        cursor.execute("SELECT MAX(mktdate) FROM mktdatecalendar WHERE mktdate <= %s", (as_of,))
        row = cursor.fetchone()
        cursor.close()
        resolved = to_date(row[0]) if row and row[0] else None
        if resolved is None:
            return {"error": "no trading day on or before selected date"}, 404
        as_of = resolved

        d21 = trading_days_ending(conn, as_of, 21)
        d22 = trading_days_ending(conn, as_of, 22)
        d3 = trading_days_ending(conn, as_of, 3)
        if len(d21) < 21 or len(d22) < 2:
            return {"error": "not enough trading days in calendar"}, 500

        start_21d = d21[0]
        start_22d = d22[0]
        start_3d = d3[0] if d3 else as_of
        symbols = load_gmlist_symbols()
        if not symbols:
            return {
                "as_of": safe(as_of),
                "window_21d": {"start": safe(start_21d), "end": safe(d21[-1])},
                "source_file": "updated_gmlist",
                "universe": 0,
                "lv21": [],
                "lowvol21": [],
                "inside_days": [],
                "hd": [],
                "strong_start": [],
                "strong_start_status": "Open the Strong Start tab to scan with Kite.",
                "strong_start_loaded": False,
            }, 200

        max_workers = min(12, len(symbols)) or 1
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(build_gmlist_screen_card, sym, as_of, start_21d, start_22d, start_3d): i
                for i, sym in enumerate(symbols)
            }
            for fut in as_completed(futures):
                rec = fut.result()
                if rec is not None:
                    results.append(rec)

        lv21 = [r for r in results if r.get("lv21")]
        lowvol21 = [r for r in results if r.get("lowvol21")]
        inside_days = [r for r in results if r.get("inside_day")]
        hd = [r for r in results if r.get("delivery_hit")]

        lv21.sort(key=lambda r: (float(r["volatility"] or 0), r["symbol"]))
        lowvol21.sort(key=lambda r: (float(r["volume"] or 0), r["symbol"]))
        inside_days.sort(key=lambda r: (r["symbol"]))
        hd.sort(key=lambda r: (-(float(r["delivery_max_3d"] or 0)), -(int(r["delivery_days_3d"] or 0)), r["symbol"]))

        return {
            "as_of": safe(as_of),
            "window_21d": {"start": safe(start_21d), "end": safe(d21[-1])},
            "source_file": "updated_gmlist",
            "universe": len(symbols),
            "eligible": len(results),
            "lv21": lv21,
            "lowvol21": lowvol21,
            "inside_days": inside_days,
            "hd": hd,
            "strong_start": [],
            "strong_start_status": "Open the Strong Start tab to scan with Kite.",
            "strong_start_loaded": False,
        }, 200
    finally:
        if own_conn and conn is not None:
            conn.close()


def build_gmlist_live_preview_card(symbol, as_of, conn=None):
    """Build a live daily chart preview card with today's candle updated from intraday bars."""
    own_conn = conn is None
    if conn is None:
        conn = get_conn()
    try:
        as_of = to_date(as_of) or as_of
        kite = get_kite_client()
        if kite is None:
            return None
        symbol = (symbol or "").strip().upper()
        if not symbol:
            return None
        token = resolve_kite_token(symbol)
        if token is None:
            return None

        daily_rows = kite.historical_data(
            instrument_token=token,
            from_date=datetime.combine(as_of - timedelta(days=240), datetime.min.time()),
            to_date=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
            interval="day",
            continuous=False,
            oi=False,
        )
    except Exception:
        return None

    ordered = []
    for row in daily_rows or []:
        row_date = to_date(row.get("date"))
        if row_date is None or row_date > as_of:
            continue
        clean_row = dict(row)
        clean_row["date"] = row_date
        ordered.append(clean_row)

    live_current = None
    try:
        kite = get_kite_client()
        token = resolve_kite_token(symbol)
        if kite is not None and token is not None:
            intraday_rows = kite.historical_data(
                instrument_token=token,
                from_date=datetime.combine(as_of, datetime.min.time()),
                to_date=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
                interval="5minute",
                continuous=False,
                oi=False,
            )
            day_rows = []
            for row in intraday_rows or []:
                row_date = to_date(row.get("date"))
                if row_date == as_of:
                    day_rows.append(row)
            if day_rows:
                opens = [r.get("open") for r in day_rows if r.get("open") is not None]
                highs = [r.get("high") for r in day_rows if r.get("high") is not None]
                lows = [r.get("low") for r in day_rows if r.get("low") is not None]
                closes = [r.get("close") for r in day_rows if r.get("close") is not None]
                volumes = [r.get("volume") for r in day_rows if r.get("volume") is not None]
                if opens and highs and lows and closes:
                    live_current = {
                        "date": as_of,
                        "open": opens[0],
                        "high": max(float(v) for v in highs),
                        "low": min(float(v) for v in lows),
                        "close": closes[-1],
                        "volume": sum(float(v) for v in volumes) if volumes else None,
                    }
    except Exception:
        live_current = None

    if live_current is not None:
        ordered = [r for r in ordered if to_date(r.get("date")) != as_of] + [live_current]

    ordered = sorted(ordered, key=lambda r: r.get("date") or as_of)
    if len(ordered) < 20:
        return None

    closes = []
    ema5_vals = []
    ema10_vals = []
    ema20_vals = []
    ema50_vals = []

    def ema_series(values, span):
        if not values:
            return []
        alpha = 2.0 / (span + 1.0)
        out = []
        prev = float(values[0])
        out.append(prev)
        for value in values[1:]:
            prev = ((float(value) - prev) * alpha) + prev
            out.append(prev)
        return out

    closes = [safe(r.get("close")) for r in ordered]
    closes = [float(v) for v in closes if v is not None]
    ema5_vals = ema_series(closes, 5)
    ema10_vals = ema_series(closes, 10)
    ema20_vals = ema_series(closes, 20)
    ema50_vals = ema_series(closes, 50)

    rows = []
    prev_close = None
    for idx, row in enumerate(ordered):
        d = row.get("date")
        iso = safe(d) if d else None
        close_val = safe(row.get("close"))
        prev_for_diff = prev_close
        diff = None
        if close_val is not None and prev_for_diff not in (None, 0):
            try:
                diff = round(((float(close_val) - float(prev_for_diff)) / float(prev_for_diff)) * 100.0, 2)
            except Exception:
                diff = None
        if close_val is not None:
            prev_close = close_val
        rows.append({
            "mktdate": iso,
            "open": safe(row.get("open")),
            "high": safe(row.get("high")),
            "low": safe(row.get("low")),
            "close": close_val,
            "volume": safe(row.get("volume")),
            "diff": safe(diff),
            "5dma": safe(ema5_vals[idx]) if idx < len(ema5_vals) else None,
            "10dma": safe(ema10_vals[idx]) if idx < len(ema10_vals) else None,
            "20DMA": safe(ema20_vals[idx]) if idx < len(ema20_vals) else None,
            "50dma": safe(ema50_vals[idx]) if idx < len(ema50_vals) else None,
        })

    prev_highs = fetch_previous_day_highs(conn, [symbol], as_of)
    prev_high = prev_highs.get(symbol)
    return {
        "symbol": symbol,
        "as_of": safe(as_of),
        "start_date": safe(rows[0]["mktdate"]),
        "prev_high": safe(prev_high),
        "rows": rows,
    }, 200


def build_gmlist_live_screen(as_of, conn=None):
    """Build the Kite daily-chart live scan for the GMList universe."""
    own_conn = conn is None
    if conn is None:
        conn = get_conn()
    try:
        symbols = load_gmlist_symbols()
        if not symbols:
            return {
                "as_of": safe(as_of),
                "window_21d": {"start": safe(as_of), "end": safe(as_of)},
                "source_file": "kite_daily",
                "universe": 0,
                "lv21": [],
                "lowvol21": [],
                "inside_days": [],
            }, 200

        # We use the last 21 rows returned by Kite as the live window.
        # The scan is on-demand, so it can be slower than the EOD bhav scan.
        max_workers = min(8, len(symbols)) or 1
        results = []
        d21 = trading_days_ending(conn, as_of, 21)
        if len(d21) < 21:
            return {"error": "not enough trading days in calendar"}, 500
        start_21d = d21[0]

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(build_gmlist_live_card, sym, as_of, start_21d): i
                for i, sym in enumerate(symbols)
            }
            for fut in as_completed(futures):
                rec = fut.result()
                if rec is not None:
                    results.append(rec)

        lv21 = [r for r in results if r.get("lv21")]
        lowvol21 = [r for r in results if r.get("lowvol21")]
        inside_days = [r for r in results if r.get("inside_day")]

        lv21.sort(key=lambda r: (float(r["volatility"] or 0), r["symbol"]))
        lowvol21.sort(key=lambda r: (float(r["volume"] or 0), r["symbol"]))
        inside_days.sort(key=lambda r: (r["symbol"]))

        return {
            "as_of": safe(as_of),
            "window_21d": {"start": safe(start_21d), "end": safe(as_of)},
            "source_file": "kite_daily",
            "universe": len(symbols),
            "eligible": len(results),
            "lv21": lv21,
            "lowvol21": lowvol21,
            "inside_days": inside_days,
        }, 200
    finally:
        if own_conn and conn is not None:
            conn.close()


def build_gmlist_strong_start(as_of, conn=None):
    """Build the Kite-backed strong-start scan only when requested."""
    own_conn = conn is None
    if conn is None:
        conn = get_conn()
    try:
        symbols = load_gmlist_symbols()
        if not symbols:
            return {
                "as_of": safe(as_of),
                "strong_start": [],
                "strong_start_status": "GMList is empty.",
                "strong_start_loaded": True,
            }, 200

        live_cutoff = (9, 20, 30)
        now = datetime.now()
        is_today_scan = as_of == date.today()
        before_cutoff = (now.hour, now.minute, now.second) < live_cutoff

        if is_today_scan and before_cutoff:
            return {
                "as_of": safe(as_of),
                "strong_start": [],
                "strong_start_status": "Waiting for the first 5-minute candle to complete after 09:20 IST.",
                "strong_start_loaded": True,
            }, 200

        kite = get_kite_client()
        if kite is None:
            return {
                "as_of": safe(as_of),
                "strong_start": [],
                "strong_start_status": "Kite credentials not available; strong start is unavailable.",
                "strong_start_loaded": True,
            }, 200

        prev_highs = fetch_previous_day_highs(conn, symbols, as_of)
        strong_start = []
        for sym in symbols:
            prev_high = prev_highs.get(sym)
            if prev_high is None:
                continue
            token = resolve_kite_token(sym)
            if token is None:
                continue
            try:
                rows = kite.historical_data(
                    instrument_token=token,
                    from_date=datetime.combine(as_of, datetime.min.time()),
                    to_date=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
                    interval="5minute",
                    continuous=False,
                    oi=False,
                )
            except Exception:
                continue

            day_rows = []
            for row in rows or []:
                ts = row.get("date")
                row_date = to_date(ts)
                if not ts or row_date is None or row_date != as_of:
                    continue
                day_rows.append(row)
            if not day_rows:
                continue

            open_bar = day_rows[0]
            try:
                open_px = float(open_bar.get("open")) if open_bar.get("open") is not None else None
                high_px = float(open_bar.get("high")) if open_bar.get("high") is not None else None
                low_px = float(open_bar.get("low")) if open_bar.get("low") is not None else None
                close_px = float(open_bar.get("close")) if open_bar.get("close") is not None else None
            except Exception:
                continue

            if high_px is None or high_px < prev_high:
                continue

            later_high = None
            for row in day_rows[1:]:
                try:
                    row_high = float(row.get("high")) if row.get("high") is not None else None
                except Exception:
                    row_high = None
                if row_high is None:
                    continue
                later_high = row_high if later_high is None else max(later_high, row_high)
            if later_high is not None and later_high > high_px:
                continue

            open_above_prev_high = bool(open_px is not None and open_px > prev_high)
            bar_breaks_prev_high = bool(high_px is not None and high_px >= prev_high)
            basis_px = max([px for px in (open_px, high_px) if px is not None], default=None)

            strong_start.append({
                "symbol": sym,
                "as_of": safe(as_of),
                "open": safe(open_px),
                "high": safe(high_px),
                "low": safe(low_px),
                "close": safe(close_px),
                "prev_high": safe(prev_high),
                "gap_pct": safe(round(((basis_px - prev_high) / prev_high) * 100.0, 2)) if prev_high and basis_px is not None else None,
                "bar_time": safe(open_bar.get("date")),
                "above_prev_high": bar_breaks_prev_high,
                "open_above_prev_high": open_above_prev_high,
            })

        strong_start.sort(key=lambda r: (-(float(r["gap_pct"] or 0)), r["symbol"]))
        return {
            "as_of": safe(as_of),
            "strong_start": strong_start,
            "strong_start_status": "Criterion: first 5-minute candle clears previous day's high and price stays within that opening 5-minute range.",
            "strong_start_loaded": True,
        }, 200
    finally:
        if own_conn and conn is not None:
            conn.close()


def build_gmlist_strong_start_chart(as_of, symbol, conn=None):
    """Build an intraday chart payload for a strong-start symbol from Kite."""
    own_conn = conn is None
    if conn is None:
        conn = get_conn()
    try:
        kite = get_kite_client()
        if kite is None:
            return {"error": "Kite credentials not available"}, 400

        symbol = (symbol or "").strip().upper()
        if not symbol:
            return {"error": "symbol required"}, 400

        token = resolve_kite_token(symbol)
        if token is None:
            return {"error": f"no Kite token for {symbol}"}, 404

        lookback_days = 5
        start_date = as_of - timedelta(days=lookback_days * 2)
        bar_rows = []
        try:
            rows = kite.historical_data(
                instrument_token=token,
                from_date=datetime.combine(start_date, datetime.min.time()),
                to_date=datetime.combine(as_of + timedelta(days=1), datetime.min.time()),
                interval="5minute",
                continuous=False,
                oi=False,
            )
            for row in rows or []:
                ts = row.get("date")
                row_date = to_date(ts)
                if not ts or row_date is None or row_date < start_date or row_date > as_of:
                    continue
                bar_rows.append({
                    "time": chart_time_to_lightweight(ts),
                    "open": safe(row.get("open")),
                    "high": safe(row.get("high")),
                    "low": safe(row.get("low")),
                    "close": safe(row.get("close")),
                    "volume": safe(row.get("volume")),
                    "change_pct": None,
                })
        except Exception as e:
            return {"error": str(e)}, 500

        prev_highs = fetch_previous_day_highs(conn, [symbol], as_of)
        prev_high = prev_highs.get(symbol)
        return {
            "symbol": symbol,
            "as_of": safe(as_of),
            "start_date": safe(start_date),
            "prev_high": safe(prev_high),
            "candles": bar_rows,
            "volume": [
                {
                    "time": r["time"],
                    "value": r["volume"],
                    "color": "#58b65b",
                }
                for r in bar_rows
            ],
            "ema5": [],
            "ema10": [],
            "ema20": [],
            "ema50": [],
        }, 200
    finally:
        if own_conn and conn is not None:
            conn.close()


def fetch_corporate_actions_map(conn, symbols):
    """Fetch split/bonus actions for the requested symbols."""
    symbol_list = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not symbol_list:
        return {}

    placeholders = ",".join(["%s"] * len(symbol_list))
    sql = f"""
        SELECT UPPER(Symbol) AS symbol, CompanyName, ExDate, A, B, Ratio, 'split' AS action_type
        FROM splits
        WHERE UPPER(Symbol) IN ({placeholders})
        UNION ALL
        SELECT UPPER(Symbol) AS symbol, CompanyName, ExDate, A, B, Ratio, 'bonus' AS action_type
        FROM bonus
        WHERE UPPER(Symbol) IN ({placeholders})
        ORDER BY symbol, STR_TO_DATE(ExDate, '%Y-%m-%d')
    """
    params = symbol_list + symbol_list
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, params)
    action_map = {}
    for row in cursor.fetchall():
        sym = row["symbol"]
        try:
            ratio = float(row.get("Ratio"))
        except Exception:
            ratio = None
        if not ratio or ratio <= 0:
            continue
        action_map.setdefault(sym, []).append({
            "symbol": sym,
            "company_name": row.get("CompanyName"),
            "exdate": to_date(row.get("ExDate")),
            "a": row.get("A"),
            "b": row.get("B"),
            "ratio": ratio,
            "action_type": row.get("action_type"),
        })
    cursor.close()
    return action_map


def apply_corporate_actions(ordered_rows, actions):
    """
    Back-adjust OHLC/EMA/volume using explicit split/bonus rows from the DB.
    """
    if not ordered_rows:
        return []
    if not actions:
        return ordered_rows

    normalized_actions = []
    for action in actions:
        exdate = to_date(action.get("exdate"))
        ratio = action.get("ratio")
        if exdate is None or ratio in (None, 0):
            continue
        try:
            ratio = float(ratio)
        except Exception:
            continue
        if ratio <= 0:
            continue
        normalized_actions.append({**action, "exdate": exdate, "ratio": ratio})
    normalized_actions.sort(key=lambda a: a["exdate"])
    if not normalized_actions:
        return ordered_rows

    adjusted_rows = []
    for row in ordered_rows:
        row_date = to_date(row.get("mktdate"))
        if row_date is None:
            continue
        factor = 1.0
        for action in normalized_actions:
            if action["exdate"] > row_date:
                factor *= 1.0 / action["ratio"]

        adj = dict(row)
        adj["_adj_factor"] = factor
        for field in ("open", "high", "low", "close", "5dma", "10dma", "20DMA", "50dma"):
            val = row.get(field)
            adj[f"_adj_{field}"] = safe(val * factor) if val is not None else None
        volume = row.get("volume")
        adj["_adj_volume"] = safe(volume / factor) if volume is not None and factor not in (None, 0) else safe(volume)
        adjusted_rows.append(adj)

    return adjusted_rows


def build_sector_chart_card(sym, sector, chart_from, latest_date, min_avg_turnover, corporate_actions=None):
    """Build one chart payload for a symbol using a dedicated connection."""
    conn = None
    try:
        conn = get_conn()
        rows = fetch_bhav_archive_rows(conn, sym, chart_from, latest_date)
        if not rows:
            return None

        turnover_21d = avg_turnover_21d(rows)
        if turnover_21d is None or turnover_21d < min_avg_turnover:
            return None

        ordered = sorted(rows, key=lambda r: r["mktdate"] or date.min)
        adjusted_rows = apply_corporate_actions(ordered, corporate_actions or [])
        candles = []
        volumes = []
        ema5 = []
        ema10 = []
        ema20 = []
        ema50 = []
        prior_close = None
        def pick(row, key):
            adj_key = f"_adj_{key}"
            if adj_key in row and row.get(adj_key) is not None:
                return row.get(adj_key)
            return row.get(key)

        for r in adjusted_rows:
            d = to_date(r["mktdate"])
            if not d:
                continue
            iso = d.isoformat()
            close_val = safe(pick(r, "close"))
            prev_close = safe(pick(r, "prevclose"))
            if prev_close in (None, 0):
                prev_close = prior_close
            change_pct = None
            if close_val is not None and prev_close not in (None, 0):
                try:
                    change_pct = round(((float(close_val) - float(prev_close)) / float(prev_close)) * 100.0, 2)
                except Exception:
                    change_pct = None
            if close_val is not None:
                prior_close = close_val
            candles.append({
                "time": iso,
                "open": safe(pick(r, "open")),
                "high": safe(pick(r, "high")),
                "low": safe(pick(r, "low")),
                "close": close_val,
                "volume": safe(pick(r, "volume")),
                "change_pct": safe(change_pct),
                "adj_factor": safe(r.get("_adj_factor")),
            })
            volumes.append({
                "time": iso,
                "value": safe(pick(r, "volume")),
                "color": "#58b65b" if (close_val is not None and prev_close not in (None, 0) and close_val >= prev_close) else "#ef6a6a",
            })
            for series, key in [
                (ema5, "5dma"),
                (ema10, "10dma"),
                (ema20, "20DMA"),
                (ema50, "50dma"),
            ]:
                val = safe(pick(r, key))
                if val is not None:
                    series.append({"time": iso, "value": val})

        latest = ordered[-1]
        prev_close = latest.get("prevclose")
        latest_close = latest.get("close")
        move_pct = None
        if latest_close is not None and prev_close not in (None, 0):
            try:
                move_pct = round(((float(latest_close) - float(prev_close)) / float(prev_close)) * 100.0, 2)
            except Exception:
                move_pct = None

        return {
            "symbol": sym,
            "sector": sector,
            "has_data": bool(candles),
            "latest_date": safe(to_date(latest.get("mktdate"))),
            "end_close": safe(latest_close),
            "move_pct": safe(move_pct),
            "avg_turnover_21d": safe(round(turnover_21d, 2)),
            "candles": candles,
            "volume": volumes,
            "ema5": ema5,
            "ema10": ema10,
            "ema20": ema20,
            "ema50": ema50,
        }
    finally:
        if conn is not None:
            conn.close()


# ── routes ───────────────────────────────────────────────────────────────────

def render_page(page_mode: str = "stocks"):
    return render_template_string(HTML_PAGE, page_mode=page_mode)


@app.route("/")
def index():
    return render_page("stocks")


@app.route("/sectors")
def sectors_page():
    return render_page("sectors")


@app.route("/screener")
def screener_page():
    return render_page("screener")


@app.route("/gmlist")
def gmlist_page():
    return render_page("gmlist")


@app.route("/maintenance")
def maintenance_page():
    return render_page("maintenance")


@app.route("/api/symbols")
def api_symbols():
    """Return all distinct symbols for autocomplete (union across year tables)."""
    q = request.args.get("q", "").strip().upper()
    try:
        conn = get_conn()
        cursor = conn.cursor()
        like = f"%{q}%" if q else "%"
        sql = """
            SELECT DISTINCT UPPER(symbol) AS sym FROM (
                SELECT symbol FROM bhav2024 WHERE UPPER(symbol) LIKE %s
                UNION
                SELECT symbol FROM bhav2025 WHERE UPPER(symbol) LIKE %s
                UNION
                SELECT symbol FROM bhav2026 WHERE UPPER(symbol) LIKE %s
            ) t ORDER BY sym LIMIT 50
        """
        cursor.execute(sql, (like, like, like))
        rows = [r[0] for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/latest-date")
def api_latest_date():
    try:
        conn = get_conn()
        latest_date = get_latest_bhav_date(conn)
        conn.close()
        return jsonify({"latest_date": safe(latest_date)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sectors")
def api_sectors():
    """Return all distinct sector names from sector1/sector2/sector3."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT sector_name
            FROM (
                SELECT sector1 AS sector_name FROM sectors WHERE sector1 IS NOT NULL AND sector1 <> ''
                UNION
                SELECT sector2 AS sector_name FROM sectors WHERE sector2 IS NOT NULL AND sector2 <> ''
                UNION
                SELECT sector3 AS sector_name FROM sectors WHERE sector3 IS NOT NULL AND sector3 <> ''
            ) s
            WHERE UPPER(sector_name) <> 'ETF'
            ORDER BY sector_name
        """)
        rows = [r[0] for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/top-sectors")
def api_top_sectors():
    """Return sectors with the most stocks across sector1/sector2/sector3."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sector_name, COUNT(DISTINCT symbol) AS stock_count
            FROM (
                SELECT symbol, UPPER(sector1) AS sector_name FROM sectors
                WHERE sector1 IS NOT NULL AND sector1 <> ''
                UNION ALL
                SELECT symbol, UPPER(sector2) AS sector_name FROM sectors
                WHERE sector2 IS NOT NULL AND sector2 <> ''
                UNION ALL
                SELECT symbol, UPPER(sector3) AS sector_name FROM sectors
                WHERE sector3 IS NOT NULL AND sector3 <> ''
            ) s
            WHERE sector_name IS NOT NULL AND sector_name <> '' AND sector_name <> 'ETF'
            GROUP BY sector_name
            ORDER BY stock_count DESC, sector_name
            LIMIT 15
        """)
        rows = [{"sector": r[0], "count": int(r[1])} for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sector-stocks")
def api_sector_stocks():
    """Return all stocks mapped to the requested sector."""
    sector = request.args.get("sector", "").strip().upper()
    if not sector:
        return jsonify({"error": "sector required"}), 400

    try:
        conn = get_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT symbol, sector1, sector2, sector3,
                   CASE WHEN UPPER(COALESCE(sector1, '')) = %s THEN 0 ELSE 1 END AS primary_rank
            FROM sectors
            WHERE UPPER(COALESCE(sector1, '')) = %s
               OR UPPER(COALESCE(sector2, '')) = %s
               OR UPPER(COALESCE(sector3, '')) = %s
            ORDER BY primary_rank, symbol
        """, (sector, sector, sector, sector))
        rows = cursor.fetchall()
        cursor.close(); conn.close()
        return jsonify({
            "sector": sector,
            "count": len(rows),
            "stocks": rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sector-charts")
def api_sector_charts():
    """Return a small chart board for the selected sector."""
    sector = request.args.get("sector", "").strip().upper()
    if not sector:
        return jsonify({"error": "sector required"}), 400

    min_avg_turnover = 10_000_000 * 10  # 10 crore

    try:
        limit = int(request.args.get("limit", 0))
    except ValueError:
        limit = 0
    limit = max(0, limit)

    try:
        conn = get_conn()
        latest_date = get_latest_bhav_date(conn)
        if latest_date is None:
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT UPPER(symbol) AS sym,
                   CASE WHEN UPPER(COALESCE(sector1, '')) = %s THEN 0 ELSE 1 END AS primary_rank
            FROM sectors
            WHERE UPPER(COALESCE(sector1, '')) = %s
               OR UPPER(COALESCE(sector2, '')) = %s
               OR UPPER(COALESCE(sector3, '')) = %s
            ORDER BY primary_rank, sym
        """, (sector, sector, sector, sector))
        symbols = [r[0] for r in cursor.fetchall()]
        cursor.close()

        if not symbols:
            conn.close()
            return jsonify({
                "sector": sector,
                "count": 0,
                "charts": [],
            })

        from datetime import timedelta
        chart_from = max(latest_date - timedelta(days=180), date(2000, 1, 1))
        if limit:
            symbols = symbols[:limit]

        action_map = fetch_corporate_actions_map(conn, symbols)

        charts = []
        max_workers = min(16, len(symbols)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(build_sector_chart_card, sym, sector, chart_from, latest_date, min_avg_turnover, action_map.get(sym, [])): i
                for i, sym in enumerate(symbols)
            }
            collected = []
            for fut in as_completed(futures):
                idx = futures[fut]
                card = fut.result()
                if card is not None:
                    collected.append((idx, card))

        charts = [card for _, card in sorted(collected, key=lambda item: item[0])]

        conn.close()
        return jsonify({
            "sector": sector,
            "count": len(symbols),
            "charts": charts,
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/screener")
def api_screener():
    """
    Low-volume / low-volatility screener with 21-trading-day turnover filter,
    restricted to names near 52-week highs or recent highs.
    """
    try:
        limit = int(request.args.get("limit", 200))
    except ValueError:
        limit = 200
    try:
        min_turnover = float(request.args.get("min_turnover", 10 * 1e7))
    except ValueError:
        min_turnover = float(10 * 1e7)

    try:
        conn = get_conn()
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            conn.close()
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        as_of_param = (request.args.get("date") or "").strip()
        try:
            as_of = to_date(as_of_param) if as_of_param else db_max
        except Exception:
            conn.close()
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400
        if as_of is None:
            conn.close()
            return jsonify({"error": "bad date"}), 400

        cursor = conn.cursor()
        cursor.execute("SELECT MAX(mktdate) FROM mktdatecalendar WHERE mktdate <= %s", (as_of,))
        row = cursor.fetchone()
        cursor.close()
        resolved = to_date(row[0]) if row and row[0] else None
        if resolved is None:
            conn.close()
            return jsonify({"error": "no trading day on or before selected date"}), 404
        as_of = resolved

        d21 = trading_days_ending(conn, as_of, 21)
        d15 = trading_days_ending(conn, as_of, 15)
        d252 = trading_days_ending(conn, as_of, 252)
        if not d21 or not d15 or not d252:
            conn.close()
            return jsonify({"error": "not enough trading days in calendar"}), 500

        start_21d, end_21d = d21[0], d21[-1]
        start_15d, end_15d = d15[0], d15[-1]
        start_252d, end_252d = d252[0], d252[-1]

        years = sorted({start_252d.year, end_252d.year})
        params = []
        for _ in years:
            params += [start_252d, end_252d]

        cursor = conn.cursor(dictionary=True)
        cursor.execute(union_bhav_select(years), params)
        rows = cursor.fetchall()
        cursor.close()

        excluded_symbols = get_excluded_symbols(conn)
        conn.close()

        d21_set = set(d21)
        d15_set = set(d15)
        d252_set = set(d252)
        agg = {}
        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym or sym in excluded_symbols:
                continue
            mdate = to_date(r.get("mktdate"))
            if mdate is None:
                continue
            vol = r.get("volume")
            vlt = r.get("VOLATILITY")
            cls = r.get("close")

            a = agg.setdefault(sym, {
                "turn_sum": 0.0, "turn_cnt": 0,
                "vols_21d": [], "vlts_21d": [],
                "highs_15d": [], "highs_252d": [],
                "today_vol": None, "today_vlt": None, "today_close": None,
                "today_high": None,
                "today_date": None,
            })

            try:
                if vol is not None and cls is not None:
                    a["turn_sum"] += float(vol) * float(cls)
                    a["turn_cnt"] += 1
            except Exception:
                pass

            if mdate in d21_set:
                if vol is not None:
                    try:
                        a["vols_21d"].append(float(vol))
                    except Exception:
                        pass
                if vlt is not None:
                    try:
                        a["vlts_21d"].append(float(vlt))
                    except Exception:
                        pass

            if r.get("high") is not None:
                try:
                    hi = float(r.get("high"))
                except Exception:
                    hi = None
                if hi is not None:
                    if mdate in d15_set:
                        a["highs_15d"].append((hi, mdate))
                    if mdate in d252_set:
                        a["highs_252d"].append((hi, mdate))

            if mdate == as_of:
                a["today_vol"] = float(vol) if vol is not None else None
                a["today_vlt"] = float(vlt) if vlt is not None else None
                a["today_close"] = float(cls) if cls is not None else None
                try:
                    a["today_high"] = float(r.get("high")) if r.get("high") is not None else None
                except Exception:
                    a["today_high"] = None
                a["today_date"] = mdate

        low_volume = []
        low_volatility = []
        for sym, a in agg.items():
            if a["turn_cnt"] == 0 or a["today_vol"] is None:
                continue
            avg_turnover = a["turn_sum"] / a["turn_cnt"]
            if avg_turnover < min_turnover:
                continue

            high_52w = max((hi for hi, _ in a["highs_252d"]), default=None)
            high_15d = max((hi for hi, _ in a["highs_15d"]), default=None)
            if high_52w is None:
                continue

            close_val = a["today_close"]
            high_52w_gap = None
            high_15d_gap = None
            near_high_52w = False
            near_recent_high = False
            if close_val is not None and high_52w:
                high_52w_gap = ((high_52w - close_val) / high_52w) * 100.0
                near_high_52w = close_val >= (high_52w * 0.80)
            if close_val is not None and high_15d:
                high_15d_gap = ((high_15d - close_val) / high_15d) * 100.0
                near_recent_high = close_val >= (high_15d * 0.85)

            if not (near_high_52w or near_recent_high):
                continue

            row_out = {
                "symbol": sym,
                "as_of": safe(a["today_date"]),
                "close": safe(close_val),
                "volume": safe(a["today_vol"]),
                "volatility": safe(a["today_vlt"]),
                "min_vol_21d": safe(min(a["vols_21d"])) if a["vols_21d"] else None,
                "min_vlt_21d": safe(min(a["vlts_21d"])) if a["vlts_21d"] else None,
                "avg_turnover_21d": safe(round(avg_turnover, 2)),
                "sample_21d_vol": len(a["vols_21d"]),
                "sample_21d_vlt": len(a["vlts_21d"]),
                "high_52w": safe(high_52w),
                "high_52w_gap": safe(round(high_52w_gap, 2)) if high_52w_gap is not None else None,
                "high_15d": safe(high_15d),
                "high_15d_gap": safe(round(high_15d_gap, 2)) if high_15d_gap is not None else None,
                "near_high_52w": near_high_52w,
                "near_recent_high": near_recent_high,
            }

            if (row_out["min_vol_21d"] is not None
                    and row_out["volume"] is not None
                    and float(row_out["volume"]) <= float(row_out["min_vol_21d"])):
                low_volume.append(row_out)

            if (row_out["min_vlt_21d"] is not None
                    and row_out["volatility"] is not None
                    and float(row_out["volatility"]) <= float(row_out["min_vlt_21d"])):
                low_volatility.append(row_out)

        low_volume.sort(key=lambda r: (float(r["volume"] or 0), r["symbol"]))
        low_volatility.sort(key=lambda r: (float(r["volatility"] or 0), r["symbol"]))

        return jsonify({
            "as_of": safe(as_of),
            "window_21d": {"start": safe(start_21d), "end": safe(end_21d)},
            "window_15d": {"start": safe(start_15d), "end": safe(end_15d)},
            "window_252d": {"start": safe(start_252d), "end": safe(end_252d)},
            "min_turnover": min_turnover,
            "universe": sum(1 for a in agg.values()
                            if a["today_vol"] is not None and a["turn_cnt"] > 0
                            and (a["turn_sum"] / a["turn_cnt"]) >= min_turnover
                            and max((hi for hi, _ in a["highs_252d"]), default=None) is not None),
            "low_volume": low_volume[:limit],
            "low_volatility": low_volatility[:limit],
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/gmlist")
def api_gmlist():
    as_of_param = (request.args.get("date") or "").strip()
    try:
        conn = get_conn()
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            conn.close()
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        try:
            as_of = to_date(as_of_param) if as_of_param else db_max
        except Exception:
            conn.close()
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400
        if as_of is None:
            conn.close()
            return jsonify({"error": "bad date"}), 400

        result, status = build_gmlist_screen(as_of, conn=conn)
        conn.close()
        return jsonify(result), status
    except Exception as e:
        import traceback
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/gmlist-live")
def api_gmlist_live():
    as_of_param = (request.args.get("date") or "").strip()
    try:
        conn = get_conn()
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            conn.close()
            return jsonify({"error": "mktdatecalendar is empty"}), 500
        try:
            as_of = to_date(as_of_param) if as_of_param else date.today()
        except Exception:
            conn.close()
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400
        if as_of is None:
            conn.close()
            return jsonify({"error": "bad date"}), 400

        result, status = build_gmlist_live_screen(as_of, conn=conn)
        conn.close()
        return jsonify(result), status
    except Exception as e:
        import traceback
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/gmlist-live-preview")
def api_gmlist_live_preview():
    symbol = (request.args.get("symbol") or "").strip().upper()
    as_of_param = (request.args.get("date") or "").strip()
    try:
        conn = get_conn()
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            conn.close()
            return jsonify({"error": "mktdatecalendar is empty"}), 500
        try:
            as_of = to_date(as_of_param) if as_of_param else date.today()
        except Exception:
            conn.close()
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400
        if not symbol:
            conn.close()
            return jsonify({"error": "symbol required"}), 400
        if as_of is None:
            conn.close()
            return jsonify({"error": "bad date"}), 400

        result, status = build_gmlist_live_preview_card(symbol, as_of, conn=conn)
        conn.close()
        return jsonify(result), status
    except Exception as e:
        import traceback
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/gmlist-strong-start")
def api_gmlist_strong_start():
    as_of_param = (request.args.get("date") or "").strip()
    try:
        conn = get_conn()
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            conn.close()
            return jsonify({"error": "mktdatecalendar is empty"}), 500
        try:
            as_of = to_date(as_of_param) if as_of_param else db_max
        except Exception:
            conn.close()
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400
        if as_of is None:
            conn.close()
            return jsonify({"error": "bad date"}), 400

        result, status = build_gmlist_strong_start(as_of, conn=conn)
        conn.close()
        return jsonify(result), status
    except Exception as e:
        import traceback
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/gmlist-strong-start-chart")
def api_gmlist_strong_start_chart():
    symbol = (request.args.get("symbol") or "").strip().upper()
    as_of_param = (request.args.get("date") or "").strip()
    try:
        conn = get_conn()
        db_max = get_latest_bhav_date(conn)
        if db_max is None:
            conn.close()
            return jsonify({"error": "mktdatecalendar is empty"}), 500
        try:
            as_of = to_date(as_of_param) if as_of_param else db_max
        except Exception:
            conn.close()
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400
        if not symbol:
            conn.close()
            return jsonify({"error": "symbol required"}), 400

        result, status = build_gmlist_strong_start_chart(as_of, symbol, conn=conn)
        conn.close()
        return jsonify(result), status
    except Exception as e:
        import traceback
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/stock")
def api_stock():
    """
    Replicates the full stockquery.sql logic for a given symbol.
    Optional query params: from_date, to_date (YYYY-MM-DD).
    Defaults: to_date = max(mktdate), from_date = to_date - 100 calendar days.
    Returns:
      - rows        : archiverollingperiod data (OHLCV + computed cols)
      - minvol      : {63d, 21d} min volatility
      - lowvolume   : {63d, 21d} min volume
      - yesterday   : reference date used (= to_date)
      - start_21d   : 21-trading-day lookback start
      - start_63d   : 63-trading-day lookback start
    """
    from datetime import timedelta
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol required"}), 400

    try:
        conn = get_conn()
        cursor = conn.cursor(dictionary=True)

        # ── 1. max(mktdate) → @yesterday (upper bound / "to" date) ──────────
        cursor.execute("SELECT MAX(mktdate) AS d FROM mktdatecalendar")
        row = cursor.fetchone()
        db_max = to_date(row["d"])
        if db_max is None:
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        # ── resolve to_date / from_date from request params ──────────────────
        to_date_param  = request.args.get("to_date",   "").strip()
        from_date_param = request.args.get("from_date", "").strip()

        yesterday = to_date(to_date_param)   if to_date_param   else db_max
        from_date = to_date(from_date_param) if from_date_param else (yesterday - timedelta(days=100))

        # ── 2. 21-trading-day lookback start ─────────────────────────────────
        cursor.execute("""
            SELECT mktdate FROM (
                SELECT DISTINCT mktdate FROM mktdatecalendar
                WHERE mktdate <= %s ORDER BY mktdate DESC LIMIT 21
            ) m ORDER BY mktdate ASC LIMIT 1
        """, (yesterday,))
        row21 = cursor.fetchone()
        start_21d = to_date(row21["mktdate"]) if row21 else yesterday

        # ── 3. 63-trading-day lookback start ─────────────────────────────────
        cursor.execute("""
            SELECT mktdate FROM (
                SELECT DISTINCT mktdate FROM mktdatecalendar
                WHERE mktdate <= %s ORDER BY mktdate DESC LIMIT 63
            ) m ORDER BY mktdate ASC LIMIT 1
        """, (yesterday,))
        row63 = cursor.fetchone()
        start_63d = to_date(row63["mktdate"]) if row63 else yesterday

        # ── 4. Build archiverollingperiod equivalent ──────────────────────────
        #   Only query the bhavYYYY tables that overlap with [from_date, yesterday].
        select_block = """
            SELECT
                mktdate, symbol, VOLATILITY,
                open, high, low, close, prevclose,
                volume, deliveryvolume, closeindictor,
                ROUND(((close - prevclose) / prevclose) * 100, 2) AS diff,
                ROUND(((high  - close)    / close)     * 100, 2) AS jag,
                ROUND((deliveryvolume     / volume)    * 100, 2) AS delper,
                `20DMA`, `10dma`, `50dma`, `5dma`
            FROM {table}
            WHERE UPPER(symbol) = %s
              AND mktdate >= %s
              AND mktdate <= %s"""

        years_needed = list(range(from_date.year, yesterday.year + 1))
        union_parts  = [select_block.format(table=f"bhav{y}") for y in years_needed]
        archive_sql  = "\n\nUNION ALL\n\n".join(union_parts) + "\n\nORDER BY mktdate DESC"

        params = []
        for _ in years_needed:
            params += [symbol, from_date, yesterday]

        cursor.execute(archive_sql, params)
        archive_rows = cursor.fetchall()

        if not archive_rows:
            cursor.close(); conn.close()
            return jsonify({"error": f"No data found for symbol '{symbol}'"}), 404

        action_map = fetch_corporate_actions_map(conn, [symbol])
        ordered_rows = sorted(archive_rows, key=lambda r: r["mktdate"] or date.min)
        adjusted_rows = apply_corporate_actions(ordered_rows, action_map.get(symbol, []))

        def pick(row, key):
            adj_key = f"_adj_{key}"
            if row.get(adj_key) is not None:
                return row.get(adj_key)
            return row.get(key)

        clean_rows = []
        prior_close = None
        for r in adjusted_rows:
            row_date = to_date(r["mktdate"]) if r.get("mktdate") else None
            if row_date is None:
                continue

            close_val = pick(r, "close")
            prev_close = pick(r, "prevclose")
            if prev_close in (None, 0):
                prev_close = prior_close

            change_pct = None
            jag = None
            delper = None
            if close_val is not None and prev_close not in (None, 0):
                try:
                    change_pct = round(((float(close_val) - float(prev_close)) / float(prev_close)) * 100.0, 2)
                except Exception:
                    change_pct = None

            high_val = pick(r, "high")
            volume_val = pick(r, "volume")
            delivery_val = pick(r, "deliveryvolume")
            if close_val is not None and high_val is not None:
                try:
                    jag = round(((float(high_val) - float(close_val)) / float(close_val)) * 100.0, 2)
                except Exception:
                    jag = None
            if volume_val not in (None, 0) and delivery_val is not None:
                try:
                    delper = round((float(delivery_val) / float(volume_val)) * 100.0, 2)
                except Exception:
                    delper = None

            if close_val is not None:
                prior_close = close_val

            clean_rows.append({
                "mktdate": safe(r.get("mktdate")),
                "symbol": safe(r.get("symbol")),
                "VOLATILITY": safe(r.get("VOLATILITY")),
                "open": safe(pick(r, "open")),
                "high": safe(high_val),
                "low": safe(pick(r, "low")),
                "close": safe(close_val),
                "prevclose": safe(prev_close),
                "volume": safe(volume_val),
                "deliveryvolume": safe(delivery_val),
                "closeindictor": safe(r.get("closeindictor")),
                "diff": safe(change_pct),
                "jag": safe(jag),
                "delper": safe(delper),
                "5dma": safe(pick(r, "5dma")),
                "10dma": safe(pick(r, "10dma")),
                "20DMA": safe(pick(r, "20DMA")),
                "50dma": safe(pick(r, "50dma")),
                "adj_factor": safe(r.get("_adj_factor")),
            })

        clean_rows.reverse()

        # ── 5. minvol63: MIN(VOLATILITY) over 63-day window (excl. yesterday) ─
        #    minvol21: MIN(VOLATILITY) over 21-day window (excl. yesterday)
        #    (Exact same WHERE clauses as stockquery.sql)
        def minvol_from_archive(rows, start_date):
            vals = [r["VOLATILITY"] for r in rows
                    if r["mktdate"] is not None
                    and to_date(r["mktdate"]) >= start_date
                    and to_date(r["mktdate"]) <= yesterday
                    and r["VOLATILITY"] is not None]
            return min(vals) if vals else None

        minvol_63d = minvol_from_archive(clean_rows, start_63d)
        minvol_21d = minvol_from_archive(clean_rows, start_21d)

        # ── 6. lowvolume21/63  (note: names in original SQL are swapped vs dates)
        #    lowvolume21 uses @63daystart, lowvolume63 uses @21daystart
        #    Preserved exactly as written in stockquery.sql
        def minvolume_from_archive(rows, start_date):
            vals = [r["volume"] for r in rows
                    if r["mktdate"] is not None
                    and to_date(r["mktdate"]) >= start_date
                    and to_date(r["mktdate"]) <= yesterday
                    and r["volume"] is not None]
            return min(vals) if vals else None

        # lowvolume21 in original SQL uses @63daystart (preserved as-is)
        lowvol_table21 = minvolume_from_archive(clean_rows, start_63d)
        # lowvolume63 in original SQL uses @21daystart (preserved as-is)
        lowvol_table63 = minvolume_from_archive(clean_rows, start_21d)

        cursor.close(); conn.close()

        # ── 7. Serialise + tag highlight rows ────────────────────────────────
        highlighted_rows = []
        for r in clean_rows:
            d = dict(r)
            rd = to_date(r["mktdate"]) if r["mktdate"] else None
            in_21d = (rd is not None
                      and rd >= start_21d
                      and rd <= yesterday)
            d["_hl_lowvol"]  = bool(in_21d and r["volume"]     is not None
                                    and r["volume"]     == lowvol_table63)
            d["_hl_lowvolatility"] = bool(in_21d and r["VOLATILITY"] is not None
                                          and r["VOLATILITY"] == minvol_21d)
            highlighted_rows.append(d)

        return jsonify({
            "symbol":      symbol,
            "yesterday":   safe(yesterday),
            "from_date":   safe(from_date),
            "start_21d":   safe(start_21d),
            "start_63d":   safe(start_63d),
            "rows":        highlighted_rows,
            "minvol": {
                "63d": safe(minvol_63d),
                "21d": safe(minvol_21d),
            },
            "lowvolume": {
                "table21_uses_63d_window": safe(lowvol_table21),
                "table63_uses_21d_window": safe(lowvol_table63),
            },
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ── HTML page (single-file, no external dependencies except CDN) ─────────────

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NSE BHAV Viewer</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:      #0f1117;
    --surface: #1a1d27;
    --card:    #21253a;
    --border:  #2e3450;
    --accent:  #4f8ef7;
    --accent2: #38d9a9;
    --warn:    #ffa94d;
    --red:     #ff6b6b;
    --green:   #51cf66;
    --text:    #e8eaf6;
    --muted:   #8892b0;
    --th-bg:   #1c2035;
    --row-alt: #1e2236;
    --shadow:  0 4px 24px rgba(0,0,0,.45);
  }

  /* full-height layout — header fixed, main scrolls */
  html, body { height: 100%; overflow: hidden; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif;
         font-size: 14px; display: flex; flex-direction: column; }

  /* ── header ── */
  header { background: var(--surface); border-bottom: 1px solid var(--border);
           padding: 14px 28px; display: flex; align-items: center; gap: 18px;
           flex-shrink: 0; z-index: 100; box-shadow: var(--shadow); }
  header h1 { font-size: 1.15rem; font-weight: 700; color: var(--accent);
              letter-spacing: .5px; white-space: nowrap; }
  header h1 span { color: var(--accent2); }

  /* ── search area ── */
  .search-wrap { flex: 1; max-width: 440px; position: relative; }
  .search-wrap input {
    width: 100%; padding: 9px 16px; border-radius: 8px;
    border: 1.5px solid var(--border); background: var(--card);
    color: var(--text); font-size: 15px; font-weight: 600; letter-spacing: 1px;
    outline: none; transition: border-color .2s;
  }
  .search-wrap input:focus { border-color: var(--accent); }
  .search-wrap input::placeholder { color: var(--muted); font-weight: 400; letter-spacing: 0; }

  .autocomplete-list {
    position: absolute; top: calc(100% + 4px); left: 0; right: 0;
    background: var(--card); border: 1px solid var(--border); border-radius: 8px;
    max-height: 260px; overflow-y: auto; z-index: 200; display: none;
    box-shadow: var(--shadow);
  }
  .autocomplete-list.open { display: block; }
  .ac-item { padding: 9px 16px; cursor: pointer; font-weight: 600;
             letter-spacing: .5px; transition: background .15s; }
  .ac-item:hover, .ac-item.active { background: var(--border); color: var(--accent); }

  /* ── date range inputs ── */
  .date-range { display: flex; align-items: center; gap: 6px; white-space: nowrap; }
  .date-range label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                      letter-spacing: .6px; }
  .date-range input[type="date"] {
    padding: 7px 10px; border-radius: 8px; border: 1.5px solid var(--border);
    background: var(--card); color: var(--text); font-size: 13px;
    outline: none; transition: border-color .2s; cursor: pointer;
    color-scheme: dark;
  }
  .date-range input[type="date"]:focus { border-color: var(--accent); }
  .date-sep { color: var(--muted); font-size: 12px; }

  /* ── clear dates btn ── */
  #clearDatesBtn {
    padding: 5px 10px; border-radius: 6px; border: 1px solid var(--border);
    background: transparent; color: var(--muted); font-size: 11px; cursor: pointer;
    transition: all .2s; white-space: nowrap;
  }
  #clearDatesBtn:hover { border-color: var(--accent); color: var(--accent); }

  /* ── load btn ── */
  #loadBtn {
    padding: 9px 22px; border-radius: 8px; border: none; cursor: pointer;
    background: var(--accent); color: #fff; font-weight: 700; font-size: 14px;
    transition: opacity .2s; white-space: nowrap;
  }
  #loadBtn:hover { opacity: .85; }
  #loadBtn:disabled { opacity: .4; cursor: default; }

  /* ── status bar ── */
  #statusBar {
    font-size: 12px; color: var(--muted); padding: 6px 28px;
    background: var(--surface); border-bottom: 1px solid var(--border);
    min-height: 28px; transition: color .2s; flex-shrink: 0;
  }
  #statusBar.err { color: var(--red); }
  #statusBar.ok  { color: var(--accent2); }

  /* ── main layout — this is the scroll container ── */
  main { padding: 20px 28px; flex: 1; overflow-y: auto; }

  /* ── summary cards ── */
  .cards { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 22px; }
  .card {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 20px; min-width: 170px; flex: 1 1 170px;
    box-shadow: var(--shadow);
  }
  .card-label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                letter-spacing: .8px; margin-bottom: 6px; }
  .card-value { font-size: 1.45rem; font-weight: 700; color: var(--accent); }
  .card-value.green { color: var(--green); }
  .card-value.warn  { color: var(--warn);  }
  .card-sub { font-size: 11px; color: var(--muted); margin-top: 4px; }

  .top-links { display: flex; gap: 10px; align-items: center; }
  .top-links a {
    text-decoration: none; color: var(--muted); border: 1px solid var(--border);
    background: rgba(0,0,0,.12); padding: 8px 12px; border-radius: 8px;
    font-size: 13px; font-weight: 700; transition: all .15s ease;
  }
  .top-links a:hover { color: var(--text); border-color: var(--accent); }
  .top-links a.active { color: #fff; background: var(--accent); border-color: var(--accent); }

  .header-controls { display: flex; align-items: center; gap: 18px; flex: 1; justify-content: flex-end; }
  .page-sectors #stockControls { display: none; }
  .page-screener #stockControls { display: none; }
  .page-gmlist #stockControls { display: none; }
  .page-stocks #sectorPage { display: none; }
  .page-stocks #screenerPage { display: none; }
  .page-sectors .search-wrap,
  .page-sectors .date-range,
  .page-sectors #clearDatesBtn,
  .page-sectors #loadBtn,
  .page-sectors #metaInfo,
  .page-sectors #emptyState,
  .page-sectors #spinner,
  .page-sectors #contentArea { display: none !important; }
  .page-screener .search-wrap,
  .page-screener .date-range,
  .page-screener #clearDatesBtn,
  .page-screener #loadBtn,
  .page-screener #metaInfo,
  .page-screener #emptyState,
  .page-screener #spinner,
  .page-screener #contentArea,
  .page-screener #sectorPage { display: none !important; }
  .page-gmlist .search-wrap,
  .page-gmlist .date-range,
  .page-gmlist #clearDatesBtn,
  .page-gmlist #loadBtn,
  .page-gmlist #metaInfo,
  .page-gmlist #emptyState,
  .page-gmlist #spinner,
  .page-gmlist #contentArea,
  .page-gmlist #sectorPage,
  .page-gmlist #screenerPage { display: none !important; }

  .mode-card {
    background: linear-gradient(180deg, rgba(33,37,58,.95), rgba(26,29,39,.98));
    border: 1px solid var(--border); border-radius: 14px; box-shadow: var(--shadow);
    padding: 12px;
  }
  .mode-grid {
    display: grid; grid-template-columns: minmax(175px, 225px) 1fr;
    gap: 10px; align-items: start;
  }
  @media (max-width: 900px) {
    .mode-grid { grid-template-columns: 1fr; }
  }

  .sector-controls { display: flex; flex-direction: column; gap: 8px; }
  .sector-controls label {
    font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px;
  }
  .sector-controls select {
    width: 100%; padding: 8px 10px; border-radius: 8px; border: 1.5px solid var(--border);
    background: var(--card); color: var(--text); font-size: 13px; outline: none;
  }
  .sector-controls select:focus { border-color: var(--accent); }
  .sector-actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .sector-actions button {
    padding: 8px 12px; border-radius: 8px; border: none; cursor: pointer;
    background: var(--accent); color: #fff; font-weight: 700; font-size: 12px;
  }
  .sector-actions button.secondary {
    background: transparent; border: 1px solid var(--border); color: var(--muted);
  }
  .sector-actions button.secondary:hover { color: var(--text); border-color: var(--accent); }

  .sector-shortcuts-wrap {
    margin-top: 8px;
    background: rgba(0,0,0,.10);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 8px;
  }
  .sector-shortcuts-head {
    display: flex; justify-content: space-between; align-items: baseline; gap: 12px;
    margin-bottom: 8px;
  }
  .sector-shortcuts-note { font-size: 12px; color: var(--muted); }
  .sector-shortcuts {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
    gap: 8px;
  }
  .sector-shortcut {
    display: flex; flex-direction: column; gap: 4px;
    padding: 8px 10px;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--card);
    color: var(--text);
    cursor: pointer;
    text-align: left;
    transition: transform .12s ease, border-color .12s ease, background .12s ease;
  }
  .sector-shortcut:hover {
    transform: translateY(-1px);
    border-color: var(--accent);
    background: rgba(79, 142, 247, 0.08);
  }
  .sector-shortcut.active {
    border-color: var(--accent2);
    background: rgba(56, 217, 169, 0.12);
  }
  .sector-shortcut-name {
    font-size: 13px; font-weight: 800; letter-spacing: .4px; text-transform: uppercase;
  }
  .sector-shortcut-count {
    font-size: 11px; color: var(--muted);
  }

  .sector-list {
    margin-top: 10px; overflow: hidden; border-radius: 10px; border: 1px solid var(--border);
  }
  .sector-row {
    display: grid; grid-template-columns: 160px repeat(3, minmax(0, 1fr));
    gap: 0; border-bottom: 1px solid var(--border); background: var(--card);
  }
  .sector-row:nth-child(even) { background: var(--row-alt); }
  .sector-row.header {
    background: var(--th-bg); font-size: 11px; text-transform: uppercase; letter-spacing: .6px;
    color: var(--muted); font-weight: 700;
  }
  .sector-row > div {
    padding: 8px 10px; border-right: 1px solid var(--border); word-break: break-word;
  }
  .sector-row > div:last-child { border-right: none; }
  .sector-row .symbol { font-weight: 700; color: var(--accent2); }
  .sector-empty {
    padding: 26px; color: var(--muted); text-align: center;
  }
  .screener-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 14px;
    box-shadow: var(--shadow); padding: 16px;
  }
  .screener-header {
    display: flex; flex-wrap: wrap; gap: 12px 16px; align-items: end; justify-content: space-between;
    margin-bottom: 14px;
  }
  .screener-controls {
    display: flex; flex-wrap: wrap; gap: 12px; align-items: end;
  }
  .screener-controls label {
    display: block; font-size: 12px; color: var(--muted); margin-bottom: 6px; font-weight: 700;
    letter-spacing: .3px;
  }
  .screener-controls input {
    background: var(--surface); color: var(--text); border: 1px solid var(--border);
    border-radius: 8px; padding: 8px 10px; font-size: 13px;
  }
  .screener-actions { display: flex; gap: 10px; align-items: center; }
  .screener-actions button {
    padding: 9px 14px; border-radius: 8px; border: none; cursor: pointer;
    background: var(--accent); color: #fff; font-weight: 700; font-size: 13px;
  }
  .screener-actions button.secondary {
    background: transparent; border: 1px solid var(--border); color: var(--muted);
  }
  .screener-actions button.secondary:hover {
    color: var(--text); border-color: var(--accent);
  }
  .screener-status {
    margin-top: 10px; font-size: 12px; color: var(--muted); min-height: 18px;
  }
  .screener-status.ok { color: var(--accent2); }
  .screener-status.err { color: var(--red); }
  .screener-meta-strip {
    display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 14px;
  }
  .screener-meta-chip {
    background: rgba(0,0,0,.18); border: 1px solid var(--border); border-radius: 999px;
    padding: 6px 10px; font-size: 12px; color: var(--muted);
  }
  .screener-meta-chip b { color: var(--text); margin-left: 5px; }
  .screener-grid {
    display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px;
  }
  @media (max-width: 1100px) {
    .screener-grid { grid-template-columns: 1fr; }
  }
  .screener-panel {
    background: var(--surface); border: 1px solid var(--border); border-radius: 14px;
    box-shadow: var(--shadow); overflow: hidden;
  }
  .screener-panel.loading { opacity: .65; }
  .screener-panel-head {
    display: flex; justify-content: space-between; align-items: center; gap: 12px;
    padding: 12px 14px; border-bottom: 1px solid var(--border);
  }
  .screener-panel-title { font-size: 15px; font-weight: 800; color: var(--text); }
  .screener-panel-count { font-size: 12px; color: var(--accent2); font-weight: 800; }
  .screener-table-wrap { max-height: 62vh; overflow: auto; }
  .screener-table {
    width: 100%; border-collapse: collapse; font-size: 13px;
  }
  .screener-table thead th {
    position: sticky; top: 0; background: var(--surface); z-index: 1;
    border-bottom: 1px solid var(--border); color: var(--muted); font-size: 11px;
    text-transform: uppercase; letter-spacing: .6px; padding: 10px 12px; text-align: right;
  }
  .screener-table thead th:first-child,
  .screener-table tbody td:first-child { text-align: left; }
  .screener-table tbody td {
    padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,.05); text-align: right;
  }
  .screener-table tbody tr:hover { background: rgba(79, 142, 247, .08); }
  .screener-sym { font-weight: 800; color: var(--accent2); text-decoration: none; }
  .screener-sym {
    border: none; background: transparent; padding: 0; cursor: pointer;
    font: inherit; font-weight: 800; color: var(--accent2); text-align: left;
  }
  .screener-sym:hover { text-decoration: underline; }
  .screener-sym:focus { outline: none; text-decoration: underline; }
  .screener-empty { padding: 24px; color: var(--muted); text-align: center; }

  .gmlist-tabs {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 12px 0 14px;
  }
  .gmlist-tab {
    border: 1px solid var(--border);
    border-radius: 10px;
    background: rgba(0,0,0,.10);
    color: var(--muted);
    padding: 10px 14px;
    cursor: pointer;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: .5px;
    display: flex;
    align-items: center;
    gap: 8px;
    transition: all .15s ease;
  }
  .gmlist-tab .gmlist-tab-count {
    font-size: 11px;
    color: var(--accent2);
    font-weight: 800;
  }
  .gmlist-tab.active {
    color: #fff;
    border-color: var(--accent2);
    background: rgba(56, 217, 169, .12);
  }
  .gmlist-panel {
    display: none;
  }
  .gmlist-panel.active {
    display: block;
  }
  .gmlist-panel-note {
    font-size: 12px;
    color: var(--muted);
    margin: 10px 0 0;
  }
  .gmlist-coming-soon {
    padding: 18px;
    color: var(--muted);
    border: 1px dashed var(--border);
    border-radius: 10px;
    background: rgba(0,0,0,.08);
  }
  .gmlist-intraday-wrap {
    margin-top: 14px;
  }
  .gmlist-intraday-card {
    margin-top: 10px;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: rgba(0,0,0,.10);
    padding: 10px;
  }
  .sector-spinner {
    width: 34px; height: 34px; border: 3px solid var(--border); border-top-color: var(--accent);
    border-radius: 50%; animation: spin .7s linear infinite; margin: 20px 0; display: none;
  }

  .sector-board {
    margin-top: 12px;
  }
  .sector-board-head {
    display: flex; justify-content: space-between; align-items: baseline; gap: 12px;
    margin-bottom: 10px;
  }
  .sector-board-note {
    font-size: 12px; color: var(--muted);
  }
  .sector-chart-grid {
    display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }
  @media (max-width: 1200px) {
    .sector-chart-grid { grid-template-columns: 1fr; }
  }
  @media (max-width: 760px) {
    .sector-chart-grid { grid-template-columns: 1fr; }
  }
  .sector-chart-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 14px;
    box-shadow: var(--shadow); padding: 10px 10px 8px;
  }
  .sector-chart-head {
    display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;
    margin-bottom: 8px;
  }
  .sector-chart-title {
    font-size: 15px; font-weight: 800; color: var(--text); letter-spacing: .3px;
  }
  .sector-chart-sector {
    font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px;
    margin-top: 2px;
  }
  .sector-chart-meta {
    font-size: 12px; font-weight: 700; color: var(--accent2); white-space: nowrap;
    display: flex; align-items: center; gap: 8px;
  }
  .sector-chart-expand {
    border: 1px solid var(--border); background: transparent; color: var(--muted);
    border-radius: 8px; padding: 4px 8px; font-size: 11px; font-weight: 700;
    cursor: pointer; transition: all .15s ease;
  }
  .sector-chart-expand:hover { color: var(--text); border-color: var(--accent); }
  .sector-zoom-overlay {
    position: fixed; inset: 0; background: rgba(8, 10, 16, 0.88); backdrop-filter: blur(8px);
    z-index: 2000; display: none; padding: 22px;
  }
  .sector-zoom-overlay.open { display: block; }
  .screener-preview-overlay {
    background: transparent !important;
    backdrop-filter: none !important;
    pointer-events: none;
    padding: 0 !important;
  }
  .screener-preview-overlay.open {
    display: block;
  }
  .screener-preview-overlay .screener-preview-card {
    pointer-events: auto;
    position: fixed;
    top: 110px;
    left: 50%;
    transform: translateX(-50%);
    width: min(1180px, calc(100vw - 40px));
    height: min(740px, calc(100vh - 140px));
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    box-shadow: 0 22px 70px rgba(0,0,0,.45);
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .sector-zoom-card {
    height: 100%; max-width: 1400px; margin: 0 auto; background: var(--surface);
    border: 1px solid var(--border); border-radius: 16px; box-shadow: var(--shadow);
    display: flex; flex-direction: column; overflow: hidden;
  }
  .sector-zoom-head {
    display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;
    padding: 14px 16px; border-bottom: 1px solid var(--border); background: rgba(0,0,0,.1);
  }
  .sector-zoom-title {
    font-size: 18px; font-weight: 900; color: var(--text);
  }
  .sector-zoom-subtitle {
    font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px;
    margin-top: 2px;
  }
  .sector-zoom-meta {
    font-size: 13px; font-weight: 800; color: var(--accent2); white-space: nowrap;
  }
  .sector-zoom-nav {
    display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
  }
  .sector-zoom-step {
    border: 1px solid var(--border); background: transparent; color: var(--text);
    border-radius: 10px; padding: 8px 12px; font-weight: 700; cursor: pointer;
  }
  .sector-zoom-step:hover { border-color: var(--accent); }
  .sector-zoom-step:disabled {
    opacity: .45; cursor: not-allowed;
  }
  .sector-zoom-counter {
    font-size: 12px; color: var(--muted); min-width: 84px; text-align: center;
  }
  .sector-zoom-close {
    border: 1px solid var(--border); background: transparent; color: var(--text);
    border-radius: 10px; padding: 8px 12px; font-weight: 700; cursor: pointer;
  }
  .sector-zoom-close:hover { border-color: var(--accent); }
  .sector-zoom-chart {
    flex: 1; min-height: 0; padding: 12px;
  }
  .sector-zoom-chart-inner {
    width: 100%; height: 100%; min-height: 520px; border-radius: 12px; overflow: hidden;
    position: relative;
  }
  .sector-mini-chart {
    width: 100%; height: 260px; border-radius: 10px; overflow: hidden;
    position: relative;
  }
  .sector-chart-hud {
    position: absolute; left: 10px; top: 10px; z-index: 25;
    min-width: 190px; max-width: 260px;
    padding: 4px 6px; border-radius: 6px;
    border: none;
    background: transparent;
    box-shadow: none;
    color: var(--text); font-size: 10px; line-height: 1.2;
    text-shadow: 0 1px 2px rgba(0,0,0,.35);
    pointer-events: none;
  }
  .sector-chart-hud.is-zoom {
    top: 12px; left: 12px;
    min-width: 220px; max-width: 300px;
    font-size: 11px;
    line-height: 1.25;
  }
  .sector-chart-hud .hud-grid {
    display: grid;
    grid-template-columns: repeat(3, max-content);
    gap: 4px 12px;
    align-items: center;
  }
  .sector-chart-hud .hud-item {
    display: inline-flex; align-items: center; gap: 4px;
    white-space: nowrap;
  }
  .sector-chart-hud .hud-key { color: var(--muted); font-weight: 700; }
  .sector-chart-hud .hud-val { font-weight: 800; color: var(--text); }
  .sector-chart-hud.is-zoom .hud-key,
  .sector-chart-hud.is-zoom .hud-val {
    font-size: 11px;
  }
  .sector-chart-hud .hud-pos { color: var(--accent2); }
  .sector-chart-hud .hud-neg { color: #ef6a6a; }
  .sector-chart-hud .hud-empty {
    color: var(--muted); font-size: 10px; letter-spacing: .3px;
  }
  .intraday-day-separator {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 1px;
    background: rgba(245, 158, 11, 0.28);
    box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.06);
    pointer-events: none;
    z-index: 22;
  }
  .sector-board-empty {
    padding: 24px; text-align: center; color: var(--muted); border: 1px dashed var(--border);
    border-radius: 12px; background: rgba(0,0,0,.08);
  }

  /* ── section title ── */
  .section-title {
    font-size: 12px; font-weight: 700; color: var(--muted); text-transform: uppercase;
    letter-spacing: 1px; margin-bottom: 10px; border-left: 3px solid var(--accent);
    padding-left: 10px;
  }

  /* ── table container ── */
  .tbl-wrap {
    overflow-x: auto; border-radius: 10px; border: 1px solid var(--border);
    box-shadow: var(--shadow);
  }
  table { border-collapse: collapse; width: 100%; min-width: 1100px; }
  thead tr { background: var(--th-bg); position: sticky; top: 0; z-index: 50; }
  thead th {
    padding: 10px 12px; text-align: right; font-size: 11px; font-weight: 700;
    color: var(--muted); text-transform: uppercase; letter-spacing: .6px;
    border-bottom: 2px solid var(--border); white-space: nowrap; cursor: pointer;
    user-select: none;
  }
  thead th:first-child,
  thead th:nth-child(2) { text-align: left; }
  thead th:hover { color: var(--accent); }
  thead th .sort-arrow { opacity: .4; font-size: 10px; margin-left: 3px; }
  thead th.sorted .sort-arrow { opacity: 1; color: var(--accent); }

  tbody tr { border-bottom: 1px solid var(--border); transition: background .1s; }
  tbody tr:nth-child(even) { background: var(--row-alt); }
  tbody tr:hover { background: var(--border); }
  td { padding: 7px 12px; text-align: right; font-size: 13px; white-space: nowrap; }
  td:first-child { text-align: left; font-weight: 600; color: var(--muted); }
  td:nth-child(2) { text-align: left; font-weight: 700; font-size: 13px; }

  /* colour cells */
  .pos { color: var(--green); font-weight: 600; }
  .neg { color: var(--red);   font-weight: 600; }
  .close-hi { color: var(--accent2); font-weight: 700; }
  .badge {
    display: inline-block; padding: 1px 8px; border-radius: 4px; font-size: 11px;
    font-weight: 700; letter-spacing: .4px;
  }
  .badge-n { background: #1a2e20; color: var(--green); }
  .badge-y { background: #2e1a1a; color: var(--red); }

  /* ── empty / loader ── */
  #emptyState { text-align: center; padding: 80px 20px; color: var(--muted); display: flex;
                flex-direction: column; align-items: center; gap: 14px; }
  #emptyState .big-icon { font-size: 3.5rem; }
  #emptyState p { font-size: 1rem; }
  #emptyState small { font-size: .85rem; }

  .spinner { width: 40px; height: 40px; border: 3px solid var(--border);
             border-top-color: var(--accent); border-radius: 50%;
             animation: spin .7s linear infinite; margin: 60px auto; display: none; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── highlight rows ── */
  tr.hl-lowvol td {
    background: rgba(81, 207, 102, 0.13) !important;
  }
  tr.hl-lowvol td.hl-cell-vol {
    background: rgba(81, 207, 102, 0.45) !important;
    color: #a8f5b8 !important; font-weight: 800;
    box-shadow: inset 0 0 0 1px #51cf66;
  }
  tr.hl-lowvol td:first-child {
    border-left: 4px solid #51cf66;
    color: #51cf66 !important; font-weight: 700;
  }
  tr.hl-lowvol td:first-child::after { content: ' 📦'; font-size: 11px; }

  tr.hl-lowvolatility td {
    background: rgba(79, 142, 247, 0.13) !important;
  }
  tr.hl-lowvolatility td.hl-cell-vol {
    background: rgba(79, 142, 247, 0.45) !important;
    color: #a8cbff !important; font-weight: 800;
    box-shadow: inset 0 0 0 1px #4f8ef7;
  }
  tr.hl-lowvolatility td:first-child {
    border-left: 4px solid #4f8ef7;
    color: #4f8ef7 !important; font-weight: 700;
  }
  tr.hl-lowvolatility td:first-child::after { content: ' 🧊'; font-size: 11px; }

  /* both at once — purple override */
  tr.hl-lowvol.hl-lowvolatility td {
    background: rgba(204, 119, 255, 0.15) !important;
  }
  tr.hl-lowvol.hl-lowvolatility td:first-child {
    border-left: 4px solid #cc77ff;
    color: #cc77ff !important;
  }
  tr.hl-lowvol.hl-lowvolatility td:first-child::after { content: ' 📦🧊'; font-size: 11px; }

  /* legend */
  .hl-legend { display: flex; gap: 18px; margin-bottom: 10px; flex-wrap: wrap; }
  .hl-legend-item { display: flex; align-items: center; gap: 7px; font-size: 12px;
                    color: var(--muted); }
  .hl-dot { width: 12px; height: 12px; border-radius: 3px; flex-shrink: 0; }

  /* ── scrollbar ── */
  ::-webkit-scrollbar { width: 7px; height: 7px; }
  ::-webkit-scrollbar-track { background: var(--surface); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--accent); }
</style>
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body class="page-{{ page_mode }}">

<header>
  <h1>NSE <span>BHAV</span> Viewer</h1>

  <div class="top-links">
    <a href="/" class="{{ 'active' if page_mode == 'stocks' else '' }}">Stocks</a>
    <a href="/sectors" class="{{ 'active' if page_mode == 'sectors' else '' }}">Sectors</a>
    <a href="/screener" class="{{ 'active' if page_mode == 'screener' else '' }}">Screener</a>
    <a href="/gmlist" class="{{ 'active' if page_mode == 'gmlist' else '' }}">GMList</a>
  </div>

  <div class="search-wrap">
    <input id="symInput" type="text" placeholder="Type symbol e.g. KTKBANK"
           autocomplete="off" spellcheck="false">
    <div class="autocomplete-list" id="acList"></div>
  </div>

  <div class="date-range">
    <label>From</label>
    <input type="date" id="fromDate" title="From date (leave blank for last 100 days)">
    <span class="date-sep">→</span>
    <label>To</label>
    <input type="date" id="toDate" title="To date (leave blank for latest available)">
    <button id="clearDatesBtn" onclick="clearDates()" title="Reset to default (last 100 days)">✕ Clear</button>
  </div>

  <button id="loadBtn" onclick="loadStock()">Load</button>

  <div id="metaInfo" style="font-size:12px;color:var(--muted);white-space:nowrap;"></div>
</header>

<div id="statusBar"></div>
<div id="sectorZoomOverlay" class="sector-zoom-overlay" onclick="closeSectorZoom(event)">
  <div class="sector-zoom-card">
    <div class="sector-zoom-head">
      <div>
        <div class="sector-zoom-title" id="sectorZoomTitle"></div>
        <div class="sector-zoom-subtitle" id="sectorZoomSubtitle"></div>
      </div>
      <div class="sector-zoom-nav">
        <button class="sector-zoom-step" type="button" onclick="stepSectorZoom(-1)">Prev</button>
        <div class="sector-zoom-counter" id="sectorZoomCounter">0 / 0</div>
        <button class="sector-zoom-step" type="button" onclick="stepSectorZoom(1)">Next</button>
        <div class="sector-zoom-meta" id="sectorZoomMeta"></div>
        <button class="sector-zoom-close" type="button" onclick="closeSectorZoom()">Close</button>
      </div>
    </div>
    <div class="sector-zoom-chart">
      <div id="sectorZoomChart" class="sector-zoom-chart-inner"></div>
    </div>
  </div>
</div>

<div id="screenerPreviewOverlay" class="sector-zoom-overlay screener-preview-overlay" onclick="closeScreenerPreview(event)">
  <div class="sector-zoom-card screener-preview-card" id="screenerPreviewCard">
    <div class="sector-zoom-head">
      <div>
        <div class="sector-zoom-title" id="screenerPreviewTitle"></div>
        <div class="sector-zoom-subtitle" id="screenerPreviewSubtitle"></div>
      </div>
      <div class="sector-zoom-nav">
        <div class="sector-zoom-meta" id="screenerPreviewMeta"></div>
        <button class="sector-zoom-close" type="button" onclick="closeScreenerPreview()">Close</button>
      </div>
    </div>
    <div class="sector-zoom-chart">
      <div id="screenerPreviewChart" class="sector-zoom-chart-inner"></div>
    </div>
  </div>
</div>

<div id="gmlistStrongStartOverlay" class="sector-zoom-overlay screener-preview-overlay" onclick="closeGmListStrongStartOverlay(event)">
  <div class="sector-zoom-card screener-preview-card" id="gmlistStrongStartCard" style="width:min(98vw, 1550px); height:min(760px, calc(100vh - 140px)); top:110px;"
       onpointerenter="handleGmStrongStartOverlayEnter()" onpointerleave="handleGmStrongStartOverlayLeave()">
    <div class="sector-zoom-head">
      <div>
        <div class="sector-zoom-title" id="gmlistStrongStartTitle"></div>
        <div class="sector-zoom-subtitle" id="gmlistStrongStartSubtitle"></div>
      </div>
      <div class="sector-zoom-nav">
        <div class="sector-zoom-meta" id="gmlistStrongStartMeta"></div>
        <button class="sector-zoom-close" type="button" onclick="closeGmListStrongStartOverlay()">Close</button>
      </div>
    </div>
    <div class="sector-zoom-chart">
      <div id="gmlistStrongStartChart" class="sector-zoom-chart-inner"></div>
    </div>
  </div>
</div>

<main>
  <section id="sectorPage" style="display:none;">
    <div class="mode-card">
      <div class="mode-grid">
        <div class="sector-controls">
          <div>
            <label for="sectorSelect">Choose a sector</label>
            <select id="sectorSelect">
              <option value="">Loading sectors...</option>
            </select>
          </div>
          <div class="sector-actions">
            <button id="sectorLoadBtn" onclick="loadSectorCharts()">Load Charts</button>
          </div>
          <div class="sector-shortcuts-wrap">
            <div class="sector-shortcuts-head">
              <div class="section-title" style="margin:0;">Top 15 Sectors</div>
              <div class="sector-shortcuts-note">Click a sector to load its charts.</div>
            </div>
            <div class="sector-shortcuts" id="sectorShortcuts">
              <div class="sector-shortcuts-note">Loading shortcuts...</div>
            </div>
          </div>
        </div>
        <div>
          <div id="sectorSpinner" class="sector-spinner"></div>
          <div class="sector-board">
            <div class="sector-board-head">
              <div class="section-title" style="margin:0;">Sector Chart Board</div>
              <div class="sector-board-note" id="sectorBoardNote">Choose a sector to load chart cards.</div>
            </div>
            <div id="sectorBoardEmpty" class="sector-board-empty">No sector charts loaded yet.</div>
            <div id="sectorBoardGrid" class="sector-chart-grid" style="display:none;"></div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <section id="screenerPage" style="display:none;">
    <div class="screener-card">
      <div class="screener-header">
      <div>
          <div class="section-title" style="margin:0;">Bhav Screener</div>
          <div class="section-note">Low-volume and low-volatility stocks on 21-day windows, filtered to names near 52-week highs.</div>
        </div>
        <div class="screener-controls">
          <div>
            <label for="screenerAsOf">As of</label>
            <input id="screenerAsOf" type="date">
          </div>
          <div>
            <label for="screenerTurnover">Min avg turnover</label>
            <input id="screenerTurnover" type="number" min="1" step="0.1" value="10" style="width:110px">
          </div>
          <div>
            <label for="screenerLimit">Limit</label>
            <input id="screenerLimit" type="number" min="10" max="1000" value="200" style="width:85px">
          </div>
          <div class="screener-actions">
            <button id="screenerRunBtn" type="button">Run</button>
            <button id="screenerTodayBtn" type="button" class="secondary">Today</button>
          </div>
        </div>
      </div>

      <div id="screenerStatus" class="screener-status">Pick a date and click Run. (Defaults to the latest trading date in bhav.)</div>
      <div id="screenerMetaStrip" class="screener-meta-strip"></div>

      <div class="screener-grid">
        <div class="screener-panel" id="screenerPanelVol">
          <div class="screener-panel-head">
            <div class="screener-panel-title">Low Volume</div>
            <div class="screener-panel-count"><span id="screenerCountVol">0</span> stocks</div>
          </div>
          <div class="screener-table-wrap">
            <table class="screener-table">
              <thead>
                <tr>
                  <th>Symbol</th><th>Close</th><th>Volume</th><th>21d Min</th><th>21d Turnover (cr)</th>
                </tr>
              </thead>
              <tbody id="screenerTbodyVol"></tbody>
            </table>
          </div>
          <div id="screenerEmptyVol" class="screener-empty" style="display:none;">No low-volume names found.</div>
        </div>

        <div class="screener-panel" id="screenerPanelVlt">
          <div class="screener-panel-head">
            <div class="screener-panel-title">Low Volatility</div>
            <div class="screener-panel-count"><span id="screenerCountVlt">0</span> stocks</div>
          </div>
          <div class="screener-table-wrap">
            <table class="screener-table">
              <thead>
                <tr>
                  <th>Symbol</th><th>Close</th><th>Volatility</th><th>21d Min</th><th>21d Turnover (cr)</th>
                </tr>
              </thead>
              <tbody id="screenerTbodyVlt"></tbody>
            </table>
          </div>
          <div id="screenerEmptyVlt" class="screener-empty" style="display:none;">No low-volatility names found.</div>
        </div>
      </div>
    </div>
  </section>

  <section id="gmlistPage" style="display:none;">
    <div class="screener-card">
      <div class="screener-header">
        <div>
          <div class="section-title" style="margin:0;">GMList Screener</div>
          <div class="section-note">Screens only the names in <code>gmlist/updated_gmlist.txt</code>. Use the date chooser, then switch between the four sub tabs.</div>
        </div>
        <div class="screener-controls">
          <div>
            <label for="gmlistAsOf">As of</label>
            <input id="gmlistAsOf" type="date">
          </div>
          <div class="screener-actions">
            <button id="gmlistRunBtn" type="button">Run</button>
            <button id="gmlistTodayBtn" type="button" class="secondary">Today</button>
          </div>
        </div>
      </div>

      <div id="gmlistStatus" class="screener-status">Pick a date and click Run.</div>
      <div id="gmlistMetaStrip" class="screener-meta-strip"></div>

      <div class="gmlist-tabs" id="gmlistTabs">
        <button class="gmlist-tab active" type="button" data-tab="lv21">
          lv21 <span class="gmlist-tab-count" id="gmlistCountTabLv21">0</span>
        </button>
        <button class="gmlist-tab" type="button" data-tab="lowvol21">
          lowvol21 <span class="gmlist-tab-count" id="gmlistCountTabLowvol21">0</span>
        </button>
        <button class="gmlist-tab" type="button" data-tab="inside_days">
          inside days <span class="gmlist-tab-count" id="gmlistCountTabInsideDays">0</span>
        </button>
        <button class="gmlist-tab" type="button" data-tab="hd">
          hd <span class="gmlist-tab-count" id="gmlistCountTabHd">0</span>
        </button>
        <button class="gmlist-tab" type="button" data-tab="live">
          live <span class="gmlist-tab-count" id="gmlistCountTabLive">0</span>
        </button>
        <button class="gmlist-tab" type="button" data-tab="strong_start">
          strong start <span class="gmlist-tab-count" id="gmlistCountTabStrongStart">0</span>
        </button>
      </div>

      <div class="gmlist-panel active" data-panel="lv21">
        <div class="screener-panel-head">
          <div class="screener-panel-title">Least Volatile in 21 Days</div>
          <div class="screener-panel-count"><span id="gmlistCountPanelLv21">0</span> stocks</div>
        </div>
        <div class="screener-table-wrap">
          <table class="screener-table">
            <thead>
              <tr>
                <th>Symbol</th><th>Close</th><th>Volatility</th><th>21d Min</th><th>21d Turnover (cr)</th>
              </tr>
            </thead>
            <tbody id="gmlistTbodyLv21"></tbody>
          </table>
        </div>
        <div id="gmlistEmptyLv21" class="screener-empty" style="display:none;">No lv21 names found.</div>
      </div>

      <div class="gmlist-panel" data-panel="lowvol21">
        <div class="screener-panel-head">
          <div class="screener-panel-title">Lowest Volume in 21 Days</div>
          <div class="screener-panel-count"><span id="gmlistCountPanelLowvol21">0</span> stocks</div>
        </div>
        <div class="screener-table-wrap">
          <table class="screener-table">
            <thead>
              <tr>
                <th>Symbol</th><th>Close</th><th>Volume</th><th>21d Min</th><th>21d Turnover (cr)</th>
              </tr>
            </thead>
            <tbody id="gmlistTbodyLowvol21"></tbody>
          </table>
        </div>
        <div id="gmlistEmptyLowvol21" class="screener-empty" style="display:none;">No lowvol21 names found.</div>
      </div>

      <div class="gmlist-panel" data-panel="inside_days">
        <div class="screener-panel-head">
          <div class="screener-panel-title">Inside Days</div>
          <div class="screener-panel-count"><span id="gmlistCountPanelInsideDays">0</span> stocks</div>
        </div>
        <div class="screener-table-wrap">
          <table class="screener-table">
            <thead>
              <tr>
                <th>Symbol</th><th>Close</th><th>High</th><th>Low</th><th>Prev High</th><th>Prev Low</th><th>21d Turnover (cr)</th>
              </tr>
            </thead>
            <tbody id="gmlistTbodyInsideDays"></tbody>
          </table>
        </div>
        <div id="gmlistEmptyInsideDays" class="screener-empty" style="display:none;">No inside-day names found.</div>
      </div>

      <div class="gmlist-panel" data-panel="hd">
        <div class="screener-panel-head">
          <div class="screener-panel-title">High Delivery 3D</div>
          <div class="screener-panel-count"><span id="gmlistCountPanelHd">0</span> stocks</div>
        </div>
        <div class="screener-table-wrap">
          <table class="screener-table">
            <thead>
              <tr>
                <th>Symbol</th><th>Close</th><th>Latest Del %</th><th>Days >= 60%</th><th>Best Del %</th><th>Best Day</th>
              </tr>
            </thead>
            <tbody id="gmlistTbodyHd"></tbody>
          </table>
        </div>
        <div id="gmlistEmptyHd" class="screener-empty" style="display:none;">No high-delivery names found.</div>
      </div>

      <div class="gmlist-panel" data-panel="live">
        <div class="screener-panel-head">
          <div class="screener-panel-title">Live Intraday</div>
          <div class="screener-panel-count"><span id="gmlistCountPanelLive">0</span> stocks</div>
        </div>
        <div class="screener-controls" style="margin-bottom:10px;">
          <div>
            <label for="gmlistLiveAsOf">Live as of</label>
            <input id="gmlistLiveAsOf" type="date">
          </div>
          <div class="screener-actions">
            <button id="gmlistLiveRunBtn" type="button">Run</button>
            <button id="gmlistLiveTodayBtn" type="button" class="secondary">Today</button>
          </div>
        </div>
        <div id="gmlistLiveStatus" class="gmlist-panel-note"></div>
        <div id="gmlistLiveMetaStrip" class="screener-meta-strip"></div>
        <div class="screener-grid">
          <div class="screener-panel" id="gmlistLivePanelLv21">
            <div class="screener-panel-head">
              <div class="screener-panel-title">Live Least Volatile</div>
              <div class="screener-panel-count"><span id="gmlistCountPanelLiveLv21">0</span> stocks</div>
            </div>
            <div class="screener-table-wrap">
              <table class="screener-table">
                <thead>
                  <tr>
                    <th>Symbol</th><th>Close</th><th>Volatility</th><th>21d Min</th><th>21d Turnover (cr)</th>
                  </tr>
                </thead>
                <tbody id="gmlistLiveTbodyLv21"></tbody>
              </table>
            </div>
            <div id="gmlistLiveEmptyLv21" class="screener-empty" style="display:none;">No live lv21 names found.</div>
          </div>
          <div class="screener-panel" id="gmlistLivePanelLowvol21">
            <div class="screener-panel-head">
              <div class="screener-panel-title">Live Lowest Volume</div>
              <div class="screener-panel-count"><span id="gmlistCountPanelLiveLowvol21">0</span> stocks</div>
            </div>
            <div class="screener-table-wrap">
              <table class="screener-table">
                <thead>
                  <tr>
                    <th>Symbol</th><th>Close</th><th>Volume</th><th>21d Min</th><th>21d Turnover (cr)</th>
                  </tr>
                </thead>
                <tbody id="gmlistLiveTbodyLowvol21"></tbody>
              </table>
            </div>
            <div id="gmlistLiveEmptyLowvol21" class="screener-empty" style="display:none;">No live lowvol21 names found.</div>
          </div>
          <div class="screener-panel" id="gmlistLivePanelInsideDays">
            <div class="screener-panel-head">
              <div class="screener-panel-title">Live Inside Days</div>
              <div class="screener-panel-count"><span id="gmlistCountPanelLiveInsideDays">0</span> stocks</div>
            </div>
            <div class="screener-table-wrap">
              <table class="screener-table">
                <thead>
                  <tr>
                    <th>Symbol</th><th>Close</th><th>High</th><th>Low</th><th>Prev High</th><th>Prev Low</th><th>21d Turnover (cr)</th>
                  </tr>
                </thead>
                <tbody id="gmlistLiveTbodyInsideDays"></tbody>
              </table>
            </div>
            <div id="gmlistLiveEmptyInsideDays" class="screener-empty" style="display:none;">No live inside-day names found.</div>
          </div>
        </div>
        <div class="gmlist-panel-note">Live scan uses Kite daily chart data and a volatility proxy from daily high/low range.</div>
      </div>

      <div class="gmlist-panel" data-panel="strong_start">
        <div class="screener-panel-head">
          <div class="screener-panel-title">Strong Start</div>
          <div class="screener-panel-count"><span id="gmlistCountPanelStrongStart">0</span> stocks</div>
        </div>
        <div class="screener-controls" style="margin-bottom:10px;">
          <div>
            <label for="gmlistStrongStartAsOf">Strong start as of</label>
            <input id="gmlistStrongStartAsOf" type="date">
          </div>
          <div class="screener-actions">
            <button id="gmlistStrongStartRunBtn" type="button">Run</button>
          </div>
        </div>
        <div id="gmlistStrongStartNote" class="gmlist-panel-note"></div>
        <div class="screener-table-wrap">
          <table class="screener-table">
            <thead>
              <tr>
                <th>Symbol</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Prev High</th><th>Gap %</th>
              </tr>
            </thead>
            <tbody id="gmlistTbodyStrongStart"></tbody>
          </table>
        </div>
        <div id="gmlistEmptyStrongStart" class="screener-empty" style="display:none;">No strong-start names found.</div>
        <div class="gmlist-panel-note">Hover a symbol to open its 5-minute Kite popup chart covering roughly the last 5 trading days.</div>
      </div>
    </div>
  </section>

  <div id="emptyState">
    <div class="big-icon">📊</div>
    <p>Enter a stock symbol to view BHAV data</p>
    <small>Default: last 100 days · Use From/To dates to customise range · bhav2024 · bhav2025 · bhav2026</small>
  </div>

  <div class="spinner" id="spinner"></div>

  <div id="contentArea" style="display:none;">
    <div class="cards" id="summaryCards"></div>
    <div class="section-title">OHLCV · DMAs · Computed Metrics</div>
    <div class="hl-legend">
      <div class="hl-legend-item">
        <div class="hl-dot" style="background:#51cf66"></div>
        <span>📦 Lowest volume in 21-day window</span>
      </div>
      <div class="hl-legend-item">
        <div class="hl-dot" style="background:#4f8ef7"></div>
        <span>🧊 Least volatile in 21-day window</span>
      </div>
      <div class="hl-legend-item">
        <div class="hl-dot" style="background:#cc77ff"></div>
        <span>📦🧊 Both</span>
      </div>
    </div>
    <div class="tbl-wrap">
      <table id="mainTable">
        <thead>
          <tr>
            <th data-col="mktdate">Date <span class="sort-arrow">▼</span></th>
            <th data-col="symbol">Symbol <span class="sort-arrow"></span></th>
            <th data-col="close">Close <span class="sort-arrow"></span></th>
            <th data-col="open">Open <span class="sort-arrow"></span></th>
            <th data-col="high">High <span class="sort-arrow"></span></th>
            <th data-col="low">Low <span class="sort-arrow"></span></th>
            <th data-col="prevclose">Prev Close <span class="sort-arrow"></span></th>
            <th data-col="diff">Diff % <span class="sort-arrow"></span></th>
            <th data-col="volume">Volume <span class="sort-arrow"></span></th>
            <th data-col="deliveryvolume">Del Vol <span class="sort-arrow"></span></th>
            <th data-col="delper">Del % <span class="sort-arrow"></span></th>
            <th data-col="VOLATILITY">Volatility <span class="sort-arrow"></span></th>
            <th data-col="jag">Jag % <span class="sort-arrow"></span></th>
            <th data-col="closeindictor">Close Ind <span class="sort-arrow"></span></th>
            <th data-col="5dma">5 DMA <span class="sort-arrow"></span></th>
            <th data-col="10dma">10 DMA <span class="sort-arrow"></span></th>
            <th data-col="20DMA">20 DMA <span class="sort-arrow"></span></th>
            <th data-col="50dma">50 DMA <span class="sort-arrow"></span></th>
          </tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
  </div>
</main>

<script>
// ── state ──────────────────────────────────────────────────────────────────
const PAGE_MODE = {{ page_mode|tojson }};
let allRows = [];
let sortCol = 'mktdate';
let sortAsc = false;   // default: newest first
let acSelected = -1;

// ── autocomplete ────────────────────────────────────────────────────────────
const symInput = document.getElementById('symInput');
const acList   = document.getElementById('acList');
let acTimeout;

symInput.addEventListener('input', () => {
  clearTimeout(acTimeout);
  const q = symInput.value.trim();
  acTimeout = setTimeout(() => fetchAC(q), 180);
});

symInput.addEventListener('keydown', e => {
  const items = acList.querySelectorAll('.ac-item');
  if (e.key === 'ArrowDown') {
    acSelected = Math.min(acSelected + 1, items.length - 1);
    highlightAC(items);
  } else if (e.key === 'ArrowUp') {
    acSelected = Math.max(acSelected - 1, 0);
    highlightAC(items);
  } else if (e.key === 'Enter') {
    if (acSelected >= 0 && items[acSelected]) {
      symInput.value = items[acSelected].textContent;
      closeAC();
    }
    loadStock();
  } else if (e.key === 'Escape') {
    closeAC();
  }
});

document.addEventListener('click', e => {
  if (!symInput.contains(e.target) && !acList.contains(e.target)) closeAC();
});

function highlightAC(items) {
  items.forEach((el, i) => el.classList.toggle('active', i === acSelected));
  if (acSelected >= 0 && items[acSelected]) {
    items[acSelected].scrollIntoView({ block: 'nearest' });
  }
}

async function fetchAC(q) {
  if (!q) { closeAC(); return; }
  try {
    const res = await fetch(`/api/symbols?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    if (data.error || !data.length) { closeAC(); return; }
    acSelected = -1;
    acList.innerHTML = data.map(s =>
      `<div class="ac-item" onclick="pickAC('${s}')">${s}</div>`
    ).join('');
    acList.classList.add('open');
  } catch { closeAC(); }
}

function pickAC(sym) {
  symInput.value = sym;
  closeAC();
  loadStock();
}

function closeAC() {
  acList.classList.remove('open');
  acList.innerHTML = '';
}

// ── clear dates ──────────────────────────────────────────────────────────────
// â”€â”€ sector browser â”€â”€
const sectorSelect = document.getElementById('sectorSelect');
const sectorSpinner = document.getElementById('sectorSpinner');
const sectorPage = document.getElementById('sectorPage');
const sectorShortcuts = document.getElementById('sectorShortcuts');
const sectorBoardEmpty = document.getElementById('sectorBoardEmpty');
const sectorBoardGrid = document.getElementById('sectorBoardGrid');
const sectorBoardNote = document.getElementById('sectorBoardNote');
const sectorZoomOverlay = document.getElementById('sectorZoomOverlay');
const sectorZoomTitle = document.getElementById('sectorZoomTitle');
const sectorZoomSubtitle = document.getElementById('sectorZoomSubtitle');
const sectorZoomMeta = document.getElementById('sectorZoomMeta');
const sectorZoomCounter = document.getElementById('sectorZoomCounter');
const sectorZoomChartEl = document.getElementById('sectorZoomChart');
const screenerPreviewOverlay = document.getElementById('screenerPreviewOverlay');
const screenerPreviewTitle = document.getElementById('screenerPreviewTitle');
const screenerPreviewSubtitle = document.getElementById('screenerPreviewSubtitle');
const screenerPreviewMeta = document.getElementById('screenerPreviewMeta');
const screenerPreviewChartEl = document.getElementById('screenerPreviewChart');
const screenerPreviewCard = document.getElementById('screenerPreviewCard');
const screenerPage = document.getElementById('screenerPage');
const gmlistPage = document.getElementById('gmlistPage');
const gmlistAsOfEl = document.getElementById('gmlistAsOf');
const gmlistStrongStartAsOfEl = document.getElementById('gmlistStrongStartAsOf');
const gmlistRunBtn = document.getElementById('gmlistRunBtn');
const gmlistStrongStartRunBtn = document.getElementById('gmlistStrongStartRunBtn');
const gmlistTodayBtn = document.getElementById('gmlistTodayBtn');
const gmlistStatus = document.getElementById('gmlistStatus');
const gmlistMetaStrip = document.getElementById('gmlistMetaStrip');
const gmlistTabs = document.getElementById('gmlistTabs');
const gmlistCountTabLv21 = document.getElementById('gmlistCountTabLv21');
const gmlistCountTabLowvol21 = document.getElementById('gmlistCountTabLowvol21');
const gmlistCountTabInsideDays = document.getElementById('gmlistCountTabInsideDays');
const gmlistCountTabHd = document.getElementById('gmlistCountTabHd');
const gmlistCountTabLive = document.getElementById('gmlistCountTabLive');
const gmlistCountTabStrongStart = document.getElementById('gmlistCountTabStrongStart');
const gmlistCountPanelLv21 = document.getElementById('gmlistCountPanelLv21');
const gmlistCountPanelLowvol21 = document.getElementById('gmlistCountPanelLowvol21');
const gmlistCountPanelInsideDays = document.getElementById('gmlistCountPanelInsideDays');
const gmlistCountPanelHd = document.getElementById('gmlistCountPanelHd');
const gmlistCountPanelLive = document.getElementById('gmlistCountPanelLive');
const gmlistCountPanelLiveLv21 = document.getElementById('gmlistCountPanelLiveLv21');
const gmlistCountPanelLiveLowvol21 = document.getElementById('gmlistCountPanelLiveLowvol21');
const gmlistCountPanelLiveInsideDays = document.getElementById('gmlistCountPanelLiveInsideDays');
const gmlistCountPanelStrongStart = document.getElementById('gmlistCountPanelStrongStart');
const gmlistTbodyLv21 = document.getElementById('gmlistTbodyLv21');
const gmlistTbodyLowvol21 = document.getElementById('gmlistTbodyLowvol21');
const gmlistTbodyInsideDays = document.getElementById('gmlistTbodyInsideDays');
const gmlistTbodyHd = document.getElementById('gmlistTbodyHd');
const gmlistLiveAsOfEl = document.getElementById('gmlistLiveAsOf');
const gmlistLiveRunBtn = document.getElementById('gmlistLiveRunBtn');
const gmlistLiveTodayBtn = document.getElementById('gmlistLiveTodayBtn');
const gmlistLiveStatus = document.getElementById('gmlistLiveStatus');
const gmlistLiveMetaStrip = document.getElementById('gmlistLiveMetaStrip');
const gmlistLiveTbodyLv21 = document.getElementById('gmlistLiveTbodyLv21');
const gmlistLiveTbodyLowvol21 = document.getElementById('gmlistLiveTbodyLowvol21');
const gmlistLiveTbodyInsideDays = document.getElementById('gmlistLiveTbodyInsideDays');
const gmlistLiveEmptyLv21 = document.getElementById('gmlistLiveEmptyLv21');
const gmlistLiveEmptyLowvol21 = document.getElementById('gmlistLiveEmptyLowvol21');
const gmlistLiveEmptyInsideDays = document.getElementById('gmlistLiveEmptyInsideDays');
const gmlistEmptyHd = document.getElementById('gmlistEmptyHd');
const gmlistTbodyStrongStart = document.getElementById('gmlistTbodyStrongStart');
const gmlistEmptyLv21 = document.getElementById('gmlistEmptyLv21');
const gmlistEmptyLowvol21 = document.getElementById('gmlistEmptyLowvol21');
const gmlistEmptyInsideDays = document.getElementById('gmlistEmptyInsideDays');
const gmlistEmptyStrongStart = document.getElementById('gmlistEmptyStrongStart');
const gmlistStrongStartNote = document.getElementById('gmlistStrongStartNote');
const gmlistStrongStartOverlay = document.getElementById('gmlistStrongStartOverlay');
const gmlistStrongStartCard = document.getElementById('gmlistStrongStartCard');
const gmlistStrongStartTitle = document.getElementById('gmlistStrongStartTitle');
const gmlistStrongStartSubtitle = document.getElementById('gmlistStrongStartSubtitle');
const gmlistStrongStartMeta = document.getElementById('gmlistStrongStartMeta');
const gmlistStrongStartChartEl = document.getElementById('gmlistStrongStartChart');
const screenerAsOfEl = document.getElementById('screenerAsOf');
const screenerTurnoverEl = document.getElementById('screenerTurnover');
const screenerLimitEl = document.getElementById('screenerLimit');
const screenerRunBtn = document.getElementById('screenerRunBtn');
const screenerTodayBtn = document.getElementById('screenerTodayBtn');
const screenerStatus = document.getElementById('screenerStatus');
const screenerMetaStrip = document.getElementById('screenerMetaStrip');
const screenerPanelVol = document.getElementById('screenerPanelVol');
const screenerPanelVlt = document.getElementById('screenerPanelVlt');
const screenerTbodyVol = document.getElementById('screenerTbodyVol');
const screenerTbodyVlt = document.getElementById('screenerTbodyVlt');
const screenerCountVol = document.getElementById('screenerCountVol');
const screenerCountVlt = document.getElementById('screenerCountVlt');
const screenerEmptyVol = document.getElementById('screenerEmptyVol');
const screenerEmptyVlt = document.getElementById('screenerEmptyVlt');
let sectorChartInstances = [];
let sectorChartObservers = [];
let sectorBoardObserver = null;
let sectorBoardPayload = [];
let sectorZoomChart = null;
let sectorZoomObserver = null;
let sectorZoomIndex = -1;
let screenerPreviewChart = null;
let screenerPreviewObserver = null;
let screenerPreviewSymbol = '';
let screenerPreviewTimer = null;
let screenerPreviewCloseTimer = null;
let screenerPreviewLoading = null;
const screenerPreviewCache = new Map();
let gmlistLivePreviewChart = null;
let gmlistLivePreviewObserver = null;
let gmlistLivePreviewSymbol = '';
let gmlistLivePreviewTimer = null;
let gmlistLivePreviewCloseTimer = null;
let gmlistLivePreviewLoading = null;
const gmlistLivePreviewCache = new Map();
let gmlistData = null;
let gmlistActiveTab = 'lv21';
let gmlistLiveData = [];
let gmlistLiveLoadedDate = '';
let gmlistStrongStartData = [];
let gmlistStrongStartLoadedDate = '';
let gmlistStrongStartChart = null;
let gmlistStrongStartChartObserver = null;
let gmlistStrongStartHoverTimer = null;
let gmlistStrongStartHoverSymbol = '';
let gmlistStrongStartHoverDate = '';
let gmlistStrongStartHoverOpen = false;
let gmlistStrongStartCloseTimer = null;

sectorSelect.addEventListener('change', () => {
  if (sectorSelect.value) loadSectorCharts();
  else clearSectorBoard();
});

if (screenerRunBtn) screenerRunBtn.addEventListener('click', loadScreener);
if (screenerTodayBtn) {
  screenerTodayBtn.addEventListener('click', async () => {
    await initScreenerDefaults();
    loadScreener();
  });
}
if (screenerAsOfEl) {
  screenerAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadScreener(); });
}

if (gmlistRunBtn) gmlistRunBtn.addEventListener('click', loadGmList);
if (gmlistTodayBtn) {
  gmlistTodayBtn.addEventListener('click', async () => {
    await initGmListDefaults();
    loadGmList();
  });
}
if (gmlistAsOfEl) {
  gmlistAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadGmList(); });
}
if (gmlistStrongStartRunBtn) {
  gmlistStrongStartRunBtn.addEventListener('click', () => loadGmListStrongStart(true));
}
if (gmlistLiveRunBtn) {
  gmlistLiveRunBtn.addEventListener('click', () => loadGmListLive(true));
}
if (gmlistStrongStartAsOfEl) {
  gmlistStrongStartAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadGmListStrongStart(true); });
}
if (gmlistLiveTodayBtn) {
  gmlistLiveTodayBtn.addEventListener('click', async () => {
    if (gmlistLiveAsOfEl) gmlistLiveAsOfEl.value = localIsoDate();
    await loadGmListLive(true);
  });
}
if (gmlistLiveAsOfEl) {
  gmlistLiveAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadGmListLive(true); });
}
if (gmlistTabs) {
  gmlistTabs.addEventListener('click', (event) => {
    const btn = event.target.closest('.gmlist-tab');
    if (!btn) return;
    switchGmListTab(btn.dataset.tab);
  });
}

if (PAGE_MODE === 'sectors' && sectorPage) {
  sectorPage.style.display = 'block';
  clearSectorBoard();
  loadSectors();
  loadTopSectors();
}

if (PAGE_MODE === 'screener' && screenerPage) {
  screenerPage.style.display = 'block';
  initScreenerDefaults().then(loadScreener);
}

if (PAGE_MODE === 'gmlist' && gmlistPage) {
  gmlistPage.style.display = 'block';
  initGmListDefaults().then(() => {
    const initialTab = isNseMarketOpenNow() ? 'live' : 'lv21';
    switchGmListTab(initialTab);
    if (initialTab !== 'live') {
      loadGmList();
    }
  });
}

if (PAGE_MODE === 'stocks') {
  const initialParams = new URLSearchParams(window.location.search);
  const initialSymbol = (initialParams.get('symbol') || '').trim().toUpperCase();
  if (initialSymbol) {
    symInput.value = initialSymbol;
    const fromParam = (initialParams.get('from_date') || '').trim();
    const toParam = (initialParams.get('to_date') || '').trim();
    if (fromParam) document.getElementById('fromDate').value = fromParam;
    if (toParam) document.getElementById('toDate').value = toParam;
    setTimeout(() => loadStock(), 0);
  }
}

async function reloadSectors() {
  await Promise.all([loadSectors(true), loadTopSectors()]);
}

async function loadSectors(resetSelection = false) {
  sectorSelect.disabled = true;
  setSectorLoading(true);
  try {
    const res = await fetch('/api/sectors');
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    sectorSelect.innerHTML = ['<option value="">-- choose a sector --</option>']
      .concat(data.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`))
      .join('');
    if (resetSelection) {
      sectorSelect.value = '';
      clearSectorBoard();
    }
    setStatus(`Loaded ${data.length} sectors`, 'ok');
  } catch (e) {
    sectorSelect.innerHTML = '<option value="">Unable to load sectors</option>';
    setStatus(`Error: ${e.message}`, 'err');
  } finally {
    sectorSelect.disabled = false;
    setSectorLoading(false);
  }
}

async function loadTopSectors() {
  try {
    const res = await fetch('/api/top-sectors');
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    sectorShortcuts.innerHTML = data.map(item => `
      <button class="sector-shortcut" type="button" onclick="pickSector('${escapeJs(item.sector)}')" title="${escapeHtml(item.sector)}">
        <div class="sector-shortcut-name">${escapeHtml(item.sector)}</div>
        <div class="sector-shortcut-count">${item.count} stocks</div>
      </button>
    `).join('');
    highlightSectorShortcut(sectorSelect.value);
  } catch (e) {
    sectorShortcuts.innerHTML = `<div class="sector-shortcuts-note">Shortcut load failed: ${escapeHtml(e.message)}</div>`;
  }
}

function setScreenerStatus(msg, kind) {
  if (!screenerStatus) return;
  screenerStatus.textContent = msg;
  screenerStatus.className = kind ? `screener-status ${kind}` : 'screener-status';
}

function renderScreenerRows(rows, tbody, valueKey, minKey, fmtFn, attachPreviewListeners = true) {
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmtFn(r[valueKey])}</td>
      <td>${fmtFn(r[minKey])}</td>
      <td>${fmt(r.avg_turnover_21d == null ? null : (r.avg_turnover_21d / 1e7), 2)}</td>
    </tr>
  `).join('');
  if (attachPreviewListeners) attachScreenerPreviewListeners(tbody);
  return rows.length;
}

function renderScreenerMeta(d) {
  if (!screenerMetaStrip) return;
  screenerMetaStrip.innerHTML = `
    <span class="screener-meta-chip">As of<b>${escapeHtml(d.as_of)}</b></span>
    <span class="screener-meta-chip">21d window<b>${escapeHtml(d.window_21d.start)} → ${escapeHtml(d.window_21d.end)}</b></span>
    <span class="screener-meta-chip">52w / 15d<b>Near highs only</b></span>
    <span class="screener-meta-chip">Min turnover<b>${fmt(d.min_turnover / 1e7, 2)} cr</b></span>
    <span class="screener-meta-chip">Universe<b>${d.universe}</b></span>
  `;
}

async function initScreenerDefaults() {
  try {
    const res = await fetch('/api/latest-date');
    const data = await res.json();
    if (data.latest_date) {
      screenerAsOfEl.value = data.latest_date;
    } else {
      const now = new Date();
      screenerAsOfEl.value = now.toISOString().slice(0, 10);
    }
  } catch {
    const now = new Date();
    screenerAsOfEl.value = now.toISOString().slice(0, 10);
  }
}

function setGmListStatus(msg, kind) {
  if (!gmlistStatus) return;
  gmlistStatus.textContent = msg;
  gmlistStatus.className = kind ? `screener-status ${kind}` : 'screener-status';
}

function renderGmListMeta(d) {
  if (!gmlistMetaStrip) return;
  gmlistMetaStrip.innerHTML = `
    <span class="screener-meta-chip">As of<b>${escapeHtml(d.as_of)}</b></span>
    <span class="screener-meta-chip">21d window<b>${escapeHtml(d.window_21d.start)} → ${escapeHtml(d.window_21d.end)}</b></span>
    <span class="screener-meta-chip">GMList<b>${escapeHtml(d.universe)}</b></span>
    <span class="screener-meta-chip">Source<b>${escapeHtml(d.source_file || 'gmlist.txt')}</b></span>
  `;
}

function renderGmRows(rows, tbody, valueKey, minKey, fmtFn, attachPreviewListeners = true) {
  return renderScreenerRows(rows, tbody, valueKey, minKey, fmtFn, attachPreviewListeners);
}

function renderGmInsideRows(rows, tbody, attachPreviewListeners = true) {
  if (!tbody) return 0;
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmt(r.high, 2)}</td>
      <td>${fmt(r.low, 2)}</td>
      <td>${fmt(r.prev_high, 2)}</td>
      <td>${fmt(r.prev_low, 2)}</td>
      <td>${fmt(r.avg_turnover_21d == null ? null : (r.avg_turnover_21d / 1e7), 2)}</td>
    </tr>
  `).join('');
  if (attachPreviewListeners) attachScreenerPreviewListeners(tbody);
  return rows.length;
}

function renderGmHdRows(rows, tbody) {
  if (!tbody) return 0;
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmtPct(r.delivery_latest)}</td>
      <td>${fmt(r.delivery_days_3d, 0)}</td>
      <td>${fmtPct(r.delivery_max_3d)}</td>
      <td>${escapeHtml((r.delivery_hits && r.delivery_hits.length) ? r.delivery_hits.map(x => x.date).join(', ') : '—')}</td>
    </tr>
  `).join('');
  attachScreenerPreviewListeners(tbody);
  return rows.length;
}

function renderGmStrongStartRows(rows, tbody) {
  if (!tbody) return 0;
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.open, 2)}</td>
      <td>${fmt(r.high, 2)}</td>
      <td>${fmt(r.low, 2)}</td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmt(r.prev_high, 2)}</td>
      <td>${fmtPct(r.gap_pct)}</td>
    </tr>
  `).join('');
  attachGmStrongStartListeners(tbody);
  return rows.length;
}

function attachGmStrongStartListeners(tbody) {
  if (!tbody) return;
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
    btn.addEventListener('pointerenter', () => queueGmStrongStartHover(btn.dataset.symbol));
    btn.addEventListener('pointerleave', () => cancelGmStrongStartHover(btn.dataset.symbol));
  });
}

function attachGmLivePreviewListeners(tbody) {
  if (!tbody) return;
  const asOf = gmlistLiveAsOfEl ? gmlistLiveAsOfEl.value : '';
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
    btn.addEventListener('pointerenter', () => queueGmLivePreview(btn.dataset.symbol, asOf));
    btn.addEventListener('pointerleave', () => cancelGmLivePreview(btn.dataset.symbol));
  });
}

function queueGmLivePreview(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (gmlistLivePreviewCloseTimer) {
    clearTimeout(gmlistLivePreviewCloseTimer);
    gmlistLivePreviewCloseTimer = null;
  }
  gmlistLivePreviewSymbol = sym;
  if (gmlistLivePreviewTimer) clearTimeout(gmlistLivePreviewTimer);
  gmlistLivePreviewTimer = setTimeout(() => {
    if (gmlistLivePreviewSymbol !== sym) return;
    openGmListLivePreview(sym, asOfOverride);
  }, 120);
}

function cancelGmLivePreview(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (gmlistLivePreviewSymbol === sym) {
    gmlistLivePreviewSymbol = '';
  }
  if (gmlistLivePreviewTimer) {
    clearTimeout(gmlistLivePreviewTimer);
    gmlistLivePreviewTimer = null;
  }
  scheduleGmLivePreviewClose();
}

function scheduleGmLivePreviewClose() {
  if (gmlistLivePreviewCloseTimer) clearTimeout(gmlistLivePreviewCloseTimer);
  gmlistLivePreviewCloseTimer = setTimeout(() => {
    if (!gmlistLivePreviewSymbol) {
      closeGmListLivePreview();
    }
  }, 220);
}

function closeGmListLivePreview() {
  if (gmlistLivePreviewLoading && gmlistLivePreviewLoading.controller) {
    try { gmlistLivePreviewLoading.controller.abort(); } catch {}
  }
  if (gmlistLivePreviewObserver) {
    gmlistLivePreviewObserver.disconnect();
    gmlistLivePreviewObserver = null;
  }
  if (gmlistLivePreviewChart) {
    gmlistLivePreviewChart.remove();
    gmlistLivePreviewChart = null;
  }
  if (screenerPreviewOverlay) screenerPreviewOverlay.classList.remove('open');
  gmlistLivePreviewSymbol = '';
  if (gmlistLivePreviewTimer) {
    clearTimeout(gmlistLivePreviewTimer);
    gmlistLivePreviewTimer = null;
  }
  if (gmlistLivePreviewCloseTimer) {
    clearTimeout(gmlistLivePreviewCloseTimer);
    gmlistLivePreviewCloseTimer = null;
  }
  gmlistLivePreviewLoading = null;
}

if (screenerPreviewCard) {
  screenerPreviewCard.addEventListener('mouseenter', () => {
    if (gmlistLivePreviewCloseTimer) {
      clearTimeout(gmlistLivePreviewCloseTimer);
      gmlistLivePreviewCloseTimer = null;
    }
  });
  screenerPreviewCard.addEventListener('mouseleave', scheduleGmLivePreviewClose);
}

async function fetchGmListLivePreviewCard(symbol, signal, asOfOverride = '') {
  const cacheKey = `${String(symbol || '').trim().toUpperCase()}|${String(asOfOverride || '').trim()}`;
  if (!cacheKey.startsWith('|') && gmlistLivePreviewCache.has(cacheKey)) {
    return gmlistLivePreviewCache.get(cacheKey);
  }
  const qs = new URLSearchParams();
  qs.set('symbol', String(symbol || '').trim().toUpperCase());
  if (asOfOverride) qs.set('date', asOfOverride);
  const res = await fetch('/api/gmlist-live-preview?' + qs.toString(), { signal });
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error || res.statusText);
  }
  const rows = data.rows || [];
  const card = buildPreviewCardFromRows(String(symbol || '').trim().toUpperCase(), rows);
  gmlistLivePreviewCache.set(cacheKey, card);
  return card;
}

async function openGmListLivePreview(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  closeScreenerPreview();
  gmlistLivePreviewSymbol = sym;
  if (screenerPreviewTitle) screenerPreviewTitle.textContent = sym;
  if (screenerPreviewSubtitle) screenerPreviewSubtitle.textContent = 'Live daily chart preview';
  if (screenerPreviewMeta) screenerPreviewMeta.textContent = 'Loading chart...';
  if (screenerPreviewOverlay) screenerPreviewOverlay.classList.add('open');
  if (screenerPreviewChartEl) screenerPreviewChartEl.innerHTML = '';

  if (gmlistLivePreviewObserver) {
    gmlistLivePreviewObserver.disconnect();
    gmlistLivePreviewObserver = null;
  }
  if (gmlistLivePreviewChart) {
    gmlistLivePreviewChart.remove();
    gmlistLivePreviewChart = null;
  }
  if (gmlistLivePreviewLoading && gmlistLivePreviewLoading.controller) {
    try { gmlistLivePreviewLoading.controller.abort(); } catch {}
  }

  const controller = new AbortController();
  gmlistLivePreviewLoading = { symbol: sym, controller };

  try {
    const card = await fetchGmListLivePreviewCard(sym, controller.signal, asOfOverride);
    if (!card || gmlistLivePreviewSymbol !== sym) return;
    if (screenerPreviewMeta) {
      screenerPreviewMeta.textContent = card.end_close == null ? '' : `Close ${fmt(card.end_close, 2)}`;
    }
    requestAnimationFrame(() => {
      if (gmlistLivePreviewSymbol !== sym) return;
      gmlistLivePreviewChart = drawSectorChart(screenerPreviewChartEl, card, true);
      gmlistLivePreviewObserver = new ResizeObserver(() => {
        if (!gmlistLivePreviewChart) return;
        gmlistLivePreviewChart.applyOptions({
          width: screenerPreviewChartEl.clientWidth,
          height: screenerPreviewChartEl.clientHeight,
        });
        gmlistLivePreviewChart.timeScale().fitContent();
      });
      gmlistLivePreviewObserver.observe(screenerPreviewChartEl);
    });
  } catch (e) {
    if (gmlistLivePreviewSymbol !== sym) return;
    if (e.name === 'AbortError') return;
    if (screenerPreviewMeta) screenerPreviewMeta.textContent = `Error: ${e.message}`;
    if (screenerPreviewChartEl) {
      screenerPreviewChartEl.innerHTML = '<div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">Unable to load preview</div>';
    }
  }
}

function getChartDayKey(time) {
  const raw = normalizeChartTime(time);
  if (!raw) return '';
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && String(numeric) === raw) {
    const dt = new Date(numeric < 1e12 ? numeric * 1000 : numeric);
    if (!Number.isNaN(dt.getTime())) {
      return dt.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
    }
  }
  const dt = new Date(raw);
  if (!Number.isNaN(dt.getTime())) {
    return dt.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
  }
  return raw.slice(0, 10);
}

function queueGmStrongStartHover(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (gmlistStrongStartCloseTimer) {
    clearTimeout(gmlistStrongStartCloseTimer);
    gmlistStrongStartCloseTimer = null;
  }
  gmlistStrongStartHoverSymbol = sym;
  gmlistStrongStartHoverDate = String(asOfOverride || '').trim();
  gmlistStrongStartHoverTimer = setTimeout(() => {
    if (gmlistStrongStartHoverSymbol !== sym) return;
    loadGmListStrongStartChart(sym, gmlistStrongStartHoverDate);
  }, 90);
}

function cancelGmStrongStartHover(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (gmlistStrongStartHoverSymbol === sym) {
    gmlistStrongStartHoverSymbol = '';
    gmlistStrongStartHoverDate = '';
  }
  if (gmlistStrongStartHoverTimer) {
    clearTimeout(gmlistStrongStartHoverTimer);
    gmlistStrongStartHoverTimer = null;
  }
  scheduleGmStrongStartClose();
}

function scheduleGmStrongStartClose() {
  if (gmlistStrongStartCloseTimer) clearTimeout(gmlistStrongStartCloseTimer);
  gmlistStrongStartCloseTimer = setTimeout(() => {
    if (!gmlistStrongStartHoverOpen && !gmlistStrongStartHoverSymbol) {
      closeGmListStrongStartOverlay();
    }
  }, 220);
}

function setGmStrongStartHoverOpen(open) {
  gmlistStrongStartHoverOpen = !!open;
  if (gmlistStrongStartCloseTimer) {
    clearTimeout(gmlistStrongStartCloseTimer);
    gmlistStrongStartCloseTimer = null;
  }
}

function attachGmStrongStartPreviewListeners(tbody) {
  if (!tbody) return;
  const asOf = gmlistLiveAsOfEl ? gmlistLiveAsOfEl.value : '';
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
    btn.addEventListener('pointerenter', () => queueGmStrongStartHover(btn.dataset.symbol, asOf));
    btn.addEventListener('pointerleave', () => cancelGmStrongStartHover(btn.dataset.symbol));
  });
}

function handleGmStrongStartOverlayEnter() {
  setGmStrongStartHoverOpen(true);
}

function handleGmStrongStartOverlayLeave() {
  setGmStrongStartHoverOpen(false);
  scheduleGmStrongStartClose();
}

function switchGmListTab(tab) {
  const target = String(tab || 'lv21').trim();
  gmlistActiveTab = target || 'lv21';
  if (gmlistTabs) {
    gmlistTabs.querySelectorAll('.gmlist-tab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tab === gmlistActiveTab);
    });
  }
  document.querySelectorAll('#gmlistPage .gmlist-panel').forEach(panel => {
    panel.classList.toggle('active', panel.dataset.panel === gmlistActiveTab);
  });
  if (gmlistActiveTab === 'live') {
    if (gmlistLiveAsOfEl && !gmlistLiveAsOfEl.value) gmlistLiveAsOfEl.value = localIsoDate();
    loadGmListLive(true);
  }
  if (gmlistActiveTab === 'strong_start') {
    if (gmlistStrongStartAsOfEl) gmlistStrongStartAsOfEl.value = localIsoDate();
    loadGmListStrongStart(true);
  }
}

async function initGmListDefaults() {
  try {
    const res = await fetch('/api/latest-date');
    const data = await res.json();
    if (data.latest_date) {
      gmlistAsOfEl.value = data.latest_date;
    } else {
      gmlistAsOfEl.value = localIsoDate();
    }
  } catch {
    gmlistAsOfEl.value = localIsoDate();
  }
  if (gmlistStrongStartAsOfEl) {
    gmlistStrongStartAsOfEl.value = localIsoDate();
  }
  if (gmlistLiveAsOfEl) {
    gmlistLiveAsOfEl.value = localIsoDate();
  }
}

function localIsoDate() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function getIndiaNowParts() {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    hour12: false,
    weekday: 'short',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).formatToParts(new Date());
  const map = {};
  for (const part of parts) {
    if (part.type !== 'literal') map[part.type] = part.value;
  }
  return map;
}

function isNseMarketOpenNow() {
  const p = getIndiaNowParts();
  const weekday = (p.weekday || '').slice(0, 3).toLowerCase();
  if (['sat', 'sun'].includes(weekday)) return false;
  const hour = parseInt(p.hour || '0', 10);
  const minute = parseInt(p.minute || '0', 10);
  const total = hour * 60 + minute;
  return total >= (9 * 60 + 15) && total <= (15 * 60 + 30);
}

function buildPreviewCardFromRows(symbol, rows) {
  const ordered = [...(rows || [])].slice().sort((a, b) => {
    const ta = new Date(a.mktdate || a.time || 0).getTime();
    const tb = new Date(b.mktdate || b.time || 0).getTime();
    return ta - tb;
  });
  const candles = [];
  const volumes = [];
  const ema5 = [];
  const ema10 = [];
  const ema20 = [];
  const ema50 = [];
  let latestClose = null;
  let latestTime = null;
  for (const r of ordered) {
    const time = r.mktdate || r.time;
    if (!time) continue;
    const candle = {
      time,
      open: r.open,
      high: r.high,
      low: r.low,
      close: r.close,
      volume: r.volume,
      change_pct: r.diff ?? r.change_pct ?? null,
    };
    candles.push(candle);
    volumes.push({
      time,
      value: r.volume,
      color: (r.diff != null && Number(r.diff) >= 0) ? '#58b65b' : '#ef6a6a',
    });
    if (r['5dma'] != null) ema5.push({ time, value: r['5dma'] });
    if (r['10dma'] != null) ema10.push({ time, value: r['10dma'] });
    if (r['20DMA'] != null) ema20.push({ time, value: r['20DMA'] });
    if (r['50dma'] != null) ema50.push({ time, value: r['50dma'] });
    latestClose = r.close;
    latestTime = time;
  }
  return {
    symbol,
    sector: 'Screener Preview',
    has_data: candles.length > 0,
    latest_date: latestTime,
    end_close: latestClose,
    move_pct: ordered.length ? (ordered[ordered.length - 1].diff ?? null) : null,
    avg_turnover_21d: null,
    candles,
    volume: volumes,
    ema5,
    ema10,
    ema20,
    ema50,
  };
}

async function fetchScreenerPreviewCard(symbol, signal) {
  const cacheKey = String(symbol || '').trim().toUpperCase();
  if (!cacheKey) return null;
  if (screenerPreviewCache.has(cacheKey)) {
    return screenerPreviewCache.get(cacheKey);
  }
  const res = await fetch(`/api/stock?symbol=${encodeURIComponent(cacheKey)}`, { signal });
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error || res.statusText);
  }
  const card = buildPreviewCardFromRows(cacheKey, data.rows || []);
  screenerPreviewCache.set(cacheKey, card);
  return card;
}

function closeScreenerPreview(event) {
  if (event && event.target && event.target.id !== 'screenerPreviewOverlay' && event.target.id !== 'screenerPreviewCard') return;
  screenerPreviewOverlay.classList.remove('open');
  screenerPreviewSymbol = '';
  if (screenerPreviewObserver) {
    screenerPreviewObserver.disconnect();
    screenerPreviewObserver = null;
  }
  if (screenerPreviewChart) {
    screenerPreviewChart.remove();
    screenerPreviewChart = null;
  }
  screenerPreviewChartEl.innerHTML = '';
  if (screenerPreviewTimer) {
    clearTimeout(screenerPreviewTimer);
    screenerPreviewTimer = null;
  }
  if (screenerPreviewCloseTimer) {
    clearTimeout(screenerPreviewCloseTimer);
    screenerPreviewCloseTimer = null;
  }
}

function scheduleScreenerPreview(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (screenerPreviewCloseTimer) {
    clearTimeout(screenerPreviewCloseTimer);
    screenerPreviewCloseTimer = null;
  }
  if (screenerPreviewSymbol === sym && screenerPreviewOverlay.classList.contains('open')) {
    return;
  }
  if (screenerPreviewTimer) {
    clearTimeout(screenerPreviewTimer);
  }
  screenerPreviewTimer = setTimeout(() => openScreenerPreview(sym), 180);
}

function queueCloseScreenerPreview() {
  if (gmlistLivePreviewSymbol) return;
  if (screenerPreviewTimer) {
    clearTimeout(screenerPreviewTimer);
    screenerPreviewTimer = null;
  }
  if (screenerPreviewCloseTimer) {
    clearTimeout(screenerPreviewCloseTimer);
  }
  screenerPreviewCloseTimer = setTimeout(() => {
    if (!screenerPreviewCard || !screenerPreviewCard.matches(':hover')) {
      closeScreenerPreview();
    }
  }, 220);
}

function attachScreenerPreviewListeners(tbody) {
  if (!tbody) return;
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
  btn.addEventListener('mouseenter', () => scheduleScreenerPreview(btn.dataset.symbol));
  btn.addEventListener('focus', () => scheduleScreenerPreview(btn.dataset.symbol));
  btn.addEventListener('mouseleave', queueCloseScreenerPreview);
  btn.addEventListener('blur', queueCloseScreenerPreview);
  });
}

if (screenerPreviewCard) {
  screenerPreviewCard.addEventListener('mouseenter', () => {
    if (screenerPreviewCloseTimer) {
      clearTimeout(screenerPreviewCloseTimer);
      screenerPreviewCloseTimer = null;
    }
  });
  screenerPreviewCard.addEventListener('mouseleave', queueCloseScreenerPreview);
}

async function openScreenerPreview(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  screenerPreviewSymbol = sym;
  screenerPreviewTitle.textContent = sym;
  screenerPreviewSubtitle.textContent = 'Bhav chart preview';
  screenerPreviewMeta.textContent = 'Loading chart...';
  screenerPreviewOverlay.classList.add('open');
  screenerPreviewChartEl.innerHTML = '';

  if (screenerPreviewObserver) {
    screenerPreviewObserver.disconnect();
    screenerPreviewObserver = null;
  }
  if (screenerPreviewChart) {
    screenerPreviewChart.remove();
    screenerPreviewChart = null;
  }
  if (screenerPreviewLoading && screenerPreviewLoading.controller) {
    try { screenerPreviewLoading.controller.abort(); } catch {}
  }

  const controller = new AbortController();
  screenerPreviewLoading = { symbol: sym, controller };

  try {
    const card = await fetchScreenerPreviewCard(sym, controller.signal);
    if (!card || screenerPreviewSymbol !== sym) return;
    screenerPreviewMeta.textContent = card.end_close == null ? '' : `Close ${fmt(card.end_close, 2)}`;
    requestAnimationFrame(() => {
      if (screenerPreviewSymbol !== sym) return;
      screenerPreviewChart = drawSectorChart(screenerPreviewChartEl, card, true);
      screenerPreviewObserver = new ResizeObserver(() => {
        if (!screenerPreviewChart) return;
        screenerPreviewChart.applyOptions({
          width: screenerPreviewChartEl.clientWidth,
          height: screenerPreviewChartEl.clientHeight,
        });
        screenerPreviewChart.timeScale().fitContent();
      });
      screenerPreviewObserver.observe(screenerPreviewChartEl);
    });
  } catch (e) {
    if (screenerPreviewSymbol !== sym) return;
    if (e.name === 'AbortError') return;
    screenerPreviewMeta.textContent = `Error: ${e.message}`;
    screenerPreviewChartEl.innerHTML = '<div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">Unable to load preview</div>';
  }
}

async function loadScreener() {
  if (!screenerPage) return;
  setScreenerStatus('Running…');
  screenerRunBtn.disabled = true;
  screenerPanelVol.classList.add('loading');
  screenerPanelVlt.classList.add('loading');
  screenerEmptyVol.style.display = 'none';
  screenerEmptyVlt.style.display = 'none';
  try {
    const d = screenerAsOfEl.value;
    const mt = (parseFloat(screenerTurnoverEl.value) || 10) * 1e7;
    const lm = Math.max(10, parseInt(screenerLimitEl.value, 10) || 200);
    const qs = new URLSearchParams({ min_turnover: mt, limit: lm });
    if (d) qs.set('date', d);
    const res = await fetch('/api/screener?' + qs.toString());
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || res.statusText);
    }

    renderScreenerMeta(data);
    const nV = renderScreenerRows(data.low_volume || [], screenerTbodyVol, 'volume', 'min_vol_21d', fmtVol);
    const nL = renderScreenerRows(data.low_volatility || [], screenerTbodyVlt, 'volatility', 'min_vlt_21d', (v) => fmt(v, 4));
    screenerCountVol.textContent = nV;
    screenerCountVlt.textContent = nL;
    screenerEmptyVol.style.display = nV ? 'none' : 'block';
    screenerEmptyVlt.style.display = nL ? 'none' : 'block';
    screenerAsOfEl.value = data.as_of;
    setScreenerStatus(`OK — ${nV} low-volume · ${nL} low-volatility stocks on ${data.as_of}.`, 'ok');
  } catch (e) {
    setScreenerStatus(`Error: ${e.message}`, 'err');
  } finally {
    screenerPanelVol.classList.remove('loading');
    screenerPanelVlt.classList.remove('loading');
    screenerRunBtn.disabled = false;
  }
}

async function loadGmList() {
  if (!gmlistPage) return;
  setGmListStatus('Running…');
  if (gmlistRunBtn) gmlistRunBtn.disabled = true;
  if (gmlistTabs) gmlistTabs.classList.add('loading');
  try {
    const d = gmlistAsOfEl.value;
    const qs = new URLSearchParams();
    if (d) qs.set('date', d);
    const res = await fetch('/api/gmlist?' + qs.toString());
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || res.statusText);
    }

    gmlistData = data;
    renderGmListMeta(data);

    const lv21 = data.lv21 || [];
    const lowvol21 = data.lowvol21 || [];
    const insideDays = data.inside_days || [];
    const hd = data.hd || [];

    const nLv21 = renderGmRows(lv21, gmlistTbodyLv21, 'volatility', 'min_vlt_21d', (v) => fmt(v, 4));
    const nLowvol21 = renderGmRows(lowvol21, gmlistTbodyLowvol21, 'volume', 'min_vol_21d', fmtVol);
    const nInsideDays = renderGmInsideRows(insideDays, gmlistTbodyInsideDays);
    const nHd = renderGmHdRows(hd, gmlistTbodyHd);

    if (gmlistCountTabLv21) gmlistCountTabLv21.textContent = nLv21;
    if (gmlistCountTabLowvol21) gmlistCountTabLowvol21.textContent = nLowvol21;
    if (gmlistCountTabInsideDays) gmlistCountTabInsideDays.textContent = nInsideDays;
    if (gmlistCountTabHd) gmlistCountTabHd.textContent = nHd;

    if (gmlistCountPanelLv21) gmlistCountPanelLv21.textContent = nLv21;
    if (gmlistCountPanelLowvol21) gmlistCountPanelLowvol21.textContent = nLowvol21;
    if (gmlistCountPanelInsideDays) gmlistCountPanelInsideDays.textContent = nInsideDays;
    if (gmlistCountPanelHd) gmlistCountPanelHd.textContent = nHd;

    if (gmlistEmptyLv21) gmlistEmptyLv21.style.display = nLv21 ? 'none' : 'block';
    if (gmlistEmptyLowvol21) gmlistEmptyLowvol21.style.display = nLowvol21 ? 'none' : 'block';
    if (gmlistEmptyInsideDays) gmlistEmptyInsideDays.style.display = nInsideDays ? 'none' : 'block';
    if (gmlistEmptyHd) gmlistEmptyHd.style.display = nHd ? 'none' : 'block';

    gmlistAsOfEl.value = data.as_of;
    const source = data.source_file ? data.source_file.replace(/^.*[\\/]/, '') : 'gmlist.txt';
    setGmListStatus(`OK — ${nLv21} lv21 · ${nLowvol21} lowvol21 · ${nInsideDays} inside-day · ${nHd} hd stocks from ${source}.`, 'ok');
    gmlistStrongStartData = [];
    gmlistStrongStartLoadedDate = '';
    if (gmlistCountTabStrongStart) gmlistCountTabStrongStart.textContent = 0;
    if (gmlistCountPanelStrongStart) gmlistCountPanelStrongStart.textContent = 0;
    if (gmlistEmptyStrongStart) gmlistEmptyStrongStart.style.display = 'block';
    if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = 'Open the Strong Start tab to scan with Kite.';
    gmlistLiveData = [];
    gmlistLiveLoadedDate = '';
    if (gmlistCountTabLive) gmlistCountTabLive.textContent = 0;
    if (gmlistCountPanelLive) gmlistCountPanelLive.textContent = 0;
    if (gmlistCountPanelLiveLv21) gmlistCountPanelLiveLv21.textContent = 0;
    if (gmlistCountPanelLiveLowvol21) gmlistCountPanelLiveLowvol21.textContent = 0;
    if (gmlistCountPanelLiveInsideDays) gmlistCountPanelLiveInsideDays.textContent = 0;
    if (gmlistLiveEmptyLv21) gmlistLiveEmptyLv21.style.display = 'block';
    if (gmlistLiveEmptyLowvol21) gmlistLiveEmptyLowvol21.style.display = 'block';
    if (gmlistLiveEmptyInsideDays) gmlistLiveEmptyInsideDays.style.display = 'block';
    if (gmlistLiveStatus) gmlistLiveStatus.textContent = 'Open the Live tab to scan Kite daily bars.';
    switchGmListTab(gmlistActiveTab || 'lv21');
  } catch (e) {
    setGmListStatus(`Error: ${e.message}`, 'err');
  } finally {
    if (gmlistRunBtn) gmlistRunBtn.disabled = false;
    if (gmlistTabs) gmlistTabs.classList.remove('loading');
  }
}

async function loadGmListStrongStart(force = false) {
  if (!gmlistPage) return;
  const d = gmlistStrongStartAsOfEl ? gmlistStrongStartAsOfEl.value : '';
  if (!force && gmlistStrongStartData.length) return;
  if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = 'Scanning Kite 5m bars...';
  try {
    const qs = new URLSearchParams();
    if (d) qs.set('date', d);
    const res = await fetch('/api/gmlist-strong-start?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    gmlistStrongStartData = data.strong_start || [];
    gmlistStrongStartLoadedDate = d || '';
    if (gmlistCountTabStrongStart) gmlistCountTabStrongStart.textContent = gmlistStrongStartData.length;
    if (gmlistCountPanelStrongStart) gmlistCountPanelStrongStart.textContent = gmlistStrongStartData.length;
    if (gmlistEmptyStrongStart) gmlistEmptyStrongStart.style.display = gmlistStrongStartData.length ? 'none' : 'block';
    if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = data.strong_start_status || '';
    renderGmStrongStartRows(gmlistStrongStartData, gmlistTbodyStrongStart);
    attachGmStrongStartListeners(gmlistTbodyStrongStart);
  } catch (e) {
    if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = `Error: ${e.message}`;
  }
}

function renderGmLiveMeta(d) {
  if (!gmlistLiveMetaStrip) return;
  gmlistLiveMetaStrip.innerHTML = `
    <span class="screener-meta-chip">As of<b>${escapeHtml(d.as_of)}</b></span>
    <span class="screener-meta-chip">Live source<b>${escapeHtml(d.source_file || 'kite_daily')}</b></span>
    <span class="screener-meta-chip">GMList<b>${escapeHtml(d.universe)}</b></span>
  `;
}

async function loadGmListLive(force = false) {
  if (!gmlistPage) return;
  const d = gmlistLiveAsOfEl ? gmlistLiveAsOfEl.value : '';
  if (!force && gmlistLiveData.length && d === gmlistLiveLoadedDate) return;
  if (gmlistLiveStatus) gmlistLiveStatus.textContent = 'Scanning Kite daily bars...';
  try {
    const qs = new URLSearchParams();
    if (d) qs.set('date', d);
    const res = await fetch('/api/gmlist-live?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    gmlistLiveData = data;
    gmlistLiveLoadedDate = data.as_of || d || '';
    renderGmLiveMeta(data);

    const lv21 = data.lv21 || [];
    const lowvol21 = data.lowvol21 || [];
    const insideDays = data.inside_days || [];

    const nLv21 = renderGmRows(lv21, gmlistLiveTbodyLv21, 'volatility', 'min_vlt_21d', (v) => fmt(v, 4), false);
    const nLowvol21 = renderGmRows(lowvol21, gmlistLiveTbodyLowvol21, 'volume', 'min_vol_21d', fmtVol, false);
    const nInsideDays = renderGmInsideRows(insideDays, gmlistLiveTbodyInsideDays, false);
    attachGmLivePreviewListeners(gmlistLiveTbodyLv21);
    attachGmLivePreviewListeners(gmlistLiveTbodyLowvol21);
    attachGmLivePreviewListeners(gmlistLiveTbodyInsideDays);

    if (gmlistCountTabLive) gmlistCountTabLive.textContent = data.eligible != null ? data.eligible : data.universe;
    if (gmlistCountPanelLive) gmlistCountPanelLive.textContent = data.eligible != null ? data.eligible : data.universe;
    if (gmlistCountPanelLiveLv21) gmlistCountPanelLiveLv21.textContent = nLv21;
    if (gmlistCountPanelLiveLowvol21) gmlistCountPanelLiveLowvol21.textContent = nLowvol21;
    if (gmlistCountPanelLiveInsideDays) gmlistCountPanelLiveInsideDays.textContent = nInsideDays;

    if (gmlistLiveEmptyLv21) gmlistLiveEmptyLv21.style.display = nLv21 ? 'none' : 'block';
    if (gmlistLiveEmptyLowvol21) gmlistLiveEmptyLowvol21.style.display = nLowvol21 ? 'none' : 'block';
    if (gmlistLiveEmptyInsideDays) gmlistLiveEmptyInsideDays.style.display = nInsideDays ? 'none' : 'block';

    if (gmlistLiveAsOfEl && data.as_of) gmlistLiveAsOfEl.value = data.as_of;
    if (gmlistLiveStatus) gmlistLiveStatus.textContent = `OK — ${nLv21} live lv21 · ${nLowvol21} live lowvol21 · ${nInsideDays} live inside-day stocks.`;
  } catch (e) {
    if (gmlistLiveStatus) gmlistLiveStatus.textContent = `Error: ${e.message}`;
  }
}

function closeGmListStrongStartOverlay(event) {
  if (event && event.target && event.target.id !== 'gmlistStrongStartOverlay') return;
  if (gmlistStrongStartOverlay) {
    gmlistStrongStartOverlay.classList.remove('open');
  }
  setGmStrongStartHoverOpen(false);
  if (gmlistStrongStartChartObserver) {
    gmlistStrongStartChartObserver.disconnect();
    gmlistStrongStartChartObserver = null;
  }
  if (gmlistStrongStartChart) {
    gmlistStrongStartChart.remove();
    gmlistStrongStartChart = null;
  }
}

async function loadGmListStrongStartChart(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (gmlistStrongStartTitle) gmlistStrongStartTitle.textContent = sym;
  if (gmlistStrongStartSubtitle) gmlistStrongStartSubtitle.textContent = 'Kite 5-minute intraday chart';
  if (gmlistStrongStartMeta) gmlistStrongStartMeta.textContent = 'Loading...';
  if (gmlistStrongStartOverlay) gmlistStrongStartOverlay.classList.add('open');
  try {
    const qs = new URLSearchParams();
    qs.set('symbol', sym);
    const d = String(asOfOverride || (gmlistStrongStartAsOfEl ? gmlistStrongStartAsOfEl.value : '') || '').trim();
    if (d) qs.set('date', d);
    qs.set('days', '5');
    const res = await fetch('/api/gmlist-strong-start-chart?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    const card = {
      symbol: data.symbol,
      sector: 'Kite Intraday',
      is_intraday: true,
      has_data: (data.candles || []).length > 0,
      latest_date: data.candles && data.candles.length ? data.candles[data.candles.length - 1].time : null,
      end_close: data.candles && data.candles.length ? data.candles[data.candles.length - 1].close : null,
      move_pct: null,
      avg_turnover_21d: null,
      candles: data.candles || [],
      volume: data.volume || [],
      ema5: data.ema5 || [],
      ema10: data.ema10 || [],
      ema20: data.ema20 || [],
      ema50: data.ema50 || [],
    };
    if (gmlistStrongStartMeta) {
      const rangeStart = data.start_date ? ` from ${data.start_date}` : '';
      gmlistStrongStartMeta.textContent = `${sym} 5m chart${rangeStart}${data.prev_high != null ? ` | prev high ${fmt(data.prev_high, 2)}` : ''}`;
    }
    if (gmlistStrongStartChartObserver) {
      gmlistStrongStartChartObserver.disconnect();
      gmlistStrongStartChartObserver = null;
    }
    if (gmlistStrongStartChart) {
      gmlistStrongStartChart.remove();
      gmlistStrongStartChart = null;
    }
    if (!gmlistStrongStartChartEl) return;
    gmlistStrongStartChartEl.innerHTML = '';
    gmlistStrongStartChart = drawSectorChart(gmlistStrongStartChartEl, card, true);
    const totalCandles = (card.candles || []).length;
    const visibleCandles = Math.min(90, totalCandles || 90);
    const applyStrongStartWindow = () => {
      if (!gmlistStrongStartChart) return;
      if (totalCandles > 0) {
        const from = Math.max(0, totalCandles - visibleCandles);
        const to = totalCandles - 1;
        gmlistStrongStartChart.timeScale().setVisibleLogicalRange({ from, to });
      }
    };
    requestAnimationFrame(applyStrongStartWindow);
    gmlistStrongStartChartObserver = new ResizeObserver(() => {
      if (!gmlistStrongStartChart) return;
      gmlistStrongStartChart.applyOptions({
        width: gmlistStrongStartChartEl.clientWidth,
        height: gmlistStrongStartChartEl.clientHeight,
      });
      applyStrongStartWindow();
    });
    gmlistStrongStartChartObserver.observe(gmlistStrongStartChartEl);
    setGmStrongStartHoverOpen(true);
  } catch (e) {
    if (gmlistStrongStartMeta) gmlistStrongStartMeta.textContent = `Error: ${e.message}`;
    if (gmlistStrongStartChartEl) {
      gmlistStrongStartChartEl.innerHTML = `
        <div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;text-align:center;padding:16px;">
          <div>
            <div style="font-weight:700;margin-bottom:6px;">Strong Start chart unavailable</div>
            <div style="opacity:.85;max-width:420px;">${escapeHtml(e.message || 'Unknown error')}</div>
          </div>
        </div>
      `;
    }
  }
}

async function loadSectorCharts() {
  const sector = sectorSelect.value.trim().toUpperCase();
  if (!sector) {
    setStatus('Please choose a sector', 'err');
    return;
  }

  setSectorLoading(true);
  highlightSectorShortcut(sector);
  try {
    setStatus(`Loading charts for ${sector}...`, '');
    await loadSectorChartBoard(sector);
  } catch (e) {
    setStatus(`Error: ${e.message}`, 'err');
    clearSectorBoard(`No sector charts loaded for ${sector}.`);
  } finally {
    setSectorLoading(false);
  }
}

function pickSector(sector) {
  sectorSelect.value = sector;
  loadSectorCharts();
}

function highlightSectorShortcut(sector) {
  if (!sectorShortcuts) return;
  const target = String(sector || '').trim().toUpperCase();
  sectorShortcuts.querySelectorAll('.sector-shortcut').forEach(btn => {
    const label = (btn.querySelector('.sector-shortcut-name')?.textContent || '').trim().toUpperCase();
    btn.classList.toggle('active', label === target);
  });
}

function setSectorLoading(on) {
  sectorSpinner.style.display = on ? 'block' : 'none';
}

function sectorChartOptions(chartEl, isZoom = false) {
  return {
    width: Math.max(chartEl.clientWidth, 320),
    height: Math.max(chartEl.clientHeight, isZoom ? 560 : 260),
    layout: {
      background: { type: 'solid', color: '#1f2329' },
      textColor: '#94a3b8',
      fontFamily: 'Segoe UI, Tahoma, sans-serif',
      fontSize: isZoom ? 11 : 10,
    },
    grid: {
      vertLines: { color: '#303744' },
      horzLines: { color: '#303744' },
    },
    rightPriceScale: {
      borderColor: '#3a4250',
      scaleMargins: isZoom ? { top: 0.06, bottom: 0.22 } : { top: 0.08, bottom: 0.22 },
    },
    leftPriceScale: { visible: false },
    timeScale: {
      borderColor: '#3a4250',
      timeVisible: false,
      secondsVisible: false,
      barSpacing: isZoom ? 6.5 : 3.5,
      minBarSpacing: isZoom ? 2.5 : 1.5,
      rightOffset: isZoom ? 12 : 6,
      fixLeftEdge: true,
      fixRightEdge: true,
      lockVisibleTimeRangeOnResize: true,
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
    handleScroll: isZoom,
    handleScale: isZoom,
  };
}

function normalizeChartTime(time) {
  if (!time) return '';
  if (typeof time === 'string') return time;
  if (typeof time === 'object' && time.year && time.month && time.day) {
    const mm = String(time.month).padStart(2, '0');
    const dd = String(time.day).padStart(2, '0');
    return `${time.year}-${mm}-${dd}`;
  }
  return String(time);
}

function formatSectorTooltipDate(time) {
  const raw = normalizeChartTime(time);
  if (!raw) return '';
  const numeric = Number(raw);
  const dt = Number.isFinite(numeric) && String(numeric) === raw
    ? new Date(numeric < 1e12 ? numeric * 1000 : numeric)
    : new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleDateString('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  });
}

function createSectorHud(chartEl, isZoom = false) {
  const hud = document.createElement('div');
  hud.className = `sector-chart-hud${isZoom ? ' is-zoom' : ''}`;
  hud.innerHTML = `
    <div class="hud-grid">
      <div class="hud-item"><span class="hud-key">O:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">H:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">V:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">C:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">L:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">%</span><span class="hud-val">–</span></div>
    </div>
  `;
  chartEl.appendChild(hud);
  return hud;
}

function renderSectorHud(hud, candleData) {
  if (!hud || !candleData) return;
  const change = candleData.change_pct;
  const changeClass = change == null ? '' : (change >= 0 ? 'hud-pos' : 'hud-neg');
  hud.innerHTML = `
    <div class="hud-grid">
      <div class="hud-item"><span class="hud-key">O:</span><span class="hud-val">${fmt(candleData.open, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">H:</span><span class="hud-val">${fmt(candleData.high, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">V:</span><span class="hud-val">${fmtVol(candleData.volume)}</span></div>
      <div class="hud-item"><span class="hud-key">C:</span><span class="hud-val">${fmt(candleData.close, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">L:</span><span class="hud-val">${fmt(candleData.low, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">%</span><span class="hud-val ${changeClass}">${fmtPct(change)}</span></div>
    </div>
  `;
}

function drawSectorChart(chartEl, card, isZoom = false) {
  const chart = LightweightCharts.createChart(chartEl, sectorChartOptions(chartEl, isZoom));
  const candleMeta = new Map((card.candles || []).map(c => [normalizeChartTime(c.time), c]));
  const candles = card.candles || [];
  let daySeparator = null;
  const showIntradaySeparator = !!(isZoom && card && card.is_intraday && candles.length);
  const updateIntradaySeparator = () => {
    if (!showIntradaySeparator || !daySeparator || !candles.length) return;
    const latestDay = getChartDayKey(candles[candles.length - 1].time);
    const firstToday = candles.find(c => getChartDayKey(c.time) === latestDay);
    if (!firstToday) {
      daySeparator.style.display = 'none';
      return;
    }
    const coord = chart.timeScale().timeToCoordinate(firstToday.time);
    if (coord == null || Number.isNaN(coord)) {
      daySeparator.style.display = 'none';
      return;
    }
    daySeparator.style.display = 'block';
    daySeparator.style.left = `${Math.round(coord)}px`;
  };
  const candleSeries = chart.addCandlestickSeries({
    upColor: '#58b65b',
    downColor: '#ef6a6a',
    borderUpColor: '#58b65b',
    borderDownColor: '#ef6a6a',
    wickUpColor: '#58b65b',
    wickDownColor: '#ef6a6a',
    priceLineVisible: false,
    lastValueVisible: false,
  });
  candleSeries.setData(candles);

  const volumeSeries = chart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    priceScaleId: '',
    lastValueVisible: false,
    priceLineVisible: false,
  });
  volumeSeries.priceScale().applyOptions({
    scaleMargins: isZoom ? { top: 0.82, bottom: 0.02 } : { top: 0.80, bottom: 0.02 },
  });
  volumeSeries.setData(card.volume || []);

  chart.addLineSeries({
    color: '#f59e0b',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema5 || []);

  chart.addLineSeries({
    color: '#2563eb',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema10 || []);

  chart.addLineSeries({
    color: '#10b981',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema20 || []);

  chart.addLineSeries({
    color: '#8b5cf6',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema50 || []);

  const hud = createSectorHud(chartEl, isZoom);
  const latestKey = (card.candles && card.candles.length)
    ? normalizeChartTime(card.candles[card.candles.length - 1].time)
    : null;
  if (latestKey && candleMeta.has(latestKey)) {
    renderSectorHud(hud, candleMeta.get(latestKey));
  }

  chart.subscribeCrosshairMove((param) => {
    if (!param || !param.time) {
      if (latestKey && candleMeta.has(latestKey)) {
        renderSectorHud(hud, candleMeta.get(latestKey));
      }
      return;
    }
    const candleData = candleMeta.get(normalizeChartTime(param.time));
    if (!candleData) {
      return;
    }
    renderSectorHud(hud, candleData);
  });

  chart.timeScale().fitContent();
  if (isZoom) {
    chart.timeScale().applyOptions({
      barSpacing: 12,
      minBarSpacing: 4,
      rightOffset: 18,
    });
    if (candles.length > 90) {
      const start = candles.length - 90;
      const end = candles.length - 1;
      chart.timeScale().setVisibleLogicalRange({ from: start, to: end });
    }
    if (showIntradaySeparator) {
      daySeparator = document.createElement('div');
      daySeparator.className = 'intraday-day-separator';
      chartEl.appendChild(daySeparator);
      updateIntradaySeparator();
      chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
        updateIntradaySeparator();
      });
    }
  }
  return chart;
}

function updateSectorZoomNav() {
  const total = sectorBoardPayload.length;
  const prevBtn = document.querySelector('.sector-zoom-step[onclick="stepSectorZoom(-1)"]');
  const nextBtn = document.querySelector('.sector-zoom-step[onclick="stepSectorZoom(1)"]');
  if (sectorZoomCounter) {
    sectorZoomCounter.textContent = total ? `${sectorZoomIndex + 1} / ${total}` : '0 / 0';
  }
  if (prevBtn) prevBtn.disabled = sectorZoomIndex <= 0;
  if (nextBtn) nextBtn.disabled = sectorZoomIndex < 0 || sectorZoomIndex >= total - 1;
}

function renderSectorZoom(idx) {
  const card = sectorBoardPayload[idx];
  if (!card || !card.has_data || !window.LightweightCharts) return;
  sectorZoomIndex = idx;
  sectorZoomTitle.textContent = card.symbol || '';
  sectorZoomSubtitle.textContent = card.sector || '';
  sectorZoomMeta.textContent = `Close ${fmt(card.end_close, 2)} ${card.move_pct == null ? '' : `| ${fmtPct(card.move_pct)}`}`;
  updateSectorZoomNav();

  sectorZoomOverlay.classList.add('open');
  sectorZoomChartEl.innerHTML = '';
  if (sectorZoomObserver) {
    sectorZoomObserver.disconnect();
    sectorZoomObserver = null;
  }
  if (sectorZoomChart) {
    sectorZoomChart.remove();
    sectorZoomChart = null;
  }
  requestAnimationFrame(() => {
    sectorZoomChart = drawSectorChart(sectorZoomChartEl, card, true);
    sectorZoomObserver = new ResizeObserver(() => {
      if (!sectorZoomChart) return;
      sectorZoomChart.applyOptions({
        width: sectorZoomChartEl.clientWidth,
        height: sectorZoomChartEl.clientHeight,
      });
      sectorZoomChart.timeScale().fitContent();
    });
    sectorZoomObserver.observe(sectorZoomChartEl);
  });
}

function renderSectorChartCard(idx) {
  const card = sectorBoardPayload[idx];
  const chartEl = document.getElementById(`sectorChart_${idx}`);
  if (!card || !chartEl || chartEl.dataset.rendered === '1') return;

  chartEl.dataset.rendered = '1';
  if (!card.has_data || !window.LightweightCharts) {
    chartEl.innerHTML = '<div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">No chart data</div>';
    return;
  }

  chartEl.innerHTML = '';
  const chart = drawSectorChart(chartEl, card, false);
  sectorChartInstances[idx] = chart;

  const observer = new ResizeObserver(() => {
    chart.applyOptions({
      width: chartEl.clientWidth,
      height: chartEl.clientHeight,
    });
    chart.timeScale().fitContent();
  });
  observer.observe(chartEl);
  sectorChartObservers[idx] = observer;
}

function setupSectorBoardLazyCharts() {
  if (!sectorBoardGrid || !sectorBoardPayload.length) return;
  if (sectorBoardObserver) {
    sectorBoardObserver.disconnect();
    sectorBoardObserver = null;
  }

  const cards = Array.from(sectorBoardGrid.querySelectorAll('.sector-mini-chart'));
  if (!('IntersectionObserver' in window)) {
    cards.forEach((el) => {
      const idx = Number(el.dataset.chartIdx);
      renderSectorChartCard(idx);
    });
    return;
  }

  sectorBoardObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      const idx = Number(entry.target.dataset.chartIdx);
      renderSectorChartCard(idx);
      sectorBoardObserver.unobserve(entry.target);
    });
  }, {
    root: null,
    rootMargin: '250px 0px',
    threshold: 0.08,
  });

  cards.forEach((el, idx) => {
    if (idx < 4) {
      renderSectorChartCard(idx);
    } else {
      sectorBoardObserver.observe(el);
    }
  });
}

function openSectorZoom(idx) {
  renderSectorZoom(idx);
}

function stepSectorZoom(delta) {
  if (!sectorBoardPayload.length) return;
  const nextIdx = sectorZoomIndex + delta;
  if (nextIdx < 0 || nextIdx >= sectorBoardPayload.length) return;
  renderSectorZoom(nextIdx);
}

function closeSectorZoom(event) {
  if (event && event.target && event.target.id !== 'sectorZoomOverlay') return;
  sectorZoomOverlay.classList.remove('open');
  sectorZoomIndex = -1;
  updateSectorZoomNav();
  if (sectorZoomObserver) {
    sectorZoomObserver.disconnect();
    sectorZoomObserver = null;
  }
  if (sectorZoomChart) {
    sectorZoomChart.remove();
    sectorZoomChart = null;
  }
}

function clearSectorBoard(message = 'No sector charts loaded yet.') {
  closeSectorZoom();
  if (sectorBoardObserver) {
    sectorBoardObserver.disconnect();
    sectorBoardObserver = null;
  }
  sectorChartInstances.forEach(chart => {
    try { chart.remove(); } catch {}
  });
  sectorChartObservers.forEach(obs => {
    try { obs.disconnect(); } catch {}
  });
  sectorChartInstances = [];
  sectorChartObservers = [];
  sectorBoardPayload = [];
  sectorBoardGrid.innerHTML = '';
  sectorBoardGrid.style.display = 'none';
  sectorBoardEmpty.textContent = message;
  sectorBoardEmpty.style.display = 'block';
  sectorBoardNote.textContent = message;
}

document.addEventListener('keydown', (event) => {
  const sectorOpen = sectorZoomOverlay.classList.contains('open');
  const previewOpen = screenerPreviewOverlay.classList.contains('open');
  if (!sectorOpen && !previewOpen) return;
  if (event.key === 'Escape') {
    event.preventDefault();
    if (previewOpen) closeScreenerPreview();
    if (sectorOpen) closeSectorZoom();
  } else if (sectorOpen && event.key === 'ArrowLeft') {
    event.preventDefault();
    stepSectorZoom(-1);
  } else if (sectorOpen && event.key === 'ArrowRight') {
    event.preventDefault();
    stepSectorZoom(1);
  }
});

async function loadSectorChartBoard(sector) {
    if (!sector) {
      clearSectorBoard();
      return;
    }

  sectorBoardNote.textContent = `Loading chart cards for ${sector}...`;
  clearSectorBoard(`Loading chart cards for ${sector}...`);
  try {
    const res = await fetch(`/api/sector-charts?sector=${encodeURIComponent(sector)}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    if (!data.charts || !data.charts.length) {
      clearSectorBoard(`No charts met the 21D avg turnover filter for ${sector}.`);
      return;
    }

    sectorBoardPayload = data.charts;
    sectorBoardGrid.innerHTML = data.charts.map((card, idx) => `
      <div class="sector-chart-card">
        <div class="sector-chart-head">
          <div>
            <div class="sector-chart-title">${escapeHtml(card.symbol || '')}</div>
            <div class="sector-chart-sector">${escapeHtml(card.sector || '')}</div>
          </div>
          <div class="sector-chart-meta">
            Close ${fmt(card.end_close, 2)} ${card.move_pct == null ? '' : `| ${fmtPct(card.move_pct)}`}
            <button class="sector-chart-expand" type="button" onclick="openSectorZoom(${idx})">Expand</button>
          </div>
        </div>
        <div id="sectorChart_${idx}" class="sector-mini-chart" data-chart-idx="${idx}">
          <div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">Loading chart...</div>
        </div>
      </div>
    `).join('');

    sectorBoardEmpty.style.display = 'none';
    sectorBoardGrid.style.display = 'grid';
    sectorBoardNote.textContent = `Showing ${data.charts.length} chart card(s) for ${sector}.`;
    sectorChartInstances = [];
    sectorChartObservers.forEach(obs => {
      try { obs.disconnect(); } catch {}
    });
    sectorChartObservers = [];
    setupSectorBoardLazyCharts();
  } catch (e) {
    clearSectorBoard(`Chart board error: ${e.message}`);
  }
}

function clearDates() {
  document.getElementById('fromDate').value = '';
  document.getElementById('toDate').value   = '';
}

// ── load stock ───────────────────────────────────────────────────────────────
async function loadStock() {
  const sym      = symInput.value.trim().toUpperCase();
  if (!sym) { setStatus('Please enter a symbol', 'err'); return; }
  closeAC();

  const fromVal = document.getElementById('fromDate').value.trim();
  const toVal   = document.getElementById('toDate').value.trim();

  const btn = document.getElementById('loadBtn');
  btn.disabled = true;
  setStatus(`Loading data for ${sym} …`, '');
  showSpinner(true);
  hideContent();

  try {
    let url = `/api/stock?symbol=${encodeURIComponent(sym)}`;
    if (fromVal) url += `&from_date=${encodeURIComponent(fromVal)}`;
    if (toVal)   url += `&to_date=${encodeURIComponent(toVal)}`;

    const res  = await fetch(url);
    const data = await res.json();

    if (data.error) {
      setStatus(`Error: ${data.error}`, 'err');
      showSpinner(false);
      showEmpty();
      btn.disabled = false;
      return;
    }

    allRows  = data.rows || [];
    sortCol  = 'mktdate';
    sortAsc  = false;

    const rangeLabel = `${data.from_date} → ${data.yesterday}`;
    document.getElementById('metaInfo').innerHTML =
      `<b style="color:var(--text)">${data.symbol}</b> &nbsp;|&nbsp; ` +
      `Range: <b>${rangeLabel}</b> &nbsp;|&nbsp; ` +
      `21D start: ${data.start_21d} &nbsp;|&nbsp; ` +
      `63D start: ${data.start_63d} &nbsp;|&nbsp; ` +
      `<b style="color:var(--accent2)">${allRows.length} rows</b>`;

    renderSummary(data, sym);
    renderTable();
    showContent();
    setStatus(`Loaded ${allRows.length} rows for ${sym} (${rangeLabel})`, 'ok');
  } catch (e) {
    setStatus(`Network error: ${e.message}`, 'err');
    showEmpty();
  }

  showSpinner(false);
  btn.disabled = false;
}

// ── summary cards ────────────────────────────────────────────────────────────
function renderSummary(data, sym) {
  const rows = data.rows;
  const mv   = data.minvol;
  const lv   = data.lowvolume;

  // quick stats from latest row
  const latest = rows[0] || {};
  const oldest = rows[rows.length - 1] || {};
  const hi52  = rows.reduce((a, r) => Math.max(a, r.high || 0), 0);
  const lo52  = rows.reduce((a, r) => Math.min(a, r.low  || Infinity), Infinity);
  const pctFH = latest.close && hi52 ? (((latest.close - hi52) / hi52) * 100).toFixed(2) : '–';
  const pctFL = latest.close && lo52 && lo52 < Infinity
    ? (((latest.close - lo52) / lo52) * 100).toFixed(2) : '–';

  const cards = [
    { label: 'Latest Close',     value: fmt(latest.close, 2),     sub: latest.mktdate, cls: 'close-hi' },
    { label: 'Latest Diff %',    value: fmtPct(latest.diff),      sub: 'vs prev close', cls: colClass(latest.diff) },
    { label: '52W High',         value: fmt(hi52, 2),             sub: `${pctFH}% from high`, cls: '' },
    { label: '52W Low',          value: fmt(lo52 < Infinity ? lo52 : null, 2), sub: `+${pctFL}% from low`, cls: 'green' },
    { label: 'Min Vol (63D win)',value: fmt(lv.table21_uses_63d_window, 0), sub: 'table lowvolume21 — 63D window', cls: 'warn' },
    { label: 'Min Vol (21D win)',value: fmt(lv.table63_uses_21d_window, 0), sub: 'table lowvolume63 — 21D window', cls: 'warn' },
    { label: 'Min Volatility 63D', value: fmt(mv['63d'], 2), sub: '63 trading-day window', cls: '' },
    { label: 'Min Volatility 21D', value: fmt(mv['21d'], 2), sub: '21 trading-day window', cls: '' },
  ];

  document.getElementById('summaryCards').innerHTML = cards.map(c => `
    <div class="card">
      <div class="card-label">${c.label}</div>
      <div class="card-value ${c.cls}">${c.value ?? '–'}</div>
      <div class="card-sub">${c.sub || ''}</div>
    </div>
  `).join('');
}

// ── table render ─────────────────────────────────────────────────────────────
function renderTable() {
  const sorted = [...allRows].sort((a, b) => {
    let av = a[sortCol], bv = b[sortCol];
    if (av == null) av = sortAsc ? Infinity  : -Infinity;
    if (bv == null) bv = sortAsc ? Infinity  : -Infinity;
    if (typeof av === 'string') return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    return sortAsc ? av - bv : bv - av;
  });

  // update sort arrows
  document.querySelectorAll('thead th').forEach(th => {
    const col = th.dataset.col;
    const arrow = th.querySelector('.sort-arrow');
    th.classList.toggle('sorted', col === sortCol);
    if (arrow) arrow.textContent = col === sortCol ? (sortAsc ? '▲' : '▼') : '';
  });

  document.getElementById('tableBody').innerHTML = sorted.map(r => {
    const diffCls  = colClass(r.diff);
    const volClass = r.VOLATILITY != null && r.VOLATILITY > 8 ? 'warn' : '';
    const ci = r.closeindictor === 'Y' || r.closeindictor === 'y'
               ? `<span class="badge badge-y">Y</span>`
               : `<span class="badge badge-n">N</span>`;
    const trCls = [
      r._hl_lowvol         ? 'hl-lowvol'         : '',
      r._hl_lowvolatility  ? 'hl-lowvolatility'  : '',
    ].filter(Boolean).join(' ');
    const volCellCls  = [volClass,  r._hl_lowvolatility ? 'hl-cell-vol' : ''].filter(Boolean).join(' ');
    const volCellVol  = r._hl_lowvol ? 'hl-cell-vol' : '';
    return `
      <tr class="${trCls}">
        <td>${r.mktdate || '–'}</td>
        <td>${r.symbol || '–'}</td>
        <td class="close-hi">${fmt(r.close, 2)}</td>
        <td>${fmt(r.open, 2)}</td>
        <td>${fmt(r.high, 2)}</td>
        <td>${fmt(r.low,  2)}</td>
        <td>${fmt(r.prevclose, 2)}</td>
        <td class="${diffCls}">${fmtPct(r.diff)}</td>
        <td class="${volCellVol}">${fmtVol(r.volume)}</td>
        <td>${fmtVol(r.deliveryvolume)}</td>
        <td>${fmt(r.delper, 2)}</td>
        <td class="${volCellCls}">${fmt(r.VOLATILITY, 2)}</td>
        <td class="${colClass(r.jag)}">${fmt(r.jag, 2)}</td>
        <td>${ci}</td>
        <td>${fmt(r['5dma'],  2)}</td>
        <td>${fmt(r['10dma'], 2)}</td>
        <td>${fmt(r['20DMA'], 2)}</td>
        <td>${fmt(r['50dma'], 2)}</td>
      </tr>`;
  }).join('');
}

// ── sort on header click ──────────────────────────────────────────────────────
document.querySelectorAll('thead th[data-col]').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (sortCol === col) sortAsc = !sortAsc;
    else { sortCol = col; sortAsc = false; }
    renderTable();
  });
});

// ── helpers ──────────────────────────────────────────────────────────────────
function fmt(v, d = 2)   { return v == null ? '–' : Number(v).toLocaleString('en-IN', { minimumFractionDigits: d, maximumFractionDigits: d }); }
function fmtPct(v)       { if (v == null) return '–'; return (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%'; }
function fmtVol(v)       { return v == null ? '–' : Number(v).toLocaleString('en-IN'); }
function escapeHtml(v) {
  return String(v)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
function escapeJs(v) {
  return String(v)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}
function colClass(v)     { if (v == null) return ''; return v > 0 ? 'pos' : v < 0 ? 'neg' : ''; }
function setStatus(m, t) { const s = document.getElementById('statusBar'); s.textContent = m; s.className = t; }
function showSpinner(on) { document.getElementById('spinner').style.display = on ? 'block' : 'none'; }
function showContent()   { document.getElementById('contentArea').style.display = 'block'; document.getElementById('emptyState').style.display = 'none'; }
function hideContent()   { document.getElementById('contentArea').style.display = 'none'; }
function showEmpty()     { document.getElementById('emptyState').style.display = 'flex'; }

// ── enter key on input ────────────────────────────────────────────────────────
symInput.addEventListener('keydown', e => { if (e.key === 'Enter') loadStock(); });

</script>
</body>
</html>
"""

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    print(f"\n  NSE BHAV Viewer  →  http://{args.host}:{args.port}\n")
    app.run(host=args.host, port=args.port, debug=args.debug)
