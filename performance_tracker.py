#!/usr/bin/env python3
"""
Institutional Picks — Performance Tracker
==========================================
Streamlit app: loads institutional_picks_*.txt, fetches OHLC via Kite API,
benchmarks against Nifty Smallcap 250 from bhav.indexbhav (MySQL).

Launch:  python -m streamlit run performance_tracker.py
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import mysql.connector
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "reports"
TOKEN_FILE  = BASE_DIR / "kite_token.txt"

DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "bhav"

SLEEP_S = 0.35          # delay between Kite calls

PERIODS: Dict[str, int] = {      # label -> calendar days from cutoff
    "1 Week":  7,
    "15 Days": 15,
    "1 Month": 30,
}
PERIOD_KEYS  = list(PERIODS.keys())
TODAY        = date.today()

# ─────────────────────────────────────────────────────────────────────────────
# Page config  (MUST be first Streamlit call)
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Picks Performance",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Global CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ── Kill ALL Streamlit chrome ─────────────────────────── */
#stDecoration, header[data-testid="stHeader"],
[data-testid="stHeader"], [data-testid="stToolbar"],
[data-testid="stMainMenu"], [data-testid="stStatusWidget"],
[data-testid="stSidebarHeader"], .viewerBadge_container__r5tak,
footer, #MainMenu { display: none !important; height:0 !important; }

/* ── Nuke every source of top whitespace ───────────────── */
.main .block-container          { padding-top: 0.5rem !important; }
section[data-testid="stMain"]   { padding-top: 0 !important; }
.appview-container              { padding-top: 0 !important; }
.appview-container .main section{ padding-top: 0 !important; }
[data-testid="block-container"] { padding-top: 0.5rem !important; }
[data-testid="stSidebar"] > div:first-child { padding-top: 0.8rem !important; }

/* ── Selectbox control box ──────────────────────────────── */
[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: #161e2c !important;
    border: 1px solid #2e4460 !important;
    border-radius: 8px !important;
}
/* Selected value text */
[data-testid="stSidebar"] [data-baseweb="select"] [data-testid="stMarkdown"] p,
[data-testid="stSidebar"] [data-baseweb="select"] > div > div > div,
[data-testid="stSidebar"] [data-baseweb="select"] input,
[data-testid="stSidebar"] [data-baseweb="select"] span {
    color: #e0eeff !important;
    font-weight: 700 !important;
    font-size: 0.9rem !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] svg { fill: #4a7aaa !important; }

/* ── Dropdown popup list ─────────────────────────────────── */
[data-baseweb="popover"] { z-index: 9999 !important; }
[data-baseweb="popover"] > div,
ul[data-baseweb="menu"],
[data-baseweb="menu"] {
    background-color: #101828 !important;
    border: 1px solid #1e3858 !important;
    border-radius: 8px !important;
    box-shadow: 0 8px 32px rgba(0,0,0,0.7) !important;
}
/* Each option — target role="option" not li */
[data-baseweb="menu"] [role="option"],
[data-baseweb="menu"] li {
    background-color: #101828 !important;
    color: #c0d8f0 !important;
    font-size: 0.88rem !important;
    font-weight: 600 !important;
    padding: 8px 14px !important;
}
[data-baseweb="menu"] [role="option"]:hover,
[data-baseweb="menu"] [aria-selected="true"],
[data-baseweb="menu"] li:hover {
    background-color: #1a3358 !important;
    color: #ffffff !important;
}

/* ── Selectbox label ────────────────────────────────────── */
[data-testid="stSidebar"] label {
    color: #3a5570 !important;
    font-size: 0.64rem !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: 1.5px;
}

/* ── Custom HTML table ──────────────────────────────────── */
.perf-table-wrap {
    overflow-x: auto;
    border-radius: 10px;
    border: 1px solid #1a2638;
    box-shadow: 0 4px 20px rgba(0,0,0,0.6);
    margin-top: 8px;
}
.perf-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
    font-size: 0.85rem;
    background: #0c1118;
}
.perf-table thead th {
    background: #0a1020;
    color: #4a7090;
    font-size: 0.68rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    padding: 10px 14px;
    border-bottom: 1px solid #1a2638;
    text-align: right;
    white-space: nowrap;
}
.perf-table thead th:first-child { text-align: left; }
.perf-table tbody tr { border-bottom: 1px solid #111820; }
.perf-table tbody tr:nth-child(even) td { background: #0a1118; }
.perf-table tbody tr:hover td { filter: brightness(1.15); }
.perf-table td {
    padding: 8px 14px;
    text-align: right;
    white-space: nowrap;
    color: #8aaac8;
    font-size: 0.84rem;
    font-weight: 500;
}
.perf-table td.stock-name {
    text-align: left;
    color: #ddeeff;
    font-weight: 700;
    font-size: 0.87rem;
    letter-spacing: 0.3px;
    background: #0c1320 !important;
}
.perf-table td.ret-cell {
    font-weight: 700;
    font-size: 0.86rem;
    color: #ffffff;
}

/* ── Base ──────────────────────────────────────────────── */
html, body, [data-testid="stAppViewContainer"],
[data-testid="stMain"], [data-testid="block-container"] {
    background: #10141a !important;
    color: #f0f4f8 !important;
    font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
}

/* ── Sidebar ───────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #0c1018 !important;
    border-right: 1px solid #1e2530 !important;
}
[data-testid="stSidebar"] * { color: #d0dae6 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stCaption { color: #7a8fa6 !important; }

/* ── Metric cards ──────────────────────────────────────── */
[data-testid="stMetric"] {
    background: #171d27 !important;
    border: 1px solid #242e3d !important;
    border-radius: 12px !important;
    padding: 18px 22px !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.5) !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.55rem !important;
    font-weight: 800 !important;
    color: #f0f4f8 !important;
    letter-spacing: -0.5px;
}
[data-testid="stMetricLabel"] {
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: #6b7f96 !important;
}
[data-testid="stMetricDelta"] svg { display: none !important; }
[data-testid="stMetricDelta"] > div {
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    color: #7a8fa6 !important;
}

/* ── Primary button ────────────────────────────────────── */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #1a6b34 0%, #238636 100%) !important;
    border: 1px solid #2ea043 !important;
    border-radius: 8px !important;
    color: #ffffff !important;
    font-weight: 700 !important;
    font-size: 0.92rem !important;
    letter-spacing: 0.3px;
    padding: 10px 0 !important;
    box-shadow: 0 2px 10px rgba(35,134,54,0.4) !important;
    transition: all 0.15s ease !important;
}
.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #238636 0%, #2ea043 100%) !important;
    box-shadow: 0 4px 18px rgba(46,160,67,0.55) !important;
    transform: translateY(-1px);
}

/* ── Section label ─────────────────────────────────────── */
.section-label {
    font-size: 0.65rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.8px;
    color: #4a6278;
    margin: 4px 0 10px 0;
    padding-bottom: 6px;
    border-bottom: 1px solid #1e2a38;
}

/* ── Banner ────────────────────────────────────────────── */
.banner {
    background: linear-gradient(135deg, #131920 0%, #171f2b 50%, #131920 100%);
    border: 1px solid #1e2a38;
    border-left: 4px solid #2563eb;
    border-radius: 12px;
    padding: 18px 26px;
    margin-bottom: 22px;
}
.banner h1 {
    margin: 0 0 6px 0;
    font-size: 1.45rem;
    font-weight: 800;
    color: #f0f4f8;
    letter-spacing: -0.3px;
}
.banner .meta { color: #6b7f96; font-size: 0.82rem; margin: 0; line-height: 1.7; }
.banner .meta b { color: #c9d8e8; font-weight: 600; }

/* ── Skip chip ─────────────────────────────────────────── */
.skip-chip {
    display: inline-block;
    background: #1f0e0e;
    border: 1px solid #5c2020;
    border-radius: 5px;
    padding: 3px 9px;
    font-size: 0.73rem;
    color: #f87171;
    margin: 2px 4px 2px 0;
    font-family: 'Courier New', monospace;
}

/* ── Sidebar section label ─────────────────────────────── */
.sidebar-section {
    font-size: 0.62rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.8px;
    color: #3b82f6;
    margin: 18px 0 7px 0;
}

hr { border-color: #1e2a38 !important; margin: 18px 0 !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# File helpers
# ─────────────────────────────────────────────────────────────────────────────
_DATE_PAT  = re.compile(r"institutional_picks_(\d{2})([a-zA-Z]{3})(\d{4})\.txt$", re.I)
_MONTH_MAP = {m: i+1 for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}


def discover_picks_files() -> Dict[date, Path]:
    if not REPORTS_DIR.exists():
        return {}
    result: Dict[date, Path] = {}
    for f in REPORTS_DIR.glob("institutional_picks_*.txt"):
        m = _DATE_PAT.match(f.name)
        if not m:
            continue
        dd, mon, yyyy = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        mon_n = _MONTH_MAP.get(mon)
        if mon_n:
            try:
                result[date(yyyy, mon_n, dd)] = f
            except ValueError:
                pass
    return dict(sorted(result.items(), reverse=True))


def parse_picks_file(path: Path) -> List[str]:
    """Return plain symbols (no exchange prefix)."""
    out, seen = [], set()
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        sym = line.split(":", 1)[-1].strip().upper()
        if sym and sym not in seen:
            out.append(sym)
            seen.add(sym)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Kite helpers
# ─────────────────────────────────────────────────────────────────────────────
def read_token_file(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip().upper()] = v.strip()
    return data


@st.cache_resource(show_spinner=False)
def get_kite(api_key: str, access_token: str):
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _norm(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper().replace("&", "AND"))


@st.cache_data(show_spinner=False, ttl=3600)
def load_nse_instruments(api_key: str, access_token: str) -> Tuple[Dict, Dict]:
    kite = get_kite(api_key, access_token)
    rows = kite.instruments("NSE")
    exact: Dict[str, int] = {}
    normd: Dict[str, int] = {}
    for r in rows:
        ts    = str(r.get("tradingsymbol", "")).strip().upper()
        tok   = int(r.get("instrument_token", 0))
        itype = str(r.get("instrument_type", "")).upper()
        seg   = str(r.get("segment", "")).upper()
        if not ts or not tok:
            continue
        if itype in ("INDEX",) or "INDICES" in seg:
            continue
        exact.setdefault(ts, tok)
        normd.setdefault(_norm(ts), tok)
    return exact, normd


def resolve_token(exact: Dict, normd: Dict, sym: str) -> Optional[int]:
    su = sym.upper().strip()
    return exact.get(su) or normd.get(_norm(su))


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_kite_history(api_key: str, access_token: str,
                       token: int, from_d: date, to_d: date) -> pd.DataFrame:
    kite = get_kite(api_key, access_token)
    rows = kite.historical_data(
        instrument_token=token,
        from_date=datetime.combine(from_d, datetime.min.time()),
        to_date=datetime.combine(to_d, datetime.min.time()),
        interval="day", continuous=False, oi=False,
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"]  = pd.to_datetime(df["date"]).dt.date
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# MySQL / indexbhav helpers
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_db_conn():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
    )


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_smallcap250(from_d: date, to_d: date) -> pd.DataFrame:
    """Pull Nifty Smallcap 250 close prices from bhav.indexbhav."""
    sql_exact = """
        SELECT mktdate, close
        FROM   indexbhav
        WHERE  UPPER(symbol) = 'NIFTY SMALLCAP 250'
          AND  mktdate BETWEEN %s AND %s
        ORDER  BY mktdate
    """
    sql_like = """
        SELECT mktdate, close
        FROM   indexbhav
        WHERE  UPPER(symbol) LIKE %s
          AND  mktdate BETWEEN %s AND %s
        ORDER  BY mktdate
    """
    try:
        conn = get_db_conn()
        df = pd.read_sql(sql_exact, conn, params=[from_d, to_d])
        if df.empty:
            for pat in ("%SMALLCAP%250%", "%NIFTY%SMALLCAP%"):
                df = pd.read_sql(sql_like, conn, params=[pat, from_d, to_d])
                if not df.empty:
                    break
        if df.empty:
            return pd.DataFrame(columns=["mktdate", "close"])
        df["mktdate"] = pd.to_datetime(df["mktdate"]).dt.date
        df["close"]   = pd.to_numeric(df["close"], errors="coerce")
        return df.drop_duplicates("mktdate").sort_values("mktdate").reset_index(drop=True)
    except Exception as e:
        st.warning(f"Could not read indexbhav: {e}")
        return pd.DataFrame(columns=["mktdate", "close"])


# ─────────────────────────────────────────────────────────────────────────────
# Price lookup helpers
# ─────────────────────────────────────────────────────────────────────────────
def close_on_or_before(df: pd.DataFrame, d: date,
                        date_col: str = "date",
                        close_col: str = "close") -> Optional[float]:
    sub = df[df[date_col] <= d]
    return float(sub.iloc[-1][close_col]) if not sub.empty else None


def close_on_or_after(df: pd.DataFrame, d: date,
                       date_col: str = "date",
                       close_col: str = "close") -> Optional[Tuple[float, date]]:
    sub = df[df[date_col] >= d]
    if sub.empty:
        return None
    row = sub.iloc[0]
    return float(row[close_col]), row[date_col]


def pct(base: float, end: float) -> float:
    return (end - base) / base * 100.0 if base else float("nan")


# ─────────────────────────────────────────────────────────────────────────────
# Core analyser
# ─────────────────────────────────────────────────────────────────────────────
def run_analysis(
    api_key: str, access_token: str,
    symbols: List[str], cutoff: date,
    progress,
) -> Tuple[pd.DataFrame, Dict, Dict, List[str]]:
    """
    Returns
    -------
    df        : one row per symbol with base + period close/return columns
    bm_ret    : {period_label: return_pct}
    bm_asof   : {period_label: actual_date}  — may be < target if period not over
    skipped   : list of symbols that had no data
    """
    fetch_from = cutoff - timedelta(days=5)
    fetch_to   = min(TODAY, cutoff + timedelta(days=40))

    # ── Benchmark from MySQL ─────────────────────────────────────────────────
    bm_df   = fetch_smallcap250(fetch_from, fetch_to)
    bm_ret  : Dict[str, Optional[float]] = {}
    bm_asof : Dict[str, Optional[date]]  = {}
    bm_base : Optional[float] = None

    if not bm_df.empty:
        bm_base = close_on_or_before(bm_df, cutoff, "mktdate", "close")
        for label, days in PERIODS.items():
            target = cutoff + timedelta(days=days)
            # If target is in future, use the latest available date
            if target > TODAY:
                result = close_on_or_after(bm_df, cutoff + timedelta(days=1),
                                           "mktdate", "close")
                # actually get the most recent available
                sub = bm_df[bm_df["mktdate"] > cutoff]
                if not sub.empty:
                    row = sub.iloc[-1]
                    bm_ret[label]  = pct(bm_base, float(row["close"])) if bm_base else None
                    bm_asof[label] = row["mktdate"]
                else:
                    bm_ret[label] = bm_asof[label] = None
            else:
                result = close_on_or_after(bm_df, target, "mktdate", "close")
                if result and bm_base:
                    bm_ret[label]  = pct(bm_base, result[0])
                    bm_asof[label] = result[1]
                else:
                    bm_ret[label] = bm_asof[label] = None
    else:
        for label in PERIODS:
            bm_ret[label] = bm_asof[label] = None

    # ── Instruments ─────────────────────────────────────────────────────────
    exact, normd = load_nse_instruments(api_key, access_token)

    # ── Stocks ───────────────────────────────────────────────────────────────
    rows: List[dict] = []
    skipped: List[str] = []
    n = len(symbols)

    for i, sym in enumerate(symbols):
        progress.progress((i + 1) / n, text=f"Fetching {sym}  ({i+1}/{n})")

        token = resolve_token(exact, normd, sym)
        if token is None:
            skipped.append(f"{sym}  — not found in NSE instruments")
            continue

        df_h = fetch_kite_history(api_key, access_token, token, fetch_from, fetch_to)
        time.sleep(SLEEP_S)

        if df_h.empty:
            skipped.append(f"{sym}  — no OHLC data")
            continue

        base = close_on_or_before(df_h, cutoff)
        if base is None:
            skipped.append(f"{sym}  — no data on or before cutoff")
            continue

        row: dict = {"Symbol": sym, "Base Close": base}

        for label, days in PERIODS.items():
            target = cutoff + timedelta(days=days)
            if target > TODAY:
                # Partial: use latest available date
                sub = df_h[df_h["date"] > cutoff]
                if not sub.empty:
                    last_row    = sub.iloc[-1]
                    end_close   = float(last_row["close"])
                    end_date    = last_row["date"]
                    row[f"{label}|close"] = end_close
                    row[f"{label}|date"]  = end_date
                    row[f"{label}|ret"]   = pct(base, end_close)
                    row[f"{label}|note"]  = f"till {end_date.strftime('%d %b')}"
                else:
                    row[f"{label}|close"] = row[f"{label}|date"] = row[f"{label}|ret"] = row[f"{label}|note"] = None
            else:
                result = close_on_or_after(df_h, target)
                if result:
                    end_close, end_date = result
                    row[f"{label}|close"] = end_close
                    row[f"{label}|date"]  = end_date
                    row[f"{label}|ret"]   = pct(base, end_close)
                    row[f"{label}|note"]  = None
                else:
                    row[f"{label}|close"] = row[f"{label}|date"] = row[f"{label}|ret"] = row[f"{label}|note"] = None

        rows.append(row)

    df = pd.DataFrame(rows)
    return df, bm_ret, bm_asof, skipped


# ─────────────────────────────────────────────────────────────────────────────
# Colour helpers
# ─────────────────────────────────────────────────────────────────────────────
def _ret_colour(val, bm_val):
    """
    Solid background, always white text — maximum contrast.
    Intensity of background scales with how far above/below benchmark.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "background-color:#141a22; color:#3d4f61; font-weight:500"

    diff = abs(val - bm_val) if (bm_val is not None and not pd.isna(bm_val)) else abs(val)
    mag  = diff

    # Green palette  (dark → vivid as magnitude grows)
    G = ["#0d2218", "#0f2d1e", "#114225", "#17572e", "#1a6b35", "#238636"]
    # Red palette
    R = ["#220d0d", "#2d1010", "#3f1414", "#561a1a", "#6e2020", "#8b2525"]

    if mag < 2:   tier = 0
    elif mag < 4: tier = 1
    elif mag < 7: tier = 2
    elif mag < 10: tier = 3
    elif mag < 15: tier = 4
    else:          tier = 5

    is_positive = val >= (bm_val if bm_val is not None else 0)
    bg = G[tier] if is_positive else R[tier]

    # Text: white for darker tiers, very bright for lightest tiers
    fg = "#ffffff" if tier >= 2 else ("#a7f3c0" if is_positive else "#fca5a5")

    return f"background-color:{bg}; color:{fg}; font-weight:700; font-size:0.88rem"


def _sign(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"


# ─────────────────────────────────────────────────────────────────────────────
# HTML table builder  (no iframe, no white borders, full CSS control)
# ─────────────────────────────────────────────────────────────────────────────
def _ret_cell_style(val, bm_val) -> str:
    """Return inline style string for a return cell."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "background:#0c1118; color:#2a3d52;"
    diff = abs(val - bm_val) if (bm_val is not None and not pd.isna(bm_val)) else abs(val)
    G = ["#0d2218","#0f2d1e","#114225","#17572e","#1a6b35","#238636"]
    R = ["#220d0d","#2d1010","#3f1414","#561a1a","#6e2020","#8b2525"]
    tier = 0 if diff<2 else 1 if diff<4 else 2 if diff<7 else 3 if diff<10 else 4 if diff<15 else 5
    is_pos = val >= (bm_val if bm_val is not None else 0)
    bg  = G[tier] if is_pos else R[tier]
    fg  = "#ffffff" if tier >= 2 else ("#a7f3c0" if is_pos else "#fca5a5")
    return f"background:{bg}; color:{fg};"


def build_html_table(df: pd.DataFrame, bm_ret: Dict, cutoff: date, sort_col: str) -> str:
    """Render a fully themed HTML table — no iframe, no white border."""
    period_headers = []
    for label in PERIOD_KEYS:
        target = cutoff + timedelta(days=PERIODS[label])
        partial = target > TODAY
        suffix  = f"<br><span style='font-size:0.6rem;color:#2e4a62;font-weight:500'>" \
                  f"{'⏳ partial' if partial else target.strftime('%d %b')}</span>"
        period_headers.append((label, suffix))

    # ── Header ───────────────────────────────────────────────────────────────
    th = lambda txt, extra="": f'<th style="padding:10px 14px;background:#080e18;color:#3d6080;font-size:0.66rem;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;border-bottom:2px solid #162030;white-space:nowrap;text-align:right{extra}">{txt}</th>'

    header  = '<thead><tr>'
    header += th("#", ";text-align:center;width:36px;color:#1e3040")
    header += th("Stock", ";text-align:left;color:#5080a0")
    header += th(f"Base Close<br><span style='font-size:0.6rem;color:#2e4a62'>{cutoff.strftime('%d %b')}</span>")
    for label, suffix in period_headers:
        header += th(f"{label} Close{suffix}")
        lbl_clean = label.upper()
        bm_val = bm_ret.get(label)
        bm_str = f"<br><span style='font-size:0.58rem;color:#1e5038'>BM {_sign(bm_val)}</span>" if bm_val is not None else ""
        header += th(f"{lbl_clean} RET%{bm_str}")
    header += '</tr></thead>'

    # ── Rows ─────────────────────────────────────────────────────────────────
    td_base = "padding:8px 14px; border-bottom:1px solid #0e1620; text-align:right; font-size:0.84rem; font-weight:500; color:#6a90b0; white-space:nowrap;"
    rows_html = "<tbody>"
    for i, (_, r) in enumerate(df.iterrows()):
        row_bg = "#0a1018" if i % 2 == 0 else "#0c1320"
        rows_html += f'<tr style="background:{row_bg}">'

        # Row number
        rows_html += f'<td style="{td_base}text-align:center;color:#1e3040;font-size:0.72rem">{i+1}</td>'

        # Stock name
        rows_html += f'<td style="{td_base}text-align:left;color:#ddeeff;font-weight:700;font-size:0.87rem;letter-spacing:0.3px;background:#080f1a">{r["Symbol"]}</td>'

        # Base close
        base = r.get("Base Close")
        base_str = f"{base:,.2f}" if base is not None else "—"
        rows_html += f'<td style="{td_base}">{base_str}</td>'

        # Period columns
        for label in PERIOD_KEYS:
            c   = r.get(f"{label}|close")
            ret = r.get(f"{label}|ret")
            bm_val = bm_ret.get(label)
            c_str   = f"{c:,.2f}" if c is not None else "—"
            ret_str = _sign(ret)
            rows_html += f'<td style="{td_base}">{c_str}</td>'
            cell_style = _ret_cell_style(ret, bm_val)
            rows_html += f'<td class="ret-cell" style="{td_base}font-weight:700;font-size:0.86rem;{cell_style}">{ret_str}</td>'

        rows_html += "</tr>"
    rows_html += "</tbody>"

    return (
        f'<div class="perf-table-wrap">'
        f'<table class="perf-table">{header}{rows_html}</table>'
        f'</div>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    # ── Token — silent, no UI clutter ───────────────────────────────────────
    creds        = read_token_file(TOKEN_FILE)
    api_key      = creds.get("API_KEY", "")
    access_token = creds.get("ACCESS_TOKEN", "")
    if not (api_key and access_token):
        st.error("kite_token.txt missing — run kite_get_access_token.py")
        st.stop()

    # ── Title inside sidebar ─────────────────────────────────────────────────
    st.markdown("""
    <div style="padding:4px 0 14px 0; border-bottom:1px solid #1a2a3a; margin-bottom:12px;">
      <div style="font-size:1.05rem;font-weight:800;color:#ddeeff;letter-spacing:-0.2px;">
        📈 Institutional Picks
      </div>
      <div style="font-size:0.68rem;color:#3a5570;font-weight:600;
                  text-transform:uppercase;letter-spacing:1.2px;margin-top:3px;">
        Performance Tracker
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">Cutoff Date</div>', unsafe_allow_html=True)

    files_map = discover_picks_files()
    if not files_map:
        st.error(f"No institutional_picks_*.txt in:\n`{REPORTS_DIR}`")
        st.stop()

    dates      = list(files_map.keys())
    date_lbls  = [d.strftime("%d %b %Y") for d in dates]
    sel_idx    = st.selectbox("Picks date", range(len(dates)),
                               format_func=lambda i: date_lbls[i])
    cutoff     = dates[sel_idx]
    picks_path = files_map[cutoff]

    st.markdown('<div class="sidebar-section" style="margin-top:14px;">Analysis Windows</div>', unsafe_allow_html=True)
    for label, days in PERIODS.items():
        target = cutoff + timedelta(days=days)
        if target <= TODAY:
            st.markdown(
                f'<div style="font-size:0.78rem;color:#3a6a4a;padding:2px 0;">'
                f'<span style="color:#2a5a3a">●</span> '
                f'<span style="color:#6ab880;font-weight:600">{label}</span>'
                f'<span style="color:#2a4a3a"> → {target.strftime("%d %b %Y")} ✓</span></div>',
                unsafe_allow_html=True)
        else:
            lag = (TODAY - cutoff).days
            st.markdown(
                f'<div style="font-size:0.78rem;padding:2px 0;">'
                f'<span style="color:#4a6a8a">●</span> '
                f'<span style="color:#6090b0;font-weight:600">{label}</span>'
                f'<span style="color:#2e4a62"> → {target.strftime("%d %b %Y")}'
                f' <em>({lag}d so far)</em></span></div>',
                unsafe_allow_html=True)

    st.markdown('<div style="margin-top:16px;"></div>', unsafe_allow_html=True)
    run_btn = st.button("🚀  Run Analysis", type="primary", use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Main area — starts directly with content (no banner)
# ─────────────────────────────────────────────────────────────────────────────
symbols = parse_picks_file(picks_path)

if not run_btn:
    st.markdown(
        f'<p style="color:#2e4a62;font-size:0.82rem;margin:4px 0 12px 0;">'
        f'Cutoff <b style="color:#4a7090">{cutoff.strftime("%d %b %Y")}</b>'
        f' · {len(symbols)} stocks · Nifty Smallcap 250 benchmark</p>',
        unsafe_allow_html=True)
    st.info("👈 Select a date in the sidebar and click **Run Analysis**.")
    with st.expander("📋 Symbols in this file"):
        cols = st.columns(5)
        for i, sym in enumerate(symbols):
            cols[i % 5].markdown(f"`{sym}`")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────────────────────────────────────
prog    = st.progress(0, text="Initialising…")
df, bm_ret, bm_asof, skipped = run_analysis(
    api_key, access_token, symbols, cutoff, prog
)
prog.empty()

if df.empty:
    st.error("No data returned. Check token validity.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Benchmark cards
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    f'<div style="display:flex;align-items:baseline;gap:10px;margin-bottom:10px;">'
    f'<span style="font-size:0.95rem;font-weight:800;color:#c8dff0;">Cutoff '
    f'<span style="color:#4a9eff">{cutoff.strftime("%d %b %Y")}</span></span>'
    f'<span style="color:#1e3040;font-size:0.75rem;">·</span>'
    f'<span style="font-size:0.75rem;color:#2e4a60;font-weight:600;">'
    f'{len(symbols)} stocks · Nifty Smallcap 250 benchmark</span>'
    f'</div>',
    unsafe_allow_html=True)
st.markdown('<p class="section-label">Nifty Smallcap 250 Benchmark Returns</p>', unsafe_allow_html=True)
bm_cols = st.columns(len(PERIODS))
for col_ui, label in zip(bm_cols, PERIOD_KEYS):
    val   = bm_ret.get(label)
    asof  = bm_asof.get(label)
    asof_str = asof.strftime("%d %b") if asof else "N/A"
    target   = cutoff + timedelta(days=PERIODS[label])
    partial  = target > TODAY
    col_ui.metric(
        label=f"{label}{'  ⏳' if partial else ''}",
        value=_sign(val) if val is not None else "N/A",
        delta=f"as of {asof_str}" + (" (partial)" if partial else ""),
        delta_color="off",
    )

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# Summary statistics
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<p class="section-label">Portfolio Snapshot</p>', unsafe_allow_html=True)
sum_cols = st.columns(len(PERIODS) * 2)
ci = 0
for label in PERIOD_KEYS:
    ret_col = f"{label}|ret"
    if ret_col not in df.columns:
        ci += 2
        continue
    series  = df[ret_col].dropna()
    bm_val  = bm_ret.get(label)
    n_total = len(series)
    avg_ret = series.mean() if n_total else None
    excess  = (avg_ret - bm_val) if avg_ret is not None and bm_val is not None else None
    n_beat  = int((series > bm_val).sum()) if bm_val is not None and n_total else 0
    pct_beat = n_beat / n_total * 100 if n_total else 0

    sum_cols[ci].metric(
        label=f"{label} Avg Return",
        value=_sign(avg_ret) if avg_ret is not None else "N/A",
        delta=f"{excess:+.2f}% vs benchmark" if excess is not None else "",
    )
    ci += 1
    sum_cols[ci].metric(
        label=f"Beat BM ({label})",
        value=f"{n_beat} / {n_total}",
        delta=f"{pct_beat:.0f}% of picks",
        delta_color="normal" if pct_beat >= 50 else "inverse",
    )
    ci += 1

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# Table
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<p class="section-label">Stock-level Returns</p>', unsafe_allow_html=True)

sort_options = {
    "1 Month Ret%":  "1 Month|ret",
    "15 Days Ret%":  "15 Days|ret",
    "1 Week Ret%":   "1 Week|ret",
    "Symbol (A→Z)":  "Symbol",
}
sort_choice = st.selectbox(
    "Sort by",
    options=list(sort_options.keys()),
    index=0,
    label_visibility="collapsed",
)
sort_col = sort_options[sort_choice]
asc      = sort_choice == "Symbol (A→Z)"

df_sorted = df.sort_values(sort_col, ascending=asc, na_position="last") \
              .reset_index(drop=True)

html_table = build_html_table(df_sorted, bm_ret, cutoff, sort_col)
st.markdown(html_table, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<p class="section-label">Visual Analysis</p>', unsafe_allow_html=True)

chart_tab1, chart_tab2 = st.tabs(["📊 Return Bars", "🔵 1W vs 1M Scatter"])

# ── Tab 1: bar chart ─────────────────────────────────────────────────────────
with chart_tab1:
    for label in PERIOD_KEYS:
        ret_col = f"{label}|ret"
        if ret_col not in df.columns:
            continue
        bm_val = bm_ret.get(label)
        partial = cutoff + timedelta(days=PERIODS[label]) > TODAY

        chart_df = df[["Symbol", ret_col]].dropna().copy()
        chart_df.columns = ["Symbol", "Return%"]
        chart_df = chart_df.sort_values("Return%", ascending=False)
        chart_df["colour"] = chart_df["Return%"].apply(
            lambda v: "#3fb950" if (bm_val is None or v >= bm_val) else "#f85149"
        )

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=chart_df["Symbol"],
            y=chart_df["Return%"],
            marker_color=chart_df["colour"],
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>Return: %{y:.2f}%<extra></extra>",
        ))
        if bm_val is not None:
            asof_lbl = bm_asof.get(label)
            asof_str = asof_lbl.strftime("%d %b") if asof_lbl else ""
            fig.add_hline(
                y=bm_val, line_dash="dash", line_color="#f0883e", line_width=1.5,
                annotation_text=f"  Nifty SC 250: {bm_val:+.2f}%{' ('+asof_str+')' if asof_str else ''}",
                annotation_font_color="#f0883e",
                annotation_position="top left",
            )
        title_suffix = " ⏳ (partial)" if partial else ""
        fig.update_layout(
            title=dict(text=f"{label} Returns{title_suffix}", font_size=14,
                       font_color="#8b949e", x=0),
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font_color="#e6edf3",
            xaxis=dict(tickangle=-45, gridcolor="#21262d", tickfont_size=11),
            yaxis=dict(gridcolor="#21262d", zeroline=True,
                       zerolinecolor="#30363d", ticksuffix="%"),
            margin=dict(t=50, b=100, l=50, r=20),
            height=360,
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 2: scatter ────────────────────────────────────────────────────────────
with chart_tab2:
    if "1 Week|ret" in df.columns and "1 Month|ret" in df.columns:
        sc = df[["Symbol", "1 Week|ret", "1 Month|ret"]].dropna().copy()
        sc.columns = ["Symbol", "1W", "1M"]
        bm_1w = bm_ret.get("1 Week")
        bm_1m = bm_ret.get("1 Month")

        colours = []
        for _, row in sc.iterrows():
            above_1w = bm_1w is None or row["1W"] >= bm_1w
            above_1m = bm_1m is None or row["1M"] >= bm_1m
            if above_1w and above_1m:
                colours.append("#3fb950")   # green — beat both
            elif not above_1w and not above_1m:
                colours.append("#f85149")   # red — lagged both
            else:
                colours.append("#d29922")   # amber — mixed

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=sc["1W"], y=sc["1M"],
            mode="markers+text",
            text=sc["Symbol"],
            textposition="top center",
            textfont=dict(size=9, color="#8b949e"),
            marker=dict(color=colours, size=11, line=dict(width=1, color="#21262d")),
            hovertemplate="<b>%{text}</b><br>1W: %{x:.2f}%<br>1M: %{y:.2f}%<extra></extra>",
        ))
        if bm_1w is not None:
            fig2.add_vline(x=bm_1w, line_dash="dash", line_color="#f0883e", line_width=1.2,
                           annotation_text=" BM 1W", annotation_font_color="#f0883e")
        if bm_1m is not None:
            fig2.add_hline(y=bm_1m, line_dash="dash", line_color="#f0883e", line_width=1.2,
                           annotation_text=" BM 1M", annotation_font_color="#f0883e")

        fig2.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font_color="#e6edf3",
            xaxis=dict(title="1 Week Return (%)", gridcolor="#21262d",
                       zeroline=True, zerolinecolor="#30363d"),
            yaxis=dict(title="1 Month Return (%)", gridcolor="#21262d",
                       zeroline=True, zerolinecolor="#30363d"),
            height=500,
            margin=dict(t=20, b=60, l=60, r=20),
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("🟢 Beat both periods  🟡 Mixed  🔴 Lagged both  |  Orange dashed = benchmark")

# ─────────────────────────────────────────────────────────────────────────────
# Skipped
# ─────────────────────────────────────────────────────────────────────────────
if skipped:
    st.markdown("---")
    with st.expander(f"⚠️  {len(skipped)} stocks skipped"):
        html = " ".join(f'<span class="skip-chip">{s}</span>' for s in skipped)
        st.markdown(html, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("---")
export_df = df.copy()
export_df = export_df.rename(columns={"Symbol": "Stock"})
for label in PERIOD_KEYS:
    bm_val = bm_ret.get(label)
    ret_col = f"{label}|ret"
    if ret_col in export_df.columns and bm_val is not None:
        export_df[f"{label}|vs_bm"] = export_df[ret_col] - bm_val

csv = export_df.to_csv(index=False).encode("utf-8")
st.download_button(
    "📥  Download CSV",
    csv,
    file_name=f"perf_{cutoff.strftime('%d%b%Y').lower()}.csv",
    mime="text/csv",
)
