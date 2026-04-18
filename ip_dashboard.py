#!/usr/bin/env python3
"""
Institutional Picks — Interactive Fire Status Dashboard
========================================================
Run with:
    streamlit run ip_dashboard.py

Opens at http://localhost:8501
"""

from __future__ import annotations

import sys
import time
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

# ── import core helpers from ip_fire_report in the same folder ────────────────
sys.path.insert(0, str(Path(__file__).parent))
from ip_fire_report import (
    build_token_lookups,
    classify_stock,
    fetch_daily_closes,
    parse_picks_file,
    resolve_token,
)

# ─────────────────────────────────────────────────────────────────────────────
# Fixed paths — edit here if folder layout changes
# ─────────────────────────────────────────────────────────────────────────────
_BASE        = Path(__file__).parent
REPORTS_DIR  = _BASE / "reports"
TOKEN_FILE   = _BASE / "kite_token.txt"
TRADEBOOK_DIR = _BASE / "input" / "tradebook"
TRADEBOOK_GLOB = "tradebook-*.csv"
SECTOR_CACHE_FILE = _BASE / "bse_master.csv"

IST = ZoneInfo("Asia/Kolkata")


@st.cache_data(show_spinner=False)
def load_local_sector_map(cache_file: str) -> Dict[str, str]:
    path = Path(cache_file)
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, usecols=["symbol", "sector"])
    except Exception:
        return {}
    if df.empty:
        return {}
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["sector"] = df["sector"].astype(str).str.strip()
    df = df[(df["symbol"] != "") & (df["sector"] != "")]
    return dict(zip(df["symbol"], df["sector"]))


def order_symbols_by_sector(symbols: List[str], sector_map: Dict[str, str]) -> List[str]:
    if not symbols:
        return []
    original_index = {sym: idx for idx, sym in enumerate(symbols)}
    sector_order: Dict[str, int] = {}
    for sym in symbols:
        sector = sector_map.get(sym.upper(), "Unknown")
        if sector not in sector_order:
            sector_order[sector] = len(sector_order)
    return sorted(
        symbols,
        key=lambda sym: (
            sector_order.get(sector_map.get(sym.upper(), "Unknown"), 10_000),
            sector_map.get(sym.upper(), "Unknown"),
            original_index[sym],
        ),
    )


def previous_weekday(d: date) -> date:
    out = d - timedelta(days=1)
    while out.weekday() >= 5:
        out -= timedelta(days=1)
    return out


def latest_completed_market_day(now: Optional[datetime] = None) -> date:
    """
    Treat today's bar as available only after 3:30 PM India time on weekdays.
    Weekends fall back to the previous weekday.
    Note: exchange holidays are not modeled here.
    """
    now_ist = now.astimezone(IST) if now is not None else datetime.now(IST)
    today_ist = now_ist.date()

    if today_ist.weekday() >= 5:
        return previous_weekday(today_ist)

    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if now_ist >= market_close:
        return today_ist
    return previous_weekday(today_ist)


def available_cutoff_dates(reports_dir: Path) -> List[date]:
    """Return all cutoff dates for which an institutional picks file exists, newest first."""
    dates: List[date] = []
    for path in reports_dir.glob("institutional_picks_*.txt"):
        tag = path.stem.replace("institutional_picks_", "").strip().lower()
        try:
            dates.append(datetime.strptime(tag, "%d%b%Y").date())
        except ValueError:
            continue
    return sorted(dates, reverse=True)

def latest_available_cutoff_date(reports_dir: Path) -> date:
    dates = available_cutoff_dates(reports_dir)
    return dates[0] if dates else (date.today() - timedelta(days=14))

# ─────────────────────────────────────────────────────────────────────────────
# Page configuration
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IP Fire Status Dashboard",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Global CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Hide Streamlit's top toolbar and collapse its space ── */
header[data-testid="stHeader"]          { display: none !important; }
div[data-testid="stDecoration"]         { display: none !important; }
#MainMenu, footer                        { display: none !important; }
.block-container {
    padding-top:    0.5rem  !important;
    padding-bottom: 0.5rem  !important;
    max-width: 100% !important;
}

/* ── Tighten element vertical gaps ── */
div[data-testid="stVerticalBlock"] > div { gap: 0.25rem; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] { padding-top: 0.5rem; }
section[data-testid="stSidebar"] .block-container { padding-top: 0.5rem !important; }

/* ── Card styles ── */
.card {
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 7px;
    border-left: 5px solid;
}
.card-ytf  { background:#e8f5e9; border-color:#00B050; }
.card-jft  { background:#fff3e0; border-color:#FF8C00; }
.card-ret  { background:#f3e5f5; border-color:#9370DB; }
.card-ext  { background:#fff8e1; border-color:#FFA500; }
.card-hext { background:#ffebee; border-color:#FF4444; }
.card-lag  { background:#f5f5f5; border-color:#C0C0C0; }

.card h4   { margin: 0 0 3px 0; font-size: 15px; }
.card p    { margin: 1px 0; font-size: 13px; color: #444; }
.card .badge {
    display: inline-block;
    border-radius: 10px;
    padding: 1px 8px;
    font-size: 11px;
    font-weight: bold;
    color: white;
    margin-bottom: 4px;
}
.badge-ytf  { background:#00B050; }
.badge-jft  { background:#FF8C00; }
.badge-ret  { background:#9370DB; }
.badge-ext  { background:#FFA500; color:#000; }
.badge-hext { background:#FF4444; }
.badge-lag  { background:#888; }

/* ── Compact metric strip ── */
.metric-row {
    display: flex; gap: 6px; flex-wrap: nowrap; margin-bottom: 8px;
}
.metric-box {
    background: white;
    border-radius: 7px;
    padding: 6px 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,.10);
    text-align: center;
    min-width: 82px;
    flex: 1;
}
.metric-box .num  { font-size: 20px; font-weight: 700; line-height: 1.2; }
.metric-box .lbl  { font-size: 10px; color: #666; margin-top: 1px; white-space: nowrap; }
.num-green  { color: #00B050; }
.num-red    { color: #FF4444; }
.num-orange { color: #FF8C00; }
.num-purple { color: #9370DB; }
.num-gold   { color: #B8860B; }
.num-gray   { color: #888; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Status palette
# ─────────────────────────────────────────────────────────────────────────────
STATUS_META = {
    "YET TO FIRE":        ("⏳", "#00B050", "ytf",  "✅ Focus Today"),
    "JUST FIRED TODAY":   ("⚡", "#FF8C00", "jft",  "⚡ Breaking out now"),
    "FIRED & RETREATING": ("🔄", "#9370DB", "ret",  "🔄 Wait for re-base"),
    "FIRED":              ("🔥", "#FFD700", "ext",  "🔥 Hold / trail stop"),
    "STEADY RUNNER":      ("📈", "#70AD47", "ytf",  "📈 Monitor"),
    "EXTENDED":           ("⚠️",  "#FFA500", "ext",  "⚠️ Raise stop"),
    "HIGHLY EXTENDED":    ("🚀", "#FF4444", "hext", "🚫 Avoid — extended"),
    "LAGGARD":            ("❌", "#C0C0C0", "lag",  "❌ Skip"),
}


# ─────────────────────────────────────────────────────────────────────────────
# Kite token loader
# ─────────────────────────────────────────────────────────────────────────────
def load_kite(token_file: str):
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        st.error("kiteconnect not installed.  Run: `pip install kiteconnect`")
        st.stop()
    tf = Path(token_file)
    if not tf.exists():
        st.error(f"Token file not found: {tf}")
        st.stop()
    values: Dict[str, str] = {}
    for line in tf.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        values[k.strip().upper()] = v.strip()
    kite = KiteConnect(api_key=values["API_KEY"])
    kite.set_access_token(values["ACCESS_TOKEN"])
    return kite


def fetch_daily_ohlcv(kite, token: int, from_dt: date, to_dt: date) -> pd.DataFrame:
    rows = kite.historical_data(
        instrument_token=token,
        from_date=datetime.combine(from_dt, datetime.min.time()),
        to_date=datetime.combine(to_dt, datetime.min.time()),
        interval="day",
        continuous=False,
        oi=False,
    )
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["date", "open", "high", "low", "close", "volume"]].dropna().sort_values("date")


# ─────────────────────────────────────────────────────────────────────────────
# Data fetching (cached — only re-fetches when inputs change)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def fetch_report_data(
    picks_file: str,
    token_file: str,
    cutoff_iso: str,
    end_iso: str,
    big_day_pct: float,
    fired_total: float,
    extended_total: float,
    laggard_total: float = 6.0,
) -> Tuple[List[dict], List[date], pd.DataFrame, Dict[str, float]]:
    """
    Returns (stocks, ref_dates, pivot_df, end_close_map).
    """
    cutoff_date = date.fromisoformat(cutoff_iso)
    end_date    = date.fromisoformat(end_iso)
    start_date  = cutoff_date + timedelta(days=1)
    fetch_from  = cutoff_date - timedelta(days=25)

    pairs = parse_picks_file(Path(picks_file))
    exchanges = sorted({e for e, _ in pairs})

    kite = load_kite(token_file)

    token_lookups: Dict[str, tuple] = {}
    for exch in exchanges:
        token_lookups[exch] = build_token_lookups(kite, exch)

    ref_dates: Optional[List[date]] = None
    symbol_data: Dict[str, Dict[date, float]] = {}
    symbol_ohlcv: Dict[str, pd.DataFrame] = {}
    end_close_map: Dict[str, float] = {}
    # symbol → external chart urls
    chart_url_map: Dict[str, Dict[str, str]] = {}

    for exch, raw_sym in pairs:
        by_upper, by_norm = token_lookups[exch]
        token, _ = resolve_token(by_upper, by_norm, raw_sym)
        if token is None:
            continue
        chart_url_map[raw_sym] = {
            "kite": (
                f"https://kite.zerodha.com/markets/ext/chart/web/ciq/"
                f"{exch}/{raw_sym}/{token}"
            ),
            "tv": (
                f"https://www.tradingview.com/chart/"
                f"?symbol={exch}%3A{raw_sym}&interval=D"
            ),
        }
        df = fetch_daily_ohlcv(kite, token, fetch_from, end_date)
        if df.empty or len(df) < 2:
            continue
        df["pct_change"] = df["close"].pct_change() * 100.0
        df["range_pct"] = ((df["high"] - df["low"]) / df["close"]).replace([float("inf"), -float("inf")], pd.NA) * 100.0
        mask = (df["date"] >= start_date) & (df["date"] <= end_date)
        df2  = df[mask]
        if df2.empty:
            continue
        last_close = df.loc[df["date"] <= end_date, "close"]
        if not last_close.empty and pd.notna(last_close.iloc[-1]):
            end_close_map[raw_sym] = round(float(last_close.iloc[-1]), 2)
        symbol_ohlcv[raw_sym] = df.copy()
        symbol_data[raw_sym] = {
            r_d: float(r_p)
            for r_d, r_p in zip(df2["date"], df2["pct_change"])
            if pd.notna(r_p)
        }
        candidates = sorted(df2["date"].tolist())
        if ref_dates is None or len(candidates) > len(ref_dates):
            ref_dates = candidates
        time.sleep(0.35)

    if not ref_dates or not symbol_data:
        return [], [], pd.DataFrame()

    # ── Never include today — its bar is still in progress ────────────────────
    latest_day = latest_completed_market_day()
    ref_dates  = [d for d in ref_dates if d <= latest_day]
    if not ref_dates:
        return [], [], pd.DataFrame()

    rows = []
    for sym, d2p in symbol_data.items():
        for d in ref_dates:
            if d in d2p:
                rows.append({"symbol": sym, "date": d, "pct_change": round(d2p[d], 2)})
    long_df = pd.DataFrame(rows)
    pivot = (long_df
             .pivot_table(index="symbol", columns="date",
                          values="pct_change", aggfunc="first")
             .sort_index()
             .reindex(columns=ref_dates))

    stocks: List[dict] = []
    for sym in pivot.index:
        daily = [float(v) for v in pivot.loc[sym].values if pd.notna(v)]
        if daily:
            st_data = classify_stock(sym, daily, big_day_pct, fired_total, extended_total,
                                     laggard_total=laggard_total)
            full_df = symbol_ohlcv.get(sym, pd.DataFrame()).copy()
            if not full_df.empty:
                full_df = full_df[full_df["date"] <= end_date].copy()
            lookback_df = full_df.tail(min(len(full_df), 15)).copy() if not full_df.empty else pd.DataFrame()
            recent2_df = lookback_df.tail(min(len(lookback_df), 2)).copy()
            recent3_df = lookback_df.tail(min(len(lookback_df), 3)).copy()
            price_muted = bool(
                not recent3_df.empty and
                (recent3_df["pct_change"].abs().max() <= max(2.5, big_day_pct * 0.6))
            )
            range_tight = bool(
                len(lookback_df) >= 6 and not recent2_df.empty and
                (recent2_df["range_pct"].mean() <= lookback_df["range_pct"].quantile(0.35))
            )
            volume_tight = bool(
                len(lookback_df) >= 6 and not recent2_df.empty and
                (recent2_df["volume"].mean() <= lookback_df["volume"].quantile(0.35))
            )
            st_data["tight_steady_setup"] = bool(
                st_data["status"] == "STEADY RUNNER"
                and st_data.get("n_big", 0) >= 1
                and price_muted
                and range_tight
                and volume_tight
            )
            st_data["tight_setup_range_pct"] = round(float(recent2_df["range_pct"].mean()), 2) if not recent2_df.empty else None
            st_data["tight_setup_volume_avg"] = round(float(recent2_df["volume"].mean()), 0) if not recent2_df.empty else None
            urls = chart_url_map.get(sym, {})
            st_data["chart_url_kite"] = urls.get("kite", "")
            st_data["chart_url_tv"]   = urls.get("tv",   "")
            stocks.append(st_data)

    return stocks, ref_dates, pivot, end_close_map


# ─────────────────────────────────────────────────────────────────────────────
# Positions + holdings from Kite  (short TTL — prices change live)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def fetch_kite_holdings(token_file: str) -> pd.DataFrame:
    """
    EOD holdings snapshot from kite.holdings().

    Includes T1 shares (bought today, settling tomorrow) because you own
    them — Kite reports them separately as t1_quantity.
    Total holding = quantity (settled) + t1_quantity (pending settlement).

    Skips any row where total holding <= 0.
    """
    kite = load_kite(token_file)
    rows: List[dict] = []
    try:
        for h in kite.holdings():
            settled  = h.get("quantity",    0)
            t1       = h.get("t1_quantity", 0)
            total    = settled + t1
            if total <= 0:
                continue
            last  = h.get("last_price",    0.0)
            avg   = h.get("average_price", 0.0)
            rows.append({
                "symbol":      h["tradingsymbol"],
                "exchange":    h.get("exchange", "NSE"),
                "qty_settled": settled,
                "qty_t1":      t1,
                "qty_total":   total,
                "avg_price":   avg,
                "last_price":  last,
                "invested":    round(avg  * total, 2),
                "cur_value":   round(last * total, 2),
                "pnl":         round((last - avg) * total, 2),
                "pnl_pct":     round((last - avg) / avg * 100, 2) if avg else 0.0,
            })
    except Exception as exc:
        st.warning(f"Could not fetch holdings: {exc}")
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(show_spinner=False)
def fetch_tradebook_history(tradebook_dir: str) -> Tuple[pd.DataFrame, Optional[pd.Timestamp], List[str]]:
    """
    Load local Zerodha tradebook CSV exports and deduplicate by trade_id.
    This supports a mix of:
    - one-time historical Console backfill files
    - rolling yearly tradebook CSVs updated from the daily Kite API capture
    These files give us a historical view for exited positions, which Kite's
    live holdings endpoint cannot provide.
    """
    base = Path(tradebook_dir)
    paths = sorted(base.glob(TRADEBOOK_GLOB))
    if not paths:
        return pd.DataFrame(), None, []

    frames: List[pd.DataFrame] = []
    used_files: List[str] = []
    for path in paths:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty or "symbol" not in df.columns or "trade_date" not in df.columns:
            continue
        df["source_file"] = path.name
        frames.append(df)
        used_files.append(path.name)

    if not frames:
        return pd.DataFrame(), None, []

    trades = pd.concat(frames, ignore_index=True)
    trades["symbol"] = trades["symbol"].astype(str).str.upper().str.strip()
    trades["trade_type"] = trades["trade_type"].astype(str).str.lower().str.strip()
    trades["exchange"] = trades.get("exchange", "NSE")
    trades["quantity"] = pd.to_numeric(trades["quantity"], errors="coerce").fillna(0.0)
    trades["price"] = pd.to_numeric(trades["price"], errors="coerce").fillna(0.0)
    trade_date_text = trades["trade_date"].astype(str).str.strip()
    iso_mask = trade_date_text.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    trades["trade_date"] = pd.NaT
    if iso_mask.any():
        trades.loc[iso_mask, "trade_date"] = pd.to_datetime(
            trade_date_text.loc[iso_mask], format="%Y-%m-%d", errors="coerce"
        )
    if (~iso_mask).any():
        trades.loc[~iso_mask, "trade_date"] = pd.to_datetime(
            trade_date_text.loc[~iso_mask], dayfirst=True, errors="coerce"
        )
    trades["order_execution_time"] = pd.to_datetime(
        trades.get("order_execution_time"), errors="coerce"
    )
    trades = trades.dropna(subset=["trade_date"])
    trades = trades[trades["trade_type"].isin(["buy", "sell"])].copy()
    if trades.empty:
        return pd.DataFrame(), None, used_files

    if "trade_id" in trades.columns:
        trades["trade_id"] = trades["trade_id"].astype(str).str.strip()
    else:
        trades["trade_id"] = (
            trades["symbol"].astype(str) + "|" +
            trades["trade_date"].dt.strftime("%Y-%m-%d") + "|" +
            trades["trade_type"].astype(str) + "|" +
            trades["quantity"].astype(str) + "|" +
            trades["price"].astype(str)
        )

    trades = trades.sort_values(
        by=["trade_date", "order_execution_time", "source_file", "trade_id"],
        kind="stable",
    ).drop_duplicates(subset=["trade_id"], keep="last")

    trades["signed_qty"] = trades["quantity"].where(
        trades["trade_type"].eq("buy"), -trades["quantity"]
    )
    trades["gross_value"] = trades["quantity"] * trades["price"]
    history_max = trades["trade_date"].max()
    return trades, history_max, used_files


@st.cache_data(show_spinner=False)
def fetch_chart_board_data(
    picks_file: str,
    token_file: str,
    cutoff_iso: str,
    end_iso: str,
    symbols_csv: str,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch daily OHLC for a focused set of symbols so we can render a separate
    chart-board view frozen to the selected end date.
    """
    cutoff_date = date.fromisoformat(cutoff_iso)
    end_date = date.fromisoformat(end_iso)
    chart_from = max(cutoff_date - timedelta(days=120), date(2000, 1, 1))
    requested = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    if not requested:
        return {}

    pairs = parse_picks_file(Path(picks_file))
    pair_map = {sym.upper(): exch for exch, sym in pairs}
    exchanges = sorted({pair_map.get(sym, "NSE") for sym in requested})

    kite = load_kite(token_file)
    token_lookups: Dict[str, tuple] = {}
    for exch in exchanges:
        token_lookups[exch] = build_token_lookups(kite, exch)

    out: Dict[str, pd.DataFrame] = {}
    for sym in requested:
        exch = pair_map.get(sym, "NSE")
        by_upper, by_norm = token_lookups[exch]
        token, _ = resolve_token(by_upper, by_norm, sym)
        if token is None:
            continue
        rows = kite.historical_data(
            instrument_token=int(token),
            from_date=chart_from,
            to_date=end_date,
            interval="day",
            continuous=False,
            oi=False,
        )
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = (
            df[["date", "open", "high", "low", "close", "volume"]]
            .dropna(subset=["date", "open", "high", "low", "close"])
            .sort_values("date")
        )
        if not df.empty:
            out[sym] = df
        time.sleep(0.2)
    return out

def build_exited_watchlist_positions(
    trades_df: pd.DataFrame,
    watchlist_syms: Set[str],
    current_watchlist_symbols: Set[str],
    cutoff_dt: date,
    end_dt: date,
    wl_status_map: Dict[str, str],
    wl_total_map: Dict[str, float],
    wl_today_map: Dict[str, float],
) -> pd.DataFrame:
    """
    Identify watchlist names that had a real position during the chosen window
    and were fully exited by the end of that window.
    """
    if trades_df.empty or not watchlist_syms:
        return pd.DataFrame()

    cutoff_ts = pd.Timestamp(cutoff_dt)
    end_ts = pd.Timestamp(end_dt)
    scope = trades_df[
        trades_df["symbol"].isin(watchlist_syms) &
        (trades_df["trade_date"] <= end_ts)
    ].copy()
    if scope.empty:
        return pd.DataFrame()

    rows: List[dict] = []
    scope = scope.sort_values(["symbol", "trade_date", "order_execution_time", "trade_id"], kind="stable")

    for sym, grp in scope.groupby("symbol", sort=False):
        before_qty = float(grp.loc[grp["trade_date"] < cutoff_ts, "signed_qty"].sum())
        window = grp[(grp["trade_date"] >= cutoff_ts) & (grp["trade_date"] <= end_ts)].copy()
        if window.empty:
            continue

        window["qty_before_trade"] = before_qty + window["signed_qty"].cumsum() - window["signed_qty"]
        window["qty_after_trade"] = before_qty + window["signed_qty"].cumsum()

        had_position = before_qty > 0 or bool((window["qty_after_trade"] > 0).any())
        exit_rows = window[
            (window["qty_before_trade"] > 0) &
            (window["qty_after_trade"] <= 0) &
            (window["trade_type"] == "sell")
        ]
        if not had_position or exit_rows.empty:
            continue
        for _, exit_row in exit_rows.iterrows():
            last_sell_price = float(exit_row["price"]) if pd.notna(exit_row["price"]) else None
            rows.append({
                "Symbol": sym,
                "Exit Date": exit_row["trade_date"].date(),
                "Exit Px": round(last_sell_price, 2) if last_sell_price is not None else None,
                "WL Total%": float(wl_total_map.get(sym)) if pd.notna(wl_total_map.get(sym)) else None,
                "WL Today%": float(wl_today_map.get(sym)) if pd.notna(wl_today_map.get(sym)) else None,
            })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["Exit Date", "WL Total%"], ascending=[False, False], kind="stable")


def build_holdings_snapshot_from_trades(
    trades_df: pd.DataFrame,
    end_dt: date,
    end_close_map: Dict[str, float],
) -> pd.DataFrame:
    """
    Reconstruct open positions as of the selected end date using the tradebook,
    so the holdings view stays consistent with the chosen historical window.
    """
    if trades_df.empty:
        return pd.DataFrame(columns=[
            "symbol", "qty_total", "avg_price", "last_price",
            "invested", "cur_value", "pnl", "pnl_pct"
        ])

    end_ts = pd.Timestamp(end_dt)
    scope = trades_df[trades_df["trade_date"] <= end_ts].copy()
    if scope.empty:
        return pd.DataFrame(columns=[
            "symbol", "qty_total", "avg_price", "last_price",
            "invested", "cur_value", "pnl", "pnl_pct"
        ])

    rows: List[dict] = []
    scope = scope.sort_values(["symbol", "trade_date", "order_execution_time", "trade_id"], kind="stable")
    for sym, grp in scope.groupby("symbol", sort=False):
        qty = 0.0
        avg_cost = 0.0
        for _, row in grp.iterrows():
            trade_qty = float(row["quantity"])
            price = float(row["price"])
            trade_type = str(row["trade_type"])
            if trade_type == "buy":
                total_cost = qty * avg_cost + trade_qty * price
                qty += trade_qty
                avg_cost = total_cost / qty if qty > 0 else 0.0
            elif trade_type == "sell":
                sell_qty = min(trade_qty, qty) if qty > 0 else 0.0
                qty = max(qty - sell_qty, 0.0)
                if qty == 0:
                    avg_cost = 0.0
        if qty <= 0:
            continue
        last_price = end_close_map.get(sym)
        invested = round(avg_cost * qty, 2)
        cur_value = round(float(last_price) * qty, 2) if last_price is not None else invested
        pnl = round(cur_value - invested, 2) if last_price is not None else None
        pnl_pct = (
            round((float(last_price) - avg_cost) / avg_cost * 100.0, 2)
            if last_price is not None and avg_cost
            else None
        )
        rows.append({
            "symbol": sym,
            "qty_total": round(qty, 2),
            "avg_price": round(avg_cost, 2),
            "last_price": round(float(last_price), 2) if last_price is not None else None,
            "invested": invested,
            "cur_value": cur_value,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
        })

    return pd.DataFrame(rows)


def build_position_timeline(
    trades_df: pd.DataFrame,
    watchlist_syms: Set[str],
    ref_dates: List[date],
    current_holdings_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a day-level positioned timeline for watchlist names across the
    selected trading dates. A day is marked as positioned if the stock was
    held at any point during that trading day, even if it was fully exited
    before the close. This is more aligned with campaign timing review than
    strict end-of-day holdings.
    """
    if not ref_dates or not watchlist_syms:
        return pd.DataFrame()

    ref_ts = [pd.Timestamp(d) for d in ref_dates]
    timeline: List[dict] = []
    holdings_qty_map = {}
    if not current_holdings_df.empty:
        holdings_qty_map = {
            str(r["symbol"]).upper(): int(r["qty_total"])
            for _, r in current_holdings_df.iterrows()
            if pd.notna(r.get("symbol"))
        }

    scope = trades_df[trades_df["symbol"].isin(watchlist_syms)].copy() if not trades_df.empty else pd.DataFrame()
    if not scope.empty:
        scope = scope.sort_values(["symbol", "trade_date", "order_execution_time", "trade_id"], kind="stable")

    for sym in sorted(watchlist_syms):
        grp = scope[scope["symbol"] == sym].copy() if not scope.empty else pd.DataFrame()
        if grp.empty:
            current_qty = holdings_qty_map.get(sym, 0)
            row = {"symbol": sym}
            for dt in ref_ts:
                row[dt.date()] = 0
            if current_qty > 0:
                row[ref_ts[-1].date()] = 1
            timeline.append(row)
            continue

        qty_by_date = grp.groupby("trade_date", as_index=True)["signed_qty"].sum().sort_index()
        before_first = float(qty_by_date[qty_by_date.index < ref_ts[0]].sum())
        running = before_first
        row = {"symbol": sym}
        for dt in ref_ts:
            day_trades = grp[grp["trade_date"] == dt].copy()
            positioned_today = running > 0
            if not day_trades.empty:
                for _, trade in day_trades.iterrows():
                    running += float(trade["signed_qty"])
                    if running > 0:
                        positioned_today = True
            row[dt.date()] = 1 if positioned_today else 0

        if holdings_qty_map.get(sym, 0) > 0:
            row[ref_ts[-1].date()] = 1
        timeline.append(row)

    if not timeline:
        return pd.DataFrame()
    cols = ["symbol"] + ref_dates
    return pd.DataFrame(timeline).reindex(columns=cols)


def build_campaign_return_map(trades_df: pd.DataFrame, end_dt: date) -> Dict[Tuple[str, date], float]:
    """
    Return realized campaign return % keyed by (symbol, exit_date).
    A campaign starts when quantity moves from 0 to positive and ends when it
    comes back to 0.
    """
    if trades_df.empty:
        return {}

    end_ts = pd.Timestamp(end_dt)
    scope = trades_df[trades_df["trade_date"] <= end_ts].copy()
    if scope.empty:
        return {}

    out: Dict[Tuple[str, date], float] = {}
    scope = scope.sort_values(["symbol", "trade_date", "order_execution_time", "trade_id"], kind="stable")
    for sym, grp in scope.groupby("symbol", sort=False):
        qty = 0.0
        avg_cost = 0.0
        campaign_buy_value = 0.0
        campaign_realized_pnl = 0.0
        for _, row in grp.iterrows():
            trade_qty = float(row["quantity"])
            price = float(row["price"])
            trade_type = str(row["trade_type"])
            trade_date = row["trade_date"].date()
            if trade_type == "buy":
                if qty <= 0:
                    qty = 0.0
                    avg_cost = 0.0
                    campaign_buy_value = 0.0
                    campaign_realized_pnl = 0.0
                total_cost = qty * avg_cost + trade_qty * price
                qty += trade_qty
                avg_cost = total_cost / qty if qty > 0 else 0.0
                campaign_buy_value += trade_qty * price
            elif trade_type == "sell":
                sell_qty = min(trade_qty, qty) if qty > 0 else 0.0
                campaign_realized_pnl += (price - avg_cost) * sell_qty
                qty = max(qty - trade_qty, 0.0)
                if qty == 0 and campaign_buy_value > 0:
                    out[(sym, trade_date)] = round(campaign_realized_pnl / campaign_buy_value * 100.0, 2)
                    avg_cost = 0.0
                    campaign_buy_value = 0.0
                    campaign_realized_pnl = 0.0
    return out


def build_campaign_meta_maps(
    trades_df: pd.DataFrame,
    end_dt: date,
) -> Tuple[Dict[Tuple[str, date], dict], Dict[str, dict]]:
    """
    Closed campaigns keyed by (symbol, exit_date), and open campaign metadata
    keyed by symbol.
    """
    if trades_df.empty:
        return {}, {}

    end_ts = pd.Timestamp(end_dt)
    scope = trades_df[trades_df["trade_date"] <= end_ts].copy()
    if scope.empty:
        return {}, {}

    closed: Dict[Tuple[str, date], dict] = {}
    open_meta: Dict[str, dict] = {}
    scope = scope.sort_values(["symbol", "trade_date", "order_execution_time", "trade_id"], kind="stable")

    for sym, grp in scope.groupby("symbol", sort=False):
        qty = 0.0
        entry_date: Optional[date] = None
        for _, row in grp.iterrows():
            trade_qty = float(row["quantity"])
            trade_type = str(row["trade_type"])
            trade_date = row["trade_date"].date()
            if trade_type == "buy":
                if qty <= 0:
                    qty = 0.0
                    entry_date = trade_date
                qty += trade_qty
            elif trade_type == "sell":
                qty = max(qty - trade_qty, 0.0)
                if qty == 0 and entry_date is not None:
                    closed[(sym, trade_date)] = {
                        "Entry Date": entry_date,
                        "Hold Days": (trade_date - entry_date).days + 1,
                    }
                    entry_date = None
        if qty > 0 and entry_date is not None:
            open_meta[sym] = {
                "Entry Date": entry_date,
                "Hold Days": (end_dt - entry_date).days + 1,
            }
    return closed, open_meta


def filter_trades_to_recent_campaigns(
    trades_df: pd.DataFrame,
    eligible_from_dt: date,
    end_dt: date,
) -> pd.DataFrame:
    """
    Keep only trades belonging to campaigns whose entry date is on or after
    eligible_from_dt. This lets the positioning tab ignore older carried
    positions even if they remain open during the selected window.
    """
    if trades_df.empty:
        return trades_df.copy()

    end_ts = pd.Timestamp(end_dt)
    scope = trades_df[trades_df["trade_date"] <= end_ts].copy()
    if scope.empty:
        return scope

    scope = scope.sort_values(["symbol", "trade_date", "order_execution_time", "trade_id"], kind="stable")
    entry_map: Dict[int, Optional[date]] = {}

    for sym, grp in scope.groupby("symbol", sort=False):
        qty = 0.0
        current_entry: Optional[date] = None
        for idx, row in grp.iterrows():
            trade_qty = float(row["quantity"])
            trade_type = str(row["trade_type"])
            trade_date = row["trade_date"].date()

            if trade_type == "buy":
                if qty <= 0:
                    current_entry = trade_date
                qty += trade_qty
                entry_map[idx] = current_entry
            elif trade_type == "sell":
                entry_map[idx] = current_entry
                qty = max(qty - trade_qty, 0.0)
                if qty == 0:
                    current_entry = None
            else:
                entry_map[idx] = None

    filtered = scope.copy()
    filtered["campaign_entry_date"] = filtered.index.map(entry_map.get)
    filtered = filtered[
        filtered["campaign_entry_date"].notna() &
        (filtered["campaign_entry_date"] >= eligible_from_dt)
    ].copy()
    return filtered.drop(columns=["campaign_entry_date"])


def calc_capture_pct(my_ret_pct: Optional[float], wl_move_pct: Optional[float]) -> Optional[float]:
    if my_ret_pct is None or wl_move_pct is None:
        return None
    if abs(float(wl_move_pct)) < 1e-9:
        return None
    return round(float(my_ret_pct) / float(wl_move_pct) * 100.0, 1)


def reconstruct_daily_closes(
    sym: str,
    pivot: pd.DataFrame,
    ref_dates: List[date],
    last_price: float,
) -> List[float]:
    """
    Work backwards from last_price using daily % changes to reconstruct
    the approximate close price on each trading day in ref_dates.
    """
    if sym not in pivot.index:
        return []
    pcts = [float(pivot.loc[sym, d]) if d in pivot.columns and pd.notna(pivot.loc[sym, d]) else 0.0
            for d in ref_dates]
    closes: List[float] = [0.0] * len(pcts)
    closes[-1] = last_price
    for i in range(len(pcts) - 2, -1, -1):
        divisor = 1.0 + pcts[i + 1] / 100.0
        closes[i] = closes[i + 1] / divisor if divisor != 0 else closes[i + 1]
    return closes


def render_focus_board_lightweight(
    board_data: Dict[str, pd.DataFrame],
    symbols: List[str],
    cutoff_dt: date,
    end_dt: date,
) -> None:
    sector_map = load_local_sector_map(str(SECTOR_CACHE_FILE))
    ordered_symbols = order_symbols_by_sector(symbols, sector_map)
    payload: Dict[str, dict] = {}
    for sym in ordered_symbols:
        df = board_data.get(sym)
        if df is None or df.empty:
            payload[sym] = {
                "has_data": False,
                "sector": sector_map.get(sym.upper(), "Unknown"),
            }
            continue

        df = df.copy().sort_values("date")
        df["date"] = pd.to_datetime(df["date"])
        df["time"] = df["date"].dt.strftime("%Y-%m-%d")
        df["ema8"] = df["close"].ewm(span=8, adjust=False).mean()
        df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
        df["dma50"] = df["close"].rolling(50, min_periods=1).mean()
        df["pct_change"] = df["close"].pct_change() * 100.0

        cutoff_rows = df[df["date"].dt.date <= cutoff_dt]
        cutoff_close = float(cutoff_rows["close"].iloc[-1]) if not cutoff_rows.empty else None
        end_close = float(df["close"].iloc[-1])
        move_pct = None
        if cutoff_close and cutoff_close != 0:
            move_pct = round((end_close / cutoff_close - 1.0) * 100.0, 2)

        payload[sym] = {
            "has_data": True,
            "sector": sector_map.get(sym.upper(), "Unknown"),
            "end_close": round(end_close, 2),
            "move_pct": move_pct,
            "candles": [
                {
                    "time": row.time,
                    "open": round(float(row.open), 2),
                    "high": round(float(row.high), 2),
                    "low": round(float(row.low), 2),
                    "close": round(float(row.close), 2),
                    "pct_change": round(float(row.pct_change), 2) if pd.notna(row.pct_change) else None,
                }
                for row in df.itertuples(index=False)
            ],
            "volume": [
                {
                    "time": row.time,
                    "value": int(float(row.volume)),
                    "color": "#58b65b" if float(row.close) >= float(row.open) else "#ef6a6a",
                }
                for row in df.itertuples(index=False)
            ],
            "ema8": [
                {"time": row.time, "value": round(float(row.ema8), 2)}
                for row in df.itertuples(index=False)
            ],
            "ema21": [
                {"time": row.time, "value": round(float(row.ema21), 2)}
                for row in df.itertuples(index=False)
            ],
            "dma50": [
                {"time": row.time, "value": round(float(row.dma50), 2)}
                for row in df.itertuples(index=False)
            ],
        }

    rows = max(1, math.ceil(len(ordered_symbols) / 3))
    frame_height = rows * 395 + 72
    cards_html = []
    for sym in ordered_symbols:
        cards_html.append(
            f"""
            <section class="focus-card">
              <div class="focus-head">
                <div>
                  <div class="focus-title">{sym}</div>
                  <div class="focus-sector" id="sector-{sym}"></div>
                </div>
                <div class="focus-actions">
                  <div class="focus-meta" id="meta-{sym}"></div>
                  <button class="zoom-btn" type="button" onclick="openZoom('{sym}')">⛶</button>
                </div>
              </div>
              <div class="chart-wrap">
                <div class="chart-tooltip" id="tooltip-{sym}"></div>
                <div class="focus-chart" id="chart-{sym}" onclick="openZoom('{sym}')"></div>
              </div>
            </section>
            """
        )

    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
      <style>
        body {{
          margin: 0;
          background: #171a1f;
          font-family: "Segoe UI", Tahoma, sans-serif;
          color: #e5e7eb;
        }}
        .board {{
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 18px;
          padding: 4px 2px 8px 2px;
        }}
        .focus-card {{
          border: 1px solid #303744;
          border-radius: 10px;
          background: #1f2329;
          padding: 10px 10px 6px 10px;
          box-shadow: 0 1px 2px rgba(0, 0, 0, 0.22);
        }}
        .focus-head {{
          display: flex;
          align-items: baseline;
          justify-content: space-between;
          gap: 10px;
          margin-bottom: 8px;
        }}
        .focus-title {{
          font-size: 14px;
          font-weight: 700;
          letter-spacing: 0.02em;
        }}
        .focus-sector {{
          font-size: 11px;
          color: #94a3b8;
          margin-top: 2px;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }}
        .focus-actions {{
          display: flex;
          align-items: center;
          gap: 8px;
        }}
        .focus-meta {{
          font-size: 11px;
          color: #cbd5e1;
          white-space: nowrap;
        }}
        .focus-meta.positive {{
          color: #6ccf6b;
          font-weight: 600;
        }}
        .focus-meta.negative {{
          color: #ff6b6b;
          font-weight: 600;
        }}
        .focus-chart {{
          width: 100%;
          height: 300px;
          cursor: zoom-in;
        }}
        .focus-empty {{
          height: 300px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #94a3b8;
          font-size: 12px;
          background: #171a1f;
          border-radius: 8px;
        }}
        .zoom-btn {{
          border: 1px solid #3a4250;
          background: #1f2329;
          color: #cbd5e1;
          border-radius: 6px;
          width: 28px;
          height: 28px;
          font-size: 14px;
          cursor: pointer;
        }}
        .zoom-btn:hover {{
          background: #262b33;
          border-color: #64748b;
        }}
        .zoom-overlay {{
          position: fixed;
          inset: 0;
          background: rgba(15, 23, 42, 0.55);
          display: none;
          align-items: flex-start;
          justify-content: center;
          z-index: 9999;
          padding: 14px 24px 24px 24px;
        }}
        .zoom-overlay.open {{
          display: flex;
        }}
        .zoom-card {{
          width: min(1200px, 96vw);
          height: min(760px, calc(100vh - 28px));
          background: #1f2329;
          border-radius: 14px;
          box-shadow: 0 20px 50px rgba(15, 23, 42, 0.25);
          display: flex;
          flex-direction: column;
          overflow: hidden;
        }}
        .zoom-head {{
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 10px;
          padding: 12px 16px;
          border-bottom: 1px solid #303744;
        }}
        .zoom-title {{
          font-size: 18px;
          font-weight: 700;
          color: #f8fafc;
        }}
        .zoom-subtitle {{
          font-size: 12px;
          color: #94a3b8;
          margin-top: 2px;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }}
        .zoom-meta {{
          font-size: 12px;
          color: #cbd5e1;
        }}
        .zoom-close {{
          border: 1px solid #3a4250;
          background: #1f2329;
          color: #cbd5e1;
          border-radius: 8px;
          padding: 6px 10px;
          font-size: 13px;
          cursor: pointer;
        }}
        .zoom-close:hover {{
          background: #262b33;
        }}
        .chart-wrap {{
          position: relative;
          width: 100%;
          height: 100%;
        }}
        .chart-tooltip {{
          position: absolute;
          top: 8px;
          left: 8px;
          z-index: 20;
          background: rgba(15, 23, 42, 0.92);
          color: #e5e7eb;
          border: 1px solid #334155;
          border-radius: 8px;
          padding: 6px 8px;
          font-size: 11px;
          line-height: 1.35;
          pointer-events: none;
          min-width: 180px;
          display: none;
          box-shadow: 0 6px 18px rgba(0, 0, 0, 0.22);
        }}
        .chart-tooltip.visible {{
          display: block;
        }}
        .chart-tooltip .tt-date {{
          color: #cbd5e1;
          font-weight: 600;
          margin-bottom: 2px;
        }}
        .chart-tooltip .tt-pos {{
          color: #6ccf6b;
          font-weight: 600;
        }}
        .chart-tooltip .tt-neg {{
          color: #ff6b6b;
          font-weight: 600;
        }}
        .zoom-chart {{
          width: 100%;
          height: 100%;
          min-height: 0;
        }}
        @media (max-width: 1200px) {{
          .board {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }}
        }}
        @media (max-width: 760px) {{
          .board {{
            grid-template-columns: 1fr;
          }}
        }}
      </style>
    </head>
    <body>
      <div class="board">
        {''.join(cards_html)}
      </div>
      <div class="zoom-overlay" id="zoom-overlay" onclick="closeZoomOnBackdrop(event)">
        <div class="zoom-card">
          <div class="zoom-head">
            <div>
              <div class="zoom-title" id="zoom-title"></div>
              <div class="zoom-subtitle" id="zoom-sector"></div>
            </div>
            <div style="display:flex;align-items:center;gap:12px;">
              <div class="zoom-meta" id="zoom-meta"></div>
              <button class="zoom-close" type="button" onclick="closeZoom()">Close</button>
            </div>
          </div>
          <div class="chart-wrap" style="flex:1;min-height:0;">
            <div class="chart-tooltip" id="tooltip-zoom"></div>
            <div class="zoom-chart" id="zoom-chart"></div>
          </div>
        </div>
      </div>
      <script>
        const payload = {json.dumps(payload)};
        const symbols = {json.dumps(ordered_symbols)};
        let zoomChart = null;
        let zoomResizeObserver = null;

        function formatVolume(value) {{
          if (value === null || value === undefined) return 'n/a';
          const abs = Math.abs(Number(value));
          if (abs >= 10000000) return (value / 10000000).toFixed(2) + 'Cr';
          if (abs >= 100000) return (value / 100000).toFixed(2) + 'L';
          if (abs >= 1000) return (value / 1000).toFixed(1) + 'K';
          return String(Math.round(value));
        }}

        function setMeta(metaEl, card) {{
          const moveText = card.move_pct === null ? 'n/a' : ((card.move_pct > 0 ? '+' : '') + card.move_pct.toFixed(2) + '%');
          metaEl.textContent = 'Close ' + card.end_close.toFixed(2) + ' | ' + moveText;
          metaEl.classList.toggle('positive', card.move_pct !== null && card.move_pct > 0);
          metaEl.classList.toggle('negative', card.move_pct !== null && card.move_pct < 0);
        }}

        function bindTooltip(chart, card, tooltipEl) {{
          const byTime = new Map(card.candles.map(row => [row.time, row]));
          const byVolumeTime = new Map((card.volume || []).map(row => [row.time, row]));
          chart.subscribeCrosshairMove((param) => {{
            if (!tooltipEl) return;
            if (!param || !param.time || !param.point || param.point.x < 0 || param.point.y < 0) {{
              tooltipEl.classList.remove('visible');
              return;
            }}
            const key = typeof param.time === 'string'
              ? param.time
              : `${{param.time.year}}-${{String(param.time.month).padStart(2, '0')}}-${{String(param.time.day).padStart(2, '0')}}`;
            const row = byTime.get(key);
            if (!row) {{
              tooltipEl.classList.remove('visible');
              return;
            }}
            const volumeRow = byVolumeTime.get(key);
            const pct = row.pct_change;
            const pctClass = pct === null || pct === undefined ? '' : (pct >= 0 ? 'tt-pos' : 'tt-neg');
            const pctText = pct === null || pct === undefined ? 'n/a' : `${{pct >= 0 ? '+' : ''}}${{pct.toFixed(2)}}%`;
            tooltipEl.innerHTML = `
              <div class="tt-date">${{key}}</div>
              <div>O ${{row.open.toFixed(2)}}  H ${{row.high.toFixed(2)}}</div>
              <div>L ${{row.low.toFixed(2)}}  C ${{row.close.toFixed(2)}}</div>
              <div>Vol ${{formatVolume(volumeRow ? volumeRow.value : null)}}  <span class="${{pctClass}}">${{pctText}}</span></div>
            `;
            tooltipEl.classList.add('visible');
          }});
        }}

        function drawChart(chartEl, card, tooltipEl = null, isZoom = false) {{
          const chart = LightweightCharts.createChart(chartEl, {{
            width: Math.max(chartEl.clientWidth, 400),
            height: Math.max(chartEl.clientHeight, isZoom ? 520 : 300),
            layout: {{
              background: {{ type: 'solid', color: '#1f2329' }},
              textColor: '#94a3b8',
              fontFamily: 'Segoe UI, Tahoma, sans-serif',
              fontSize: 11,
            }},
            grid: {{
              vertLines: {{ color: '#303744' }},
              horzLines: {{ color: '#303744' }},
            }},
            rightPriceScale: {{
              borderColor: '#3a4250',
              scaleMargins: isZoom ? {{ top: 0.06, bottom: 0.22 }} : {{ top: 0.08, bottom: 0.24 }},
            }},
            leftPriceScale: {{
              visible: false,
            }},
            timeScale: {{
              borderColor: '#3a4250',
              timeVisible: false,
              secondsVisible: false,
              fixLeftEdge: true,
              fixRightEdge: true,
            }},
            crosshair: {{
              mode: LightweightCharts.CrosshairMode.Normal,
            }},
            handleScroll: isZoom,
            handleScale: isZoom,
          }});

          const candleSeries = chart.addCandlestickSeries({{
            upColor: '#58b65b',
            downColor: '#ef6a6a',
            borderUpColor: '#58b65b',
            borderDownColor: '#ef6a6a',
            wickUpColor: '#58b65b',
            wickDownColor: '#ef6a6a',
            priceLineVisible: false,
            lastValueVisible: false,
          }});
          candleSeries.setData(card.candles);

          const volumeSeries = chart.addHistogramSeries({{
            priceFormat: {{
              type: 'volume',
            }},
            priceScaleId: '',
            lastValueVisible: false,
            priceLineVisible: false,
          }});
          volumeSeries.priceScale().applyOptions({{
            scaleMargins: isZoom ? {{ top: 0.82, bottom: 0.02 }} : {{ top: 0.80, bottom: 0.02 }},
          }});
          volumeSeries.setData(card.volume);

          const ema8Series = chart.addLineSeries({{
            color: '#f59e0b',
            lineWidth: 2,
            lastValueVisible: false,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
          }});
          ema8Series.setData(card.ema8);

          const ema21Series = chart.addLineSeries({{
            color: '#2563eb',
            lineWidth: 2,
            lastValueVisible: false,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
          }});
          ema21Series.setData(card.ema21);

          const dma50Series = chart.addLineSeries({{
            color: '#16a34a',
            lineWidth: 2,
            lastValueVisible: false,
            priceLineVisible: false,
            crosshairMarkerVisible: false,
          }});
          dma50Series.setData(card.dma50);

          bindTooltip(chart, card, tooltipEl);
          chart.timeScale().fitContent();
          return chart;
        }}

        function applyChart(sym) {{
          const card = payload[sym];
          const chartEl = document.getElementById('chart-' + sym);
          const tooltipEl = document.getElementById('tooltip-' + sym);
          const metaEl = document.getElementById('meta-' + sym);
          const sectorEl = document.getElementById('sector-' + sym);
          sectorEl.textContent = (card && card.sector) ? card.sector : 'Unknown';
          if (!card || !card.has_data) {{
            chartEl.innerHTML = '<div class="focus-empty">No chart data</div>';
            metaEl.textContent = '';
            return;
          }}

          setMeta(metaEl, card);
          const chart = drawChart(chartEl, card, tooltipEl, false);
          new ResizeObserver(() => {{
            chart.applyOptions({{
              width: chartEl.clientWidth,
              height: chartEl.clientHeight,
            }});
            chart.timeScale().fitContent();
          }}).observe(chartEl);
        }}

        function openZoom(sym) {{
          const card = payload[sym];
          if (!card || !card.has_data) return;
          const overlay = document.getElementById('zoom-overlay');
          const zoomEl = document.getElementById('zoom-chart');
          const tooltipEl = document.getElementById('tooltip-zoom');
          document.getElementById('zoom-title').textContent = sym;
          document.getElementById('zoom-sector').textContent = card.sector || 'Unknown';
          setMeta(document.getElementById('zoom-meta'), card);
          zoomEl.innerHTML = '';
          tooltipEl.classList.remove('visible');
          overlay.classList.add('open');
          window.scrollTo({{ top: 0, behavior: 'smooth' }});
          if (zoomChart) {{
            zoomChart.remove();
            zoomChart = null;
          }}
          if (zoomResizeObserver) {{
            zoomResizeObserver.disconnect();
            zoomResizeObserver = null;
          }}
          requestAnimationFrame(() => {{
            zoomChart = drawChart(zoomEl, card, tooltipEl, true);
            zoomResizeObserver = new ResizeObserver(() => {{
              if (!zoomChart) return;
              zoomChart.applyOptions({{
                width: Math.max(zoomEl.clientWidth, 400),
                height: Math.max(zoomEl.clientHeight, 520),
              }});
              zoomChart.timeScale().fitContent();
            }});
            zoomResizeObserver.observe(zoomEl);
          }});
        }}

        function closeZoom() {{
          const overlay = document.getElementById('zoom-overlay');
          overlay.classList.remove('open');
          if (zoomResizeObserver) {{
            zoomResizeObserver.disconnect();
            zoomResizeObserver = null;
          }}
          if (zoomChart) {{
            zoomChart.remove();
            zoomChart = null;
          }}
        }}

        function closeZoomOnBackdrop(event) {{
          if (event.target && event.target.id === 'zoom-overlay') {{
            closeZoom();
          }}
        }}

        symbols.forEach(applyChart);
        document.addEventListener('keydown', (event) => {{
          if (event.key === 'Escape') {{
            closeZoom();
          }}
        }});
      </script>
    </body>
    </html>
    """
    components.html(html, height=frame_height, scrolling=True)


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar — inputs
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Session-state for date pickers
# The end-date widget has NO key — we manage its value entirely via _end_val.
# This avoids the "cannot modify after widget is instantiated" error because
# Streamlit never "owns" that widget in session_state.
# Buttons update _end_val and rerun; the widget always renders from _end_val.
# ─────────────────────────────────────────────────────────────────────────────
_latest_market_day = latest_completed_market_day()


def _parse_iso_or_default(raw: object, fallback: date) -> date:
    try:
        return date.fromisoformat(str(raw))
    except Exception:
        return fallback


_qp_view = str(st.query_params.get("view", "")).strip().lower()
if _qp_view == "focus_board":
    _default_cutoff = latest_available_cutoff_date(REPORTS_DIR)
    _cutoff_q = _parse_iso_or_default(st.query_params.get("cutoff"), _default_cutoff)
    _end_q = _parse_iso_or_default(st.query_params.get("end"), min(_default_cutoff + timedelta(days=10), _latest_market_day))
    _symbols_q = str(st.query_params.get("symbols", "")).strip()
    _board_kind = str(st.query_params.get("board", "focus")).strip().lower()
    _board_label = "Institutional Chart Board" if _board_kind == "institutional" else "Focus Chart Board"
    _symbols = [s.strip().upper() for s in _symbols_q.split(",") if s.strip()]
    _picks_file = REPORTS_DIR / f"institutional_picks_{_cutoff_q.strftime('%d%b%Y').lower()}.txt"

    st.markdown(
        f"<h3 style='margin:0 0 10px 0'>📊 {_board_label}"
        f"<span style='font-size:14px;color:#666;font-weight:400'>"
        f"  |  Cutoff: {_cutoff_q.strftime('%d %b %Y')}  →  End: {_end_q.strftime('%d %b %Y')}"
        f"</span></h3>",
        unsafe_allow_html=True,
    )
    if not _symbols:
        st.info("No focus symbols were supplied for the chart board.")
        st.stop()
    if not _picks_file.exists():
        st.error(f"Picks file not found for cutoff: {_picks_file.name}")
        st.stop()

    board_data = fetch_chart_board_data(
        picks_file=str(_picks_file),
        token_file=str(TOKEN_FILE),
        cutoff_iso=_cutoff_q.isoformat(),
        end_iso=_end_q.isoformat(),
        symbols_csv=",".join(_symbols),
    )
    render_focus_board_lightweight(board_data, _symbols, _cutoff_q, _end_q)
    st.stop()

_all_cutoff_dates = available_cutoff_dates(REPORTS_DIR)

if "_co_init" not in st.session_state:
    _default_co = _all_cutoff_dates[0] if _all_cutoff_dates else date.today() - timedelta(days=14)
    st.session_state["_cutoff_val"] = _default_co
    st.session_state["_end_val"]    = min(_default_co + timedelta(days=10), _latest_market_day)
    st.session_state["_co_init"]    = True

def _on_cutoff_change():
    """Auto-advance end date when cutoff selection changes."""
    co = st.session_state["_cutoff_sel"]          # selectbox key
    st.session_state["_cutoff_val"] = co
    auto_end = min(co + timedelta(days=10), _latest_market_day)
    if auto_end <= co:
        auto_end = _latest_market_day
    st.session_state["_end_val"] = auto_end

with st.sidebar:
    st.markdown("### 🔥 Fire Status")

    # ── Cutoff date — dropdown of available picks files ───────────────────────
    if not _all_cutoff_dates:
        st.error("No institutional picks files found in reports folder.")
        st.stop()

    # Format dates for display: "02 Apr 2026 (Wed)"
    _fmt = lambda d: d.strftime("%d %b %Y  (%a)")
    _date_options  = _all_cutoff_dates                        # list of date objects
    _label_options = [_fmt(d) for d in _date_options]

    # Find current selection index
    _cur_co  = st.session_state["_cutoff_val"]
    _sel_idx = _date_options.index(_cur_co) if _cur_co in _date_options else 0

    st.selectbox(
        "📅 Cutoff Date  *(picks generated)*",
        options=_date_options,
        index=_sel_idx,
        format_func=_fmt,
        key="_cutoff_sel",
        on_change=_on_cutoff_change,
        help=f"{len(_all_cutoff_dates)} picks file(s) found in reports folder",
    )
    cutoff_date = st.session_state["_cutoff_val"]

    # ── End date with ◀ ▶ day-stepper ────────────────────────────────────────
    # NO key= on the date_input — widget value is driven entirely by _end_val.
    # Buttons update _end_val, then rerun; widget re-renders from the new value.
    st.markdown(
        "<p style='font-size:13px;font-weight:500;margin:4px 0 2px 0'>"
        "📅 End Date <em style='font-weight:400'>(last completed day)</em></p>",
        unsafe_allow_html=True,
    )
    _ec1, _ec2, _ec3 = st.columns([1, 5, 1])
    with _ec1:
        if st.button("<", key="_end_prev", help="Back 1 day",
                     use_container_width=True):
            _cand = st.session_state["_end_val"] - timedelta(days=1)
            if _cand > st.session_state["_cutoff_val"]:
                st.session_state["_end_val"] = _cand
                st.rerun()
    with _ec2:
        # No key= — Streamlit does not own this widget in session_state
        _picked_end = st.date_input(
            "End",
            value=st.session_state["_end_val"],
            max_value=_latest_market_day,
            label_visibility="collapsed",
        )
        # Capture manual changes the user makes by typing/clicking the calendar
        if _picked_end != st.session_state["_end_val"]:
            st.session_state["_end_val"] = _picked_end
            st.rerun()
    with _ec3:
        if st.button(">", key="_end_next", help="Forward 1 day",
                     use_container_width=True):
            _cand = st.session_state["_end_val"] + timedelta(days=1)
            if _cand <= _latest_market_day:
                st.session_state["_end_val"] = _cand
                st.rerun()
    end_date = st.session_state["_end_val"]

    # Picks file resolved from selected cutoff date (always exists — selectbox only shows real files)
    cutoff_tag = cutoff_date.strftime("%d%b%Y").lower()
    picks_file = REPORTS_DIR / f"institutional_picks_{cutoff_tag}.txt"

    # ── Thresholds ────────────────────────────────────────────────────────────
    with st.expander("⚙️ Thresholds", expanded=False):
        big_day_pct    = st.slider("Big-day %",          3.0, 10.0,  5.0, 0.5)
        fired_total    = st.slider("Fired total %",      5.0, 25.0, 10.0, 1.0)
        extended_total = st.slider("Extended total %",  15.0, 50.0, 20.0, 1.0)
        laggard_total  = st.slider("Laggard threshold %", 3.0, 15.0,  6.0, 0.5,
                                   help="LAGGARD if total ≤ −n% OR fallen ≥ n% from intra-period peak")
        st.markdown("---")
        cooldown_pct   = st.slider("Focus cooldown trigger %", 1.0, 8.0, 3.0, 0.5,
                                   help="Hide from focus list for 2 trading days after a move this large")

    st.markdown("---")
    load_btn = st.button("🚀 Load / Refresh", use_container_width=True, type="primary")
    if load_btn:
        st.cache_data.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Main header
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    f"<h3 style='margin:0 0 6px 0; line-height:1.2'>🔥 Institutional Picks — Fire Status &nbsp;"
    f"<span style='font-size:14px; color:#666; font-weight:400'>"
    f"Cutoff: {cutoff_date.strftime('%d %b %Y')} → End: {end_date.strftime('%d %b %Y')}"
    f"</span></h3>",
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────
if not picks_file.exists():
    st.info(f"👈  Picks file not found: `{picks_file.name}` — check Cutoff Date in the sidebar.")
    st.stop()

if end_date <= cutoff_date:
    st.error("End Date must be after Cutoff Date.")
    st.stop()

with st.spinner("Fetching data from Kite API…"):
    stocks, ref_dates, pivot, end_close_map = fetch_report_data(
        picks_file     = str(picks_file),
        token_file     = str(TOKEN_FILE),
        cutoff_iso     = cutoff_date.isoformat(),
        end_iso        = end_date.isoformat(),
        big_day_pct    = big_day_pct,
        fired_total    = fired_total,
        extended_total = extended_total,
        laggard_total  = laggard_total,
    )

if not stocks:
    st.error("No data returned. Check Kite token and reports directory.")
    st.stop()

date_labels = [d.strftime("%d-%b") for d in ref_dates]
n_td        = len(ref_dates)


# ─────────────────────────────────────────────────────────────────────────────
# Top-level summary metrics
# ─────────────────────────────────────────────────────────────────────────────
def count_status(st_name):
    return sum(1 for s in stocks if s["status"] == st_name)

n_ytf  = count_status("YET TO FIRE")
n_jft  = count_status("JUST FIRED TODAY")
n_ret  = count_status("FIRED & RETREATING")
n_fire = count_status("FIRED")
n_hext = count_status("HIGHLY EXTENDED")
n_ext  = count_status("EXTENDED")
n_lag  = count_status("LAGGARD")

st.markdown(f"""
<div class="metric-row">
  <div class="metric-box"><div class="num num-green">{n_ytf}</div><div class="lbl">⏳ Yet to Fire</div></div>
  <div class="metric-box"><div class="num num-orange">{n_jft}</div><div class="lbl">⚡ Just Fired Today</div></div>
  <div class="metric-box"><div class="num num-purple">{n_ret}</div><div class="lbl">🔄 Fired & Retreating</div></div>
  <div class="metric-box"><div class="num num-gold">{n_fire}</div><div class="lbl">🔥 Fired</div></div>
  <div class="metric-box"><div class="num num-red">{n_hext + n_ext}</div><div class="lbl">🚀 Extended</div></div>
  <div class="metric-box"><div class="num num-gray">{n_lag}</div><div class="lbl">❌ Laggard</div></div>
  <div class="metric-box"><div class="num" style="color:#1F4E79">{len(stocks)}</div><div class="lbl">📊 Total Stocks</div></div>
  <div class="metric-box"><div class="num" style="color:#1F4E79">{n_td}</div><div class="lbl">📅 Trading Days</div></div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_focus, tab_positions, tab_status, tab_heatmap, tab_extended, tab_charts = st.tabs([
    "⏳ Today's Focus",
    "💼 My Positions",
    "🎯 Fire Status",
    "🌡️ Daily Heatmap",
    "🚫 Extended — Avoid",
    "📈 Charts",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Today's Focus
# ══════════════════════════════════════════════════════════════════════════════
with tab_focus:
    # ── Cooldown filter ───────────────────────────────────────────────────────
    # If a stock moved ≥ cooldown_pct% on either of the 2 trading days BEFORE
    # the end date, suppress it from the focus list for those 2 days.
    def in_cooldown(s: dict) -> bool:
        """True if the stock had a big move in the 2 days preceding the end date."""
        # daily[-1] = end date (today in dashboard context)
        # daily[-2] = 1 day before end  → if it ran, suppress today
        # daily[-3] = 2 days before end → if it ran, suppress today (2nd cool day)
        prior_2 = s["daily"][-(2 + 1):-1]   # up to 2 days before end date
        return any(d >= cooldown_pct for d in prior_2)

    ytf_all   = [s for s in stocks if s["status"] == "YET TO FIRE"]
    jft_all   = [s for s in stocks if s["status"] == "JUST FIRED TODAY"]
    ret_all   = [s for s in stocks if s["status"] == "FIRED & RETREATING"]
    steady_tight_all = [s for s in stocks if s.get("tight_steady_setup")]

    ytf_stocks = [s for s in ytf_all if not in_cooldown(s)]
    jft_stocks = [s for s in jft_all if not in_cooldown(s)]
    ret_stocks = [s for s in ret_all if not in_cooldown(s)]
    steady_tight_stocks = [s for s in steady_tight_all if not in_cooldown(s)]

    cooled_down = (
        [s for s in ytf_all if in_cooldown(s)] +
        [s for s in jft_all if in_cooldown(s)] +
        [s for s in ret_all if in_cooldown(s)] +
        [s for s in steady_tight_all if in_cooldown(s)]
    )
    if cooled_down:
        cooled_names = ", ".join(s["symbol"] for s in cooled_down)
        st.caption(
            f"⏸ **{len(cooled_down)} stock(s) in cooldown** (ran ≥{cooldown_pct:.0f}% in last 2 days, "
            f"resuming after 2 sessions): {cooled_names}"
        )

    focus_pool = sorted(
        {s["symbol"]: s for s in (ytf_stocks + jft_stocks + ret_stocks + steady_tight_stocks)}.values(),
        key=lambda x: (x["order"], -x["total"], x["symbol"]),
    )
    if focus_pool:
        board_qs = urlencode({
            "view": "focus_board",
            "board": "focus",
            "cutoff": cutoff_date.isoformat(),
            "end": end_date.isoformat(),
            "symbols": ",".join(s["symbol"] for s in focus_pool),
        })
        inst_board_qs = urlencode({
            "view": "focus_board",
            "board": "institutional",
            "cutoff": cutoff_date.isoformat(),
            "end": end_date.isoformat(),
            "symbols": ",".join(s["symbol"] for s in stocks),
        })
        st.markdown(
            f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin:4px 0 10px 0;">'
            f'<a href="?{board_qs}" target="_blank" rel="noopener" '
            f'style="display:inline-block;padding:6px 10px;'
            f'border:1px solid #1F4E79;border-radius:6px;text-decoration:none;'
            f'color:#1F4E79;font-size:13px;font-weight:600;">🗂 Open Focus Chart Board</a>'
            f'<a href="?{inst_board_qs}" target="_blank" rel="noopener" '
            f'style="display:inline-block;padding:6px 10px;'
            f'border:1px solid #6E2F8A;border-radius:6px;text-decoration:none;'
            f'color:#6E2F8A;font-size:13px;font-weight:600;">🏦 Open Institutional Chart Board</a>'
            f'</div>',
            unsafe_allow_html=True,
        )

    def render_card(s: dict):
        meta   = STATUS_META.get(s["status"], ("?", "#888", "lag", ""))
        emo, color, css, action = meta
        if s.get("tight_steady_setup"):
            emo, css, action = "📈", "ytf", "📈 Tight base — monitor for expansion"
        card_css  = {"ytf": "card-ytf", "jft": "card-jft", "ret": "card-ret",
                     "ext": "card-ext", "hext": "card-hext", "lag": "card-lag"}.get(css, "card-lag")
        badge_css = f"badge-{css}"
        kite_url = s.get("chart_url_kite", "")
        tv_url   = s.get("chart_url_tv",   "")
        _link_style = (
            "text-decoration:none;font-size:11px;border-radius:4px;"
            "padding:1px 6px;line-height:1.6;margin-left:4px;"
        )
        chart_link = ""
        if kite_url:
            chart_link += (
                f'<a href="{kite_url}" target="_blank" rel="noopener" '
                f'style="{_link_style}color:#1F4E79;border:1px solid #1F4E79">'
                f'📊 Kite</a>'
            )
        if tv_url:
            chart_link += (
                f'<a href="{tv_url}" target="_blank" rel="noopener" '
                f'style="{_link_style}color:#1a6b3c;border:1px solid #1a6b3c">'
                f'📈 TV Live</a>'
            )
        if chart_link:
            chart_link = f'<span style="float:right">{chart_link}</span>'
        st.markdown(f"""
        <div class="card {card_css}">
          <h4>{emo} {s['symbol']}{chart_link}</h4>
          <span class="badge {badge_css}">{s['status']}</span>
          <p>Today: <b>{s['today']:+.2f}%</b> &nbsp;|&nbsp;
             Total: <b>{s['total']:+.2f}%</b> &nbsp;|&nbsp;
             Max: <b>{s['max_day']:+.2f}%</b> &nbsp;|&nbsp;
             3D: <b>{s['recent3']:+.2f}%</b></p>
          <p style="color:#1F4E79; font-weight:600; font-size:12px">{action}</p>
        </div>
        """, unsafe_allow_html=True)

    if steady_tight_stocks:
        st.markdown("<b style='color:#1F4E79'>📈 Tight Steady Runners — Contraction Setup</b>",
                    unsafe_allow_html=True)
        cols = st.columns(min(len(steady_tight_stocks), 4))
        for i, s in enumerate(sorted(steady_tight_stocks, key=lambda x: (x["recent3"], x["total"]), reverse=True)):
            with cols[i % len(cols)]:
                render_card(s)

    if ytf_stocks:
        st.markdown("<b style='color:#00703C'>⏳ Yet to Fire — Prime Watchlist</b>",
                    unsafe_allow_html=True)
        cols = st.columns(min(len(ytf_stocks), 4))
        for i, s in enumerate(sorted(ytf_stocks, key=lambda x: x["total"], reverse=True)):
            with cols[i % len(cols)]:
                render_card(s)
    elif not steady_tight_stocks:
        st.info("No 'Yet to Fire' stocks in this period.")

    if jft_stocks:
        st.markdown("<b style='color:#FF8C00'>⚡ Just Fired Today — Breakout in Progress</b>",
                    unsafe_allow_html=True)
        cols = st.columns(min(len(jft_stocks), 4))
        for i, s in enumerate(sorted(jft_stocks, key=lambda x: x["today"], reverse=True)):
            with cols[i % len(cols)]:
                render_card(s)

    if ret_stocks:
        st.markdown("<b style='color:#9370DB'>🔄 Fired & Retreating — Re-entry Setup</b>",
                    unsafe_allow_html=True)
        cols = st.columns(min(len(ret_stocks), 4))
        for i, s in enumerate(ret_stocks):
            with cols[i % len(cols)]:
                render_card(s)

    if not ytf_stocks and not jft_stocks and not ret_stocks and not steady_tight_stocks:
        st.success("All stocks have already fired or are extended. "
                   "No fresh entry opportunities today.")



# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: My Holdings — EOD Positioning Quality
# ══════════════════════════════════════════════════════════════════════════════
with tab_positions:
    watchlist_syms = {s["symbol"] for s in stocks}
    wl_status_map  = {s["symbol"]: s["status"] for s in stocks}
    wl_total_map   = {s["symbol"]: s["total"]  for s in stocks}
    wl_today_map   = {s["symbol"]: s["today"]  for s in stocks}
    eligible_entry_from = cutoff_date - timedelta(days=1)

    trades_df, trade_history_max, trade_files = fetch_tradebook_history(str(TRADEBOOK_DIR))
    trades_recent_df = filter_trades_to_recent_campaigns(
        trades_df=trades_df,
        eligible_from_dt=eligible_entry_from,
        end_dt=end_date,
    )
    hld_df = build_holdings_snapshot_from_trades(
        trades_df=trades_recent_df,
        end_dt=end_date,
        end_close_map=end_close_map,
    )

    if hld_df.empty:
        hld_df = pd.DataFrame(columns=[
            "symbol", "qty_total", "avg_price", "last_price",
            "invested", "cur_value", "pnl", "pnl_pct"
        ])

    # ── Enrich: cross-reference with watchlist ────────────────────────────────
    hld_df["in_watchlist"]  = hld_df["symbol"].isin(watchlist_syms)
    hld_df["wl_status"]     = hld_df["symbol"].map(wl_status_map)
    hld_df["wl_total_pct"]  = hld_df["symbol"].map(wl_total_map)
    hld_df["wl_today_pct"]  = hld_df["symbol"].map(wl_today_map)

    in_wl = hld_df[hld_df["in_watchlist"]].copy()
    current_watchlist_symbols = set(in_wl["symbol"].tolist())

    exited_wl = build_exited_watchlist_positions(
        trades_df=trades_recent_df,
        watchlist_syms=watchlist_syms,
        current_watchlist_symbols=current_watchlist_symbols,
        cutoff_dt=cutoff_date,
        end_dt=end_date,
        wl_status_map=wl_status_map,
        wl_total_map=wl_total_map,
        wl_today_map=wl_today_map,
    )
    campaign_return_map = build_campaign_return_map(trades_df=trades_recent_df, end_dt=end_date)
    closed_campaign_meta, open_campaign_meta = build_campaign_meta_maps(trades_df=trades_recent_df, end_dt=end_date)
    position_timeline = build_position_timeline(
        trades_df=trades_recent_df,
        watchlist_syms=watchlist_syms,
        ref_dates=ref_dates,
        current_holdings_df=in_wl,
    )

    total_invested    = hld_df["invested"].sum()
    in_wl_invested    = in_wl["invested"].sum() if not in_wl.empty else 0.0
    coverage_pct      = in_wl_invested / total_invested * 100 if total_invested else 0.0
    actioned_watchlist = (
        current_watchlist_symbols | set(exited_wl["Symbol"].tolist())
        if not exited_wl.empty else current_watchlist_symbols
    )
    positioned_leader_days = 0
    positioned_days_pct = 0.0
    if not position_timeline.empty:
        qty_matrix = position_timeline.drop(columns=["symbol"])
        positioned_leader_days = int((qty_matrix > 0).sum().sum())
        positioned_days_pct = (
            positioned_leader_days / float(qty_matrix.shape[0] * qty_matrix.shape[1]) * 100.0
            if qty_matrix.shape[0] and qty_matrix.shape[1] else 0.0
        )

    # ── Top-level positioning quality metrics ─────────────────────────────────
    cov_color  = "num-green" if coverage_pct >= 60 else "num-orange" if coverage_pct >= 30 else "num-red"
    act_color  = "num-green" if len(actioned_watchlist) >= 5 else "num-orange" if len(actioned_watchlist) >= 2 else "num-gray"
    day_color  = "num-green" if positioned_days_pct >= 30 else "num-orange" if positioned_days_pct >= 15 else "num-gray"
    st.markdown(f"""
    <div class="metric-row">
      <div class="metric-box">
        <div class="num {act_color}">{len(actioned_watchlist)}</div>
        <div class="lbl">🎯 WL Names Acted On</div>
      </div>
      <div class="metric-box">
        <div class="num num-green">{len(in_wl)}</div>
        <div class="lbl">✅ Open WL @ End</div>
      </div>
      <div class="metric-box">
        <div class="num num-orange">{len(exited_wl)}</div>
        <div class="lbl">📤 Exited in Range</div>
      </div>
      <div class="metric-box">
        <div class="num {day_color}">{positioned_leader_days}</div>
        <div class="lbl">📅 Positioned Leader-Days</div>
      </div>
      <div class="metric-box">
        <div class="num {cov_color}">{positioned_days_pct:.0f}%</div>
        <div class="lbl">🧭 Watchlist Day Coverage</div>
      </div>
      <div class="metric-box">
        <div class="num num-green">{coverage_pct:.0f}%</div>
        <div class="lbl">💰 Capital in WL @ End</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if not trade_files:
        st.warning(
            f"No tradebook export found in `{TRADEBOOK_DIR}`. "
            "Add Zerodha Console tradebook CSVs there to reconstruct historical positioning."
        )
    if trade_history_max is not None and pd.Timestamp(end_date) > trade_history_max:
        st.warning(
            f"Trade history currently runs only until {trade_history_max.strftime('%d-%b-%Y')}. "
            f"Exited positions after that date will appear once a newer tradebook CSV is exported."
        )

    st.markdown("---")
    left_col = st.container()

    # ── Left: Holdings tables ─────────────────────────────────────────────────
    with left_col:
        # ── Current in-watchlist holdings ─────────────────────────────────────
        st.markdown("#### ✅ Open Holdings — In Watchlist on End Date")
        if in_wl.empty:
            st.info("No open watchlist holding was present on the selected end date.")
        else:
            iw_rows = []
            for _, row in in_wl.sort_values("invested", ascending=False).iterrows():
                sym    = row["symbol"]
                status = str(row.get("wl_status") or "")
                meta   = STATUS_META.get(status, ("—", "#888", "lag", "—"))
                wl_tot = row.get("wl_total_pct")
                wl_tot = float(wl_tot) if pd.notna(wl_tot) else None
                my_ret = round(float(row["pnl_pct"]), 2) if pd.notna(row.get("pnl_pct")) else None
                open_meta = open_campaign_meta.get(sym, {})
                iw_rows.append({
                    "Symbol":       sym,
                    "Entry Date":   open_meta.get("Entry Date"),
                    "Hold Days":    open_meta.get("Hold Days"),
                    "WL Total%":    wl_tot,
                    "My Ret%":      my_ret,
                    "Captured%":    calc_capture_pct(my_ret, wl_tot),
                    "WL Today%":    round(float(row["wl_today_pct"]), 2)
                                    if pd.notna(row.get("wl_today_pct")) else None,
                })
            iw_disp = pd.DataFrame(iw_rows)

            def colour_wl_total(col):
                out = []
                for v in col:
                    if not isinstance(v, (int, float)) or pd.isna(v):
                        out.append("")
                    elif v >= 20:
                        out.append("background-color:#ffebee; color:#C00000")   # extended
                    elif v >= 10:
                        out.append("background-color:#fff8e1; color:#B8860B")   # fired
                    elif v > 0:
                        out.append("background-color:#e8f5e9; color:#00703C")   # positive
                    else:
                        out.append("background-color:#f5f5f5; color:#888")
                return out

            styled_iw = (iw_disp.style
                .apply(colour_wl_total, subset=["WL Total%"])
                .format({
                    "Entry Date":   lambda d: d.strftime("%d-%b-%Y") if pd.notna(d) else "—",
                    "WL Total%":    "{:+.2f}%",
                    "My Ret%":      "{:+.2f}%",
                    "Captured%":    "{:.1f}%",
                    "WL Today%":    "{:+.2f}%",
                }, na_rep="—"))
            st.dataframe(styled_iw, use_container_width=True,
                         height=min(480, len(iw_rows) * 38 + 42))

        # ── Exited watchlist holdings in selected range ──────────────────────
        st.markdown("#### 📤 Exited Holdings — In Selected Range")
        if exited_wl.empty:
            st.info("No fully exited watchlist positions were found in the selected range.")
        else:
            exited_disp = exited_wl.copy()
            exited_disp["Entry Date"] = exited_disp.apply(
                lambda r: closed_campaign_meta.get((str(r["Symbol"]).upper(), r["Exit Date"]), {}).get("Entry Date"),
                axis=1,
            )
            exited_disp["Hold Days"] = exited_disp.apply(
                lambda r: closed_campaign_meta.get((str(r["Symbol"]).upper(), r["Exit Date"]), {}).get("Hold Days"),
                axis=1,
            )
            exited_disp["My Ret%"] = exited_disp.apply(
                lambda r: campaign_return_map.get((str(r["Symbol"]).upper(), r["Exit Date"])),
                axis=1,
            )
            exited_disp["Captured%"] = exited_disp.apply(
                lambda r: calc_capture_pct(r["My Ret%"], r["WL Total%"]),
                axis=1,
            )
            exit_cols = [
                "Symbol", "Entry Date", "Exit Date", "Hold Days",
                "WL Total%", "My Ret%", "Captured%", "WL Today%"
            ]

            def colour_exit_total(col):
                out = []
                for v in col:
                    if not isinstance(v, (int, float)) or pd.isna(v):
                        out.append("")
                    elif v >= 20:
                        out.append("background-color:#ffebee; color:#C00000")
                    elif v >= 10:
                        out.append("background-color:#fff8e1; color:#B8860B")
                    elif v > 0:
                        out.append("background-color:#e8f5e9; color:#00703C")
                    else:
                        out.append("background-color:#f5f5f5; color:#888")
                return out

            styled_exit = (exited_disp[exit_cols].style
                .apply(colour_exit_total, subset=["WL Total%"])
                .format({
                    "Entry Date": lambda d: d.strftime("%d-%b-%Y") if pd.notna(d) else "—",
                    "Exit Date": lambda d: d.strftime("%d-%b-%Y") if pd.notna(d) else "—",
                    "WL Total%": "{:+.2f}%",
                    "My Ret%":   "{:+.2f}%",
                    "Captured%": "{:.1f}%",
                    "WL Today%": "{:+.2f}%",
                }, na_rep="—"))
            st.dataframe(styled_exit, use_container_width=True,
                         height=min(420, len(exited_wl) * 38 + 42))

        st.markdown("#### Positioned Status — Full Watchlist")
        if position_timeline.empty:
            st.info("No historical positioning could be reconstructed for the selected window.")
        else:
            tl_df = position_timeline.copy()
            tl_df["WL Total%"] = tl_df["symbol"].map(lambda s: round(float(wl_total_map.get(s, 0.0)), 2))
            tl_df = tl_df.sort_values(["WL Total%", "symbol"], ascending=[False, True], kind="stable")
            qty_only = tl_df.drop(columns=["symbol", "WL Total%"])
            z_vals = (qty_only > 0).astype(int).values.tolist()
            text_vals = []
            hover_vals = []
            for _, row in tl_df.iterrows():
                row_text = []
                row_hover = []
                for d in ref_dates:
                    qty = int(row[d])
                    row_text.append("IN" if qty > 0 else "")
                    row_hover.append(
                        f"{row['symbol']}<br>{d.strftime('%d-%b-%Y')}"
                        f"<br>Positioned: {'Yes' if qty > 0 else 'No'}"
                        f"<br>Qty: {qty}"
                        f"<br>WL Total: {float(row['WL Total%']):+.2f}%"
                    )
                text_vals.append(row_text)
                hover_vals.append(row_hover)

            fig_pos = go.Figure(go.Heatmap(
                z=z_vals,
                x=[d.strftime("%d-%b") for d in ref_dates],
                y=tl_df["symbol"].tolist(),
                text=text_vals,
                texttemplate="%{text}",
                textfont={"size": 12, "family": "Arial"},
                hovertext=hover_vals,
                hoverinfo="text",
                colorscale=[[0.0, "#F5F5F5"], [0.4999, "#F5F5F5"], [0.5, "#CFE8FF"], [1.0, "#1F78FF"]],
                showscale=False,
                xgap=1,
                ygap=1,
            ))
            fig_pos.update_layout(
                height=max(260, 36 * len(tl_df) + 60),
                margin=dict(l=0, r=0, t=10, b=10),
                xaxis=dict(side="top", fixedrange=True, tickfont=dict(size=12)),
                yaxis=dict(fixedrange=True, tickfont=dict(size=12)),
                paper_bgcolor="white",
                plot_bgcolor="white",
            )
            st.plotly_chart(fig_pos, use_container_width=True, config={"displayModeBar": False})

    # ── Right: Positioning quality charts ────────────────────────────────────
    if False:
        st.markdown("#### Positioned Status — Watchlist Timeline")
        pie_labels = ["In Watchlist (Leaders)", "Outside Watchlist"]
        pie_values = [in_wl_invested, max(total_invested - in_wl_invested, 0)]
        pie_colors = ["#00B050", "#C0C0C0"]
        fig_pie = go.Figure(go.Pie(
            labels=pie_labels,
            values=pie_values,
            hole=0.55,
            marker_colors=pie_colors,
            textinfo="label+percent",
            textfont_size=11,
            hovertemplate="%{label}<br>₹%{value:,.0f}<br>%{percent}<extra></extra>",
        ))
        fig_pie.update_layout(
            height=240,
            margin=dict(t=10, b=10, l=0, r=0),
            showlegend=False,
            annotations=[dict(
                text=f"<b>{coverage_pct:.0f}%</b><br>Leaders",
                x=0.5, y=0.5, showarrow=False,
                font=dict(size=14, color="#1F4E79"),
                xanchor="center", yanchor="middle",
            )],
        )
        st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})

        if not in_wl.empty:
            st.markdown("#### Current Holdings vs Leader Move")
            bar_syms = in_wl.sort_values("invested", ascending=False)["symbol"].tolist()
            bar_vals = [wl_total_map.get(s, 0.0) for s in bar_syms]
            bar_colors = []
            for v in bar_vals:
                if v >= 20:
                    bar_colors.append("#FF4444")
                elif v >= 10:
                    bar_colors.append("#FFD700")
                elif v > 0:
                    bar_colors.append("#00B050")
                else:
                    bar_colors.append("#C0C0C0")

            fig_bar = go.Figure(go.Bar(
                x=bar_syms,
                y=bar_vals,
                marker_color=bar_colors,
                text=[f"{v:+.1f}%" for v in bar_vals],
                textposition="auto",
                hovertemplate="%{x}<br>WL Move: %{y:+.2f}%<extra></extra>",
            ))
            fig_bar.update_layout(
                height=240,
                margin=dict(t=10, b=10, l=0, r=0),
                plot_bgcolor="#FAFAFA",
                paper_bgcolor="white",
                yaxis=dict(tickformat="+.0f", title="WL Total%"),
                xaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

    # ── Outside-watchlist holdings ────────────────────────────────────────────
        elif not exited_wl.empty:
            st.markdown("#### Exited Leaders vs Leader Move")
            exit_bar = exited_wl.head(12).sort_values("WL Total%", ascending=False)
            fig_bar = go.Figure(go.Bar(
                x=exit_bar["Symbol"],
                y=exit_bar["WL Total%"],
                marker_color="#FF8C00",
                text=[f"{v:+.1f}%" if pd.notna(v) else "—" for v in exit_bar["WL Total%"]],
                           textposition="auto",
                hovertemplate="%{x}<br>WL Move: %{y:+.2f}%<extra></extra>",
            ))
            fig_bar.update_layout(
                height=240,
                margin=dict(t=10, b=10, l=0, r=0),
                plot_bgcolor="#FAFAFA",
                paper_bgcolor="white",
                yaxis=dict(tickformat="+.0f", title="WL Total%"),
                xaxis=dict(tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: Full Fire Status Table
# ══════════════════════════════════════════════════════════════════════════════
with tab_status:
    st.markdown("<p style='margin:4px 0 6px 0; font-size:13px; color:#555'>"
                "Color-coded by status · Filter and sort below</p>",
                unsafe_allow_html=True)

    # Filter controls
    col_f1, col_f2 = st.columns([2, 2])
    with col_f1:
        status_filter = st.multiselect(
            "Filter by status",
            options=list(STATUS_META.keys()),
            default=list(STATUS_META.keys()),
        )
    with col_f2:
        sort_by = st.selectbox("Sort by", ["Status (priority)", "Total Return %", "Today %", "Symbol"])

    # Build display DataFrame
    half = n_td // 2
    rows = []
    for s in stocks:
        if s["status"] not in status_filter:
            continue
        meta  = STATUS_META.get(s["status"], ("?", "#888", "lag", ""))
        rows.append({
            "Symbol":          s["symbol"],
            "Status":          f"{meta[0]} {s['status']}",
            "Total %":         s["total"],
            f"Today ({end_date.strftime('%d-%b')}) %": s["today"],
            "Max Day %":       s["max_day"],
            f"Big Days (≥{big_day_pct:.0f}%)": s["n_big"],
            "Recent 3D %":     s["recent3"],
            f"Wk1 ({date_labels[0]}–{date_labels[half-1]}) %": round(sum(s["daily"][:half]), 2),
            f"Wk2 ({date_labels[half]}–{date_labels[-1]}) %":  round(sum(s["daily"][half:]), 2),
            "Action":          meta[3],
            "_order":          s["order"],
            "_color":          s["color"],
        })

    df_status = pd.DataFrame(rows)
    if df_status.empty:
        st.info("No stocks match the selected filters.")
    else:
        sort_map = {
            "Status (priority)": ("_order", True),
            "Total Return %":    ("Total %", False),
            "Today %":           (f"Today ({end_date.strftime('%d-%b')}) %", False),
            "Symbol":            ("Symbol", True),
        }
        sc, sa = sort_map[sort_by]
        df_status = df_status.sort_values(sc, ascending=sa).reset_index(drop=True)

        # Row colour map  (hex → tailwind-ish light version)
        COLOR_BG = {
            "FF4444": "#ffebee", "FF8C00": "#fff3e0", "FFA500": "#fff8e1",
            "FFD700": "#fffde7", "00B050": "#e8f5e9", "9370DB": "#f3e5f5",
            "70AD47": "#f1f8e9", "C0C0C0": "#f5f5f5",
        }

        def row_style(row):
            color = df_status.loc[row.name, "_color"] if row.name in df_status.index else "#fff"
            bg    = COLOR_BG.get(color, "#ffffff")
            return [f"background-color: {bg}"] * len(row)

        display_cols = [c for c in df_status.columns if not c.startswith("_")]
        styled = (df_status[display_cols]
                  .style
                  .apply(row_style, axis=1)
                  .format({
                      "Total %":    "{:+.2f}%",
                      "Max Day %":  "{:+.2f}%",
                      "Recent 3D %": "{:+.2f}%",
                  }, na_rep="—")
                  .format(lambda x: f"{x:+.2f}%",
                          subset=[c for c in display_cols
                                  if "%" in c and c not in ("Status", "Action",
                                     f"Big Days (≥{big_day_pct:.0f}%)")]))

        st.dataframe(styled, use_container_width=True, height=min(600, len(df_status) * 36 + 40))


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: Daily Heatmap
# ══════════════════════════════════════════════════════════════════════════════
with tab_heatmap:
    hm_sort = st.radio("Sort by", ["Total Return %", "Symbol (A-Z)"],
                       horizontal=True, key="hm_sort", label_visibility="collapsed")

    sorted_syms = sorted(stocks, key=lambda x: -x["total"] if hm_sort.startswith("Total") else x["symbol"])
    syms_order  = [s["symbol"] for s in sorted_syms]

    # Row height that keeps all symbols visible without scrolling
    row_h_px   = max(22, min(32, 560 // max(len(syms_order), 1)))
    chart_h    = len(syms_order) * row_h_px + 60

    # Build heatmap matrix
    z_vals, hover = [], []
    for s in sorted_syms:
        row_z, row_h = [], []
        for i, d in enumerate(ref_dates):
            val = float(pivot.loc[s["symbol"], d]) if (s["symbol"] in pivot.index
                  and d in pivot.columns and pd.notna(pivot.loc[s["symbol"], d])) else None
            row_z.append(val)
            row_h.append(f"{s['symbol']}<br>{date_labels[i]}: {val:+.2f}%" if val is not None
                         else f"{s['symbol']}<br>{date_labels[i]}: —")
        z_vals.append(row_z)
        hover.append(row_h)

    fig_hm = go.Figure(go.Heatmap(
        z            = z_vals,
        x            = date_labels,
        y            = syms_order,
        text         = [[f"{v:+.1f}%" if v is not None else "—" for v in row] for row in z_vals],
        texttemplate = "%{text}",
        textfont     = {"size": 13, "family": "Arial"},
        hovertext    = hover,
        hoverinfo    = "text",
        colorscale   = [
            [0.0,  "#C00000"],
            [0.35, "#FF9999"],
            [0.48, "#FFE5E5"],
            [0.5,  "#FFFFFF"],
            [0.52, "#E8F5E9"],
            [0.70, "#70AD47"],
            [0.85, "#00B050"],
            [1.0,  "#005C00"],
        ],
        zmid         = 0,
        zmin         = -10,
        zmax         = 12,
        colorbar     = dict(title="Daily %", tickformat="+.0f",
                            thickness=12, len=0.9, tickfont=dict(size=10)),
    ))
    fig_hm.update_layout(
        height    = chart_h,
        margin    = dict(l=0, r=40, t=30, b=4),
        xaxis     = dict(side="top", tickfont=dict(size=13, family="Arial"),
                         fixedrange=True),
        yaxis     = dict(autorange="reversed",
                         tickfont=dict(size=13, family="Arial", color="#222"),
                         fixedrange=True),
        paper_bgcolor = "white",
        plot_bgcolor  = "white",
    )

    # Total-return bar — side by side with heatmap
    df_tot  = pd.DataFrame([
        {"Symbol": s["symbol"], "Total %": s["total"], "Status": s["status"]}
        for s in sorted_syms
    ])
    bar_colors = [STATUS_META.get(s, ("?", "#888888"))[1] for s in df_tot["Status"]]
    fig_bar = go.Figure(go.Bar(
        x             = df_tot["Total %"],
        y             = df_tot["Symbol"],
        orientation   = "h",
        marker_color  = bar_colors,
        text          = [f"{v:+.1f}%" for v in df_tot["Total %"]],
        textposition  = "auto",
        textfont      = dict(size=12, family="Arial"),
        hovertemplate = "%{y}: %{x:+.2f}%<extra></extra>",
    ))
    fig_bar.update_layout(
        title  = dict(text="Total Return %", font=dict(size=13), x=0.5),
        height = chart_h,
        margin = dict(l=0, r=4, t=30, b=4),
        xaxis  = dict(tickformat="+.0f", tickfont=dict(size=11), fixedrange=True),
        yaxis  = dict(autorange="reversed", tickfont=dict(size=13, family="Arial"),
                      fixedrange=True),
        paper_bgcolor = "white",
        plot_bgcolor  = "#FAFAFA",
    )

    hm_left, hm_right = st.columns([3, 1])
    with hm_left:
        st.plotly_chart(fig_hm, use_container_width=True, config={"displayModeBar": False})
    with hm_right:
        st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: Extended — Avoid
# ══════════════════════════════════════════════════════════════════════════════
with tab_extended:
    st.markdown("### 🚫 Extended Stocks — Avoid Chasing")
    st.warning("These stocks have already made significant moves. "
               "Chasing here risks buying at the top.", icon="⚠️")

    ext_list = [s for s in stocks
                if s["status"] in ("HIGHLY EXTENDED", "EXTENDED", "JUST FIRED TODAY")]
    ext_list.sort(key=lambda x: x["total"], reverse=True)

    if not ext_list:
        st.info("No extended stocks in this period.")
    else:
        for s in ext_list:
            meta = STATUS_META.get(s["status"], ("?", "#888", "hext", ""))
            emo, color, css, _ = meta
            card_css  = f"card-{css}"
            badge_css = f"badge-{css}"
            half      = n_td // 2
            wk1       = sum(s["daily"][:half])
            wk2       = sum(s["daily"][half:])

            RISK = {
                "HIGHLY EXTENDED":  "Multiple big days — parabolic. High probability of mean reversion.",
                "EXTENDED":         "Strong cumulative run. Risk of consolidation or profit-taking.",
                "JUST FIRED TODAY": "Breaking out today — do NOT chase gap. Wait for next base.",
            }
            st.markdown(f"""
            <div class="card {card_css}">
              <h4>{emo} {s['symbol']}</h4>
              <span class="badge {badge_css}">{s['status']}</span>
              <p>Total: <b>{s['total']:+.2f}%</b> &nbsp;|&nbsp;
                 Max day: <b>{s['max_day']:+.2f}%</b> &nbsp;|&nbsp;
                 Big days: <b>{s['n_big']}</b> &nbsp;|&nbsp;
                 Wk1: <b>{wk1:+.2f}%</b> &nbsp;|&nbsp;
                 Wk2: <b>{wk2:+.2f}%</b></p>
              <p style="color:#C00000; font-weight:600">⚠️ {RISK.get(s['status'], '')}</p>
            </div>
            """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5: Charts
# ══════════════════════════════════════════════════════════════════════════════
with tab_charts:
    st.markdown("### 📈 Charts & Analytics")

    c1, c2 = st.columns(2)

    # Pie: status distribution
    with c1:
        pie_data = {}
        for s in stocks:
            pie_data[s["status"]] = pie_data.get(s["status"], 0) + 1
        pie_df = pd.DataFrame(list(pie_data.items()), columns=["Status", "Count"])
        pie_df["Emoji"] = pie_df["Status"].map(lambda x: STATUS_META.get(x, ("?",))[0])
        pie_df["Color"] = pie_df["Status"].map(lambda x: STATUS_META.get(x, ("?","#888888"))[1])
        fig_pie = px.pie(
            pie_df, values="Count",
            names=[f"{r['Emoji']} {r['Status']}" for _, r in pie_df.iterrows()],
            color_discrete_sequence=pie_df["Color"].tolist(),
            title="Status Distribution",
        )
        fig_pie.update_traces(textinfo="label+value", textfont_size=12)
        fig_pie.update_layout(showlegend=False, height=350, margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig_pie, use_container_width=True)

    # Bar: daily average % per date
    with c2:
        if not pivot.empty:
            avg_by_date = pivot.mean(axis=0, skipna=True)
            fig_avg = go.Figure(go.Bar(
                x     = date_labels,
                y     = avg_by_date.values,
                marker_color = ["#00B050" if v >= 0 else "#FF4444" for v in avg_by_date.values],
                text  = [f"{v:+.2f}%" for v in avg_by_date.values],
                textposition = "auto",
            ))
            fig_avg.update_layout(
                title  = "Average Daily % — All Picks",
                yaxis  = dict(tickformat="+.1f", title="Avg %"),
                height = 350,
                margin = dict(t=40, b=0, l=0, r=0),
                plot_bgcolor  = "#FAFAFA",
                paper_bgcolor = "white",
            )
            st.plotly_chart(fig_avg, use_container_width=True)

    # Cumulative return lines (interactive — hover to compare)
    st.markdown("#### Cumulative Return Comparison")
    selected_syms = st.multiselect(
        "Select stocks to compare",
        options=sorted([s["symbol"] for s in stocks]),
        default=sorted([s["symbol"] for s in stocks])[:8],
    )

    if selected_syms and not pivot.empty:
        cum_data = []
        for sym in selected_syms:
            if sym not in pivot.index:
                continue
            vals = [float(v) if pd.notna(v) else 0.0 for v in pivot.loc[sym].values]
            cumulative = 0.0
            for i, v in enumerate(vals):
                cumulative += v
                cum_data.append({"Symbol": sym, "Date": date_labels[i], "Cumulative %": round(cumulative, 2)})
        if cum_data:
            cum_df = pd.DataFrame(cum_data)
            status_color = {s["symbol"]: STATUS_META.get(s["status"], ("?","#888888"))[1]
                            for s in stocks}
            fig_cum = px.line(
                cum_df, x="Date", y="Cumulative %", color="Symbol",
                color_discrete_map=status_color,
                markers=True,
                title="Cumulative % Return — Individual Stocks",
            )
            fig_cum.update_layout(
                height = 420,
                hovermode = "x unified",
                plot_bgcolor  = "#FAFAFA",
                paper_bgcolor = "white",
                margin = dict(t=50, b=10, l=0, r=0),
            )
            fig_cum.add_hline(y=0, line_color="gray", line_dash="dash", line_width=1)
            st.plotly_chart(fig_cum, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Footer
# ────────────────────────────────────────────────────�
