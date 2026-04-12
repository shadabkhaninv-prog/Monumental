"""
Stock rating application based on Stock_Rating_Spec v1.0 / v2.

Required positional arguments:
    python stock_rating.py 2026-04-11 2026-03-04

Optional flags:
    --symbols PATH      Explicit symbol list file
    --token PATH        Kite token file path (default: kite_token.txt)
    --output-dir PATH   Output directory (default: ./output)
    --index-symbol STR  Preferred index symbol in indexbhav

Dependencies:
    pip install kiteconnect mysql-connector-python pandas numpy openpyxl
"""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import mysql.connector
except Exception:  # pragma: no cover - import failure handled at runtime
    mysql = None

try:
    from kiteconnect import KiteConnect
except Exception:  # pragma: no cover - import failure handled at runtime
    KiteConnect = None

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


LOOKBACK_52W = 250
LOOKBACK_12M = 250
LOOKBACK_6M = 125
LOOKBACK_3M = 60
TURNOVER_LOOKBACK = 42
ATR_LOOKBACK = 21
RS_SLOPE_LOOKBACK = 21
SPIKE_LOOKBACK = 125
SPIKE_WINDOW_10 = 10
SPIKE_WINDOW_30 = 30
SPIKE_WINDOW_60 = 60
TURNOVER_CRORE_DIVISOR = 10_000_000.0
MIN_MEDIAN_TURNOVER_CRORES = 15.0
GAPUP_LOOKBACK = 60
UPTREND_CONSISTENCY_LOOKBACK = 60

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "bhav",
}

# The sector section of the spec defines the methodology but not the points.
# Assumption used here:
#   - Sector strength = share of sector stocks that landed in the top quartile
#     of pre-sector composite score.
#   - Stocks in sectors within the top 10% of sector-strength distribution get +4.
#   - Stocks in sectors within the top 30% (but not top 10%) get +2.
SECTOR_TOP10_POINTS = 4
SECTOR_TOP30_POINTS = 2


@dataclass
class WarningLog:
    symbol: str
    message: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rate NSE stocks using the Stock_Rating_Spec scoring model."
    )
    parser.add_argument("cutoff_date", help="As-of date in YYYY-MM-DD format")
    parser.add_argument("reset_date", help="Reset date in YYYY-MM-DD format")
    parser.add_argument(
        "--symbols",
        help="Optional explicit symbol file path. Default: auto-resolve gmlist_<DDMMMYYYY>.txt.",
    )
    parser.add_argument(
        "--symbols-dir",
        default="gmlist",
        help="Directory to search for gmlist files when --symbols is not provided.",
    )
    parser.add_argument(
        "--token",
        default="kite_token.txt",
        help="Path to kite_token.txt (default: kite_token.txt).",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for the generated workbook (default: ./output).",
    )
    parser.add_argument(
        "--index-symbol",
        default="Nifty Smallcap 250",
        help="Preferred index symbol label in indexbhav (default: Nifty Smallcap 250).",
    )
    args = parser.parse_args()
    args.cutoff_date = parse_iso_date(args.cutoff_date, "cutoff_date")
    args.reset_date = parse_iso_date(args.reset_date, "reset_date")
    if args.reset_date > args.cutoff_date:
        raise SystemExit("reset_date must be on or before cutoff_date")
    return args


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid {field_name}: {value}. Expected YYYY-MM-DD.") from exc


def locate_symbol_file(cutoff_date: date, explicit_path: Optional[str], symbols_dir: str) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise SystemExit(f"Symbol file not found: {path}")
        return path

    expected_name = f"gmlist_{cutoff_date.strftime('%d%b%Y')}.txt"
    candidates = [
        Path.cwd() / expected_name,
        Path.cwd() / symbols_dir / expected_name,
        Path(__file__).resolve().parent / expected_name,
        Path(__file__).resolve().parent / symbols_dir / expected_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    tried = "\n".join(f"  - {p}" for p in candidates)
    raise SystemExit(f"Could not locate symbol file {expected_name}. Tried:\n{tried}")


def read_symbols(path: Path) -> List[str]:
    symbols: List[str] = []
    seen = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        symbol = line.upper()
        if symbol.startswith("NSE:"):
            symbol = symbol[4:]
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    if not symbols:
        raise SystemExit(f"Symbol file is empty: {path}")
    return symbols


def read_kite_token_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        raise SystemExit(f"Token file not found: {path}")
    text = None
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    for encoding in encodings:
        try:
            text = path.read_text(encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise SystemExit(f"Unable to decode token file: {path}")
    values: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    if "API_KEY" not in values or "ACCESS_TOKEN" not in values:
        raise SystemExit("kite_token.txt must contain API_KEY and ACCESS_TOKEN")
    generated = values.get("GENERATED")
    if generated:
        token_day = generated[:10]
        today = datetime.now().strftime("%Y-%m-%d")
        if token_day != today:
            print(f"WARNING: kite_token.txt GENERATED is {token_day}; today is {today}.")
    return values


def get_kite_client(token_file: Path):
    if KiteConnect is None:
        raise SystemExit("kiteconnect is not installed. Install it with pip.")
    creds = read_kite_token_file(token_file)
    kite = KiteConnect(api_key=creds["API_KEY"])
    kite.set_access_token(creds["ACCESS_TOKEN"])
    print("Kite session initialized")
    return kite


def get_db_connection():
    if mysql is None:
        raise SystemExit("mysql-connector-python is not installed. Install it with pip.")
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Exception as exc:
        raise SystemExit(f"Failed to connect to MySQL bhav database: {exc}") from exc


def load_sector_map(conn, symbols: Iterable[str]) -> Dict[str, str]:
    symbol_list = list(symbols)
    placeholders = ",".join(["%s"] * len(symbol_list))
    sql = (
        f"SELECT UPPER(symbol) AS symbol, sector1 "
        f"FROM sectors WHERE UPPER(symbol) IN ({placeholders})"
    )
    cursor = conn.cursor()
    cursor.execute(sql, [s.upper() for s in symbol_list])
    mapping = {row[0]: (row[1] or "Unknown") for row in cursor.fetchall()}
    cursor.close()
    return {sym: mapping.get(sym.upper(), "Unknown") for sym in symbol_list}


def load_index_history(conn, start_date: date, cutoff_date: date, preferred_symbol: str) -> pd.DataFrame:
    print(f"Loading index data from indexbhav for {start_date} to {cutoff_date}")
    cursor = conn.cursor()
    exact_sql = """
        SELECT mktdate, open, high, low, close, symbol
        FROM indexbhav
        WHERE mktdate BETWEEN %s AND %s
          AND UPPER(symbol) = UPPER(%s)
        ORDER BY mktdate
    """
    cursor.execute(exact_sql, (start_date, cutoff_date, preferred_symbol))
    rows = cursor.fetchall()
    if not rows:
        like_sql = """
            SELECT mktdate, open, high, low, close, symbol
            FROM indexbhav
            WHERE mktdate BETWEEN %s AND %s
              AND UPPER(symbol) LIKE %s
            ORDER BY mktdate
        """
        cursor.execute(like_sql, (start_date, cutoff_date, "%SMALLCAP%250%"))
        rows = cursor.fetchall()
    cursor.close()
    if not rows:
        raise SystemExit(
            f"Could not load Nifty Smallcap 250 data from indexbhav between {start_date} and {cutoff_date}."
        )
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "symbol"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").set_index("date")
    return df


def get_existing_yearly_bhav_tables(conn, start_date: date, cutoff_date: date) -> List[str]:
    table_names = [f"bhav{year}" for year in range(start_date.year, cutoff_date.year + 1)]
    cursor = conn.cursor()
    existing: List[str] = []
    for table_name in table_names:
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if cursor.fetchone():
            existing.append(table_name)
    cursor.close()
    return existing


def load_turnover_map(
    conn,
    symbols: Iterable[str],
    start_date: date,
    cutoff_date: date,
) -> Dict[str, Dict[str, float]]:
    symbol_list = list(symbols)
    if not symbol_list:
        return {}

    tables = get_existing_yearly_bhav_tables(conn, start_date, cutoff_date)
    if not tables:
        return {}
    print(f"Loading turnover filter data from tables: {', '.join(tables)}")

    symbol_placeholders = ",".join(["%s"] * len(symbol_list))
    union_parts = []
    params: List[object] = []
    for table_name in tables:
        union_parts.append(
            f"""
            SELECT UPPER(SYMBOL) AS symbol, MKTDATE AS trade_date, CLOSE, VOLUME
            FROM {table_name}
            WHERE MKTDATE BETWEEN %s AND %s
              AND UPPER(SYMBOL) IN ({symbol_placeholders})
            """
        )
        params.extend([start_date, cutoff_date, *[s.upper() for s in symbol_list]])

    sql = f"""
        SELECT symbol, trade_date, CLOSE, VOLUME
        FROM (
            {" UNION ALL ".join(union_parts)}
        ) turnover_rows
        ORDER BY symbol, trade_date
    """

    df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return {}

    df["turnover"] = pd.to_numeric(df["CLOSE"], errors="coerce") * pd.to_numeric(df["VOLUME"], errors="coerce")
    turnover_map: Dict[str, Dict[str, float]] = {}
    for symbol, group in df.groupby("symbol"):
        recent = group.sort_values("trade_date").tail(TURNOVER_LOOKBACK)
        turnover_map[str(symbol).upper()] = {
            "avg_turnover_42d": float(recent["turnover"].mean()) / TURNOVER_CRORE_DIVISOR if not recent.empty else math.nan,
            "median_turnover_42d": float(recent["turnover"].median()) / TURNOVER_CRORE_DIVISOR if not recent.empty else math.nan,
        }
    return turnover_map


def filter_symbols_by_turnover(
    symbols: Iterable[str],
    turnover_map: Dict[str, Dict[str, float]],
    min_median_turnover_crores: float,
    warnings: List[WarningLog],
) -> List[str]:
    eligible: List[str] = []
    for symbol in symbols:
        turnover_info = turnover_map.get(symbol.upper())
        median_turnover = turnover_info.get("median_turnover_42d", math.nan) if turnover_info else math.nan
        if pd.isna(median_turnover):
            warnings.append(WarningLog(symbol, "Median turnover unavailable; excluded from rating universe."))
            continue
        if float(median_turnover) < min_median_turnover_crores:
            warnings.append(
                WarningLog(
                    symbol,
                    f"Median turnover {round(float(median_turnover), 2)} Cr below {min_median_turnover_crores:.2f} Cr; excluded from rating universe.",
                )
            )
            continue
        eligible.append(symbol)
    return eligible


def build_instrument_map(kite, symbols: Iterable[str]) -> Dict[str, Dict[str, object]]:
    print("Calling Kite: instruments('NSE')")
    instruments = pd.DataFrame(kite.instruments("NSE"))
    if instruments.empty:
        raise SystemExit("kite.instruments('NSE') returned no rows.")
    if "segment" in instruments.columns:
        instruments = instruments[instruments["segment"] == "NSE"]
    filtered = instruments[instruments["tradingsymbol"].isin(list(symbols))]
    print(f"Kite instruments mapped: {len(filtered)}/{len(list(symbols))}")
    instrument_map: Dict[str, Dict[str, object]] = {}
    for _, row in filtered.iterrows():
        listing_date = row.get("listing_date")
        parsed_listing_date = None
        if pd.notna(listing_date):
            try:
                parsed_listing_date = pd.to_datetime(listing_date).date()
            except Exception:
                parsed_listing_date = None
        instrument_map[row["tradingsymbol"]] = {
            "instrument_token": int(row["instrument_token"]),
            "listing_date": parsed_listing_date,
        }
    return instrument_map


def fetch_history_with_retry(
    kite,
    instrument_token: int,
    symbol: str,
    start_date: date,
    cutoff_date: date,
    retries: int = 3,
) -> Optional[pd.DataFrame]:
    for attempt in range(1, retries + 1):
        try:
            print(f"Calling Kite: historical_data('{symbol}') attempt {attempt}")
            rows = kite.historical_data(
                instrument_token=instrument_token,
                from_date=datetime.combine(start_date, datetime.min.time()),
                to_date=datetime.combine(cutoff_date, datetime.min.time()),
                interval="day",
                continuous=False,
                oi=False,
            )
            if not rows:
                return None
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df.sort_values("date").set_index("date")
            return df[["open", "high", "low", "close", "volume"]]
        except Exception as exc:
            err_text = str(exc).lower()
            if "invalid token" in err_text or "token is invalid" in err_text:
                print(f"WARNING: historical_data failed for {symbol}: {exc}")
                return None
            if attempt == retries:
                print(f"WARNING: historical_data failed for {symbol}: {exc}")
                return None
            sleep_seconds = attempt * 2
            print(f"WARNING: retrying {symbol} after error: {exc} (sleep {sleep_seconds}s)")
            time.sleep(sleep_seconds)
    return None


def trading_lookback_value(series: pd.Series, lookback: int) -> float:
    clean = series.dropna()
    if clean.empty:
        return math.nan
    if len(clean) > lookback:
        return float(clean.iloc[-(lookback + 1)])
    return float(clean.iloc[0])


def safe_return(current_value: float, prior_value: float) -> float:
    if pd.isna(current_value) or pd.isna(prior_value) or prior_value == 0:
        return math.nan
    return (current_value - prior_value) / prior_value


def compute_atr_percent(df: pd.DataFrame, period: int = ATR_LOOKBACK) -> float:
    if df.empty:
        return math.nan
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    current_close = float(df["close"].iloc[-1])
    if current_close == 0:
        return math.nan
    return float((atr.iloc[-1] / current_close) * 100.0)


def first_row_on_or_after(df: pd.DataFrame, target_date: date) -> Optional[pd.Series]:
    subset = df[df.index >= target_date]
    if subset.empty:
        return None
    return subset.iloc[0]


def compute_stock_metrics(
    symbol: str,
    df: pd.DataFrame,
    index_df: pd.DataFrame,
    sector: str,
    cutoff_date: date,
    reset_date: date,
    listing_date: Optional[date],
    turnover_override: Optional[Dict[str, float]],
    warnings: List[WarningLog],
) -> Optional[Dict[str, object]]:
    history = df[df.index <= cutoff_date].copy()
    if history.empty:
        warnings.append(WarningLog(symbol, "No price history returned by Kite; skipped."))
        return None

    if listing_date is not None:
        listed_days = len(history[history.index >= listing_date])
    else:
        listed_days = len(history)

    current_close = float(history["close"].iloc[-1])
    price_window = history.tail(LOOKBACK_52W)
    if len(history) < LOOKBACK_52W:
        warnings.append(WarningLog(symbol, "Insufficient history (<250 trading days); used available history for 52W high."))

    high_52w = float(price_window["high"].max())
    high_rows = price_window[price_window["high"] == high_52w]
    high_date = high_rows.index[-1]
    days_since_high = len(price_window.loc[high_date:]) - 1
    pct_from_high = ((high_52w - current_close) / high_52w) if high_52w else math.nan

    turnover_42d = (history["close"] * history["volume"]).tail(TURNOVER_LOOKBACK)
    avg_turnover_42d = (
        float(turnover_42d.mean()) / TURNOVER_CRORE_DIVISOR if not turnover_42d.empty else math.nan
    )
    median_turnover_42d = (
        float(turnover_42d.median()) / TURNOVER_CRORE_DIVISOR if not turnover_42d.empty else math.nan
    )
    if turnover_override:
        avg_value = turnover_override.get("avg_turnover_42d", math.nan)
        median_value = turnover_override.get("median_turnover_42d", math.nan)
        if not pd.isna(avg_value):
            avg_turnover_42d = float(avg_value)
        if not pd.isna(median_value):
            median_turnover_42d = float(median_value)

    stock_close_12m = trading_lookback_value(history["close"], LOOKBACK_12M)
    stock_close_6m = trading_lookback_value(history["close"], LOOKBACK_6M)
    stock_close_3m = trading_lookback_value(history["close"], LOOKBACK_3M)
    ret_12m = safe_return(current_close, stock_close_12m)
    ret_6m = safe_return(current_close, stock_close_6m)
    ret_3m = safe_return(current_close, stock_close_3m)

    reset_row = first_row_on_or_after(history, reset_date)
    reset_low = float(reset_row["low"]) if reset_row is not None else math.nan
    reset_date_used = reset_row.name if reset_row is not None else None
    reset_recovery = safe_return(current_close, reset_low)

    aligned = history[["close"]].join(index_df[["close"]].rename(columns={"close": "index_close"}), how="inner")
    if aligned.empty:
        warnings.append(WarningLog(symbol, "No overlapping stock/index history; skipped."))
        return None

    index_current_close = float(aligned["index_close"].iloc[-1])
    index_close_12m = trading_lookback_value(aligned["index_close"], LOOKBACK_12M)
    index_close_6m = trading_lookback_value(aligned["index_close"], LOOKBACK_6M)
    index_close_3m = trading_lookback_value(aligned["index_close"], LOOKBACK_3M)
    index_ret_12m = safe_return(index_current_close, index_close_12m)
    index_ret_6m = safe_return(index_current_close, index_close_6m)
    index_ret_3m = safe_return(index_current_close, index_close_3m)

    rs_12m = ret_12m - index_ret_12m if not (pd.isna(ret_12m) or pd.isna(index_ret_12m)) else math.nan
    rs_6m = ret_6m - index_ret_6m if not (pd.isna(ret_6m) or pd.isna(index_ret_6m)) else math.nan
    rs_3m = ret_3m - index_ret_3m if not (pd.isna(ret_3m) or pd.isna(index_ret_3m)) else math.nan

    rs_line = aligned["close"] / aligned["index_close"]
    rs_line_high_52w = float(rs_line.tail(LOOKBACK_52W).max()) if not rs_line.empty else math.nan
    rs_line_at_high = bool(not rs_line.empty and rs_line.iloc[-1] >= rs_line_high_52w)
    if len(rs_line) >= RS_SLOPE_LOOKBACK:
        rs_line_slope_21d = float(rs_line.iloc[-1] - rs_line.iloc[-RS_SLOPE_LOOKBACK])
    else:
        rs_line_slope_21d = math.nan

    atr_percent = compute_atr_percent(history, ATR_LOOKBACK)

    ema8 = history["close"].ewm(span=8, adjust=False).mean()
    ema21 = history["close"].ewm(span=21, adjust=False).mean()
    dma50 = history["close"].rolling(50, min_periods=1).mean()
    uptrend_stack = (ema8 > ema21) & (ema21 > dma50)
    uptrend_consistency_pct = float(uptrend_stack.tail(UPTREND_CONSISTENCY_LOOKBACK).mean()) if not uptrend_stack.empty else math.nan

    spike_window = history.tail(SPIKE_LOOKBACK).copy()
    spike_window["prev_close"] = spike_window["close"].shift(1)
    spike_window["price_change_pct"] = np.where(
        spike_window["prev_close"].fillna(0) != 0,
        ((spike_window["close"] - spike_window["prev_close"]) / spike_window["prev_close"]) * 100.0,
        np.nan,
    )
    spike_base_points = 0
    spike_bonus_points = 0
    spike_total_points = 0
    spike_label = ""
    spike_date = None
    spike_volume = math.nan
    spike_price_change_pct = math.nan
    spike_window_days = math.nan
    spike_top1_threshold = math.nan
    if not spike_window.empty:
        spike_top1_threshold = top_n_threshold(spike_window["volume"], 1)
        spike_candidates = spike_window[spike_window["volume"] >= spike_top1_threshold].copy()
        if not spike_candidates.empty:
            spike_candidates["window_days"] = spike_candidates.index.map(lambda d: len(spike_window.loc[d:]))
            spike_candidates = spike_candidates[spike_candidates["window_days"] <= SPIKE_WINDOW_60]
        if not spike_candidates.empty:
            spike_candidates = spike_candidates.sort_values(["window_days", "volume"], ascending=[True, False])
            spike_row = spike_candidates.iloc[0]
            spike_date = spike_row.name
            spike_volume = float(spike_row["volume"])
            spike_price_change_pct = float(spike_row["price_change_pct"]) if not pd.isna(spike_row["price_change_pct"]) else math.nan
            spike_window_days = int(spike_row["window_days"])
            if spike_window_days <= SPIKE_WINDOW_10:
                spike_base_points = 10
                spike_label = "10D"
            elif spike_window_days <= SPIKE_WINDOW_30:
                spike_base_points = 8
                spike_label = "30D"
            elif spike_window_days <= SPIKE_WINDOW_60:
                spike_base_points = 6
                spike_label = "60D"

            if spike_base_points:
                if not pd.isna(spike_price_change_pct) and spike_price_change_pct > 6:
                    spike_bonus_points = 4
                elif not pd.isna(spike_price_change_pct) and spike_price_change_pct > 3:
                    spike_bonus_points = 2
                spike_total_points = spike_base_points + spike_bonus_points
    if listed_days < 15:
        spike_base_points = 0
        spike_bonus_points = 0
        spike_total_points = 0
        spike_label = ""
        spike_date = None
        spike_volume = math.nan
        spike_price_change_pct = math.nan
        spike_window_days = math.nan

    gapup_window = history.tail(GAPUP_LOOKBACK).copy()
    gapup_window["prev_close"] = gapup_window["close"].shift(1)
    gapup_window["gap_up_pct"] = np.where(
        gapup_window["prev_close"].fillna(0) != 0,
        ((gapup_window["open"] - gapup_window["prev_close"]) / gapup_window["prev_close"]) * 100.0,
        np.nan,
    )
    gapup_window["close_gain_pct"] = np.where(
        gapup_window["prev_close"].fillna(0) != 0,
        ((gapup_window["close"] - gapup_window["prev_close"]) / gapup_window["prev_close"]) * 100.0,
        np.nan,
    )
    gapup_date = None
    gapup_volume = math.nan
    gapup_pct = math.nan
    gapup_close_pct = math.nan
    gapup_top1_threshold = math.nan
    score_gapup = 0
    if not gapup_window.empty:
        gapup_top1_threshold = top_n_threshold(gapup_window["volume"], 1)
        qualifying_gapups = gapup_window[
            (gapup_window["gap_up_pct"] > 3.0)
            & (gapup_window["close_gain_pct"] > 5.0)
            & (
                (gapup_window["close_gain_pct"] >= 9.0)
                | (gapup_window["volume"] >= gapup_top1_threshold)
            )
        ]
        if not qualifying_gapups.empty:
            gapup_row = qualifying_gapups.iloc[-1]
            gapup_date = gapup_row.name
            gapup_volume = float(gapup_row["volume"])
            gapup_pct = float(gapup_row["gap_up_pct"])
            gapup_close_pct = float(gapup_row["close_gain_pct"])
            score_gapup = 6

    score_new_listing = 0
    if listed_days < 30:
        score_new_listing = 4
    elif listed_days < 60:
        score_new_listing = 2

    return {
        "symbol": symbol,
        "sector": sector or "Unknown",
        "listing_date": listing_date,
        "listed_days": listed_days,
        "current_close": current_close,
        "high_52w_close": high_52w,
        "high_52w_date": high_date,
        "pct_from_52w_high": pct_from_high,
        "days_since_52w_high": days_since_high,
        "avg_turnover_42d": avg_turnover_42d,
        "median_turnover_42d": median_turnover_42d,
        "return_12m": ret_12m,
        "return_6m": ret_6m,
        "return_3m": ret_3m,
        "reset_date_used": reset_date_used,
        "reset_low": reset_low,
        "reset_recovery": reset_recovery,
        "index_return_12m": index_ret_12m,
        "index_return_6m": index_ret_6m,
        "index_return_3m": index_ret_3m,
        "rs_12m": rs_12m,
        "rs_6m": rs_6m,
        "rs_3m": rs_3m,
        "rs_line_at_52w_high": rs_line_at_high,
        "rs_line_slope_21d": rs_line_slope_21d,
        "atr_percent_21d": atr_percent,
        "uptrend_consistency_pct": uptrend_consistency_pct,
        "spike_date": spike_date,
        "spike_volume": spike_volume,
        "spike_price_change_pct": spike_price_change_pct / 100.0 if not pd.isna(spike_price_change_pct) else math.nan,
        "spike_window_days": spike_window_days,
        "spike_top1_threshold": spike_top1_threshold,
        "spike_label": spike_label,
        "score_spike_base": spike_base_points,
        "score_spike_bonus": spike_bonus_points,
        "score_spike_total": spike_total_points,
        "gapup_date": gapup_date,
        "gapup_volume": gapup_volume,
        "gapup_pct": gapup_pct / 100.0 if not pd.isna(gapup_pct) else math.nan,
        "gapup_close_pct": gapup_close_pct / 100.0 if not pd.isna(gapup_close_pct) else math.nan,
        "gapup_top1_threshold": gapup_top1_threshold,
        "score_gapup": score_gapup,
        "score_new_listing": score_new_listing,
    }


def top_n_threshold(series: pd.Series, pct: float) -> float:
    clean = series.dropna()
    if clean.empty:
        return math.nan
    rank = max(1, math.ceil(len(clean) * (pct / 100.0)))
    return float(clean.sort_values(ascending=False).iloc[rank - 1])


def bottom_n_threshold(series: pd.Series, pct: float) -> float:
    clean = series.dropna()
    if clean.empty:
        return math.nan
    rank = max(1, math.ceil(len(clean) * (pct / 100.0)))
    return float(clean.sort_values(ascending=True).iloc[rank - 1])


def apply_scoring(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    thresholds = {
        "turnover_bottom30": bottom_n_threshold(work["avg_turnover_42d"], 30),
        "median_turnover_top10": top_n_threshold(work["median_turnover_42d"], 10),
        "median_turnover_top30": top_n_threshold(work["median_turnover_42d"], 30),
        "ret12_top10": top_n_threshold(work["return_12m"], 10),
        "ret12_top20": top_n_threshold(work["return_12m"], 20),
        "ret12_top30": top_n_threshold(work["return_12m"], 30),
        "ret6_top10": top_n_threshold(work["return_6m"], 10),
        "ret6_top20": top_n_threshold(work["return_6m"], 20),
        "ret6_top30": top_n_threshold(work["return_6m"], 30),
        "ret6_bottom10": bottom_n_threshold(work["return_6m"], 10),
        "ret3_top10": top_n_threshold(work["return_3m"], 10),
        "ret3_top20": top_n_threshold(work["return_3m"], 20),
        "ret3_top30": top_n_threshold(work["return_3m"], 30),
        "ret3_bottom10": bottom_n_threshold(work["return_3m"], 10),
        "rs12_top10": top_n_threshold(work["rs_12m"], 10),
        "rs12_top30": top_n_threshold(work["rs_12m"], 30),
        "rs3_top10": top_n_threshold(work["rs_3m"], 10),
        "rs3_top30": top_n_threshold(work["rs_3m"], 30),
        "atr_bottom30": bottom_n_threshold(work["atr_percent_21d"], 30),
        "uptrend_consistency_top20": top_n_threshold(work["uptrend_consistency_pct"], 20),
    }

    eligible_52w = work["listed_days"] > 1
    work["score_52w_price"] = np.select(
        [
            eligible_52w & (work["pct_from_52w_high"] <= 0.10),
            eligible_52w & (work["pct_from_52w_high"] <= 0.15),
            eligible_52w & (work["pct_from_52w_high"] <= 0.20),
        ],
        [6, 4, 2],
        default=0,
    )
    work["score_52w_recency"] = np.select(
        [
            eligible_52w & (work["days_since_52w_high"] <= 10),
            eligible_52w & (work["days_since_52w_high"] <= 15),
        ],
        [4, 2],
        default=0,
    )
    work["score_52w_bonus"] = np.select(
        [
            eligible_52w & (work["pct_from_52w_high"] <= 0.10) & (work["days_since_52w_high"] <= 10),
            eligible_52w & (work["pct_from_52w_high"] <= 0.15) & (work["days_since_52w_high"] <= 10),
        ],
        [4, 2],
        default=0,
    )
    work["score_52w_total"] = (
        work["score_52w_price"] + work["score_52w_recency"] + work["score_52w_bonus"]
    )

    work["score_liquidity"] = np.where(
        work["avg_turnover_42d"] <= thresholds["turnover_bottom30"], -8, 0
    )
    work["score_median_turnover"] = np.select(
        [
            work["median_turnover_42d"] >= thresholds["median_turnover_top10"],
            work["median_turnover_42d"] >= thresholds["median_turnover_top30"],
        ],
        [6, 4],
        default=0,
    )

    top12_10 = work["return_12m"] >= thresholds["ret12_top10"]
    top12_20 = work["return_12m"] >= thresholds["ret12_top20"]
    top12_30 = work["return_12m"] >= thresholds["ret12_top30"]
    top6_10 = work["return_6m"] >= thresholds["ret6_top10"]
    top6_20 = work["return_6m"] >= thresholds["ret6_top20"]
    top6_30 = work["return_6m"] >= thresholds["ret6_top30"]
    top3_10 = work["return_3m"] >= thresholds["ret3_top10"]
    top3_20 = work["return_3m"] >= thresholds["ret3_top20"]
    top3_30 = work["return_3m"] >= thresholds["ret3_top30"]
    mature_listing = work["listed_days"] >= LOOKBACK_3M
    bottom6_10 = mature_listing & (work["return_6m"] <= thresholds["ret6_bottom10"])
    bottom3_10 = mature_listing & (work["return_3m"] <= thresholds["ret3_bottom10"])

    work["score_perf_12m"] = np.select([top12_10, top12_20, top12_30], [8, 6, 4], default=0)
    work["score_perf_6m"] = np.select([top6_10, top6_20, top6_30], [6, 4, 2], default=0)
    work["score_perf_3m"] = np.select([top3_10, top3_20, top3_30], [6, 4, 2], default=0)
    work["score_perf_6m_penalty"] = np.where(bottom6_10, -2, 0)
    work["score_perf_3m_penalty"] = np.where(bottom3_10, -4, 0)
    work["score_perf_bonus"] = np.where(top12_10 & (work["days_since_52w_high"] <= 10), 2, 0)
    reset_rank = work["reset_recovery"].rank(method="min", ascending=False)
    work["score_reset_recovery"] = np.select(
        [
            reset_rank <= 10,
            reset_rank <= 20,
        ],
        [4, 2],
        default=0,
    )
    work["score_performance_total"] = (
        work["score_perf_12m"]
        + work["score_perf_6m"]
        + work["score_perf_3m"]
        + work["score_perf_6m_penalty"]
        + work["score_perf_3m_penalty"]
        + work["score_perf_bonus"]
        + work["score_reset_recovery"]
    )

    top_rs12_10 = work["rs_12m"] >= thresholds["rs12_top10"]
    top_rs12_30 = work["rs_12m"] >= thresholds["rs12_top30"]
    top_rs3_10 = work["rs_3m"] >= thresholds["rs3_top10"]
    top_rs3_30 = work["rs_3m"] >= thresholds["rs3_top30"]

    work["score_rs_12m"] = np.select([top_rs12_10, top_rs12_30], [6, 3], default=0)
    work["score_rs_3m"] = np.select([top_rs3_10, top_rs3_30], [4, 2], default=0)
    work["score_rs_line_high"] = np.where(work["rs_line_at_52w_high"], 2, 0)
    work["score_rs_slope_penalty"] = np.where(work["rs_line_slope_21d"] < 0, -2, 0)
    work["score_rs_total"] = (
        work["score_rs_12m"]
        + work["score_rs_3m"]
        + work["score_rs_line_high"]
        + work["score_rs_slope_penalty"]
    )

    work["score_volatility"] = np.where(
        (work["atr_percent_21d"] <= thresholds["atr_bottom30"]) & (work["atr_percent_21d"] < 3.0),
        -6,
        0,
    )
    work["score_uptrend_consistency"] = np.where(
        work["uptrend_consistency_pct"] >= thresholds["uptrend_consistency_top20"],
        4,
        0,
    )

    work["pre_sector_score"] = (
        work["score_52w_total"]
        + work["score_liquidity"]
        + work["score_median_turnover"]
        + work["score_performance_total"]
        + work["score_rs_total"]
        + work["score_volatility"]
        + work["score_uptrend_consistency"]
        + work["score_spike_total"]
        + work["score_gapup"]
        + work["score_new_listing"]
    )
    top_quartile_threshold = top_n_threshold(work["pre_sector_score"], 25)
    work["is_top_quartile_pre_sector"] = work["pre_sector_score"] >= top_quartile_threshold

    sector_stats = (
        work.groupby("sector")
        .agg(
            sector_stock_count=("symbol", "size"),
            sector_top_quartile_count=("is_top_quartile_pre_sector", "sum"),
        )
        .reset_index()
    )
    sector_stats["sector_strength_ratio"] = np.where(
        sector_stats["sector_stock_count"] > 0,
        sector_stats["sector_top_quartile_count"] / sector_stats["sector_stock_count"],
        0.0,
    )
    sector_stats.loc[sector_stats["sector_stock_count"] <= 1, "sector_strength_ratio"] = -1.0
    sector_top10 = top_n_threshold(sector_stats["sector_strength_ratio"], 10)
    sector_top30 = top_n_threshold(sector_stats["sector_strength_ratio"], 30)
    sector_stats["score_sector"] = np.select(
        [
            (sector_stats["sector_stock_count"] > 1) & (sector_stats["sector_strength_ratio"] >= sector_top10),
            (sector_stats["sector_stock_count"] > 1) & (sector_stats["sector_strength_ratio"] >= sector_top30),
        ],
        [SECTOR_TOP10_POINTS, SECTOR_TOP30_POINTS],
        default=0,
    )
    work = work.merge(sector_stats, on="sector", how="left")

    work["composite_score"] = work["pre_sector_score"] + work["score_sector"]
    work["rank"] = work["composite_score"].rank(method="min", ascending=False).astype(int)
    work = work.sort_values(["composite_score", "symbol"], ascending=[False, True]).reset_index(drop=True)

    for name, value in thresholds.items():
        work[name] = value
    work["sector_top10_threshold"] = sector_top10
    work["sector_top30_threshold"] = sector_top30
    work["top_quartile_score_threshold"] = top_quartile_threshold
    return work


def percent_or_blank(value: object) -> object:
    if pd.isna(value):
        return ""
    return round(float(value) * 100.0, 2)


def date_or_blank(value: object) -> object:
    if pd.isna(value) or value is None:
        return ""
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def round_or_blank(value: object, digits: int = 2) -> object:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, float, np.integer, np.floating)):
        return round(float(value), digits)
    return value


def force_close_excel_workbook(path: Path) -> None:
    if not path.exists():
        return

    try:
        path.unlink()
        return
    except PermissionError:
        pass

    print(f"Workbook is open in Excel, attempting to close it: {path.name}")
    closed = False

    try:
        import win32com.client  # type: ignore

        excel = win32com.client.GetActiveObject("Excel.Application")
        for workbook in list(excel.Workbooks):
            if os.path.normcase(str(workbook.FullName)) == os.path.normcase(str(path)):
                workbook.Close(SaveChanges=False)
                closed = True
                print("Closed workbook via Excel COM")
                break
    except Exception:
        pass

    if not closed:
        try:
            escaped = str(path).replace("'", "''")
            ps = (
                "$xl = [Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application'); "
                f"$xl.Workbooks | Where-Object {{ $_.FullName -eq '{escaped}' }} | "
                "ForEach-Object { $_.Close($false) }"
            )
            subprocess.run(
                ["powershell", "-Command", ps],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            closed = True
            print("Closed workbook via PowerShell COM")
        except Exception:
            pass

    time.sleep(0.5)
    try:
        path.unlink()
    except PermissionError as exc:
        raise SystemExit(f"Could not overwrite open workbook: {path}") from exc


def export_tradingview_dayone(scored: pd.DataFrame, output_dir: Path, cutoff_date: date) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"dayone_{cutoff_date.strftime('%d%m')}.txt"
    lines = [f"NSE:{str(symbol).strip().upper().replace('NSE:', '')}" for symbol in scored.head(20)["symbol"].tolist()]
    out_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return out_path


def export_workbook(
    scored: pd.DataFrame,
    warnings: List[WarningLog],
    output_dir: Path,
    cutoff_date: date,
    reset_date: date,
    symbol_file: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"stock_rating_{cutoff_date.strftime('%d%b%Y')}.xlsx"
    force_close_excel_workbook(out_path)

    wb = Workbook()
    ws = wb.active
    ws.title = "Ratings"

    title_fill = PatternFill("solid", fgColor="1F4E78")
    header_fill = PatternFill("solid", fgColor="D9EAF7")
    good_fill = PatternFill("solid", fgColor="E2F0D9")
    bad_fill = PatternFill("solid", fgColor="FCE4D6")
    outcome_fill = PatternFill("solid", fgColor="1F4E78")
    liquidity_fill = PatternFill("solid", fgColor="CFE2F3")
    high52_fill = PatternFill("solid", fgColor="D9EAD3")
    perf_fill = PatternFill("solid", fgColor="FCE5CD")
    rs_fill = PatternFill("solid", fgColor="EAD1DC")
    spike_fill = PatternFill("solid", fgColor="FFF2CC")
    vol_fill = PatternFill("solid", fgColor="F4CCCC")
    sector_fill = PatternFill("solid", fgColor="D9D2E9")

    ws["A1"] = f"Stock Rating Report | cutoff={cutoff_date.isoformat()} | reset={reset_date.isoformat()} | symbols={symbol_file.name}"
    ws["A1"].font = Font(bold=True, color="FFFFFF")
    ws["A1"].fill = title_fill

    columns = [
        "rank",
        "symbol",
        "sector",
        "composite_score",
        "pre_sector_score",
        "score_sector",
        "score_new_listing",
        "current_close",
        "listing_date",
        "listed_days",
        "high_52w_close",
        "high_52w_date",
        "pct_from_52w_high",
        "days_since_52w_high",
        "score_52w_price",
        "score_52w_recency",
        "score_52w_bonus",
        "score_52w_total",
        "avg_turnover_42d",
        "median_turnover_42d",
        "score_liquidity",
        "score_median_turnover",
        "return_12m",
        "return_6m",
        "return_3m",
        "reset_low",
        "reset_recovery",
        "score_reset_recovery",
        "score_perf_12m",
        "score_perf_6m",
        "score_perf_3m",
        "score_perf_6m_penalty",
        "score_perf_3m_penalty",
        "score_perf_bonus",
        "score_performance_total",
        "rs_12m",
        "rs_6m",
        "rs_3m",
        "rs_line_at_52w_high",
        "rs_line_slope_21d",
        "score_rs_12m",
        "score_rs_3m",
        "score_rs_line_high",
        "score_rs_slope_penalty",
        "score_rs_total",
        "uptrend_consistency_pct",
        "score_uptrend_consistency",
        "spike_date",
        "spike_volume",
        "spike_price_change_pct",
        "spike_window_days",
        "spike_label",
        "score_spike_base",
        "score_spike_bonus",
        "score_spike_total",
        "gapup_date",
        "gapup_volume",
        "gapup_pct",
        "gapup_close_pct",
        "score_gapup",
        "atr_percent_21d",
        "score_volatility",
        "sector_stock_count",
        "sector_top_quartile_count",
        "sector_strength_ratio",
    ]
    ws.merge_cells(f"A1:{get_column_letter(len(columns))}1")

    ws.freeze_panes = "E2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(columns))}{max(2, len(scored) + 1)}"

    for col_idx, column in enumerate(columns, start=1):
        cell = ws.cell(row=2, column=col_idx, value=column)
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row_idx, (_, row) in enumerate(scored.iterrows(), start=3):
        for col_idx, column in enumerate(columns, start=1):
            value = row[column]
            if column in {
                "pct_from_52w_high",
                "return_12m",
                "return_6m",
                "return_3m",
                "reset_recovery",
                "rs_12m",
                "rs_6m",
                "rs_3m",
                "uptrend_consistency_pct",
                "spike_price_change_pct",
                "gapup_pct",
                "gapup_close_pct",
                "sector_strength_ratio",
            }:
                value = percent_or_blank(value)
            elif column in {"listing_date", "high_52w_date", "reset_date_used", "spike_date", "gapup_date"}:
                value = date_or_blank(value)
            elif isinstance(value, (int, float, np.integer, np.floating)) and column not in {"rank", "listed_days", "days_since_52w_high", "sector_stock_count", "sector_top_quartile_count"}:
                value = round_or_blank(value)
            ws.cell(row=row_idx, column=col_idx, value=value)

    for cell in ws["A"][1:]:
        cell.alignment = Alignment(horizontal="center")

    for col_cells in ws.columns:
        length = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(length + 2, 12), 24)

    for row_idx in range(3, ws.max_row + 1):
        total_cell = ws.cell(row=row_idx, column=columns.index("composite_score") + 1)
        total_cell.fill = good_fill if (total_cell.value or 0) >= 0 else bad_fill
        total_cell.font = Font(bold=True)
        ws.cell(row=row_idx, column=1).font = Font(bold=True)
        ws.cell(row=row_idx, column=2).font = Font(bold=True)
        ws.cell(row=row_idx, column=4).font = Font(bold=True)

    dashboard = wb.create_sheet("Dashboard", 0)
    dashboard["A1"] = f"Top 20 Dashboard | cutoff={cutoff_date.isoformat()}"
    dashboard["A1"].font = Font(bold=True, color="FFFFFF")
    dashboard["A1"].fill = title_fill
    dashboard.merge_cells("A1:L1")
    dashboard_columns = [
        "rank",
        "symbol",
        "sector",
        "composite_score",
        "score_new_listing",
        "current_close",
        "avg_turnover_42d",
        "median_turnover_42d",
        "score_uptrend_consistency",
        "score_spike_total",
        "score_gapup",
        "score_sector",
    ]
    for idx, column in enumerate(dashboard_columns, start=1):
        cell = dashboard.cell(row=2, column=idx, value=column)
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
    top20 = scored.head(20)
    for row_idx, (_, row) in enumerate(top20.iterrows(), start=3):
        for col_idx, column in enumerate(dashboard_columns, start=1):
            value = row[column]
            if isinstance(value, (int, float, np.integer, np.floating)) and column not in {"rank"}:
                value = round_or_blank(value)
            dashboard.cell(row=row_idx, column=col_idx, value=value)
        score_cell = dashboard.cell(row=row_idx, column=4)
        score_cell.font = Font(bold=True)
        score_cell.fill = good_fill if (score_cell.value or 0) >= 20 else header_fill if (score_cell.value or 0) >= 10 else bad_fill
    dashboard.freeze_panes = "D3"
    dashboard.auto_filter.ref = f"A2:L{max(2, len(top20) + 2)}"
    for col_cells in dashboard.columns:
        length = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        dashboard.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(length + 2, 12), 24)

    scorecard = wb.create_sheet("Scorecard")
    scorecard["A1"] = f"Scorecard | cutoff={cutoff_date.isoformat()}"
    scorecard["A1"].font = Font(bold=True, color="FFFFFF")
    scorecard["A1"].fill = title_fill
    scorecard.merge_cells("A1:Q1")
    scorecard_columns = [
        "rank",
        "symbol",
        "sector",
        "score_new_listing",
        "score_52w_total",
        "score_performance_total",
        "score_reset_recovery",
        "score_perf_6m_penalty",
        "score_perf_3m_penalty",
        "score_rs_total",
        "score_uptrend_consistency",
        "score_spike_total",
        "score_gapup",
        "score_volatility",
        "score_liquidity",
        "score_sector",
        "composite_score",
    ]
    for idx, column in enumerate(scorecard_columns, start=1):
        cell = scorecard.cell(row=2, column=idx, value=column)
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for row_idx, (_, row) in enumerate(scored.iterrows(), start=3):
        for col_idx, column in enumerate(scorecard_columns, start=1):
            value = row[column]
            if isinstance(value, (int, float, np.integer, np.floating)) and column != "rank":
                value = round_or_blank(value)
            scorecard.cell(row=row_idx, column=col_idx, value=value)
        scorecard.cell(row=row_idx, column=2).font = Font(bold=True)
        scorecard.cell(row=row_idx, column=17).font = Font(bold=True)
    scorecard.freeze_panes = "D3"
    scorecard.auto_filter.ref = f"A2:Q{max(2, len(scored) + 2)}"
    for col_cells in scorecard.columns:
        length = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        scorecard.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(length + 2, 12), 24)

    summary = wb.create_sheet("Summary")
    summary_rows = [
        ("Metric", "Value"),
        ("Cutoff Date", cutoff_date.isoformat()),
        ("Reset Date", reset_date.isoformat()),
        ("Scoring Window (12M / 52W)", "250 trading candles including cutoff"),
        ("Scoring Window (6M)", "125 trading candles including cutoff"),
        ("Scoring Window (3M)", "60 trading candles including cutoff"),
        ("Turnover Unit", "Crores"),
        ("Minimum Median Turnover Filter", f"{MIN_MEDIAN_TURNOVER_CRORES:.2f}"),
        ("Sector Eligibility", "Sector must contain more than 1 stock to qualify for top-sector scoring"),
        ("52W High Basis", "Daily high, not close"),
        ("Symbols Rated", int(len(scored))),
        ("Top Score", round_or_blank(scored["composite_score"].max()) if not scored.empty else ""),
        ("Bottom Score", round_or_blank(scored["composite_score"].min()) if not scored.empty else ""),
        ("Liquidity Bottom 30 Threshold", round_or_blank(scored["turnover_bottom30"].iloc[0]) if not scored.empty else ""),
        ("ATR% Bottom 30 Threshold", round_or_blank(scored["atr_bottom30"].iloc[0]) if not scored.empty else ""),
        ("Index Return 12M", percent_or_blank(scored["index_return_12m"].iloc[0]) if not scored.empty else ""),
        ("Index Return 6M", percent_or_blank(scored["index_return_6m"].iloc[0]) if not scored.empty else ""),
        ("Index Return 3M", percent_or_blank(scored["index_return_3m"].iloc[0]) if not scored.empty else ""),
        ("12M Top 10 Threshold", percent_or_blank(scored["ret12_top10"].iloc[0]) if not scored.empty else ""),
        ("12M Top 20 Threshold", percent_or_blank(scored["ret12_top20"].iloc[0]) if not scored.empty else ""),
        ("12M Top 30 Threshold", percent_or_blank(scored["ret12_top30"].iloc[0]) if not scored.empty else ""),
        ("6M Top 10 Threshold", percent_or_blank(scored["ret6_top10"].iloc[0]) if not scored.empty else ""),
        ("6M Top 20 Threshold", percent_or_blank(scored["ret6_top20"].iloc[0]) if not scored.empty else ""),
        ("6M Top 30 Threshold", percent_or_blank(scored["ret6_top30"].iloc[0]) if not scored.empty else ""),
        ("3M Top 10 Threshold", percent_or_blank(scored["ret3_top10"].iloc[0]) if not scored.empty else ""),
        ("3M Top 20 Threshold", percent_or_blank(scored["ret3_top20"].iloc[0]) if not scored.empty else ""),
        ("3M Top 30 Threshold", percent_or_blank(scored["ret3_top30"].iloc[0]) if not scored.empty else ""),
        ("RS 12M Top 10 Threshold", percent_or_blank(scored["rs12_top10"].iloc[0]) if not scored.empty else ""),
        ("RS 12M Top 30 Threshold", percent_or_blank(scored["rs12_top30"].iloc[0]) if not scored.empty else ""),
        ("RS 3M Top 10 Threshold", percent_or_blank(scored["rs3_top10"].iloc[0]) if not scored.empty else ""),
        ("RS 3M Top 30 Threshold", percent_or_blank(scored["rs3_top30"].iloc[0]) if not scored.empty else ""),
        ("Uptrend Consistency Top 20 Threshold", percent_or_blank(scored["uptrend_consistency_top20"].iloc[0]) if not scored.empty else ""),
        ("Pre-Sector Top Quartile Threshold", round_or_blank(scored["top_quartile_score_threshold"].iloc[0]) if not scored.empty else ""),
        ("Sector Strength Top 10 Threshold", round_or_blank(scored["sector_top10_threshold"].iloc[0]) if not scored.empty else ""),
        ("Sector Strength Top 30 Threshold", round_or_blank(scored["sector_top30_threshold"].iloc[0]) if not scored.empty else ""),
        ("Sector Scoring Assumption", "Top 10% sectors by top-quartile concentration = +4; top 30% = +2"),
    ]
    for row_idx, row in enumerate(summary_rows, start=1):
        summary.append(row)
        if row_idx == 1:
            for cell in summary[row_idx]:
                cell.font = Font(bold=True)
                cell.fill = header_fill
    summary.auto_filter.ref = f"A1:B{len(summary_rows)}"
    summary.column_dimensions["A"].width = 36
    summary.column_dimensions["B"].width = 40

    warning_ws = wb.create_sheet("Warnings")
    warning_ws.append(["Symbol", "Message"])
    warning_ws["A1"].font = Font(bold=True)
    warning_ws["B1"].font = Font(bold=True)
    warning_ws["A1"].fill = header_fill
    warning_ws["B1"].fill = header_fill
    for item in warnings:
        warning_ws.append([item.symbol, item.message])
    warning_ws.auto_filter.ref = f"A1:B{max(1, len(warnings) + 1)}"
    warning_ws.column_dimensions["A"].width = 16
    warning_ws.column_dimensions["B"].width = 90

    sector_ws = wb.create_sheet("Sector Summary")
    sector_columns = [
        "sector",
        "sector_stock_count",
        "sector_top_quartile_count",
        "sector_strength_ratio",
        "score_sector",
        "best_symbol",
        "best_composite_score",
        "avg_composite_score",
        "count_above_20_score",
        "count_top_20_names",
        "median_sector_score",
    ]
    sector_ws.append(sector_columns)
    for cell in sector_ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    sector_summary = (
        scored.sort_values(["sector", "composite_score", "symbol"], ascending=[True, False, True])
        .groupby("sector", as_index=False)
        .agg(
            sector_stock_count=("sector_stock_count", "max"),
            sector_top_quartile_count=("sector_top_quartile_count", "max"),
            sector_strength_ratio=("sector_strength_ratio", "max"),
            score_sector=("score_sector", "max"),
            best_symbol=("symbol", "first"),
            best_composite_score=("composite_score", "max"),
            avg_composite_score=("composite_score", "mean"),
            count_above_20_score=("composite_score", lambda s: int((s >= 20).sum())),
            count_top_20_names=("rank", lambda s: int((s <= 20).sum())),
            median_sector_score=("composite_score", "median"),
        )
        .sort_values(["score_sector", "sector_strength_ratio", "avg_composite_score", "sector"], ascending=[False, False, False, True])
    )

    for _, row in sector_summary.iterrows():
        sector_ws.append(
            [
                row["sector"],
                int(row["sector_stock_count"]),
                int(row["sector_top_quartile_count"]),
                round_or_blank(float(row["sector_strength_ratio"]) * 100.0),
                round_or_blank(row["score_sector"]),
                row["best_symbol"],
                round_or_blank(row["best_composite_score"]),
                round_or_blank(row["avg_composite_score"]),
                int(row["count_above_20_score"]),
                int(row["count_top_20_names"]),
                round_or_blank(row["median_sector_score"]),
            ]
        )

    for col_cells in sector_ws.columns:
        length = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
        sector_ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(length + 2, 12), 24)
    sector_ws.auto_filter.ref = f"A1:{get_column_letter(len(sector_columns))}{max(1, len(sector_summary) + 1)}"

    wb.save(out_path)
    return out_path


def main() -> int:
    args = parse_args()

    symbol_file = locate_symbol_file(args.cutoff_date, args.symbols, args.symbols_dir)
    symbols = read_symbols(symbol_file)
    token_file = Path(args.token).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    start_date = min(args.reset_date, args.cutoff_date - timedelta(days=450))
    warnings: List[WarningLog] = []

    print(f"Using symbol file: {symbol_file}")
    print(f"Loaded {len(symbols)} symbols")

    kite = get_kite_client(token_file)
    instrument_map = build_instrument_map(kite, symbols)

    conn = get_db_connection()
    try:
        print("Loading sector and index data from MySQL")
        sector_map = load_sector_map(conn, symbols)
        index_df = load_index_history(conn, start_date, args.cutoff_date, args.index_symbol)
        turnover_map = load_turnover_map(
            conn,
            symbols,
            max(args.cutoff_date - timedelta(days=120), date(args.cutoff_date.year - 1, 1, 1)),
            args.cutoff_date,
        )
    finally:
        conn.close()

    symbols = filter_symbols_by_turnover(
        symbols,
        turnover_map,
        MIN_MEDIAN_TURNOVER_CRORES,
        warnings,
    )
    if not symbols:
        raise SystemExit(
            f"No symbols remain after applying median turnover filter of {MIN_MEDIAN_TURNOVER_CRORES:.2f} Cr."
        )
    print(f"Eligible after median turnover filter: {len(symbols)} symbols")

    rows: List[Dict[str, object]] = []
    for idx, symbol in enumerate(symbols, start=1):
        print(f"[{idx}/{len(symbols)}] {symbol}")
        instrument_info = instrument_map.get(symbol)
        if instrument_info is None:
            warnings.append(WarningLog(symbol, "Symbol not found in kite.instruments('NSE'); skipped."))
            continue
        token = instrument_info["instrument_token"]
        listing_date = instrument_info.get("listing_date")
        history = fetch_history_with_retry(kite, token, symbol, start_date, args.cutoff_date)
        if history is None or history.empty:
            warnings.append(WarningLog(symbol, "No historical data returned by Kite; skipped."))
            continue
        row = compute_stock_metrics(
            symbol=symbol,
            df=history,
            index_df=index_df,
            sector=sector_map.get(symbol, "Unknown"),
            cutoff_date=args.cutoff_date,
            reset_date=args.reset_date,
            listing_date=listing_date,
            turnover_override=turnover_map.get(symbol.upper()),
            warnings=warnings,
        )
        if row is not None:
            rows.append(row)

    if not rows:
        raise SystemExit("No stocks could be rated. Check API/data availability.")

    scored = apply_scoring(pd.DataFrame(rows))
    report_path = export_workbook(
        scored=scored,
        warnings=warnings,
        output_dir=output_dir,
        cutoff_date=args.cutoff_date,
        reset_date=args.reset_date,
        symbol_file=symbol_file,
    )
    dayone_path = export_tradingview_dayone(scored, output_dir, args.cutoff_date)

    print(f"Rated {len(scored)} stocks")
    print(f"Workbook written to: {report_path}")
    print(f"TradingView file written to: {dayone_path}")
    if warnings:
        print(f"Warnings logged: {len(warnings)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
