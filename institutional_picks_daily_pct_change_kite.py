#!/usr/bin/env python3
"""
Generate daily % change report for institutional picks using Zerodha Kite.

Input format (matches your generated files):
  NSE:VEDL
  NSE:GVT&D

Output:
  reports/institutional_picks_daily_pct_change_<start>_to_<end>.csv
  reports/institutional_picks_daily_pct_change_<start>_to_<end>.xlsx
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


def read_kite_token_file(token_file: Path) -> Dict[str, str]:
    if not token_file.exists():
        raise SystemExit(f"Missing token file: {token_file}")

    values: Dict[str, str] = {}
    for raw_line in token_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        values[k.strip().upper()] = v.strip()

    if "API_KEY" not in values or "ACCESS_TOKEN" not in values:
        raise SystemExit(f"{token_file} must contain API_KEY and ACCESS_TOKEN (key=value lines).")
    return values


def normalize_symbol_for_matching(symbol: str) -> str:
    """
    Best-effort normalization to match Kite tradingsymbol variants.

    Examples:
      GVT&D -> GVTANDD
      POWER-INDIA -> POWERINDIA
    """
    s = symbol.upper().strip()
    s = s.replace("&", "AND")
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


def parse_institutional_picks(txt_path: Path) -> List[Tuple[str, str]]:
    """
    Returns list of (exchange, symbol) pairs as present in the txt file.
    """
    if not txt_path.exists():
        raise SystemExit(f"Input file not found: {txt_path}")

    pairs: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    for raw_line in txt_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("###"):
            continue
        if ":" not in line:
            # Assume NSE equity symbol if no exchange provided
            exch, sym = "NSE", line
        else:
            exch, sym = line.split(":", 1)
        exch = exch.strip().upper()
        sym = sym.strip().upper()
        if not sym:
            continue
        key = (exch, sym)
        if key not in seen:
            pairs.append(key)
            seen.add(key)

    if not pairs:
        raise SystemExit(f"No symbols found in: {txt_path}")

    return pairs


def get_kite_client(token_file: Path):
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        raise SystemExit("kiteconnect not installed. Run: pip install kiteconnect")

    creds = read_kite_token_file(token_file)
    kite = KiteConnect(api_key=creds["API_KEY"])
    kite.set_access_token(creds["ACCESS_TOKEN"])
    return kite


def build_instrument_token_lookup(
    kite,
    exchange: str,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Returns:
      token_by_tradingsymbol_upper: {TRADINGSYMBOL_UPPER -> instrument_token}
      token_by_normalized_symbol:  {normalize(tradingsymbol) -> instrument_token}
    """
    instruments = pd.DataFrame(kite.instruments(exchange))
    if instruments.empty:
        raise SystemExit(f"kite.instruments({exchange!r}) returned no rows.")

    # Keep only real equity symbols when the fields exist.
    if "segment" in instruments.columns:
        # Most NSE equities are under segment == "NSE"
        instruments = instruments[instruments["segment"] == exchange]
    if "instrument_type" in instruments.columns:
        instruments = instruments[instruments["instrument_type"] == "EQ"]

    instruments["tradingsymbol"] = instruments["tradingsymbol"].astype(str)
    instruments["tradingsymbol_upper"] = instruments["tradingsymbol"].str.upper()
    instruments["tradingsymbol_norm"] = instruments["tradingsymbol_upper"].map(normalize_symbol_for_matching)

    token_by_tradingsymbol_upper: Dict[str, int] = {}
    token_by_normalized_symbol: Dict[str, int] = {}

    for _, row in instruments.iterrows():
        tsu = row["tradingsymbol_upper"]
        token = int(row["instrument_token"])
        token_by_tradingsymbol_upper.setdefault(tsu, token)
        token_by_normalized_symbol.setdefault(row["tradingsymbol_norm"], token)

    return token_by_tradingsymbol_upper, token_by_normalized_symbol


def resolve_instrument_token(
    token_by_tradingsymbol_upper: Dict[str, int],
    token_by_normalized_symbol: Dict[str, int],
    raw_symbol: str,
) -> Tuple[Optional[int], Optional[str]]:
    sym_upper = raw_symbol.upper().strip()
    if sym_upper in token_by_tradingsymbol_upper:
        return token_by_tradingsymbol_upper[sym_upper], sym_upper

    sym_norm = normalize_symbol_for_matching(sym_upper)
    if sym_norm in token_by_normalized_symbol:
        return token_by_normalized_symbol[sym_norm], sym_norm

    return None, None


def fetch_close_history(
    kite,
    instrument_token: int,
    fetch_from: date,
    fetch_to: date,
) -> pd.DataFrame:
    """
    Fetch daily OHLCV and return sorted DataFrame with columns: date, close.
    """
    rows = kite.historical_data(
        instrument_token=instrument_token,
        from_date=datetime.combine(fetch_from, datetime.min.time()),
        to_date=datetime.combine(fetch_to, datetime.min.time()),
        interval="day",
        continuous=False,
        oi=False,
    )
    if not rows:
        return pd.DataFrame(columns=["date", "close"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[["date", "close"]].dropna(subset=["close"])
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Institutional picks daily % change via Kite.")
    parser.add_argument(
        "--input",
        default=str(Path("reports") / "institutional_picks_02apr2026.txt"),
        help="Path to institutional picks txt file.",
    )
    parser.add_argument("--start", default="2026-04-03", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD). Default: today.")
    parser.add_argument(
        "--token-file",
        default="kite_token.txt",
        help="Path to kite_token.txt (contains API_KEY and ACCESS_TOKEN).",
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.35, help="Delay between Kite calls.")
    args = parser.parse_args()

    txt_path = Path(args.input)
    pairs = parse_institutional_picks(txt_path)

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = (
        datetime.strptime(args.end, "%Y-%m-%d").date()
        if args.end
        else datetime.now().date()
    )
    if end_date < start_date:
        raise SystemExit("--end must be >= --start")

    token_file = Path(args.token_file)

    kite = get_kite_client(token_file)

    # Fetch window:
    # Need at least one prior close to compute pct_change for the first output day.
    fetch_from = start_date - timedelta(days=7)
    fetch_to = end_date

    exchanges = sorted({exch for exch, _ in pairs})
    token_lookups: Dict[str, Tuple[Dict[str, int], Dict[str, int]]] = {}
    for exch in exchanges:
        print(f"Building instrument lookup for {exch} ...")
        token_by_ts_upper, token_by_norm = build_instrument_token_lookup(kite, exch)
        token_lookups[exch] = (token_by_ts_upper, token_by_norm)

    all_long_rows: List[pd.DataFrame] = []
    missing_symbols: List[str] = []

    for exch, sym in pairs:
        token_by_ts_upper, token_by_norm = token_lookups[exch]
        token, resolved = resolve_instrument_token(token_by_ts_upper, token_by_norm, sym)
        if token is None:
            missing_symbols.append(f"{exch}:{sym}")
            continue

        print(f"Fetching {exch}:{sym} (token={token}) ...")
        df = fetch_close_history(kite, token, fetch_from, fetch_to)
        if df.empty or len(df) < 2:
            # Not enough data to compute any daily % change
            continue

        df["pct_change"] = df["close"].pct_change() * 100.0
        df = df[df["date"] >= start_date].copy()
        if df.empty:
            continue

        out = df[["date", "pct_change"]].copy()
        out["symbol"] = f"{exch}:{sym}"
        all_long_rows.append(out)

        time.sleep(args.sleep_seconds)

    if not all_long_rows:
        raise SystemExit("No data returned for any symbol.")

    long_df = pd.concat(all_long_rows, ignore_index=True)
    long_df["symbol"] = long_df["symbol"].astype(str)

    # Pivot: rows=dates, columns=symbols.
    pivot = long_df.pivot_table(
        index="date",
        columns="symbol",
        values="pct_change",
        aggfunc="first",
    ).sort_index()
    pivot.index.name = "date"

    out_dir = Path("reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    start_tag = start_date.strftime("%d%b%Y").lower()  # 03apr2026
    end_tag = end_date.strftime("%d%b%Y").lower()
    stem = f"institutional_picks_daily_pct_change_{start_tag}_to_{end_tag}"
    csv_path = out_dir / f"{stem}.csv"
    xlsx_path = out_dir / f"{stem}.xlsx"

    pivot.reset_index().to_csv(csv_path, index=False)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        pivot.reset_index().to_excel(writer, index=False, sheet_name="daily_pct_change")
        long_df.sort_values(["date", "symbol"]).to_excel(writer, index=False, sheet_name="long_format")

    print(f"\nDone.")
    # Use ASCII arrows to avoid console encoding issues on Windows (cp1252).
    print(f"CSV  -> {csv_path}")
    print(f"Excel-> {xlsx_path}")
    if missing_symbols:
        print(f"Missing (not found in Kite instruments): {', '.join(missing_symbols)}")


if __name__ == "__main__":
    main()

