#!/usr/bin/env python3
"""
Download today's trades from Kite Connect and roll them into a yearly
tradebook CSV in Console-like shape so the dashboard can read one growing file
per year instead of hundreds of daily files.

This only works for the current trading day supported by the active access
token. Historical tradebook still requires Zerodha Console download.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from kiteconnect import KiteConnect

BASE_DIR = Path(__file__).resolve().parent
TOKEN_FILE = BASE_DIR / "kite_token.txt"
OUTPUT_DIR = BASE_DIR / "input" / "tradebook"


def load_kite_creds(token_file: Path) -> Dict[str, str]:
    vals: Dict[str, str] = {}
    for line in token_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        vals[k.strip().upper()] = v.strip()
    required = {"API_KEY", "ACCESS_TOKEN"}
    missing = sorted(required - vals.keys())
    if missing:
        raise RuntimeError(f"Missing keys in {token_file}: {missing}")
    return vals


def normalize_trades(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=[
            "symbol", "isin", "trade_date", "exchange", "segment", "series",
            "trade_type", "auction", "quantity", "price", "trade_id",
            "order_id", "order_execution_time",
        ])

    df = pd.DataFrame(raw).copy()
    df["fill_timestamp"] = pd.to_datetime(df.get("fill_timestamp"), errors="coerce")
    df["exchange_timestamp"] = pd.to_datetime(df.get("exchange_timestamp"), errors="coerce")
    df["trade_date"] = df["fill_timestamp"].dt.date.astype(str)
    df["symbol"] = df["tradingsymbol"].astype(str).str.upper()
    df["trade_type"] = df["transaction_type"].astype(str).str.lower()
    df["segment"] = "EQ"
    df["series"] = "EQ"
    df["auction"] = "false"
    df["isin"] = ""
    df["price"] = pd.to_numeric(df["average_price"], errors="coerce").round(6)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").round(6)
    df["order_execution_time"] = (
        df["exchange_timestamp"]
        .fillna(df["fill_timestamp"])
        .dt.strftime("%Y-%m-%dT%H:%M:%S")
    )

    out = df[[
        "symbol", "isin", "trade_date", "exchange", "segment", "series",
        "trade_type", "auction", "quantity", "price", "trade_id",
        "order_id", "order_execution_time",
    ]].copy()
    out = out.sort_values(["trade_date", "order_execution_time", "trade_id"], kind="stable")
    return out


def merge_into_yearly_file(yearly_path: Path, today_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    columns = list(today_df.columns)
    if yearly_path.exists():
        try:
            existing = pd.read_csv(yearly_path)
        except Exception as exc:
            raise RuntimeError(f"Could not read existing yearly tradebook {yearly_path}: {exc}") from exc
        for col in columns:
            if col not in existing.columns:
                existing[col] = ""
        existing = existing[columns].copy()
    else:
        existing = pd.DataFrame(columns=columns)

    before = len(existing)
    combined = pd.concat([existing, today_df], ignore_index=True)
    combined["trade_id"] = combined["trade_id"].astype(str).str.strip()
    combined["trade_date"] = combined["trade_date"].astype(str).str.strip()
    combined["order_execution_time"] = combined["order_execution_time"].astype(str).str.strip()
    combined = combined.drop_duplicates(subset=["trade_id"], keep="last")
    combined = combined.sort_values(["trade_date", "order_execution_time", "trade_id"], kind="stable")
    added = len(combined) - before
    combined.to_csv(yearly_path, index=False)
    return combined, added


def main() -> int:
    parser = argparse.ArgumentParser(description="Download today's trades from Kite API.")
    parser.add_argument("--token-file", type=Path, default=TOKEN_FILE)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--client-id", type=str, default="")
    parser.add_argument("--write-daily-copy", action="store_true", help="Also write a separate daily snapshot CSV")
    args = parser.parse_args()

    creds = load_kite_creds(args.token_file)
    kite = KiteConnect(api_key=creds["API_KEY"])
    kite.set_access_token(creds["ACCESS_TOKEN"])

    raw = kite.trades()
    df = normalize_trades(raw)
    if df.empty:
        print("No trades returned for the current trading day.")
        return 0

    trade_day = df["trade_date"].iloc[0]
    trade_year = trade_day[:4]
    account_id = args.client_id.strip().upper() or str(pd.DataFrame(raw)["account_id"].iloc[0]).strip().upper()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    yearly_path = args.output_dir / f"tradebook-{account_id}-{trade_year}.csv"
    merged_df, added_rows = merge_into_yearly_file(yearly_path, df)

    print(f"Fetched {len(df)} current-day trade row(s) from Kite API.")
    print(f"Yearly tradebook updated: {yearly_path}")
    print(f"Rows added/updated in yearly file: {added_rows}")
    print(f"Yearly file row count: {len(merged_df)}")
    print(f"Trade date: {trade_day}")

    if args.write_daily_copy:
        daily_path = args.output_dir / f"tradebook-{account_id}-{trade_day.replace('-', '')}_{trade_day.replace('-', '')}_api.csv"
        df.to_csv(daily_path, index=False)
        print(f"Daily snapshot also written: {daily_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
