#!/usr/bin/env python3
"""
8-EMA Uptrend Screener — NSE stocks
Analysis date : 02 April 2026  (no data after this date is used)

REQUIREMENTS
    pip install kiteconnect pandas

RUN
    python 8ema_uptrend_02apr2026.py
"""

import pandas as pd
from datetime import date
from kiteconnect import KiteConnect

# ── Credentials ──────────────────────────────────────────────────────
API_KEY      = "enxrvpfmkswonhxh"
ACCESS_TOKEN = "KBXNOVUcDHcFX4s008W37s8TwFvo4Psg"

# ── Stock universe ───────────────────────────────────────────────────
STOCKS = [
    "PFOCUS", "MTARTECH", "STLTECH", "NATIONALUM", "GVT&D",
    "POWERINDIA", "BLISSGVS", "ATHERENERG", "AEROFLEX", "DEEDEV",
    "OMNI", "QPOWER", "APOLLOPIPE", "AVANTIFEED", "NETWEB",
    "TDPOWERSYS", "DATAPATTNS", "GESHIP", "BAJAJCON", "GMDCLTD",
]

# ── Date range ───────────────────────────────────────────────────────
ANALYSIS_DATE = date(2026, 4, 2)   # <-- hard ceiling, NO data after this
FROM_DATE     = date(2025, 12, 1)  # enough history for a stable 8EMA

# ─────────────────────────────────────────────────────────────────────
def calc_ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def main():
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    # ── Build instrument-token lookup ─────────────────────────────
    print("Downloading NSE instrument list …")
    inst_df  = pd.DataFrame(kite.instruments("NSE"))
    token_map = dict(zip(inst_df["tradingsymbol"], inst_df["instrument_token"]))

    results, errors = [], []

    for symbol in STOCKS:
        token = token_map.get(symbol)
        if not token:
            print(f"  ✗  {symbol:15s} — token NOT FOUND in NSE instruments")
            errors.append((symbol, "Token not found"))
            continue

        try:
            candles = kite.historical_data(
                instrument_token=token,
                from_date=FROM_DATE,
                to_date=ANALYSIS_DATE,   # strictly ≤ 02 Apr 2026
                interval="day",
            )
        except Exception as exc:
            print(f"  ✗  {symbol:15s} — API error: {exc}")
            errors.append((symbol, str(exc)))
            continue

        if not candles:
            print(f"  ✗  {symbol:15s} — no candle data returned")
            errors.append((symbol, "No data"))
            continue

        df = pd.DataFrame(candles)
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Safety: drop anything beyond ANALYSIS_DATE
        df = df[df["date"].dt.date <= ANALYSIS_DATE]

        if len(df) < 10:
            print(f"  ✗  {symbol:15s} — too few candles ({len(df)})")
            errors.append((symbol, f"Only {len(df)} candles"))
            continue

        # ── 8-EMA ─────────────────────────────────────────────────
        df["ema8"] = calc_ema(df["close"], span=8)

        last  = df.iloc[-1]
        prev  = df.iloc[-2]

        close     = last["close"]
        ema8_now  = last["ema8"]
        ema8_prev = prev["ema8"]
        last_date = last["date"].date()

        # Uptrend = price above 8EMA  AND  8EMA is rising
        above_ema  = close    > ema8_now
        ema_rising = ema8_now > ema8_prev
        uptrend    = above_ema and ema_rising

        tag = "✅ UPTREND" if uptrend else "❌ no uptrend"
        print(
            f"  {tag:14s}  {symbol:15s}  "
            f"close={close:>9.2f}  8EMA={ema8_now:>9.2f}  "
            f"prev8EMA={ema8_prev:>9.2f}  [{last_date}]"
        )

        results.append({
            "Symbol"       : symbol,
            "Last_Date"    : str(last_date),
            "Close"        : round(close,    2),
            "8EMA"         : round(ema8_now, 2),
            "8EMA_Prev"    : round(ema8_prev,2),
            "Close_>_8EMA" : above_ema,
            "8EMA_Rising"  : ema_rising,
            "8EMA_Uptrend" : uptrend,
        })

    # ── Print summary ─────────────────────────────────────────────
    print("\n" + "=" * 68)
    print("  8-EMA UPTREND RESULTS  (data up to 02 Apr 2026)")
    print("=" * 68)

    up   = [r for r in results if r["8EMA_Uptrend"]]
    down = [r for r in results if not r["8EMA_Uptrend"]]

    print(f"\n  ✅  IN UPTREND  ({len(up)} stocks)")
    for r in up:
        print(f"      {r['Symbol']:15s}  close={r['Close']:>9.2f}  8EMA={r['8EMA']:>9.2f}")

    print(f"\n  ❌  NOT in uptrend  ({len(down)} stocks)")
    for r in down:
        ema_dir = "EMA▲" if r["8EMA_Rising"] else "EMA▼"
        price_pos = "above EMA" if r["Close_>_8EMA"] else "below EMA"
        print(f"      {r['Symbol']:15s}  close={r['Close']:>9.2f}  8EMA={r['8EMA']:>9.2f}"
              f"  ({ema_dir}, {price_pos})")

    if errors:
        print(f"\n  ⚠  Errors / no data  ({len(errors)} stocks)")
        for sym, msg in errors:
            print(f"      {sym}: {msg}")

    # ── Save CSV ──────────────────────────────────────────────────
    if results:
        out = pd.DataFrame(results)
        out.to_csv("8ema_uptrend_results.csv", index=False)
        print("\n  Results saved to: 8ema_uptrend_results.csv")

    print()


if __name__ == "__main__":
    main()
