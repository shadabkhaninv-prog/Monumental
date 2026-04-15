#!/usr/bin/env python3
"""
Load listed mainline IPO reference data into bhav.ipobhav.

Stored columns:
    SYMBOL
    LISTING_DATE
    LISTING_OPEN
    LISTING_CLOSE
    ISSUE_PRICE

Data sources used:
    - Moneycontrol listed mainline IPO API for listing open/close and issue price
    - NSE public past issues API to map IPO rows to NSE trading symbols

Example:
    python moneycontrol_mainline_ipo_loader.py
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path

import mysql.connector
import requests

try:
    from kiteconnect import KiteConnect
except Exception:
    KiteConnect = None


DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "bhav",
}

MONEYCONTROL_API_URL = "https://api.moneycontrol.com/mcapi/v1/ipo/get-listed-ipo"
MONEYCONTROL_SOURCE_PAGE_URL = "https://www.moneycontrol.com/ipo/listed-ipos/mainline/"
NSE_SOURCE_PAGE_URL = "https://www.nseindia.com/market-data/all-upcoming-issues-ipo?f=null"
NSE_PUBLIC_PAST_ISSUES_URL = "https://www.nseindia.com/api/public-past-issues"
DEFAULT_TABLE = "ipobhav"
DEFAULT_TOKEN_FILE = Path(__file__).resolve().parent / "kite_token.txt"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load all listed mainline IPO rows into bhav.ipobhav."
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Destination MySQL table name (default: {DEFAULT_TABLE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Page size used for Moneycontrol pagination (default: 20).",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=2015,
        help="Minimum listing year to load from the IPO master list (default: 2015).",
    )
    parser.add_argument(
        "--token",
        default=str(DEFAULT_TOKEN_FILE),
        help=f"Path to kite_token.txt (default: {DEFAULT_TOKEN_FILE}).",
    )
    return parser.parse_args()


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def build_session(referer: str | None = None) -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    if referer:
        session.headers["Referer"] = referer
        session.headers["Origin"] = referer.split("/", 3)[:3][0] + "//" + referer.split("/", 3)[2]
    return session


def recreate_table(conn, table_name: str) -> None:
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    cursor.execute(
        f"""
        CREATE TABLE `{table_name}` (
            `SYMBOL` VARCHAR(32) NOT NULL,
            `LISTING_DATE` DATE NOT NULL,
            `LISTING_OPEN` DOUBLE DEFAULT NULL,
            `LISTING_CLOSE` DOUBLE DEFAULT NULL,
            `ISSUE_PRICE` DOUBLE DEFAULT NULL,
            PRIMARY KEY (`SYMBOL`, `LISTING_DATE`),
            KEY `idx_listing_date` (`LISTING_DATE`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    conn.commit()
    cursor.close()


def parse_date(value: object):
    if value in (None, "", "-"):
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d %b %y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_decimal(value: object):
    if value in (None, "", "-"):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return round(float(text), 2)
    except ValueError:
        return None


def derive_issue_price(issue_price: object, price_range: object):
    parsed_issue = parse_decimal(issue_price)
    if parsed_issue is not None:
        return parsed_issue
    text = str(price_range or "").strip().replace("Rs.", "").replace("Rs", "").replace(",", "")
    if not text:
        return None
    nums = re.findall(r"\d+(?:\.\d+)?", text)
    if not nums:
        return None
    try:
        return round(float(nums[-1]), 2)
    except ValueError:
        return None


def normalize_company_name(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    drop_words = {
        "limited",
        "ltd",
        "limiteds",
        "private",
        "public",
        "company",
        "co",
    }
    parts = [part for part in text.split() if part not in drop_words]
    return " ".join(parts)


def fetch_all_mainline_ipos(session: requests.Session, limit: int) -> list[dict]:
    start = 0
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    while True:
        response = session.get(
            MONEYCONTROL_API_URL,
            params={"ipo_type": "Mainline", "start": start, "limit": limit},
            timeout=30,
        )
        if response.status_code == 204 or not response.text.strip():
            break
        response.raise_for_status()
        try:
            payload = response.json()
        except Exception:
            break
        batch = payload.get("data", {}).get("listedIpo", []) or []
        if not batch:
            break

        fresh = 0
        for item in batch:
            key = (str(item.get("sc_id") or "").strip(), str(item.get("listing_date") or "").strip())
            if key in seen:
                continue
            seen.add(key)
            rows.append(item)
            fresh += 1

        print(f"Fetched start={start}: {len(batch)} rows ({fresh} new)")
        start += limit

    return rows


def fetch_nse_public_past_issues(session: requests.Session, min_year: int) -> list[dict]:
    session.get(NSE_SOURCE_PAGE_URL, timeout=30)
    response = session.get(NSE_PUBLIC_PAST_ISSUES_URL, timeout=30)
    response.raise_for_status()
    rows = response.json()
    filtered = []
    for row in rows:
        if str(row.get("securityType") or "").strip().upper() != "EQ":
            continue
        listing_date = parse_date(row.get("listingDate"))
        if listing_date is None or listing_date.year < min_year:
            continue
        filtered.append(row)
    return filtered


def build_nse_match_index(rows: list[dict]) -> dict[tuple, list[dict]]:
    lookup: dict[tuple, list[dict]] = {}
    for row in rows:
        listing_date = parse_date(row.get("listingDate"))
        issue_price = parse_decimal(row.get("issuePrice"))
        key = (listing_date, issue_price)
        lookup.setdefault(key, []).append(
            {
                "symbol": str(row.get("symbol") or "").strip().upper(),
                "normalized_company_name": normalize_company_name(row.get("company") or row.get("companyName")),
            }
        )
    return lookup


def resolve_symbol(mc_row: dict, nse_lookup: dict[tuple, list[dict]]) -> str | None:
    listing_date = parse_date(mc_row.get("listing_date"))
    issue_price = parse_decimal(mc_row.get("issue_price"))
    mc_name = normalize_company_name(mc_row.get("company_name"))
    candidates = nse_lookup.get((listing_date, issue_price), [])
    if not candidates:
        return None

    for candidate in candidates:
        if mc_name == candidate["normalized_company_name"]:
            return candidate["symbol"]

    for candidate in candidates:
        nse_name = candidate["normalized_company_name"]
        if mc_name in nse_name or nse_name in mc_name:
            return candidate["symbol"]

    return candidates[0]["symbol"]


def build_moneycontrol_index(mc_rows: list[dict], nse_lookup: dict[tuple, list[dict]]) -> dict[tuple[str, object], dict]:
    index: dict[tuple[str, object], dict] = {}
    for mc_row in mc_rows:
        symbol = resolve_symbol(mc_row, nse_lookup)
        listing_date = parse_date(mc_row.get("listing_date"))
        if not symbol or listing_date is None:
            continue
        index[(symbol, listing_date)] = mc_row
    return index


def read_kite_token_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise SystemExit(f"Token file not found: {path}")
    text = None
    for encoding in ["utf-8", "utf-8-sig", "cp1252", "latin-1"]:
        try:
            text = path.read_text(encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise SystemExit(f"Unable to decode token file: {path}")
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    if "API_KEY" not in values or "ACCESS_TOKEN" not in values:
        raise SystemExit("kite_token.txt must contain API_KEY and ACCESS_TOKEN")
    return values


def get_kite_client(token_file: Path):
    if KiteConnect is None:
        raise SystemExit("kiteconnect is not installed. Install it with pip.")
    creds = read_kite_token_file(token_file)
    kite = KiteConnect(api_key=creds["API_KEY"])
    kite.set_access_token(creds["ACCESS_TOKEN"])
    return kite


def build_kite_instrument_map(kite, symbols: set[str]) -> dict[str, int]:
    instruments = kite.instruments("NSE")
    mapping: dict[str, int] = {}
    for row in instruments:
        if row.get("segment") != "NSE":
            continue
        symbol = str(row.get("tradingsymbol") or "").strip().upper()
        if symbol in symbols:
            mapping[symbol] = int(row["instrument_token"])
    return mapping


def fetch_kite_listing_day_ohlc(kite, instrument_token: int, listing_date) -> tuple[float | None, float | None]:
    try:
        candles = kite.historical_data(
            instrument_token,
            listing_date.strftime("%Y-%m-%d"),
            listing_date.strftime("%Y-%m-%d"),
            "day",
            continuous=False,
            oi=False,
        )
    except Exception:
        return None, None
    if not candles:
        return None, None
    candle = candles[0]
    return parse_decimal(candle.get("open")), parse_decimal(candle.get("close"))


def build_rows(
    nse_rows: list[dict],
    mc_index: dict[tuple[str, object], dict],
    kite,
    instrument_map: dict[str, int],
) -> list[tuple]:
    rows_by_key: dict[tuple[str, object], tuple] = {}
    for nse_row in nse_rows:
        symbol = str(nse_row.get("symbol") or "").strip().upper()
        listing_date = parse_date(nse_row.get("listingDate"))
        issue_price = derive_issue_price(nse_row.get("issuePrice"), nse_row.get("priceRange"))
        if not symbol or listing_date is None:
            continue

        mc_row = mc_index.get((symbol, listing_date))
        listing_open = parse_decimal(mc_row.get("dt_open")) if mc_row else None
        listing_close = parse_decimal(mc_row.get("dt_close")) if mc_row else None

        if (listing_open is None or listing_close is None) and symbol in instrument_map:
            kite_open, kite_close = fetch_kite_listing_day_ohlc(kite, instrument_map[symbol], listing_date)
            if listing_open is None:
                listing_open = kite_open
            if listing_close is None:
                listing_close = kite_close

        rows_by_key[(symbol, listing_date)] = (
            symbol,
            listing_date,
            listing_open,
            listing_close,
            issue_price,
        )
    return list(rows_by_key.values())


def insert_rows(conn, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    cursor = conn.cursor()
    cursor.executemany(
        f"""
        INSERT INTO `{table_name}` (
            `SYMBOL`,
            `LISTING_DATE`,
            `LISTING_OPEN`,
            `LISTING_CLOSE`,
            `ISSUE_PRICE`
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    affected = cursor.rowcount
    cursor.close()
    return affected


def main() -> None:
    args = parse_args()
    mc_session = build_session(MONEYCONTROL_SOURCE_PAGE_URL)
    nse_session = build_session(NSE_SOURCE_PAGE_URL)
    kite = get_kite_client(Path(args.token).expanduser().resolve())
    conn = get_db_connection()
    try:
        recreate_table(conn, args.table)
        mc_rows = fetch_all_mainline_ipos(mc_session, args.limit)
        nse_rows = fetch_nse_public_past_issues(nse_session, args.min_year)
        nse_lookup = build_nse_match_index(nse_rows)
        mc_index = build_moneycontrol_index(mc_rows, nse_lookup)
        instrument_map = build_kite_instrument_map(kite, {str(row.get("symbol") or "").strip().upper() for row in nse_rows})
        rows = build_rows(nse_rows, mc_index, kite, instrument_map)
        affected = insert_rows(conn, args.table, rows)
        print(f"Loaded {len(rows)} IPO rows into bhav.{args.table} (affected rows: {affected})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
