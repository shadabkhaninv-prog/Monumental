#!/usr/bin/env python3
"""
Probe BSE XBRL quarterly extraction for a few NSE symbols and compare it to Screener.

Example:
    python bse_xbrl_probe.py --symbols MCX BPCL HFCL CDSL --cutoff 2024-08-05

Outputs:
    reports\\bse_xbrl_probe_<ddmmmyyyy>.csv
    reports\\bse_xbrl_probe_<ddmmmyyyy>.xlsx
"""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import quarterly_fundamentals_report as screener_source


REPORTS_DIR = Path(__file__).resolve().parent / "reports"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
BSE_REFERER = "https://www.bseindia.com/corporates/List_Scrips.html"
BSE_SEARCH_URL = "https://api.bseindia.com/BseIndiaAPI/api/ListScripSmartSearch/w"
BSE_RESULTS_URL = "https://www.bseindia.com/corporates/comp_results.aspx"

REVENUE_TAGS = [
    "RevenueFromOperations",
    "RevenueFromOperationsExciseDuty",
    "RevenueFromOperationsNetOfExciseDuty",
    "RevenueFromOperationsGross",
    "TotalRevenueFromOperations",
    "RevenueFromSaleOfProducts",
]
PROFIT_TAGS = [
    "ProfitOrLossAttributableToOwnersOfParent",
    "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossForPeriodAttributableToOwnersOfParent",
    "ProfitLossForPeriod",
]
EXCISE_DESCRIPTION_PATTERNS = [
    "excise duty",
    "less: excise duty",
]


@dataclass
class BseCandidate:
    security_code: str
    company_name: str
    security_id: str
    isin: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe BSE XBRL quarterly extraction for sample symbols.")
    parser.add_argument("--symbols", nargs="+", required=True, help="NSE symbols to test, e.g. MCX BPCL HFCL")
    parser.add_argument("--cutoff", required=True, help="Cutoff date in YYYY-MM-DD format")
    parser.add_argument("--quarters", type=int, default=10, help="Quarter count to keep (default: 10)")
    parser.add_argument("--delay", type=float, default=0.4, help="Delay between HTTP requests in seconds")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry attempts for BSE/NSE requests")
    parser.add_argument("--output-dir", default=str(REPORTS_DIR), help=f"Output directory (default: {REPORTS_DIR})")
    args = parser.parse_args()
    try:
        args.cutoff = datetime.strptime(args.cutoff, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid cutoff date: {args.cutoff}. Expected YYYY-MM-DD.") from exc
    args.output_dir = Path(args.output_dir).expanduser().resolve()
    return args


def quarter_token_to_date(token: str) -> date:
    return datetime.strptime(token, "%b-%y").date().replace(day=1)


def quarter_label_from_date(value: date) -> str:
    return value.strftime("%b %Y")


def first_day_to_quarter_end(value: date) -> date:
    month_end_day = {
        3: 31,
        6: 30,
        9: 30,
        12: 31,
    }[value.month]
    return value.replace(day=month_end_day)


def safe_to_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def pct_diff(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 2)


def extract_bse_candidates(html_text: str) -> list[BseCandidate]:
    if not html_text:
        return []

    if html_text.startswith('"') and html_text.endswith('"'):
        html_text = json.loads(html_text)

    pattern = re.compile(
        r"liclick\('(?P<code>\d+)','(?P<company>[^']+)'\).*?"
        r"<span>(?P<span>.*?)</span>",
        re.IGNORECASE | re.DOTALL,
    )
    results: list[BseCandidate] = []
    for match in pattern.finditer(html_text):
        span = html.unescape(re.sub(r"<.*?>", "", match.group("span")))
        span = re.sub(r"\s+", " ", span.replace("\xa0", " ")).strip()
        parts = [part.strip() for part in span.split(" ") if part.strip()]
        security_id = parts[0] if parts else ""
        isin = next((part for part in parts if part.startswith("INE") or part.startswith("INF")), "")
        results.append(
            BseCandidate(
                security_code=match.group("code"),
                company_name=match.group("company"),
                security_id=security_id.rstrip("#"),
                isin=isin,
            )
        )
    return results


def resolve_bse_code(session: requests.Session, symbol: str) -> tuple[str | None, str]:
    response = session.get(
        BSE_SEARCH_URL,
        params={"text": symbol, "Flag": "liclick"},
        headers={"Referer": BSE_REFERER},
        timeout=60,
    )
    response.raise_for_status()
    candidates = extract_bse_candidates(response.text)

    exact = [candidate for candidate in candidates if candidate.security_id.upper() == symbol.upper()]
    if exact:
        # Prefer the normal equity code series when duplicates exist.
        exact.sort(key=lambda item: (not item.security_code.startswith("5"), len(item.security_code)))
        chosen = exact[0]
        return chosen.security_code, f"{chosen.company_name} ({chosen.security_id})"

    if candidates:
        sample = ", ".join(f"{item.security_id}:{item.security_code}" for item in candidates[:5])
        return None, f"No exact BSE symbol match for {symbol}. Search returned: {sample}"
    return None, f"No BSE search match found for {symbol}"


def parse_results_page(html: str) -> list[dict[str, str]]:
    row_pattern = re.compile(
        r"<tr><td class='TTRow'>(?P<fy>[^<]+)</td>"
        r"<td class='TTRow'><a class='tablebluelink' href='(?P<details>[^']+)'>"
        r"(?P<statement>Consolidated|Standalone)-(?P<quarter>[^<]+)</a></td>"
        r"<td class='TTRow'>(?P<type>[^<]+)</td>"
        r"<td class='TTRow'>(?P<status>[^<]+)</td>"
        r"<td class='TTRow'>(?P<filed>[^<]*)</td>"
        r"<td class='TTRow'>(?P<revised>[^<]*)</td>"
        r"<td class='TTRow'>(?P<reason>[^<]*)</td>"
        r"<td class='TTRow'>(?P<standalone>.*?)</td>"
        r"<td class='TTRow'>(?P<consolidated>.*?)</td>"
        r"<td class='TTRow'>(?P<trend>.*?)</td></tr>",
        re.IGNORECASE | re.DOTALL,
    )

    rows: list[dict[str, str]] = []
    for match in row_pattern.finditer(html):
        rows.append({key: match.group(key) for key in row_pattern.groupindex})
    return rows


def get_xbrl_url(row: dict[str, str]) -> str | None:
    cell = row["consolidated"] if row["statement"].lower() == "consolidated" else row["standalone"]
    link_match = re.search(r"href='([^']+\.xml)'", cell, re.IGNORECASE)
    if not link_match:
        return None
    href = link_match.group(1).replace("//XBRLFILES", "/XBRLFILES")
    if href.startswith("http"):
        return href
    return f"https://www.bseindia.com{href}"


def fetch_bse_quarter_rows(
    session: requests.Session,
    security_code: str,
    cutoff: date,
    delay: float,
    max_pid: int = 6,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for pid in range(1, max_pid + 1):
        if pid > 1 and delay > 0:
            time.sleep(delay)

        response = session.get(
            BSE_RESULTS_URL,
            params={"Code": security_code, "PID": pid},
            headers={"Referer": "https://www.bseindia.com/"},
            timeout=60,
        )
        if response.status_code == 404:
            break
        response.raise_for_status()
        parsed = parse_results_page(response.text)
        if not parsed:
            continue

        for row in parsed:
            if row["type"].strip().lower() != "quarter":
                continue
            if row["statement"].strip().lower() != "consolidated":
                continue

            quarter_start = quarter_token_to_date(row["quarter"].strip())
            quarter_end = first_day_to_quarter_end(quarter_start)
            if quarter_end > cutoff:
                continue

            xbrl_url = get_xbrl_url(row)
            if not xbrl_url:
                continue

            rows.append(
                {
                    "quarter_token": row["quarter"].strip(),
                    "quarter_end": quarter_end,
                    "filed_at": row["filed"].strip(),
                    "xbrl_url": xbrl_url,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["quarter_token", "quarter_end", "filed_at", "xbrl_url"])

    df = pd.DataFrame(rows)
    df["filed_at_dt"] = pd.to_datetime(df["filed_at"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
    df = df.sort_values(["quarter_end", "filed_at_dt"]).drop_duplicates(["quarter_end"], keep="last")
    return df.sort_values("quarter_end").reset_index(drop=True)


def find_first_tag_value(root: ET.Element, tag_names: Iterable[str]) -> float | None:
    wanted = set(tag_names)
    for tag_name in wanted:
        for elem in root.iter():
            if elem.tag.split("}")[-1] != tag_name:
                continue
            if elem.text is None:
                continue
            value = safe_to_float(elem.text)
            if value is not None:
                return round(value / 1e7, 2)
    return None


def find_named_other_expense(root: ET.Element, wanted_names: Iterable[str]) -> float | None:
    descriptions: dict[str, str] = {}
    amounts: dict[str, float] = {}

    for elem in root.iter():
        tag = elem.tag.split("}")[-1]
        context = elem.attrib.get("contextRef")
        if not context:
            continue
        if tag == "DescriptionOfOtherExpenses" and elem.text:
            descriptions[context] = elem.text.strip().lower()
        elif tag == "OtherExpenses" and elem.text:
            value = safe_to_float(elem.text)
            if value is not None:
                amounts[context] = round(value / 1e7, 2)

    for context, description in descriptions.items():
        if any(pattern in description for pattern in wanted_names) and context in amounts:
            return amounts[context]
    return None


def parse_xbrl_metrics(xml_text: str) -> tuple[float | None, float | None]:
    root = ET.fromstring(xml_text)
    sales = find_first_tag_value(root, REVENUE_TAGS)
    profit = find_first_tag_value(root, PROFIT_TAGS)
    excise_duty = find_named_other_expense(root, EXCISE_DESCRIPTION_PATTERNS)
    if sales is not None and excise_duty is not None:
        sales = round(sales - excise_duty, 2)
    return sales, profit


def fetch_xbrl_series(
    session: requests.Session,
    symbol: str,
    security_code: str,
    cutoff: date,
    quarters: int,
    delay: float,
) -> pd.DataFrame:
    rows_df = fetch_bse_quarter_rows(session, security_code, cutoff, delay)
    if rows_df.empty:
        raise ValueError("No consolidated quarterly XBRL rows found on or before cutoff.")

    extracted: list[dict[str, object]] = []
    for index, row in rows_df.iterrows():
        if index > 0 and delay > 0:
            time.sleep(delay)
        xml_response = session.get(
            row["xbrl_url"],
            headers={"Referer": "https://www.bseindia.com/"},
            timeout=60,
        )
        xml_response.raise_for_status()
        sales, profit = parse_xbrl_metrics(xml_response.text)
        extracted.append(
            {
                "symbol": symbol,
                "bse_code": security_code,
                "quarter_label": quarter_label_from_date(row["quarter_end"]),
                "quarter_end": row["quarter_end"],
                "bse_sales": sales,
                "bse_profit": profit,
                "bse_xbrl_url": row["xbrl_url"],
                "filed_at": row["filed_at"],
            }
        )

    df = pd.DataFrame(extracted).sort_values("quarter_end").reset_index(drop=True)
    if len(df) > quarters:
        df = df.tail(quarters).reset_index(drop=True)
    return df


def fetch_screener_series(session: requests.Session, symbol: str, cutoff: date, quarters: int) -> pd.DataFrame:
    table, source_url, statement_used = screener_source.fetch_quarterly_table(
        session=session,
        symbol=symbol,
        statement_mode="auto",
        timeout=25,
    )
    df = screener_source.quarterly_table_to_long(
        symbol=symbol,
        table=table,
        cutoff=cutoff,
        quarter_count=quarters,
        source_url=source_url,
        statement_used=statement_used,
    )
    return df.rename(
        columns={
            "sales": "screener_sales",
            "net_profit": "screener_profit",
            "source_url": "screener_url",
        }
    )[
        [
            "symbol",
            "quarter_label",
            "quarter_end",
            "screener_sales",
            "screener_profit",
            "statement_used",
            "screener_url",
        ]
    ]


def merge_and_compare(bse_df: pd.DataFrame, screener_df: pd.DataFrame) -> pd.DataFrame:
    merged = bse_df.merge(
        screener_df,
        on=["symbol", "quarter_label", "quarter_end"],
        how="outer",
    ).sort_values(["symbol", "quarter_end"]).reset_index(drop=True)
    merged["sales_diff"] = [
        pct_diff(left, right)
        for left, right in zip(merged.get("bse_sales"), merged.get("screener_sales"))
    ]
    merged["profit_diff"] = [
        pct_diff(left, right)
        for left, right in zip(merged.get("bse_profit"), merged.get("screener_profit"))
    ]
    return merged


def write_outputs(output_dir: Path, cutoff: date, df: pd.DataFrame, summary: pd.DataFrame) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = cutoff.strftime("%d%b%Y").lower()
    csv_path = output_dir / f"bse_xbrl_probe_{stamp}.csv"
    xlsx_path = output_dir / f"bse_xbrl_probe_{stamp}.xlsx"
    df.to_csv(csv_path, index=False)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Comparison", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)
    return csv_path, xlsx_path


def main() -> int:
    args = parse_args()
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=args.retries,
        connect=args.retries,
        read=args.retries,
        status=args.retries,
        backoff_factor=0.7,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    comparison_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []

    print("=" * 72)
    print("BSE XBRL PROBE")
    print("=" * 72)
    print(f"Cutoff    : {args.cutoff}")
    print(f"Symbols   : {', '.join(args.symbols)}")
    print(f"Quarters  : {args.quarters}")
    print("=" * 72)

    for index, raw_symbol in enumerate(args.symbols, start=1):
        symbol = raw_symbol.upper().strip().removeprefix("NSE:")
        if index > 1 and args.delay > 0:
            time.sleep(args.delay)

        print(f"[{index}/{len(args.symbols)}] {symbol} ...", end="")
        sys.stdout.flush()

        try:
            bse_code, note = resolve_bse_code(session, symbol)
            if not bse_code:
                summary_rows.append(
                    {
                        "symbol": symbol,
                        "status": "UNRESOLVED",
                        "bse_code": "",
                        "note": note,
                    }
                )
                print(f" unresolved ({note})")
                continue

            bse_df = fetch_xbrl_series(
                session=session,
                symbol=symbol,
                security_code=bse_code,
                cutoff=args.cutoff,
                quarters=args.quarters,
                delay=args.delay,
            )
            screener_df = fetch_screener_series(
                session=session,
                symbol=symbol,
                cutoff=args.cutoff,
                quarters=args.quarters,
            )
            merged = merge_and_compare(bse_df, screener_df)
            comparison_frames.append(merged)

            max_sales_diff = (
                pd.to_numeric(merged["sales_diff"], errors="coerce").abs().max()
                if "sales_diff" in merged
                else math.nan
            )
            max_profit_diff = (
                pd.to_numeric(merged["profit_diff"], errors="coerce").abs().max()
                if "profit_diff" in merged
                else math.nan
            )

            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "OK",
                    "bse_code": bse_code,
                    "note": note,
                    "bse_quarters": len(bse_df),
                    "screener_quarters": len(screener_df),
                    "max_abs_sales_diff": None if pd.isna(max_sales_diff) else round(float(max_sales_diff), 2),
                    "max_abs_profit_diff": None if pd.isna(max_profit_diff) else round(float(max_profit_diff), 2),
                }
            )
            print(f" ok (BSE code {bse_code}, {len(bse_df)} quarters)")
        except Exception as exc:  # noqa: BLE001
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "bse_code": "",
                    "note": str(exc),
                }
            )
            print(f" error ({exc})")

    comparison_df = pd.concat(comparison_frames, ignore_index=True) if comparison_frames else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    csv_path, xlsx_path = write_outputs(args.output_dir, args.cutoff, comparison_df, summary_df)

    print("\nSummary")
    if not summary_df.empty:
        print(summary_df.fillna("").to_string(index=False))
    print(f"\nCSV  : {csv_path}")
    print(f"XLSX : {xlsx_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
