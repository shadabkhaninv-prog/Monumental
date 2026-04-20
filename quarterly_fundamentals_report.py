#!/usr/bin/env python3
"""
Fetch quarterly sales, net profit, and YoY growth for symbols in a rated-list file.

Default source:
    BSE quarterly-results XBRL filings, using consolidated filings when available.

Example:
    python quarterly_fundamentals_report.py rated_list_05aug2024.txt
    python quarterly_fundamentals_report.py reports\\rated_list_05aug2024.txt

Outputs:
    reports\\quarterly_fundamentals_<ddmmmyyyy>.xlsx
    reports\\quarterly_fundamentals_<ddmmmyyyy>.csv

Dependencies:
    pip install requests pandas openpyxl
"""

from __future__ import annotations

import argparse
import calendar
import csv
import html
import json
import math
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


REPORTS_DIR = Path(__file__).resolve().parent / "reports"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
LOCAL_BSE_CODE_CACHE = Path(__file__).resolve().parent / "bse_code_cache.json"
ROW_SALES = "sales"
ROW_NET_PROFIT = "net profit"
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
_BSE_EQUITY_CODE_MAP: dict[str, tuple[str, str]] | None = None


@dataclass
class BseCandidate:
    security_code: str
    company_name: str
    security_id: str
    isin: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch quarterly sales, net profit, sales YoY %, and profit YoY % "
            "for symbols listed in a rated-list text file."
        )
    )
    parser.add_argument(
        "input_file",
        help=(
            "Rated-list file name or path. Example: rated_list_05aug2024.txt "
            "or reports\\rated_list_05aug2024.txt"
        ),
    )
    parser.add_argument(
        "--cutoff",
        help="Override cutoff date in YYYY-MM-DD format. Defaults to date inferred from filename.",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        default=10,
        help="Number of quarters to export on or before cutoff date (default: 10).",
    )
    parser.add_argument(
        "--statement",
        choices=["auto", "consolidated", "standalone"],
        default="auto",
        help="Ignored for BSE XBRL mode. Kept for CLI compatibility.",
    )
    parser.add_argument(
        "--reports-dir",
        default=str(REPORTS_DIR),
        help=f"Directory used when input_file is passed as a bare filename (default: {REPORTS_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPORTS_DIR),
        help=f"Directory for output files (default: {REPORTS_DIR}).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.75,
        help="Delay in seconds between HTTP requests (default: 0.75).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retry attempts for BSE requests (default: 3).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional cap on number of symbols, useful for quick testing.",
    )
    args = parser.parse_args()
    if args.quarters < 1:
        raise SystemExit("--quarters must be at least 1.")
    if args.cutoff:
        args.cutoff = parse_iso_date(args.cutoff)
    args.reports_dir = Path(args.reports_dir).expanduser().resolve()
    args.output_dir = Path(args.output_dir).expanduser().resolve()
    return args


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid --cutoff value: {value}. Expected YYYY-MM-DD.") from exc


def resolve_input_file(input_value: str, reports_dir: Path) -> Path:
    raw = Path(input_value).expanduser()
    candidates = [raw]
    if not raw.is_absolute():
        candidates.append(reports_dir / raw)

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.exists():
            return resolved
    tried = "\n".join(f"  - {candidate.resolve()}" for candidate in candidates)
    raise SystemExit(f"Input file not found. Tried:\n{tried}")


def infer_cutoff_from_filename(path: Path) -> date:
    match = re.search(r"_(\d{2}[A-Za-z]{3}\d{4})\.txt$", path.name)
    if not match:
        raise SystemExit(
            "Could not infer cutoff date from filename. "
            "Use a name like rated_list_05aug2024.txt or pass --cutoff YYYY-MM-DD."
        )
    token = match.group(1)
    token = f"{token[:2]}{token[2:5].title()}{token[5:]}"
    try:
        return datetime.strptime(token, "%d%b%Y").date()
    except ValueError as exc:
        raise SystemExit(
            f"Could not parse cutoff date token '{match.group(1)}' from filename {path.name}."
        ) from exc


def load_symbols(path: Path, limit: int | None = None) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        symbol = line.upper().removeprefix("NSE:").strip()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)

    if limit is not None:
        symbols = symbols[:limit]

    if not symbols:
        raise SystemExit(f"No symbols found in {path}")
    return symbols


def month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def parse_quarter_label(label: object) -> date | None:
    text = str(label).strip()
    if not text or text.upper() == "TTM":
        return None
    try:
        parsed = datetime.strptime(text, "%b %Y").date()
    except ValueError:
        return None
    return month_end(parsed.year, parsed.month)


def normalize_metric_name(value: object) -> str:
    text = str(value).replace("\xa0", " ").strip().lower()
    text = text.replace("+", " ")
    text = re.sub(r"[^a-z0-9% ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_number(value: object) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).replace("\xa0", " ").strip()
    if not text or text in {"-", "--", "nan", "None"}:
        return None
    text = text.replace(",", "")
    text = text.replace("%", "")
    text = text.replace("₹", "")
    try:
        return float(text)
    except ValueError:
        return None


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


def pct_growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round(((current / previous) - 1.0) * 100.0, 2)


def quarter_token_to_date(token: str) -> date:
    return datetime.strptime(token, "%b-%y").date().replace(day=1)


def quarter_label_from_date(value: date) -> str:
    return value.strftime("%b %Y")


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
        security_id = parts[0].rstrip("#") if parts else ""
        isin = next((part for part in parts if part.startswith("INE") or part.startswith("INF")), "")
        results.append(
            BseCandidate(
                security_code=match.group("code"),
                company_name=match.group("company"),
                security_id=security_id,
                isin=isin,
            )
        )
    return results


def load_bse_equity_code_map(session: requests.Session, timeout: float) -> dict[str, tuple[str, str]]:
    global _BSE_EQUITY_CODE_MAP
    if _BSE_EQUITY_CODE_MAP is not None:
        return _BSE_EQUITY_CODE_MAP

    local_mapping: dict[str, tuple[str, str]] = {}
    if LOCAL_BSE_CODE_CACHE.exists():
        try:
            cache_data = json.loads(LOCAL_BSE_CODE_CACHE.read_text(encoding="utf-8"))
            for key, value in cache_data.items():
                symbol = str(key).strip().upper()
                security_code = str(value).strip()
                if symbol and security_code:
                    local_mapping[symbol] = (security_code, symbol)
        except Exception:
            local_mapping = {}

    response = session.get(
        "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w",
        params={"segment": "Equity", "status": "Active", "Group": "", "Scripcode": ""},
        headers={"Referer": BSE_REFERER},
        timeout=max(timeout, 120.0),
    )
    response.raise_for_status()

    mapping: dict[str, tuple[str, str]] = dict(local_mapping)
    reader = csv.DictReader(StringIO(response.text))
    for row in reader:
        security_id = str(row.get("Security Id", "")).strip().upper()
        security_code = str(row.get("Security Code", "")).strip()
        company_name = str(row.get("Issuer Name", "")).strip()
        if security_id and security_code:
            mapping[security_id] = (security_code, company_name)

    _BSE_EQUITY_CODE_MAP = mapping
    return mapping


def resolve_bse_code(session: requests.Session, symbol: str, timeout: float) -> tuple[str | None, str]:
    equity_map = load_bse_equity_code_map(session, timeout)
    if symbol.upper() in equity_map:
        security_code, company_name = equity_map[symbol.upper()]
        return security_code, f"{company_name} ({symbol.upper()})"

    response = session.get(
        BSE_SEARCH_URL,
        params={"text": symbol, "Flag": "liclick"},
        headers={"Referer": BSE_REFERER},
        timeout=timeout,
    )
    response.raise_for_status()
    candidates = extract_bse_candidates(response.text)

    exact = [candidate for candidate in candidates if candidate.security_id.upper() == symbol.upper()]
    if exact:
        exact.sort(key=lambda item: (not item.security_code.startswith("5"), len(item.security_code)))
        chosen = exact[0]
        return chosen.security_code, f"{chosen.company_name} ({chosen.security_id})"

    if symbol.upper() in equity_map:
        security_code, company_name = equity_map[symbol.upper()]
        return security_code, f"{company_name} ({symbol.upper()})"

    if candidates:
        sample = ", ".join(f"{item.security_id}:{item.security_code}" for item in candidates[:5])
        return None, f"No exact BSE symbol match for {symbol}. Search returned: {sample}"
    return None, f"No BSE search match found for {symbol}"


def parse_results_page(html_text: str) -> list[dict[str, str]]:
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
    return [{key: match.group(key) for key in row_pattern.groupindex} for match in row_pattern.finditer(html_text)]


def get_xbrl_url(row: dict[str, str]) -> str | None:
    cell = row["consolidated"] if row["statement"].lower() == "consolidated" else row["standalone"]
    link_match = re.search(r"href='([^']+\.xml)'", cell, re.IGNORECASE)
    if not link_match:
        return None
    href = link_match.group(1).replace("//XBRLFILES", "/XBRLFILES")
    if href.startswith("http"):
        return href
    return f"https://www.bseindia.com{href}"


def first_day_to_quarter_end(value: date) -> date:
    month_end_day = {
        3: 31,
        6: 30,
        9: 30,
        12: 31,
    }[value.month]
    return value.replace(day=month_end_day)


def parse_bse_datetime(value: str) -> pd.Timestamp:
    text = (value or "").strip()
    if not text:
        return pd.NaT
    for fmt in ("%d-%m-%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return pd.NaT


def fetch_bse_quarter_rows(
    session: requests.Session,
    security_code: str,
    cutoff: date,
    delay: float,
    timeout: float,
    max_pid: int = 6,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for pid in range(1, max_pid + 1):
        if pid > 1 and delay > 0:
            time.sleep(delay)

        response = None
        for page_attempt in range(3):
            response = session.get(
                BSE_RESULTS_URL,
                params={"Code": security_code, "PID": pid},
                headers={"Referer": "https://www.bseindia.com/"},
                timeout=timeout,
            )
            if response.status_code != 404:
                break
            if page_attempt < 2:
                time.sleep(max(delay * 4 * (page_attempt + 1), 2.5 * (page_attempt + 1)))
        if response is None or response.status_code == 404:
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
                    "quarter_end": quarter_end,
                    "quarter_label": quarter_label_from_date(quarter_end),
                    "filed_at": row["filed"].strip(),
                    "revised_at": row["revised"].strip(),
                    "effective_dt": parse_bse_datetime(row["revised"]) if row["revised"].strip() else parse_bse_datetime(row["filed"]),
                    "xbrl_url": xbrl_url,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["quarter_end", "quarter_label", "filed_at", "revised_at", "effective_dt", "xbrl_url"])

    df = pd.DataFrame(rows)
    df = df.sort_values(["quarter_end", "effective_dt"], na_position="last").drop_duplicates(["quarter_end"], keep="last")
    return df.sort_values("quarter_end").reset_index(drop=True)


def find_first_tag_value(root: ET.Element, tag_names: Iterable[str]) -> float | None:
    for tag_name in tag_names:
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


def fetch_bse_quarterly_long(
    session: requests.Session,
    symbol: str,
    cutoff: date,
    quarter_count: int,
    delay: float,
    timeout: float,
) -> tuple[pd.DataFrame, str]:
    bse_code, note = resolve_bse_code(session, symbol, timeout)
    if not bse_code:
        raise ValueError(note)

    rows_df = pd.DataFrame()
    for attempt in range(3):
        rows_df = fetch_bse_quarter_rows(
            session=session,
            security_code=bse_code,
            cutoff=cutoff,
            delay=delay,
            timeout=timeout,
        )
        if not rows_df.empty:
            break
        if attempt < 2:
            time.sleep(max(delay * 4 * (attempt + 1), 3.0 * (attempt + 1)))
    if rows_df.empty:
        raise ValueError(f"No consolidated quarterly XBRL rows found on or before cutoff for BSE code {bse_code}.")

    extracted: list[dict[str, object]] = []
    for index, row in rows_df.iterrows():
        if index > 0 and delay > 0:
            time.sleep(delay)
        xml_response = session.get(
            row["xbrl_url"],
            headers={"Referer": "https://www.bseindia.com/"},
            timeout=timeout,
        )
        xml_response.raise_for_status()
        sales, profit = parse_xbrl_metrics(xml_response.text)
        extracted.append(
            {
                "symbol": symbol,
                "quarter_label": row["quarter_label"],
                "quarter_end": row["quarter_end"],
                "sales": sales,
                "net_profit": profit,
                "statement_used": "bse_xbrl_consolidated",
                "source_url": row["xbrl_url"],
                "bse_code": bse_code,
            }
        )

    result = pd.DataFrame(extracted).sort_values("quarter_end").reset_index(drop=True)
    result["sales_yoy_pct"] = [
        pct_growth(current, previous)
        for current, previous in zip(result["sales"], result["sales"].shift(4))
    ]
    result["profit_yoy_pct"] = [
        pct_growth(current, previous)
        for current, previous in zip(result["net_profit"], result["net_profit"].shift(4))
    ]

    if len(result) > quarter_count:
        result = result.tail(quarter_count).reset_index(drop=True)
    return result, note


def build_statement_urls(symbol: str, mode: str) -> list[tuple[str, str]]:
    symbol = symbol.strip().upper()
    if mode == "consolidated":
        return [("consolidated", f"https://www.screener.in/company/{symbol}/consolidated/")]
    if mode == "standalone":
        return [("standalone", f"https://www.screener.in/company/{symbol}/")]
    return [
        ("consolidated", f"https://www.screener.in/company/{symbol}/consolidated/"),
        ("standalone", f"https://www.screener.in/company/{symbol}/"),
    ]


def is_quarterly_table(df: pd.DataFrame) -> bool:
    if df.empty or len(df.columns) < 3:
        return False

    metric_names = {normalize_metric_name(value) for value in df.iloc[:, 0]}
    if ROW_SALES not in metric_names or ROW_NET_PROFIT not in metric_names:
        return False

    parsed_columns = sum(1 for column in df.columns[1:] if parse_quarter_label(column))
    return parsed_columns >= 4


def extract_quarterly_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    for table in tables:
        if is_quarterly_table(table):
            return table.copy()
    raise ValueError("Quarterly results table not found in page.")


def fetch_quarterly_table(
    session: requests.Session,
    symbol: str,
    statement_mode: str,
    timeout: float,
) -> tuple[pd.DataFrame, str, str]:
    errors: list[str] = []

    for statement_used, url in build_statement_urls(symbol, statement_mode):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            table = extract_quarterly_table(response.text)
            return table, url, statement_used
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{statement_used}: {exc}")

    raise RuntimeError("; ".join(errors))


def quarterly_table_to_long(
    symbol: str,
    table: pd.DataFrame,
    cutoff: date,
    quarter_count: int,
    source_url: str,
    statement_used: str,
) -> pd.DataFrame:
    metric_map: dict[str, pd.Series] = {}
    for _, row in table.iterrows():
        metric_name = normalize_metric_name(row.iloc[0])
        metric_map[metric_name] = row.iloc[1:]

    if ROW_SALES not in metric_map or ROW_NET_PROFIT not in metric_map:
        raise ValueError("Sales or Net Profit row is missing from quarterly table.")

    quarter_rows: list[dict[str, object]] = []
    sales_row = metric_map[ROW_SALES]
    profit_row = metric_map[ROW_NET_PROFIT]

    for column in table.columns[1:]:
        quarter_end = parse_quarter_label(column)
        if quarter_end is None or quarter_end > cutoff:
            continue
        quarter_rows.append(
            {
                "symbol": symbol,
                "quarter_label": str(column).strip(),
                "quarter_end": quarter_end,
                "sales": parse_number(sales_row.get(column)),
                "net_profit": parse_number(profit_row.get(column)),
                "statement_used": statement_used,
                "source_url": source_url,
            }
        )

    if not quarter_rows:
        raise ValueError(f"No quarterly rows found on or before cutoff date {cutoff}.")

    result = pd.DataFrame(quarter_rows).sort_values("quarter_end").reset_index(drop=True)
    result["sales_yoy_pct"] = [
        pct_growth(current, previous)
        for current, previous in zip(result["sales"], result["sales"].shift(4))
    ]
    result["profit_yoy_pct"] = [
        pct_growth(current, previous)
        for current, previous in zip(result["net_profit"], result["net_profit"].shift(4))
    ]

    if len(result) > quarter_count:
        result = result.tail(quarter_count).reset_index(drop=True)

    return result


def build_wide_view(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df.empty:
        return pd.DataFrame()

    metrics = [
        ("sales", "Sales"),
        ("net_profit", "Profit"),
        ("sales_yoy_pct", "Sales YoY %"),
        ("profit_yoy_pct", "Profit YoY %"),
    ]
    metric_labels = dict(metrics)

    ordered_pairs = (
        long_df[["quarter_end", "quarter_label"]]
        .drop_duplicates()
        .sort_values("quarter_end")
        .to_dict("records")
    )

    melted = long_df.melt(
        id_vars=["symbol", "quarter_label", "quarter_end"],
        value_vars=[name for name, _ in metrics],
        var_name="metric",
        value_name="value",
    )
    melted["column_name"] = melted["metric"].map(metric_labels) + " " + melted["quarter_label"]

    wide = (
        melted.pivot_table(index="symbol", columns="column_name", values="value", aggfunc="first")
        .reset_index()
    )

    ordered_columns = ["symbol"]
    existing = set(wide.columns)
    for pair in ordered_pairs:
        label = pair["quarter_label"]
        for _, metric_label in metrics:
            column_name = f"{metric_label} {label}"
            if column_name in existing:
                ordered_columns.append(column_name)

    return wide.loc[:, ordered_columns]


def autofit_excel_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    worksheet = writer.sheets[sheet_name]
    for index, column in enumerate(df.columns, start=1):
        values = [str(column), *[str(value) for value in df[column].tolist()]]
        max_len = min(max(len(value) for value in values) + 2, 60)
        worksheet.column_dimensions[get_excel_column_name(index)].width = max_len


def get_excel_column_name(index: int) -> str:
    name = ""
    current = index
    while current:
        current, remainder = divmod(current - 1, 26)
        name = chr(65 + remainder) + name
    return name


def write_outputs(
    output_dir: Path,
    cutoff: date,
    long_df: pd.DataFrame,
    wide_df: pd.DataFrame,
    summary_df: pd.DataFrame,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = cutoff.strftime("%d%b%Y").lower()
    excel_path = output_dir / f"quarterly_fundamentals_{stamp}.xlsx"
    csv_path = output_dir / f"quarterly_fundamentals_{stamp}.csv"

    long_df.to_csv(csv_path, index=False)

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        long_df.to_excel(writer, sheet_name="QuarterlyLong", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        if not wide_df.empty:
            wide_df.to_excel(writer, sheet_name="QuarterlyWide", index=False)

        autofit_excel_columns(writer, "QuarterlyLong", long_df)
        autofit_excel_columns(writer, "Summary", summary_df)
        if not wide_df.empty:
            autofit_excel_columns(writer, "QuarterlyWide", wide_df)

    return excel_path, csv_path


def print_run_header(path: Path, cutoff: date, symbols: Iterable[str], args: argparse.Namespace) -> None:
    symbol_list = list(symbols)
    print("=" * 72)
    print("QUARTERLY FUNDAMENTALS REPORT")
    print("=" * 72)
    print(f"Input file     : {path}")
    print(f"Cutoff date    : {cutoff}")
    print(f"Symbols        : {len(symbol_list)}")
    print(f"Quarter count  : {args.quarters}")
    print("Source         : BSE XBRL (consolidated quarterly filings)")
    print(f"Output dir     : {args.output_dir}")
    print("=" * 72)


def main() -> int:
    args = parse_args()
    input_path = resolve_input_file(args.input_file, args.reports_dir)
    cutoff = args.cutoff or infer_cutoff_from_filename(input_path)
    symbols = load_symbols(input_path, limit=args.limit)

    print_run_header(input_path, cutoff, symbols, args)

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
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

    output_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []

    for index, symbol in enumerate(symbols, start=1):
        if index > 1 and args.delay > 0:
            time.sleep(args.delay)

        print(f"[{index:>2}/{len(symbols)}] Fetching {symbol} ...", end="")
        sys.stdout.flush()

        try:
            last_error: Exception | None = None
            result = None
            note = ""
            for attempt in range(2):
                try:
                    result, note = fetch_bse_quarterly_long(
                        session=session,
                        symbol=symbol,
                        cutoff=cutoff,
                        quarter_count=args.quarters,
                        delay=args.delay,
                        timeout=args.timeout,
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if attempt == 0:
                        time.sleep(max(args.delay * 6, 5.0))
                        continue
                    raise
            if result is None:
                raise last_error or RuntimeError("Unknown BSE XBRL fetch failure.")
            output_frames.append(result)
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "OK",
                    "bse_code": result["bse_code"].iloc[0],
                    "statement_used": result["statement_used"].iloc[0],
                    "quarters_exported": len(result),
                    "source_url": result["source_url"].iloc[-1],
                    "note": note,
                    "error": "",
                }
            )
            print(f" OK ({len(result)} quarters, BSE {result['bse_code'].iloc[0]})")
        except Exception as exc:  # noqa: BLE001
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "bse_code": "",
                    "statement_used": "",
                    "quarters_exported": 0,
                    "source_url": "",
                    "note": "",
                    "error": str(exc),
                }
            )
            print(f" ERROR ({exc})")

    long_df = (
        pd.concat(output_frames, ignore_index=True)
        if output_frames
        else pd.DataFrame(
            columns=[
                "symbol",
                "quarter_label",
                "quarter_end",
                "sales",
                "net_profit",
                "statement_used",
                "source_url",
                "bse_code",
                "sales_yoy_pct",
                "profit_yoy_pct",
            ]
        )
    )
    if not long_df.empty:
        long_df = long_df.sort_values(["symbol", "quarter_end"]).reset_index(drop=True)

    summary_df = pd.DataFrame(summary_rows).sort_values(["status", "symbol"]).reset_index(drop=True)
    wide_df = build_wide_view(long_df)

    excel_path, csv_path = write_outputs(args.output_dir, cutoff, long_df, wide_df, summary_df)

    ok_count = int((summary_df["status"] == "OK").sum()) if not summary_df.empty else 0
    error_count = int((summary_df["status"] == "ERROR").sum()) if not summary_df.empty else 0

    print("\nCompleted.")
    print(f"Successful symbols : {ok_count}")
    print(f"Failed symbols     : {error_count}")
    print(f"Excel output       : {excel_path}")
    print(f"CSV output         : {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
