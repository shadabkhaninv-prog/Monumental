#!/usr/bin/env python3
"""
Fetch quarterly sales, net profit, and YoY growth for symbols in a rated-list file.

Default source:
    Screener.in company pages, preferring consolidated statements when available.

Example:
    python quarterly_fundamentals_report.py rated_list_05aug2024.txt
    python quarterly_fundamentals_report.py reports\\rated_list_05aug2024.txt --statement standalone

Outputs:
    reports\\quarterly_fundamentals_<ddmmmyyyy>.xlsx
    reports\\quarterly_fundamentals_<ddmmmyyyy>.csv

Dependencies:
    pip install requests pandas openpyxl
"""

from __future__ import annotations

import argparse
import calendar
import math
import re
import sys
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


REPORTS_DIR = Path(__file__).resolve().parent / "reports"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
ROW_SALES = "sales"
ROW_NET_PROFIT = "net profit"


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
        help="Statement preference. 'auto' tries consolidated first, then standalone.",
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
        default=0.5,
        help="Delay in seconds between HTTP requests (default: 0.5).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="HTTP timeout in seconds (default: 25).",
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


def pct_growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round(((current / previous) - 1.0) * 100.0, 2)


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
    print(f"Statement mode : {args.statement}")
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

    output_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []

    for index, symbol in enumerate(symbols, start=1):
        if index > 1 and args.delay > 0:
            time.sleep(args.delay)

        print(f"[{index:>2}/{len(symbols)}] Fetching {symbol} ...", end="")
        sys.stdout.flush()

        try:
            table, source_url, statement_used = fetch_quarterly_table(
                session=session,
                symbol=symbol,
                statement_mode=args.statement,
                timeout=args.timeout,
            )
            result = quarterly_table_to_long(
                symbol=symbol,
                table=table,
                cutoff=cutoff,
                quarter_count=args.quarters,
                source_url=source_url,
                statement_used=statement_used,
            )
            output_frames.append(result)
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "OK",
                    "statement_used": statement_used,
                    "quarters_exported": len(result),
                    "source_url": source_url,
                    "error": "",
                }
            )
            print(f" OK ({len(result)} quarters, {statement_used})")
        except Exception as exc:  # noqa: BLE001
            summary_rows.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "statement_used": "",
                    "quarters_exported": 0,
                    "source_url": "",
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
