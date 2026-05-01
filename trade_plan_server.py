from __future__ import annotations

import importlib
import argparse
import copy
import csv
import concurrent.futures
import math
import json
import re
import traceback
import urllib.error
import urllib.request
from datetime import date, datetime, time as dt_time, timedelta
from difflib import get_close_matches
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import mysql.connector


DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "bhav",
}

DEFAULT_HTML_PATH = Path(__file__).resolve().parent / "TRADEP_12_1.htm"
PUBLIC_IPV4_PROBES = (
    "https://api.ipify.org?format=json",
    "https://checkip.amazonaws.com",
)
PUBLIC_IPV6_PROBES = (
    "https://api64.ipify.org?format=json",
    "https://api6.ipify.org?format=json",
)


def normalize_symbol(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").strip().upper())


def fetch_public_ip_from_probes(probes: Sequence[str], timeout: float = 4.0) -> Optional[str]:
    for url in probes:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="ignore").strip()
            if not raw:
                continue
            if raw.startswith("{"):
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                ip = str(payload.get("ip") or "").strip()
            else:
                ip = raw.splitlines()[0].strip()
            if ip:
                return ip
        except Exception:
            continue
    return None


def fetch_public_ip(timeout: float = 4.0) -> Optional[str]:
    return fetch_public_ip_from_probes(PUBLIC_IPV4_PROBES, timeout=timeout)


def fetch_public_ipv6(timeout: float = 4.0) -> Optional[str]:
    return fetch_public_ip_from_probes(PUBLIC_IPV6_PROBES, timeout=timeout)


def default_checklist_groups() -> List[Dict[str, object]]:
    return [
        {
            "title": "Entry",
            "count": 2,
            "items": [
                "Did not chase - entered at plan",
                "Sized to risk, not conviction",
                "",
            ],
        },
        {
            "title": "Holding",
            "count": 2,
            "items": [
                "Held winners according to plan",
                "Did not interfere with structure",
                "",
            ],
        },
        {
            "title": "Exit",
            "count": 2,
            "items": [
                "Moved or honored SL on plan",
                "Moved SL to breakeven when earned",
                "",
            ],
        },
    ]


def normalize_checklist_groups(raw_groups: object) -> List[Dict[str, object]]:
    defaults = default_checklist_groups()
    if not isinstance(raw_groups, list):
        return defaults

    groups: List[Dict[str, object]] = []
    for idx in range(3):
        raw_group = raw_groups[idx] if idx < len(raw_groups) and isinstance(raw_groups[idx], dict) else {}
        fallback = defaults[idx]
        title = str(raw_group.get("title") or fallback["title"]).strip() or str(fallback["title"])
        raw_items = raw_group.get("items") if isinstance(raw_group.get("items"), list) else []
        items: List[str] = []
        for item_idx in range(3):
            value = raw_items[item_idx] if item_idx < len(raw_items) else ""
            items.append(str(value or "").strip())
        try:
            count = int(raw_group.get("count", 0))
        except Exception:
            count = sum(1 for item in items if item)
        count = max(0, min(3, count))
        groups.append({"title": title, "count": count, "items": items})
    return groups


def legacy_checklist_values(groups: List[Dict[str, object]]) -> Dict[str, str]:
    groups = normalize_checklist_groups(groups)
    entry_items = groups[0]["items"] if groups else ["", "", ""]
    holding_items = groups[1]["items"] if len(groups) > 1 else ["", "", ""]
    exit_items = groups[2]["items"] if len(groups) > 2 else ["", "", ""]
    return {
        "checklist_entry_1": str(entry_items[0] or ""),
        "checklist_entry_2": str(entry_items[1] or ""),
        "checklist_risk_1": str(holding_items[0] or ""),
        "checklist_risk_2": str(exit_items[0] or ""),
    }


class BhavRepository:
    def __init__(self) -> None:
        self._year_tables: Optional[List[int]] = None
        self._symbol_catalog: Optional[List[str]] = None
        self._symbol_set: set[str] = set()
        self._inactive_map: Dict[str, str] = {}
        self._company_names: Dict[str, str] = {}
        self._debug_log_path = Path(__file__).resolve().parent / "logs" / "trade_plan_server.log"
        self._load_reference_data()

    def _get_conn(self):
        return mysql.connector.connect(**DB_CONFIG)

    def _log_debug(self, message: str) -> None:
        stamp = datetime.now().isoformat(timespec="seconds")
        line = f"[{stamp}] {message}\n"
        try:
            self._debug_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._debug_log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
        except Exception:
            pass

    def _is_transient_mysql_error(self, exc: Exception) -> bool:
        if not isinstance(exc, mysql.connector.Error):
            return False
        text = str(exc).lower()
        return any(
            token in text
            for token in (
                "lost connection",
                "server has gone away",
                "connection not available",
                "not connected",
                "broken pipe",
            )
        )

    def _log_db_failure(self, context: str, exc: Exception) -> None:
        self._log_debug(
            f"db failure [{context}]: {exc.__class__.__name__}: {exc}\n"
            + traceback.format_exc().rstrip()
        )

    def _with_retry_cursor(self, work, context: str = "db operation"):
        for attempt in range(2):
            conn = None
            cursor = None
            try:
                conn = self._get_conn()
                cursor = conn.cursor()
                return work(cursor)
            except Exception as exc:
                if attempt == 0 and self._is_transient_mysql_error(exc):
                    self._log_db_failure(f"{context} attempt={attempt + 1} retrying", exc)
                    continue
                self._log_db_failure(f"{context} attempt={attempt + 1}", exc)
                raise
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _load_reference_data(self) -> None:
        self._inactive_map = self._load_inactive_map()
        symbols = set(self._inactive_map.keys()) | {v for v in self._inactive_map.values() if v}
        self._company_names = {}

        def load_company_names(cursor):
            cursor.execute("SELECT UPPER(SYMBOL), COMPANY_NAME FROM nse_symbols")
            for symbol, company_name in cursor.fetchall():
                normalized = normalize_symbol(symbol)
                if not normalized:
                    continue
                symbols.add(normalized)
                if company_name:
                    self._company_names[normalized] = str(company_name)

        self._with_retry_cursor(load_company_names, "load company names from nse_symbols")

        latest_years = self.available_year_tables()[:3]
        def load_symbol_catalog(cursor):
            for year in latest_years:
                cursor.execute(f"SELECT DISTINCT UPPER(SYMBOL) FROM bhav{year}")
                for (symbol,) in cursor.fetchall():
                    normalized = normalize_symbol(symbol)
                    if normalized:
                        symbols.add(normalized)

        self._with_retry_cursor(
            load_symbol_catalog,
            f"load symbol catalog from {', '.join('bhav' + str(year) for year in latest_years) or 'bhav tables'}",
        )

        self._symbol_catalog = sorted(symbols)
        self._symbol_set = set(self._symbol_catalog)

    def _load_inactive_map(self) -> Dict[str, str]:
        result: Dict[str, str] = {}

        def load(cursor):
            cursor.execute(
                """
                SELECT UPPER(symbol) AS symbol, UPPER(TRIM(new_symbol)) AS new_symbol
                FROM inactive_symbols
                WHERE new_symbol IS NOT NULL
                  AND TRIM(new_symbol) <> ''
                """
            )
            for old_symbol, new_symbol in cursor.fetchall():
                old_norm = normalize_symbol(old_symbol)
                new_norm = normalize_symbol(new_symbol)
                if old_norm and new_norm:
                    result[old_norm] = new_norm

        self._with_retry_cursor(load, "load inactive symbol map from inactive_symbols")
        return result

    def available_year_tables(self) -> List[int]:
        if self._year_tables is None:
            years: List[int] = []

            def load(cursor):
                cursor.execute("SHOW TABLES LIKE 'bhav____'")
                for (table_name,) in cursor.fetchall():
                    suffix = table_name[4:]
                    if suffix.isdigit():
                        years.append(int(suffix))

            self._with_retry_cursor(load, "discover bhav year tables")
            self._year_tables = sorted(years, reverse=True)
        return self._year_tables

    def suggest_symbols(self, term: str, limit: int = 10) -> List[Dict[str, str]]:
        needle = normalize_symbol(term)
        if not needle:
            return []

        starts = [s for s in self._symbol_catalog or [] if s.startswith(needle)]
        contains = [s for s in self._symbol_catalog or [] if needle in s and not s.startswith(needle)]
        matches = (starts + contains)[:limit]
        return [
            {
                "symbol": symbol,
                "company_name": self._company_names.get(symbol, ""),
            }
            for symbol in matches
        ]

    def resolve_symbol(self, raw_symbol: str) -> Tuple[Optional[str], str, List[str]]:
        symbol = normalize_symbol(raw_symbol)
        if not symbol:
            return None, "empty", []

        replacement = self._inactive_map.get(symbol)
        if replacement and replacement in self._symbol_set:
            return replacement, "inactive_symbols", [replacement]

        if symbol in self._symbol_set:
            return symbol, "exact", []

        starts = [s for s in self._symbol_catalog or [] if s.startswith(symbol)]
        if len(starts) == 1:
            return starts[0], "prefix", starts[:5]
        if starts:
            return None, "ambiguous", starts[:5]

        contains = [s for s in self._symbol_catalog or [] if symbol in s]
        if len(contains) == 1:
            return contains[0], "contains", contains[:5]
        if contains:
            return None, "ambiguous", contains[:5]

        pool = [s for s in self._symbol_catalog or [] if s[:1] == symbol[:1]] or (self._symbol_catalog or [])
        close = get_close_matches(symbol, pool, n=5, cutoff=0.75)
        if len(close) == 1:
            return close[0], "fuzzy", close
        return None, "not_found", close

    def lookup_last_close(self, symbol: str, trade_date: date) -> Optional[Dict[str, object]]:
        normalized = normalize_symbol(symbol)
        if not normalized:
            return None

        candidate_years = [year for year in self.available_year_tables() if year <= trade_date.year]
        for year in candidate_years:
            row = None

            def lookup(cursor):
                cursor.execute(
                    f"""
                    SELECT UPPER(SYMBOL) AS symbol, CLOSE, MKTDATE
                    FROM bhav{year}
                    WHERE UPPER(SYMBOL) = %s
                      AND MKTDATE <= %s
                    ORDER BY MKTDATE DESC
                    LIMIT 1
                    """,
                    (normalized, trade_date),
                )
                return cursor.fetchone()

            try:
                row = self._with_retry_cursor(lookup, f"lookup_last_close symbol={normalized} table=bhav{year}")
            except Exception as exc:
                self._log_debug(f"lookup_last_close failed for {normalized} year={year}: {exc}")
                continue
            if row:
                actual_symbol, close_price, price_date = row
                return {
                    "symbol": normalize_symbol(actual_symbol),
                    "cmp": float(close_price) if close_price is not None else None,
                    "price_date": price_date.isoformat(),
                    "table": f"bhav{year}",
                }
        return None

    def fetch_daily_bars(self, symbol: str, start_date: date, end_date: date) -> List[Dict[str, object]]:
        normalized = normalize_symbol(symbol)
        if not normalized or start_date > end_date:
            return []
        candidate_years = [year for year in self.available_year_tables() if start_date.year <= year <= end_date.year]
        if not candidate_years:
            return []

        rows: List[Dict[str, object]] = []
        for year in candidate_years:
            def load_year(cursor):
                cursor.execute(
                    f"""
                    SELECT MKTDATE, OPEN, HIGH, LOW, CLOSE
                    FROM bhav{year}
                    WHERE UPPER(SYMBOL) = %s
                      AND MKTDATE BETWEEN %s AND %s
                    ORDER BY MKTDATE
                    """,
                    (normalized, start_date, end_date),
                )
                return cursor.fetchall()

            try:
                year_rows = self._with_retry_cursor(load_year, f"fetch_daily_bars symbol={normalized} table=bhav{year}")
            except Exception as exc:
                self._log_debug(f"fetch_daily_bars failed for {normalized} year={year}: {exc}")
                continue
            for mktdate, open_price, high_price, low_price, close_price in year_rows or []:
                rows.append(
                    {
                        "mktdate": mktdate,
                        "open": float(open_price) if open_price is not None else None,
                        "high": float(high_price) if high_price is not None else None,
                        "low": float(low_price) if low_price is not None else None,
                        "close": float(close_price) if close_price is not None else None,
                    }
                )
        rows.sort(key=lambda item: item["mktdate"])
        return rows

    def latest_market_date(self) -> Optional[date]:
        latest: Optional[date] = None
        for year in self.available_year_tables():
            def load(cursor):
                cursor.execute(f"SELECT MAX(MKTDATE) FROM bhav{year}")
                return cursor.fetchone()

            try:
                row = self._with_retry_cursor(load, f"latest_market_date table=bhav{year}")
            except Exception as exc:
                self._log_debug(f"latest_market_date failed for year={year}: {exc}")
                continue
            if not row:
                continue
            value = row[0]
            if value is None:
                continue
            if isinstance(value, datetime):
                value = value.date()
            elif isinstance(value, str):
                try:
                    value = date.fromisoformat(value[:10])
                except Exception:
                    parsed = self._parse_tradebook_time(value)
                    value = parsed.date() if parsed else None
            if not isinstance(value, date):
                continue
            if latest is None or value > latest:
                latest = value
        return latest

    def resolve_with_price(self, raw_symbol: str, trade_date: date) -> Dict[str, object]:
        canonical, matched_via, suggestions = self.resolve_symbol(raw_symbol)
        if not canonical:
            return {
                "ok": False,
                "input_symbol": normalize_symbol(raw_symbol),
                "matched_via": matched_via,
                "suggestions": suggestions,
                "message": "Symbol not resolved in bhav universe.",
            }

        price_info = self.lookup_last_close(canonical, trade_date)
        if not price_info:
            return {
                "ok": False,
                "input_symbol": normalize_symbol(raw_symbol),
                "canonical_symbol": canonical,
                "matched_via": matched_via,
                "suggestions": suggestions,
                "message": f"No bhav close found for {canonical} on or before {trade_date.isoformat()}.",
            }

        return {
            "ok": True,
            "input_symbol": normalize_symbol(raw_symbol),
            "canonical_symbol": canonical,
            "matched_via": matched_via,
            "suggestions": suggestions,
            "company_name": self._company_names.get(canonical, ""),
            **price_info,
        }


class TradePlanStore:
    def __init__(self, html_path: Path) -> None:
        self.html_path = html_path.resolve()
        self.save_dir = Path(__file__).resolve().parent / "trade_plan_1_data"
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.base_dir = Path(__file__).resolve().parent
        self.debug_log_path = self.base_dir / "logs" / "trade_plan_server.log"
        self.debug_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings_path = self.save_dir / "settings.json"
        self._kite_token_maps: Optional[Tuple[Dict[str, int], Dict[str, int]]] = None
        self._kite_client = None
        self._trim_date_hints_cache: Optional[Dict[str, Dict[int, date]]] = None

    def _log_debug(self, message: str) -> None:
        stamp = datetime.now().isoformat(timespec="seconds")
        line = f"[{stamp}] {message}\n"
        try:
            with self.debug_log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
        except Exception:
            pass

    def _trim_position_key(self, position: dict) -> str:
        pid = str(position.get("id") or "").strip()
        if pid:
            return pid
        return "|".join(
            [
                normalize_symbol(str(position.get("symbol") or "")),
                str(position.get("entryDate") or ""),
                str(position.get("actualEntry") or ""),
                str(position.get("coreEntry") or ""),
                str(position.get("tacticalEntry") or ""),
                str(position.get("planSL") or ""),
                str(position.get("trailOverride") or ""),
            ]
        )

    def _strip_legacy_trade_fields(self, position: Dict[str, object]) -> Dict[str, object]:
        if not isinstance(position, dict):
            return position
        normalized = copy.deepcopy(position)
        for legacy_key in ("overnightEntry", "overnightQty", "overnightSL", "intraQty", "intraEntry", "intraSL", "intraRiskPct"):
            normalized.pop(legacy_key, None)
        return normalized

    def _parse_trim_date(self, value: object) -> Optional[date]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _collect_trim_date_hints(self) -> Dict[str, Dict[int, date]]:
        if self._trim_date_hints_cache is not None:
            return self._trim_date_hints_cache
        hints: Dict[str, Dict[int, date]] = {}
        for plan_date in self.list_plan_dates():
            payload = self.load_plan_raw(plan_date)
            positions = payload.get("positions", []) if isinstance(payload, dict) else []
            if not isinstance(positions, list):
                continue
            for raw_position in positions:
                if not isinstance(raw_position, dict):
                    continue
                pid = self._trim_position_key(raw_position)
                trims = raw_position.get("trims") if isinstance(raw_position.get("trims"), list) else []
                if not pid or not isinstance(trims, list):
                    continue
                trim_map = hints.setdefault(pid, {})
                for trim_idx, trim in enumerate(trims):
                    if not isinstance(trim, dict) or not trim.get("done"):
                        continue
                    trim_day = self._parse_trim_date(trim.get("dt"))
                    if trim_day is None:
                        continue
                    existing = trim_map.get(trim_idx)
                    if existing is None or trim_day > existing:
                        trim_map[trim_idx] = trim_day
        self._trim_date_hints_cache = hints
        return hints

    def _apply_trim_date_hints(self, positions: Sequence[dict]) -> List[dict]:
        hints = self._collect_trim_date_hints()
        normalized_positions: List[dict] = []
        for raw_position in positions:
            if not isinstance(raw_position, dict):
                continue
            position = copy.deepcopy(raw_position)
            pid = self._trim_position_key(position)
            trim_hints = hints.get(pid, {})
            trims = position.get("trims") if isinstance(position.get("trims"), list) else []
            if isinstance(trims, list):
                for trim_idx, trim in enumerate(trims):
                    if not isinstance(trim, dict) or not trim.get("done"):
                        continue
                    hint_day = trim_hints.get(trim_idx)
                    current_day = self._parse_trim_date(trim.get("dt"))
                    if hint_day is None:
                        continue
                    if current_day is None or hint_day > current_day:
                        trim["dt"] = hint_day.isoformat()
            normalized_positions.append(position)
        return normalized_positions

    def plan_path(self, plan_date: str) -> Path:
        return self.save_dir / f"{plan_date}.json"

    def list_plan_dates(self) -> List[str]:
        dates: List[str] = []
        for path in self.save_dir.glob("*.json"):
            if path.name == self.settings_path.name:
                continue
            try:
                datetime.strptime(path.stem, "%Y-%m-%d")
            except ValueError:
                continue
            dates.append(path.stem)
        return sorted(dates)

    def load_settings(self) -> Dict[str, object]:
        defaults = {
            "available_capital": None,
            "daily_risk": None,
            "per_position_risk": None,
            "stop_loss_pct": 2.0,
            "checklist_groups": default_checklist_groups(),
        }
        if not self.settings_path.exists():
            legacy = legacy_checklist_values(defaults["checklist_groups"])
            return {**defaults, **legacy}
        try:
            payload = json.loads(self.settings_path.read_text(encoding="utf-8"))
        except Exception:
            legacy = legacy_checklist_values(defaults["checklist_groups"])
            return {**defaults, **legacy}
        if not isinstance(payload, dict):
            legacy = legacy_checklist_values(defaults["checklist_groups"])
            return {**defaults, **legacy}
        groups = payload.get("checklist_groups")
        if not isinstance(groups, list):
            groups = [
                {
                    "title": "Entry",
                    "count": 2,
                    "items": [
                        payload.get("checklist_entry_1") or defaults["checklist_groups"][0]["items"][0],
                        payload.get("checklist_entry_2") or defaults["checklist_groups"][0]["items"][1],
                        "",
                    ],
                },
                {
                    "title": "Holding",
                    "count": 1,
                    "items": [
                        payload.get("checklist_risk_1") or defaults["checklist_groups"][1]["items"][0],
                        "",
                        "",
                    ],
                },
                {
                    "title": "Exit",
                    "count": 1,
                    "items": [
                        payload.get("checklist_risk_2") or defaults["checklist_groups"][2]["items"][0],
                        "",
                        "",
                    ],
                },
            ]
        groups = normalize_checklist_groups(groups)
        legacy = legacy_checklist_values(groups)
        return {
            "available_capital": payload.get("available_capital"),
            "daily_risk": payload.get("daily_risk"),
            "per_position_risk": payload.get("per_position_risk"),
            "stop_loss_pct": payload.get("stop_loss_pct", 2.0),
            "checklist_groups": groups,
            **legacy,
        }

    def save_settings(self, settings: Dict[str, object]) -> Dict[str, object]:
        groups = normalize_checklist_groups(settings.get("checklist_groups"))
        legacy = legacy_checklist_values(groups)
        payload = {
            "available_capital": settings.get("available_capital"),
            "daily_risk": settings.get("daily_risk"),
            "per_position_risk": settings.get("per_position_risk"),
            "stop_loss_pct": settings.get("stop_loss_pct", 2.0),
            "checklist_groups": groups,
            **legacy,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        self.settings_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {"ok": True, "path": str(self.settings_path), **payload}

    def _latest_tradebook_path(self) -> Optional[Path]:
        trade_dir = self.base_dir / "trades"
        self._log_debug(f"tradebook lookup started: base_dir={self.base_dir} trade_dir={trade_dir}")
        if not trade_dir.exists():
            self._log_debug("tradebook lookup failed: trades folder does not exist")
            return None
        try:
            candidates = [path for path in trade_dir.glob("*.csv") if path.is_file()]
        except Exception as exc:
            self._log_debug(f"tradebook lookup failed while listing csv files: {exc}")
            return None
        if not candidates:
            self._log_debug("tradebook lookup found no csv candidates")
            return None
        try:
            chosen = max(candidates, key=lambda path: (path.stat().st_mtime, path.name.lower()))
        except Exception as exc:
            self._log_debug(f"tradebook lookup failed while selecting latest file: {exc}")
            return None
        try:
            stats = chosen.stat()
            self._log_debug(
                "tradebook lookup picked %s size=%s mtime=%s candidate_count=%s"
                % (chosen.name, stats.st_size, datetime.fromtimestamp(stats.st_mtime).isoformat(timespec="seconds"), len(candidates))
            )
        except Exception:
            self._log_debug(f"tradebook lookup picked {chosen}")
        return chosen

    def _parse_tradebook_time(self, raw_value: object) -> Optional[datetime]:
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            return None
        try:
            return datetime.fromisoformat(raw_text)
        except ValueError:
            pass
        try:
            return datetime.strptime(raw_text, "%Y-%m-%d")
        except ValueError:
            return None

    def _kite_client_for_analysis(self):
        if self._kite_client is not None:
            return self._kite_client
        kite_mod = importlib.import_module("place_kite_stop_loss_orders")
        self._kite_client = kite_mod.get_kite_client(Path(kite_mod.DEFAULT_TOKEN_FILE))
        return self._kite_client

    def _kite_symbol_token_maps(self) -> Tuple[Dict[str, int], Dict[str, int]]:
        if self._kite_token_maps is not None:
            return self._kite_token_maps
        kite = self._kite_client_for_analysis()
        instruments = kite.instruments("NSE")
        exact: Dict[str, int] = {}
        normalized: Dict[str, int] = {}
        for inst in instruments or []:
            symbol = str(inst.get("tradingsymbol") or "").strip().upper()
            if not symbol:
                continue
            segment = str(inst.get("segment") or "").strip().upper()
            if segment and segment != "NSE":
                continue
            try:
                token = int(inst.get("instrument_token"))
            except (TypeError, ValueError):
                continue
            exact.setdefault(symbol, token)
            normalized.setdefault(normalize_symbol(symbol), token)
        self._kite_token_maps = (exact, normalized)
        return self._kite_token_maps

    def _resolve_kite_token(self, symbol: str) -> Optional[int]:
        normalized = normalize_symbol(symbol)
        if not normalized:
            return None
        exact, normalized_map = self._kite_symbol_token_maps()
        if symbol.strip().upper() in exact:
            return exact[symbol.strip().upper()]
        return normalized_map.get(normalized)

    def _fetch_kite_rows(self, symbol: str, start_dt: datetime, end_dt: datetime, interval: str) -> List[Dict[str, object]]:
        token = self._resolve_kite_token(symbol)
        if token is None:
            return []
        kite = self._kite_client_for_analysis()
        def fetch_rows() -> List[Dict[str, object]]:
            try:
                rows = kite.historical_data(
                    instrument_token=token,
                    from_date=start_dt,
                    to_date=end_dt,
                    interval=interval,
                    continuous=False,
                )
            except Exception as exc:
                self._log_debug(f"kite fetch failed for {symbol} interval={interval}: {exc}")
                return []
            return rows if isinstance(rows, list) else []

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(fetch_rows)
            return future.result(timeout=8)
        except concurrent.futures.TimeoutError:
            self._log_debug(f"kite fetch timed out for {symbol} interval={interval} from {start_dt.isoformat()} to {end_dt.isoformat()}")
            return []
        except Exception as exc:
            self._log_debug(f"kite fetch wrapper failed for {symbol} interval={interval}: {exc}")
            return []
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _intraday_row_dt(self, row: Dict[str, object]) -> Optional[datetime]:
        for key in ("date", "datetime", "timestamp", "time"):
            value = row.get(key)
            if isinstance(value, datetime):
                return value.replace(tzinfo=None)
            parsed = self._parse_tradebook_time(value)
            if parsed:
                return parsed.replace(tzinfo=None)
        return None

    def _naive_dt(self, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        return value.replace(tzinfo=None)

    def _normalize_kite_bar(self, row: Dict[str, object]) -> Optional[Dict[str, object]]:
        bar_dt = self._intraday_row_dt(row)
        if bar_dt is None:
            return None
        return {
            "mktdate": bar_dt.date(),
            "open": self._as_float(row.get("open")),
            "high": self._as_float(row.get("high")),
            "low": self._as_float(row.get("low")),
            "close": self._as_float(row.get("close")),
        }

    def _evaluate_stop_path(
        self,
        symbol: str,
        entry_dt: Optional[datetime],
        entry_day: Optional[date],
        end_day: Optional[date],
        entry_price: Optional[float],
        stop_loss_pct: float,
        repo: BhavRepository,
        exit_dt: Optional[datetime] = None,
        live_return_pct: Optional[float] = None,
        simulation_end_day: Optional[date] = None,
    ) -> Dict[str, object]:
        stop_price = round((entry_price or 0.0) * (1.0 - (stop_loss_pct / 100.0)), 2) if entry_price else None
        analysis_end_day = simulation_end_day or end_day
        self._log_debug(
            "evaluate stop path: symbol=%s entry_dt=%s entry_day=%s end_day=%s analysis_end_day=%s entry_price=%s exit_dt=%s live_return=%s"
            % (
                symbol,
                entry_dt.isoformat(timespec="seconds") if entry_dt else "",
                entry_day.isoformat() if entry_day else "",
                end_day.isoformat() if end_day else "",
                analysis_end_day.isoformat() if analysis_end_day else "",
                f"{entry_price:.2f}" if entry_price is not None else "",
                exit_dt.isoformat(timespec="seconds") if exit_dt else "",
                f"{live_return_pct:.2f}" if live_return_pct is not None else "",
            )
        )
        result = {
            "stop_price": stop_price,
            "stop_touched": False,
            "stop_touch_date": "",
            "stop_touch_stage": "",
            "stop_touch_price": None,
            "counterfactual_return_pct": live_return_pct,
        }
        last_stop = stop_price

        if not entry_price or not entry_day or not analysis_end_day or analysis_end_day < entry_day:
            return result

        warm_start = max(date(2000, 1, 1), entry_day - timedelta(days=40))
        all_bars = repo.fetch_daily_bars(symbol, warm_start, analysis_end_day)
        all_bars = [bar for bar in all_bars if bar.get("mktdate")]
        if not all_bars:
            return result

        alpha = 2.0 / (5.0 + 1.0)
        ema_values: List[Optional[float]] = []
        ema_val: Optional[float] = None
        for bar in all_bars:
            close_value = self._as_float(bar.get("close"))
            if close_value <= 0:
                ema_values.append(ema_val)
                continue
            if ema_val is None:
                ema_val = close_value
            else:
                ema_val = ema_val + (alpha * (close_value - ema_val))
            ema_values.append(ema_val)

        bar_index = {bar["mktdate"]: idx for idx, bar in enumerate(all_bars)}
        entry_idx = next((idx for idx, bar in enumerate(all_bars) if bar["mktdate"] >= entry_day), None)
        if entry_idx is None:
            return result

        session_end = datetime.combine(entry_day, dt_time(15, 30))
        first_day_limit = session_end
        entry_dt = self._naive_dt(entry_dt)
        first_day_limit = self._naive_dt(first_day_limit)
        exit_dt = self._naive_dt(exit_dt)

        kite_daily_raw = self._fetch_kite_rows(symbol, datetime.combine(entry_day, dt_time(0, 0)), datetime.combine(analysis_end_day, dt_time(23, 59)), "day")
        kite_daily_rows = [bar for row in kite_daily_raw if (bar := self._normalize_kite_bar(row))]
        if kite_daily_rows:
            self._log_debug(f"kite daily rows loaded for {symbol}: {len(kite_daily_rows)}")
        daily_rows = kite_daily_rows if kite_daily_rows else all_bars

        daily_entry_idx = next((idx for idx, bar in enumerate(daily_rows) if bar["mktdate"] >= entry_day), None)
        if daily_entry_idx is None:
            return result

        for idx, bar in enumerate(daily_rows[daily_entry_idx:], start=1):
            bar_date = bar["mktdate"]
            close_value = self._as_float(bar.get("close"))
            if close_value <= 0:
                continue
            if idx == 1 and entry_dt is not None:
                if stop_price is None:
                    continue
                intraday_rows = self._fetch_kite_rows(symbol, entry_dt, first_day_limit, "30minute")
                if intraday_rows:
                    self._log_debug(f"entry-day intraday rows loaded for {symbol}: {len(intraday_rows)}")
                    intraday_rows = sorted(
                        intraday_rows,
                        key=lambda row: self._intraday_row_dt(row) or datetime.min,
                    )
                    for intrabar in intraday_rows:
                        intrabar_dt = self._intraday_row_dt(intrabar)
                        if intrabar_dt is None or intrabar_dt < entry_dt or intrabar_dt > first_day_limit:
                            continue
                        intrabar_low = self._as_float(intrabar.get("low"))
                        if intrabar_low <= 0:
                            continue
                        if stop_price is not None and intrabar_low <= stop_price:
                            result["stop_touched"] = True
                            result["stop_touch_date"] = intrabar_dt.isoformat(timespec="seconds")
                            result["stop_touch_stage"] = "initial"
                            result["stop_touch_price"] = stop_price
                            result["counterfactual_return_pct"] = round(((stop_price / entry_price) - 1.0) * 100.0, 2)
                            return result
                    continue
                self._log_debug(
                    f"stop-loss streak entry-day intraday rows missing for {symbol} on {entry_day.isoformat()}, unable to confirm whether stop was touched after buy time"
                )
                continue
            if idx <= 2:
                current_stop = stop_price
                stop_stage = "initial"
            else:
                prev_ema = ema_values[bar_index[bar_date] - 1] if bar_index[bar_date] - 1 >= 0 else None
                current_stop = round(max(entry_price, prev_ema if prev_ema is not None else entry_price), 2)
                stop_stage = "trail"
            last_stop = current_stop
            if current_stop is not None and close_value <= current_stop:
                result["stop_touched"] = True
                result["stop_touch_date"] = bar_date.isoformat()
                result["stop_touch_stage"] = stop_stage
                result["stop_touch_price"] = current_stop
                result["counterfactual_return_pct"] = round(((current_stop / entry_price) - 1.0) * 100.0, 2)
                return result

        if daily_rows and entry_price:
            final_close = self._as_float(daily_rows[-1].get("close"))
            if final_close > 0:
                result["counterfactual_return_pct"] = round(((final_close / entry_price) - 1.0) * 100.0, 2)
        else:
            final_close_info = repo.lookup_last_close(symbol, analysis_end_day)
            if final_close_info and final_close_info.get("cmp") is not None and entry_price:
                final_close = float(final_close_info["cmp"])
                result["counterfactual_return_pct"] = round(((final_close / entry_price) - 1.0) * 100.0, 2)
        self._log_debug(
            "stop path completed without touch: symbol=%s analysis_end_day=%s counterfactual=%s"
            % (
                symbol,
                analysis_end_day.isoformat() if analysis_end_day else "",
                f"{result['counterfactual_return_pct']:.2f}" if result["counterfactual_return_pct"] is not None else "",
            )
        )
        return result

    def _build_stop_loss_campaigns(
        self,
        rows: List[Dict[str, object]],
        repo: BhavRepository,
        stop_loss_pct: float,
        history_start_date: Optional[date] = None,
    ) -> Dict[str, object]:
        grouped: Dict[str, List[Dict[str, object]]] = {}
        for row in rows:
            symbol = normalize_symbol(str(row.get("symbol", "")))
            if not symbol:
                continue
            grouped.setdefault(symbol, []).append(row)

        campaigns: List[Dict[str, object]] = []
        latest_trade_dt: Optional[date] = None
        latest_market_day = repo.latest_market_date()

        for symbol, symbol_rows in grouped.items():
            symbol_rows = sorted(
                symbol_rows,
                key=lambda row: (
                    self._parse_tradebook_time(row.get("order_execution_time")) or datetime.min,
                    str(row.get("trade_date") or ""),
                    str(row.get("trade_id") or ""),
                    str(row.get("order_id") or ""),
                ),
            )
            if symbol_rows:
                row_dt = self._parse_tradebook_time(symbol_rows[-1].get("trade_date"))
                if row_dt:
                    latest_trade_dt = max(latest_trade_dt, row_dt.date()) if latest_trade_dt else row_dt.date()

            current: Optional[Dict[str, object]] = None
            for row in symbol_rows:
                trade_type = str(row.get("trade_type") or "").strip().lower()
                qty = self._as_float(row.get("quantity"))
                price = self._as_float(row.get("price"))
                if qty <= 0 or price <= 0:
                    continue
                row_time = self._parse_tradebook_time(row.get("order_execution_time")) or self._parse_tradebook_time(row.get("trade_date")) or datetime.min
                if trade_type == "buy":
                    if current is None:
                        current = {
                            "symbol": symbol,
                            "start_time": row_time,
                            "end_time": None,
                            "trade_date": str(row.get("trade_date") or ""),
                            "buy_qty": 0.0,
                            "sell_qty": 0.0,
                            "buy_value": 0.0,
                            "sell_value": 0.0,
                            "net_qty": 0.0,
                            "status": "open",
                        }
                    current["buy_qty"] = float(current["buy_qty"]) + qty
                    current["buy_value"] = float(current["buy_value"]) + (qty * price)
                    current["net_qty"] = float(current["net_qty"]) + qty
                    current["end_time"] = row_time
                elif trade_type == "sell":
                    if current is None:
                        continue
                    current["sell_qty"] = float(current["sell_qty"]) + qty
                    current["sell_value"] = float(current["sell_value"]) + (qty * price)
                    current["net_qty"] = float(current["net_qty"]) - qty
                    current["end_time"] = row_time

                if current is not None and float(current["net_qty"]) <= 1e-9 and float(current["buy_qty"]) > 0:
                    buy_qty = float(current["buy_qty"])
                    sell_qty = float(current["sell_qty"])
                    buy_value = float(current["buy_value"])
                    sell_value = float(current["sell_value"])
                    entry_price = round(buy_value / buy_qty, 2) if buy_qty else None
                    exit_price = round(sell_value / sell_qty, 2) if sell_qty else None
                    actual_return_pct = ((sell_value / buy_value) - 1.0) * 100.0 if buy_value else None
                    entry_day = current["start_time"].date() if isinstance(current.get("start_time"), datetime) else None
                    end_day = current["end_time"].date() if isinstance(current.get("end_time"), datetime) else (latest_trade_dt or entry_day)
                    path = self._evaluate_stop_path(
                        symbol=symbol,
                        entry_dt=current.get("start_time") if isinstance(current.get("start_time"), datetime) else None,
                        entry_day=entry_day,
                        end_day=end_day,
                        entry_price=entry_price,
                        stop_loss_pct=stop_loss_pct,
                        repo=repo,
                        exit_dt=current.get("end_time") if isinstance(current.get("end_time"), datetime) else None,
                        live_return_pct=actual_return_pct,
                        simulation_end_day=latest_market_day,
                    )
                    stop_price = path["stop_price"]
                    stop_touched = bool(path["stop_touched"])
                    stop_touch_date = str(path["stop_touch_date"] or "")
                    stop_touch_stage = str(path["stop_touch_stage"] or "")
                    stop_touch_price = path.get("stop_touch_price")
                    counterfactual_return_pct = path["counterfactual_return_pct"]
                    simulation_exit_price = stop_touch_price if stop_touched else (
                        round(entry_price * (1.0 + (float(counterfactual_return_pct) / 100.0)), 2)
                        if counterfactual_return_pct is not None and entry_price is not None
                        else (exit_price if exit_price is not None else None)
                    )
                    sim_qty = self._simulation_qty(entry_price, 300000.0)
                    campaigns.append(
                        {
                            "symbol": symbol,
                            "entry_date": entry_day.isoformat() if entry_day else "",
                            "start_time": current["start_time"].isoformat() if isinstance(current["start_time"], datetime) else "",
                            "end_time": current["end_time"].isoformat() if isinstance(current["end_time"], datetime) else "",
                            "status": "closed",
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "sim_qty": sim_qty,
                            "simulated_exit_price": simulation_exit_price,
                            "sim_value": round(float(simulation_exit_price or 0.0) * sim_qty, 2) if simulation_exit_price is not None and sim_qty else None,
                            "stop_price": stop_price,
                            "stop_touch_price": stop_touch_price,
                            "buy_qty": round(buy_qty, 6),
                            "sell_qty": round(sell_qty, 6),
                            "actual_return_pct": round(actual_return_pct, 2) if actual_return_pct is not None else None,
                            "return_pct": round(actual_return_pct, 2) if actual_return_pct is not None else None,
                            "honored": not stop_touched,
                            "stop_touched": stop_touched,
                            "counterfactual_return_pct": counterfactual_return_pct,
                            "breach_pct": round((actual_return_pct or 0.0) - (counterfactual_return_pct or 0.0), 2) if actual_return_pct is not None else None,
                            "stop_touch_date": stop_touch_date,
                            "stop_touch_stage": stop_touch_stage,
                            "analysis_basis": "symbol_roundtrip_day1_tactical_then_daily_close_with_day3_ema_trailing",
                        }
                    )
                    current = None

            if current is not None and float(current["buy_qty"]) > 0:
                buy_qty = float(current["buy_qty"])
                buy_value = float(current["buy_value"])
                entry_price = round(buy_value / buy_qty, 2) if buy_qty else None
                price_info = repo.lookup_last_close(symbol, latest_trade_dt) if latest_trade_dt else None
                cmp_value = None
                cmp_date = None
                if price_info and price_info.get("cmp") is not None:
                    cmp_value = float(price_info["cmp"])
                    cmp_date = str(price_info.get("price_date") or "")
                current_return = ((cmp_value / entry_price) - 1.0) * 100.0 if cmp_value and entry_price else None
                entry_day = current["start_time"].date() if isinstance(current.get("start_time"), datetime) else None
                end_day = latest_trade_dt or entry_day
                path = self._evaluate_stop_path(
                    symbol=symbol,
                    entry_dt=current.get("start_time") if isinstance(current.get("start_time"), datetime) else None,
                    entry_day=entry_day,
                    end_day=end_day,
                    entry_price=entry_price,
                    stop_loss_pct=stop_loss_pct,
                    repo=repo,
                    exit_dt=current.get("end_time") if isinstance(current.get("end_time"), datetime) else None,
                    live_return_pct=current_return,
                    simulation_end_day=latest_market_day,
                )
                stop_price = path["stop_price"]
                stop_touched = bool(path["stop_touched"])
                stop_touch_date = str(path["stop_touch_date"] or "")
                stop_touch_stage = str(path["stop_touch_stage"] or "")
                stop_touch_price = path.get("stop_touch_price")
                counterfactual_return_pct = path["counterfactual_return_pct"]
                simulation_exit_price = stop_touch_price if stop_touched else (
                    round(entry_price * (1.0 + (float(counterfactual_return_pct) / 100.0)), 2)
                    if counterfactual_return_pct is not None and entry_price is not None
                    else cmp_value
                )
                sim_qty = self._simulation_qty(entry_price, 300000.0)
                campaigns.append(
                    {
                        "symbol": symbol,
                        "entry_date": entry_day.isoformat() if entry_day else "",
                        "start_time": current["start_time"].isoformat() if isinstance(current["start_time"], datetime) else "",
                        "end_time": "",
                        "status": "open",
                        "entry_price": entry_price,
                        "exit_price": cmp_value,
                        "sim_qty": sim_qty,
                        "simulated_exit_price": simulation_exit_price,
                        "sim_value": round(cmp_value * sim_qty, 2) if cmp_value is not None and sim_qty else None,
                        "stop_price": stop_price,
                        "stop_touch_price": stop_touch_price,
                        "price_date": cmp_date,
                        "buy_qty": round(buy_qty, 6),
                        "sell_qty": round(float(current["sell_qty"]), 6),
                        "actual_return_pct": round(current_return, 2) if current_return is not None else None,
                        "return_pct": round(current_return, 2) if current_return is not None else None,
                        "honored": not stop_touched,
                        "stop_touched": stop_touched,
                        "counterfactual_return_pct": counterfactual_return_pct,
                        "breach_pct": round((current_return or 0.0) - (counterfactual_return_pct or 0.0), 2) if current_return is not None else None,
                        "stop_touch_date": stop_touch_date,
                        "stop_touch_stage": stop_touch_stage,
                        "analysis_basis": "symbol_roundtrip_day1_tactical_then_daily_close_with_day3_ema_trailing",
                    }
                )

        campaigns.sort(
            key=lambda item: (
                item.get("end_time") or item.get("start_time") or "",
                item.get("symbol") or "",
            )
        )

        if history_start_date:
            campaigns = [
                item for item in campaigns
                if (self._parse_tradebook_time(item.get("start_time")) or datetime.min).date() >= history_start_date
            ]

        closed_campaigns = [item for item in campaigns if item.get("status") == "closed"]
        open_campaigns = [item for item in campaigns if item.get("status") == "open"]

        closed_honored = [item for item in closed_campaigns if item.get("honored")]
        closed_losses = [item for item in closed_campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) < 0]
        closed_wins = [item for item in closed_campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) >= 0]
        breaches = [item for item in closed_campaigns if item.get("stop_touched")]
        actual_gain_returns = [
            float(item.get("actual_return_pct"))
            for item in closed_campaigns
            if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) >= 0
        ]
        actual_loss_returns = [
            float(item.get("actual_return_pct"))
            for item in closed_campaigns
            if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) < 0
        ]
        actual_breakeven_returns = [
            float(item.get("actual_return_pct"))
            for item in closed_campaigns
            if item.get("actual_return_pct") is not None and round(float(item.get("actual_return_pct")), 2) == 0.0
        ]
        counterfactual_wins = [
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) >= 0
        ]
        counterfactual_losses = [
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) < 0
        ]
        counterfactual_gain_returns = [
            float(item.get("counterfactual_return_pct"))
            for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) >= 0
        ]
        counterfactual_loss_returns = [
            float(item.get("counterfactual_return_pct"))
            for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) < 0
        ]
        counterfactual_breakeven_returns = [
            float(item.get("counterfactual_return_pct"))
            for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and round(float(item.get("counterfactual_return_pct")), 2) == 0.0
        ]
        counterfactual_positive_count = len([
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) > 0
        ])
        counterfactual_negative_count = len([
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) < 0
        ])
        counterfactual_decisive_count = counterfactual_positive_count + counterfactual_negative_count

        longest_streak = 0
        current_streak = 0
        for item in closed_campaigns:
            if item.get("honored"):
                current_streak += 1
                longest_streak = max(longest_streak, current_streak)
            else:
                current_streak = 0

        live_streak = 0
        for item in campaigns:
            if item.get("honored"):
                live_streak += 1
            else:
                live_streak = 0

        closed_count = len(closed_campaigns)
        honored_count = len(closed_honored)
        win_count = len(closed_wins)
        loss_count = len(closed_losses)
        breach_count = len(breaches)
        return {
            "ok": True,
            "tradebook_path": str(self._latest_tradebook_path()) if self._latest_tradebook_path() else "",
            "latest_trade_date": latest_trade_dt.isoformat() if latest_trade_dt else "",
            "stop_loss_pct": round(stop_loss_pct, 2),
            "summary": {
                "closed_campaigns": closed_count,
                "open_campaigns": len(open_campaigns),
                "honored_campaigns": honored_count,
                "stop_touched_campaigns": breach_count,
                "breach_count": breach_count,
                "honor_rate": round((honored_count / closed_count) * 100.0, 1) if closed_count else None,
                "win_rate": round((win_count / closed_count) * 100.0, 1) if closed_count else None,
                "loss_rate": round((loss_count / closed_count) * 100.0, 1) if closed_count else None,
                "longest_honor_streak": longest_streak,
                "current_honor_streak": live_streak,
                "actual_win_count": win_count,
                "actual_loss_count": loss_count,
                "actual_win_rate": round((win_count / closed_count) * 100.0, 1) if closed_count else None,
                "actual_loss_rate": round((loss_count / closed_count) * 100.0, 1) if closed_count else None,
                "actual_avg_gain_pct": round(sum(actual_gain_returns) / len(actual_gain_returns), 2) if actual_gain_returns else None,
                "actual_avg_loss_pct": round(sum(actual_loss_returns) / len(actual_loss_returns), 2) if actual_loss_returns else None,
                "actual_breakeven_count": len(actual_breakeven_returns),
                "actual_breakeven_rate": round((len(actual_breakeven_returns) / closed_count) * 100.0, 1) if closed_count else None,
                "counterfactual_win_rate": round((counterfactual_positive_count / counterfactual_decisive_count) * 100.0, 1) if counterfactual_decisive_count else None,
                "counterfactual_loss_rate": round((counterfactual_negative_count / counterfactual_decisive_count) * 100.0, 1) if counterfactual_decisive_count else None,
                "counterfactual_avg_gain_pct": round(sum(counterfactual_gain_returns) / len(counterfactual_gain_returns), 2) if counterfactual_gain_returns else None,
                "counterfactual_avg_loss_pct": round(sum(counterfactual_loss_returns) / len(counterfactual_loss_returns), 2) if counterfactual_loss_returns else None,
                "counterfactual_decisive_count": counterfactual_decisive_count,
                "counterfactual_breakeven_count": len(counterfactual_breakeven_returns),
                "counterfactual_breakeven_rate": round((len(counterfactual_breakeven_returns) / closed_count) * 100.0, 1) if closed_count else None,
                "breaches_prevented": breach_count,
                "best_return_pct": round(max((float(item.get("return_pct")) for item in closed_campaigns if item.get("return_pct") is not None), default=0.0), 2) if closed_campaigns else None,
                "worst_return_pct": round(min((float(item.get("return_pct")) for item in closed_campaigns if item.get("return_pct") is not None), default=0.0), 2) if closed_campaigns else None,
            },
            "campaigns": campaigns,
            "closed_campaigns": closed_campaigns,
            "open_campaigns_list": open_campaigns,
            "history_start_date": history_start_date.isoformat() if history_start_date else "",
            "note": "Real-life stop-loss honoring is measured on round-trip tradebook campaigns using your rule: day 1 checks tactical after the actual buy time, day 2 checks the daily chart close against the original 2% stop, and from day 3 onward the stop trails to breakeven or the prior 5-day EMA, whichever is higher.",
        }

    def _build_plan_history_streaks(self, repo: BhavRepository, history_start_date: date) -> Dict[str, object]:
        settings = self.load_settings()
        default_stop_loss_pct = self._as_float(settings.get("stop_loss_pct"))
        if default_stop_loss_pct <= 0:
            default_stop_loss_pct = 2.0
        latest_market_day = repo.latest_market_date() or history_start_date

        plan_dates = [
            d for d in self.list_plan_dates()
            if history_start_date.isoformat() <= d <= latest_market_day.isoformat()
        ]
        if not plan_dates:
            return {
                "ok": True,
                "history_start_date": history_start_date.isoformat(),
                "latest_trade_date": "",
                "stop_loss_pct": round(default_stop_loss_pct, 2),
                "summary": {
                    "closed_campaigns": 0,
                    "open_campaigns": 0,
                    "honored_campaigns": 0,
                    "stop_touched_campaigns": 0,
                    "breach_count": 0,
                    "honor_rate": None,
                    "win_rate": None,
                    "loss_rate": None,
                    "longest_honor_streak": 0,
                    "current_honor_streak": 0,
                    "actual_win_count": 0,
                    "actual_loss_count": 0,
                    "actual_win_rate": None,
                    "actual_loss_rate": None,
                    "actual_avg_gain_pct": None,
                    "actual_avg_loss_pct": None,
                    "actual_breakeven_count": 0,
                    "actual_breakeven_rate": None,
                    "counterfactual_win_rate": None,
                    "counterfactual_loss_rate": None,
                    "counterfactual_avg_gain_pct": None,
                    "counterfactual_avg_loss_pct": None,
                    "counterfactual_decisive_count": 0,
                    "counterfactual_breakeven_count": 0,
                    "counterfactual_breakeven_rate": None,
                    "breaches_prevented": 0,
                    "best_return_pct": None,
                    "worst_return_pct": None,
                },
                "campaigns": [],
                "closed_campaigns": [],
                "open_campaigns_list": [],
                "note": "No plan snapshots were found in the tracked history window.",
                "debug_log_path": str(self.debug_log_path),
            }

        first_seen: Dict[str, Dict[str, object]] = {}
        last_seen: Dict[str, Dict[str, object]] = {}
        trim_done_dates: Dict[str, Dict[int, date]] = {}
        trim_done_dates: Dict[str, Dict[int, date]] = {}
        latest_plan_date = history_start_date

        for plan_date in plan_dates:
            payload = self.load_plan(plan_date)
            if not isinstance(payload, dict):
                continue
            positions = payload.get("positions", [])
            if not isinstance(positions, list):
                continue
            for raw_position in positions:
                if not isinstance(raw_position, dict):
                    continue
                position = copy.deepcopy(raw_position)
                if self._entry_value(position) is None:
                    continue
                symbol = normalize_symbol(str(position.get("symbol") or ""))
                if not symbol:
                    continue
                pid = str(position.get("id") or "").strip()
                if not pid:
                    pid = self._trim_position_key(position)
                if pid not in first_seen:
                    first_seen[pid] = {
                        "position": position,
                        "plan_date": plan_date,
                    }
                last_seen[pid] = {
                    "position": position,
                    "plan_date": plan_date,
                }

        if not first_seen:
            return {
                "ok": True,
                "history_start_date": history_start_date.isoformat(),
                "latest_trade_date": latest_plan_date.isoformat(),
                "stop_loss_pct": round(default_stop_loss_pct, 2),
                "summary": {
                    "closed_campaigns": 0,
                    "open_campaigns": 0,
                    "honored_campaigns": 0,
                    "stop_touched_campaigns": 0,
                    "breach_count": 0,
                    "honor_rate": None,
                    "win_rate": None,
                    "loss_rate": None,
                    "longest_honor_streak": 0,
                    "current_honor_streak": 0,
                    "actual_win_count": 0,
                    "actual_loss_count": 0,
                    "actual_win_rate": None,
                    "actual_loss_rate": None,
                    "actual_avg_gain_pct": None,
                    "actual_avg_loss_pct": None,
                    "actual_breakeven_count": 0,
                    "actual_breakeven_rate": None,
                    "counterfactual_win_rate": None,
                    "counterfactual_loss_rate": None,
                    "counterfactual_avg_gain_pct": None,
                    "counterfactual_avg_loss_pct": None,
                    "counterfactual_decisive_count": 0,
                    "counterfactual_breakeven_count": 0,
                    "counterfactual_breakeven_rate": None,
                    "breaches_prevented": 0,
                    "best_return_pct": None,
                    "worst_return_pct": None,
                },
                "campaigns": [],
                "closed_campaigns": [],
                "open_campaigns_list": [],
                "note": "No executed trade-plan snapshots were found in the tracked history window.",
                "debug_log_path": str(self.debug_log_path),
            }

        campaigns: List[Dict[str, object]] = []
        for pid, first_item in first_seen.items():
            first_pos = first_item["position"]
            last_pos = last_seen.get(pid, first_item)["position"]
            symbol = normalize_symbol(str(first_pos.get("symbol") or last_pos.get("symbol") or ""))
            entry_date_raw = str(first_pos.get("entryDate") or first_item["plan_date"] or "")
            try:
                entry_day = datetime.strptime(entry_date_raw, "%Y-%m-%d").date() if entry_date_raw else None
            except ValueError:
                entry_day = None
            if entry_day is None:
                continue

            entry_price = self._entry_value(first_pos)
            if entry_price is None or entry_price <= 0:
                continue
            exec_summary = self._execution_summary(last_pos)
            qty = exec_summary.get("total_qty") or self._total_qty(first_pos)
            if qty is None or qty <= 0:
                continue

            plan_sl = self._as_float(first_pos.get("planSL"))
            if plan_sl <= 0:
                plan_sl = self._as_float(first_pos.get("_currentSL"))
            if plan_sl <= 0:
                plan_sl = round(entry_price * (1.0 - (default_stop_loss_pct / 100.0)), 2)
            derived_stop_pct = round(((entry_price - plan_sl) / entry_price) * 100.0, 2) if entry_price else default_stop_loss_pct
            stop_loss_pct = max(0.01, derived_stop_pct)

            latest_close_info = repo.lookup_last_close(symbol, latest_market_day) if symbol else None
            latest_cmp = None
            latest_cmp_date = ""
            if latest_close_info and latest_close_info.get("cmp") is not None:
                latest_cmp = float(latest_close_info["cmp"])
                latest_cmp_date = str(latest_close_info.get("price_date") or "")
            live_return_pct = ((latest_cmp / entry_price) - 1.0) * 100.0 if latest_cmp and entry_price else None

            path = self._evaluate_stop_path(
                symbol=symbol,
                entry_dt=datetime.combine(entry_day, dt_time(9, 15)),
                entry_day=entry_day,
                end_day=latest_market_day,
                entry_price=entry_price,
                stop_loss_pct=stop_loss_pct,
                repo=repo,
                exit_dt=None,
                live_return_pct=live_return_pct,
                simulation_end_day=latest_market_day,
            )
            stop_touched = bool(path.get("stop_touched"))
            stop_touch_date = str(path.get("stop_touch_date") or "")
            stop_touch_stage = str(path.get("stop_touch_stage") or "")
            stop_touch_price = self._as_float(path.get("stop_touch_price")) if stop_touched else None
            if stop_touched and stop_touch_price <= 0:
                stop_touch_price = plan_sl
            counterfactual_return_pct = path.get("counterfactual_return_pct")
            simulated_exit_price = stop_touch_price if stop_touched else (
                round(entry_price * (1.0 + (float(counterfactual_return_pct) / 100.0)), 2)
                if counterfactual_return_pct is not None and entry_price is not None
                else latest_cmp
            )
            realized_qty = self._as_float(exec_summary.get("realized_qty"))
            realized_value = self._as_float(exec_summary.get("realized_value"))
            remaining_qty = self._as_float(exec_summary.get("remaining_qty"))
            executed_sell_price = self._as_float(exec_summary.get("last_sell_price"))
            if executed_sell_price <= 0:
                executed_sell_price = self._as_float(exec_summary.get("sell_price"))
            current_value = None
            if latest_cmp is not None and remaining_qty > 0:
                current_value = round(realized_value + (remaining_qty * latest_cmp), 2)
            elif realized_value > 0:
                current_value = round(realized_value, 2)
            elif latest_cmp is not None and qty:
                current_value = round(float(latest_cmp or 0.0) * qty, 2)
            actual_return_pct = ((current_value / (entry_price * qty)) - 1.0) * 100.0 if current_value is not None and entry_price and qty else None

            campaigns.append(
                {
                    "campaign_id": pid,
                    "symbol": symbol,
                    "entry_date": entry_day.isoformat(),
                    "start_time": entry_day.isoformat(),
                    "end_time": "",
                    "status": str(last_pos.get("_status") or ("closed" if stop_touched else "open")).lower(),
                    "entry_price": round(entry_price, 2),
                    "buy_price": round(entry_price, 2),
                    "buy_qty": round(float(qty), 6),
                    "actual_qty": round(float(qty), 6),
                    "executed_sell_price": executed_sell_price,
                    "current_cmp": latest_cmp,
                    "actual_value": current_value,
                    "stop_price": round(plan_sl, 2),
                    "stop_touch_price": stop_touch_price,
                    "price_date": latest_cmp_date,
                    "sell_qty": round(float(realized_qty), 6),
                    "actual_return_pct": round(actual_return_pct, 2) if actual_return_pct is not None else None,
                    "return_pct": round(actual_return_pct, 2) if actual_return_pct is not None else None,
                    "honored": not stop_touched,
                    "stop_touched": stop_touched,
                    "counterfactual_return_pct": counterfactual_return_pct,
                    "breach_pct": round((actual_return_pct or 0.0) - (counterfactual_return_pct or 0.0), 2) if actual_return_pct is not None else None,
                    "stop_touch_date": stop_touch_date,
                    "stop_touch_stage": stop_touch_stage,
                    "analysis_basis": "trade_plan_snapshot_day1_tactical_then_daily_close_with_day3_ema_trailing",
                    "plan_status": str(last_pos.get("_status") or ""),
                    "plan_days": last_pos.get("_days"),
                    "plan_rem": last_pos.get("_rem"),
                    "plan_current_sl": last_pos.get("_currentSL"),
                    "plan_moved_be": bool(last_pos.get("movedBE")),
                    "plan_trail_override": last_pos.get("trailOverride"),
                    "realized_qty": round(float(realized_qty), 6),
                    "realized_value": round(float(realized_value), 2),
                    "remaining_qty": round(float(remaining_qty), 6),
                }
            )

        campaigns.sort(
            key=lambda item: (
                item.get("entry_date") or "",
                item.get("symbol") or "",
            )
        )
        closed_campaigns = [item for item in campaigns if item.get("status") == "closed"]
        open_campaigns = [item for item in campaigns if item.get("status") != "closed"]
        closed_honored = [item for item in closed_campaigns if item.get("honored")]
        closed_losses = [item for item in closed_campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) < 0]
        closed_wins = [item for item in closed_campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) >= 0]
        breaches = [item for item in closed_campaigns if item.get("stop_touched")]
        actual_gain_returns = [
            float(item.get("actual_return_pct"))
            for item in closed_campaigns
            if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) >= 0
        ]
        actual_loss_returns = [
            float(item.get("actual_return_pct"))
            for item in closed_campaigns
            if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) < 0
        ]
        actual_breakeven_returns = [
            float(item.get("actual_return_pct"))
            for item in closed_campaigns
            if item.get("actual_return_pct") is not None and round(float(item.get("actual_return_pct")), 2) == 0.0
        ]
        counterfactual_wins = [
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) >= 0
        ]
        counterfactual_losses = [
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) < 0
        ]
        counterfactual_gain_returns = [
            float(item.get("counterfactual_return_pct"))
            for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) >= 0
        ]
        counterfactual_loss_returns = [
            float(item.get("counterfactual_return_pct"))
            for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) < 0
        ]
        counterfactual_breakeven_returns = [
            float(item.get("counterfactual_return_pct"))
            for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and round(float(item.get("counterfactual_return_pct")), 2) == 0.0
        ]
        counterfactual_positive_count = len([
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) > 0
        ])
        counterfactual_negative_count = len([
            item for item in closed_campaigns
            if item.get("counterfactual_return_pct") is not None and float(item.get("counterfactual_return_pct")) < 0
        ])
        counterfactual_decisive_count = counterfactual_positive_count + counterfactual_negative_count
        longest_streak = 0
        current_streak = 0
        for item in closed_campaigns:
            if item.get("honored"):
                current_streak += 1
                longest_streak = max(longest_streak, current_streak)
            else:
                current_streak = 0
        live_streak = 0
        for item in campaigns:
            if item.get("honored"):
                live_streak += 1
            else:
                live_streak = 0
        closed_count = len(closed_campaigns)
        honored_count = len(closed_honored)
        win_count = len(closed_wins)
        loss_count = len(closed_losses)
        breach_count = len(breaches)
        return {
            "ok": True,
            "tradebook_path": "",
            "latest_trade_date": latest_market_day.isoformat(),
            "history_start_date": history_start_date.isoformat(),
            "stop_loss_pct": round(default_stop_loss_pct, 2),
            "summary": {
                "closed_campaigns": closed_count,
                "open_campaigns": len(open_campaigns),
                "honored_campaigns": honored_count,
                "stop_touched_campaigns": breach_count,
                "breach_count": breach_count,
                "honor_rate": round((honored_count / closed_count) * 100.0, 1) if closed_count else None,
                "win_rate": round((win_count / closed_count) * 100.0, 1) if closed_count else None,
                "loss_rate": round((loss_count / closed_count) * 100.0, 1) if closed_count else None,
                "longest_honor_streak": longest_streak,
                "current_honor_streak": live_streak,
                "actual_win_count": win_count,
                "actual_loss_count": loss_count,
                "actual_win_rate": round((win_count / closed_count) * 100.0, 1) if closed_count else None,
                "actual_loss_rate": round((loss_count / closed_count) * 100.0, 1) if closed_count else None,
                "actual_avg_gain_pct": round(sum(actual_gain_returns) / len(actual_gain_returns), 2) if actual_gain_returns else None,
                "actual_avg_loss_pct": round(sum(actual_loss_returns) / len(actual_loss_returns), 2) if actual_loss_returns else None,
                "actual_breakeven_count": len(actual_breakeven_returns),
                "actual_breakeven_rate": round((len(actual_breakeven_returns) / closed_count) * 100.0, 1) if closed_count else None,
                "counterfactual_win_rate": round((counterfactual_positive_count / counterfactual_decisive_count) * 100.0, 1) if counterfactual_decisive_count else None,
                "counterfactual_loss_rate": round((counterfactual_negative_count / counterfactual_decisive_count) * 100.0, 1) if counterfactual_decisive_count else None,
                "counterfactual_avg_gain_pct": round(sum(counterfactual_gain_returns) / len(counterfactual_gain_returns), 2) if counterfactual_gain_returns else None,
                "counterfactual_avg_loss_pct": round(sum(counterfactual_loss_returns) / len(counterfactual_loss_returns), 2) if counterfactual_loss_returns else None,
                "counterfactual_decisive_count": counterfactual_decisive_count,
                "counterfactual_breakeven_count": len(counterfactual_breakeven_returns),
                "counterfactual_breakeven_rate": round((len(counterfactual_breakeven_returns) / closed_count) * 100.0, 1) if closed_count else None,
                "breaches_prevented": breach_count,
                "best_return_pct": round(max((float(item.get("return_pct")) for item in closed_campaigns if item.get("return_pct") is not None), default=0.0), 2) if closed_campaigns else None,
                "worst_return_pct": round(min((float(item.get("return_pct")) for item in closed_campaigns if item.get("return_pct") is not None), default=0.0), 2) if closed_campaigns else None,
            },
            "campaigns": campaigns,
            "closed_campaigns": closed_campaigns,
            "open_campaigns_list": open_campaigns,
            "note": "Real-life stop-loss honoring is measured on saved trade-plan snapshots from the tracked history window. The entry date comes from the plan snapshot, and the stop path is evaluated from that plan against real market data using the plan's stored stop level.",
        }

    def _build_snapshot_streaks(self, repo: BhavRepository, history_trade_count: int = 20) -> Dict[str, object]:
        settings = self.load_settings()
        default_stop_loss_pct = self._as_float(settings.get("stop_loss_pct"))
        if default_stop_loss_pct <= 0:
            default_stop_loss_pct = 2.0

        plan_dates = sorted(self.list_plan_dates())
        limit = max(int(history_trade_count or 0), 1)
        selected_dates = plan_dates[-limit:]
        history_window_label = f"Last {limit} trades"
        if not selected_dates:
            return {
                "ok": True,
                "history_start_date": "",
                "history_window_label": history_window_label,
                "history_window_count": 0,
                "latest_trade_date": "",
                "stop_loss_pct": round(default_stop_loss_pct, 2),
                "summary": {
                    "closed_campaigns": 0,
                    "open_campaigns": 0,
                    "honored_campaigns": 0,
                    "violated_campaigns": 0,
                    "all_time_honored_count": 0,
                    "all_time_trade_count": 0,
                    "all_time_honor_rate": None,
                    "stop_touched_campaigns": 0,
                    "breach_count": 0,
                    "honor_rate": None,
                    "violation_rate": None,
                    "win_rate": None,
                    "loss_rate": None,
                    "longest_honor_streak": 0,
                    "current_honor_streak": 0,
                    "actual_win_count": 0,
                    "actual_loss_count": 0,
                    "actual_win_rate": None,
                    "actual_loss_rate": None,
                    "actual_avg_gain_pct": None,
                    "actual_avg_loss_pct": None,
                    "actual_breakeven_count": 0,
                    "actual_breakeven_rate": None,
                    "counterfactual_win_rate": None,
                    "counterfactual_loss_rate": None,
                    "counterfactual_avg_gain_pct": None,
                    "counterfactual_avg_loss_pct": None,
                    "counterfactual_decisive_count": 0,
                    "counterfactual_breakeven_count": 0,
                    "counterfactual_breakeven_rate": None,
                    "breaches_prevented": 0,
                    "best_return_pct": None,
                    "worst_return_pct": None,
                },
                "campaigns": [],
                "closed_campaigns": [],
                "open_campaigns_list": [],
                "note": "No saved trade-plan snapshots were found in the rolling last-trades window.",
                "debug_log_path": str(self.debug_log_path),
            }

        first_seen: Dict[str, Dict[str, object]] = {}
        last_seen: Dict[str, Dict[str, object]] = {}
        trim_done_dates: Dict[str, Dict[int, date]] = {}
        latest_plan_date = datetime.strptime(selected_dates[-1], "%Y-%m-%d").date()
        latest_market_day = repo.latest_market_date() or latest_plan_date

        def _count_honored_trades(date_list: List[str]) -> Tuple[int, int]:
            seen_first: Dict[str, Dict[str, object]] = {}
            seen_last: Dict[str, Dict[str, object]] = {}
            for plan_date in date_list:
                payload = self.load_plan(plan_date)
                if not isinstance(payload, dict):
                    continue
                positions = payload.get("positions", [])
                if not isinstance(positions, list):
                    continue
                for raw_position in positions:
                    if not isinstance(raw_position, dict):
                        continue
                    position = copy.deepcopy(raw_position)
                    if self._entry_value(position) is None:
                        continue
                    symbol = normalize_symbol(str(position.get("symbol") or ""))
                    if not symbol:
                        continue
                    pid = str(position.get("id") or "").strip()
                    if not pid:
                        pid = self._trim_position_key(position)
                    if pid not in seen_first:
                        seen_first[pid] = {"position": position, "plan_date": plan_date}
                    seen_last[pid] = {"position": position, "plan_date": plan_date}
            honored = 0
            total = 0
            for pid, first_item in seen_first.items():
                first_pos = first_item["position"]
                last_pos = seen_last.get(pid, first_item)["position"]
                entry_price = self._entry_value(first_pos)
                if entry_price is None or entry_price <= 0:
                    continue
                exec_summary = self._execution_summary(last_pos)
                total_qty = exec_summary.get("total_qty") or self._total_qty(first_pos)
                if total_qty is None or total_qty <= 0:
                    continue
                realized_qty = self._as_float(exec_summary.get("realized_qty"))
                realized_value = self._as_float(exec_summary.get("realized_value"))
                executed_sell_price = self._as_float(exec_summary.get("last_sell_price"))
                if executed_sell_price <= 0:
                    executed_sell_price = self._as_float(exec_summary.get("sell_price"))
                snapshot_cmp = self._as_float(last_pos.get("cmp"))
                remaining_qty = self._as_float(last_pos.get("_rem"))
                status_raw = str(last_pos.get("_status") or "").strip().lower()
                initial_sl = self._as_float(first_pos.get("planSL"))
                tactical_sl = self._primary_tactical_sl(last_pos)
                if tactical_sl <= 0:
                    tactical_sl = self._primary_tactical_sl(first_pos)
                trail_sl = self._as_float(last_pos.get("_currentSL"))
                plan_days = int(self._as_float(last_pos.get("_days")))
                moved_be = bool(last_pos.get("movedBE"))
                trail_override = self._as_float(last_pos.get("trailOverride"))
                trail_active = (
                    trail_sl > 0
                    and trail_sl > initial_sl + 0.01
                    and (plan_days >= 2 or moved_be or trail_override > 0)
                )
                plan_sl = trail_sl if trail_active else initial_sl
                if plan_sl <= 0:
                    plan_sl = self._as_float(first_pos.get("_currentSL"))
                if plan_sl <= 0:
                    continue
                execution_exit_price = self._execution_block_exit_price(last_pos, exec_summary)
                comparison_sl = plan_sl
                is_honored = False
                if status_raw == "closed" and remaining_qty <= 0:
                    if execution_exit_price is not None and execution_exit_price > 0 and comparison_sl > 0:
                        is_honored = execution_exit_price <= comparison_sl + 0.01
                    elif realized_qty > 0 and realized_value > 0 and comparison_sl > 0:
                        avg_exit = realized_value / realized_qty if realized_qty > 0 else 0.0
                        is_honored = avg_exit <= comparison_sl + 0.01
                elif remaining_qty > 0 and snapshot_cmp > 0 and comparison_sl > 0:
                    is_honored = snapshot_cmp <= comparison_sl + 0.01
                total += 1
                if is_honored:
                    honored += 1
            return honored, total

        for plan_date in selected_dates:
            try:
                plan_day = datetime.strptime(plan_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            payload = self.load_plan(plan_date)
            if not isinstance(payload, dict):
                continue
            positions = payload.get("positions", [])
            if not isinstance(positions, list):
                continue
            for raw_position in positions:
                if not isinstance(raw_position, dict):
                    continue
                position = copy.deepcopy(raw_position)
                if self._entry_value(position) is None:
                    continue
                symbol = normalize_symbol(str(position.get("symbol") or ""))
                if not symbol:
                    continue
                pid = str(position.get("id") or "").strip()
                if not pid:
                    pid = self._trim_position_key(position)
                if pid not in first_seen:
                    first_seen[pid] = {"position": position, "plan_date": plan_date}
                last_seen[pid] = {"position": position, "plan_date": plan_date}
                trims = position.get("trims") if isinstance(position.get("trims"), list) else []
                if isinstance(trims, list):
                    trim_dates = trim_done_dates.setdefault(pid, {})
                    for trim_idx, trim in enumerate(trims):
                        if not isinstance(trim, dict) or not trim.get("done"):
                            continue
                        trim_dt_raw = str(trim.get("dt") or "").strip()
                        trim_dt = None
                        if trim_dt_raw:
                            try:
                                trim_dt = datetime.strptime(trim_dt_raw, "%Y-%m-%d").date()
                            except ValueError:
                                trim_dt = None
                        effective_trim_day = trim_dt or plan_day
                        existing_day = trim_dates.get(trim_idx)
                        if existing_day is None or effective_trim_day < existing_day:
                            trim_dates[trim_idx] = effective_trim_day

        campaigns: List[Dict[str, object]] = []
        for pid, first_item in first_seen.items():
            first_pos = first_item["position"]
            last_pos = last_seen.get(pid, first_item)["position"]
            symbol = normalize_symbol(str(first_pos.get("symbol") or last_pos.get("symbol") or ""))
            entry_date_raw = str(first_pos.get("entryDate") or first_item["plan_date"] or "")
            try:
                entry_day = datetime.strptime(entry_date_raw, "%Y-%m-%d").date() if entry_date_raw else None
            except ValueError:
                entry_day = None
            if entry_day is None:
                continue

            entry_price = self._entry_value(first_pos)
            if entry_price is None or entry_price <= 0:
                continue

            exec_summary = self._execution_summary(last_pos)
            total_qty = exec_summary.get("total_qty") or self._total_qty(first_pos)
            if total_qty is None or total_qty <= 0:
                continue

            realized_qty = self._as_float(exec_summary.get("realized_qty"))
            realized_value = self._as_float(exec_summary.get("realized_value"))
            remaining_qty = self._as_float(exec_summary.get("remaining_qty"))
            executed_sell_price = self._as_float(exec_summary.get("last_sell_price"))
            if executed_sell_price <= 0:
                executed_sell_price = self._as_float(exec_summary.get("sell_price"))
            snapshot_cmp = self._as_float(last_pos.get("cmp"))
            open_pnl = self._as_float(last_pos.get("_openPnl"))
            real_pnl = self._as_float(last_pos.get("_realPnl"))
            invested = round(entry_price * float(total_qty), 2)

            actual_value = realized_value
            if snapshot_cmp > 0 and remaining_qty > 0:
                actual_value = round(realized_value + (remaining_qty * snapshot_cmp), 2)
            elif actual_value <= 0 and snapshot_cmp > 0:
                actual_value = round(float(total_qty) * snapshot_cmp, 2)
            elif real_pnl != 0 or open_pnl != 0:
                actual_value = round(invested + real_pnl + open_pnl, 2)

            actual_return_pct = ((actual_value / invested) - 1.0) * 100.0 if invested else None
            status_raw = str(last_pos.get("_status") or "").strip().lower()
            remaining_qty = self._as_float(last_pos.get("_rem"))
            initial_sl = self._as_float(first_pos.get("planSL"))
            tactical_sl = self._primary_tactical_sl(last_pos)
            if tactical_sl <= 0:
                tactical_sl = self._primary_tactical_sl(first_pos)
            trail_sl = self._as_float(last_pos.get("_currentSL"))
            plan_days = int(self._as_float(last_pos.get("_days")))
            moved_be = bool(last_pos.get("movedBE"))
            trail_override = self._as_float(last_pos.get("trailOverride"))
            trail_active = (
                trail_sl > 0
                and trail_sl > initial_sl + 0.01
                and (plan_days >= 2 or moved_be or trail_override > 0)
            )

            plan_sl = trail_sl if trail_active else initial_sl
            if plan_sl <= 0:
                plan_sl = self._as_float(first_pos.get("_currentSL"))
            if plan_sl <= 0:
                plan_sl = round(entry_price * (1.0 - (default_stop_loss_pct / 100.0)), 2)
            execution_exit_price = self._execution_block_exit_price(last_pos, exec_summary)
            comparison_sl = plan_sl

            actual_pnl = round(actual_value - invested, 2)
            stop_value = round(float(total_qty) * float(comparison_sl), 2) if comparison_sl > 0 else None
            stop_pnl = round(stop_value - invested, 2) if stop_value is not None else None
            stop_return_pct = round(((stop_value / invested) - 1.0) * 100.0, 2) if stop_value is not None and invested else None
            pnl_delta = round((stop_pnl - actual_pnl), 2) if stop_pnl is not None else None

            trims = last_pos.get("trims") if isinstance(last_pos.get("trims"), list) else first_pos.get("trims")
            completed_trims = [trim for trim in trims if isinstance(trim, dict) and trim.get("done")] if isinstance(trims, list) else []
            planned_trim_pnl = 0.0
            latest_cmp_for_plan = snapshot_cmp if snapshot_cmp > 0 else entry_price
            if completed_trims:
                for trim in completed_trims:
                    if not isinstance(trim, dict):
                        continue
                    ap = self._as_float(trim.get("ap"))
                    sq = self._as_float(trim.get("sq"))
                    if ap > 0 and sq > 0:
                        planned_trim_pnl += sq * ap
                if remaining_qty > 0 and snapshot_cmp > 0:
                    planned_trim_pnl += remaining_qty * snapshot_cmp
            completed_trim_count = len(completed_trims)
            if not completed_trims:
                planned_pnl = 0.0
                target_miss_pnl = 0.0
                target_pnl = 0.0
            else:
                if planned_trim_pnl <= 0 and latest_cmp_for_plan > 0:
                    planned_trim_pnl = float(total_qty) * latest_cmp_for_plan
                planned_pnl = round(planned_trim_pnl - invested, 2)
                target_miss_pnl = round(planned_pnl - actual_pnl, 2)
                target_pnl = round(actual_pnl + target_miss_pnl, 2)

            stop_basis = "trailing stop" if trail_active else "initial stop"
            honored = False
            violated = False
            if status_raw == "closed" and remaining_qty <= 0:
                if execution_exit_price is not None and execution_exit_price > 0 and comparison_sl > 0:
                    honored = execution_exit_price <= comparison_sl + 0.01
                    violated = not honored
                elif realized_qty > 0 and realized_value > 0 and comparison_sl > 0:
                    avg_exit = realized_value / realized_qty if realized_qty > 0 else 0.0
                    honored = avg_exit <= comparison_sl + 0.01
                    violated = not honored
            else:
                status_label = "OPEN"

            if honored:
                status_label = "HONORED"
            elif violated:
                status_label = "VIOLATED"
            else:
                status_label = "OPEN"

            campaigns.append(
                {
                    "campaign_id": pid,
                    "symbol": symbol,
                    "entry_date": entry_day.isoformat(),
                    "start_time": entry_day.isoformat(),
                    "end_time": "",
                    "status": status_label,
                    "entry_price": round(entry_price, 2),
                    "buy_price": round(entry_price, 2),
                    "buy_qty": round(float(total_qty), 6),
                    "actual_qty": round(float(total_qty), 6),
                    "tacticalSL": round(float(tactical_sl), 2) if tactical_sl and tactical_sl > 0 else None,
                    "executed_sell_price": executed_sell_price,
                    "execution_exit_price": execution_exit_price,
                    "current_cmp": snapshot_cmp if snapshot_cmp > 0 else None,
                    "actual_value": actual_value,
                    "stop_price": round(comparison_sl, 2),
                    "stop_value": stop_value,
                    "stop_pnl": stop_pnl,
                    "stop_return_pct": stop_return_pct,
                    "pnl_delta": pnl_delta,
                    "stop_touch_price": None,
                    "price_date": str(last_item["plan_date"]) if isinstance((last_item := last_seen.get(pid, first_item)), dict) else "",
                    "sell_qty": round(float(realized_qty), 6),
                    "actual_return_pct": round(actual_return_pct, 2) if actual_return_pct is not None else None,
                    "return_pct": round(actual_return_pct, 2) if actual_return_pct is not None else None,
                    "actual_pnl": actual_pnl,
                    "target_pnl": target_pnl,
                    "target_miss_pnl": target_miss_pnl,
                    "completed_trim_count": completed_trim_count,
                    "honored": honored,
                    "stop_touched": not honored,
                    "counterfactual_return_pct": None,
                    "breach_pct": None,
                    "stop_touch_date": "",
                    "stop_touch_stage": "",
                    "status_reason": (
                        f"Execution block {execution_exit_price:.2f} was at/below {stop_basis} {comparison_sl:.2f}"
                        if honored and execution_exit_price is not None and execution_exit_price > 0
                        else (
                            f"Execution block {execution_exit_price:.2f} was above {stop_basis} {comparison_sl:.2f}"
                            if violated and execution_exit_price is not None and execution_exit_price > 0
                            else (
                                f"Partial exit existed above {stop_basis} {comparison_sl:.2f}"
                                if violated and realized_qty > 0
                                else (
                                    f"Position still open with {remaining_qty:.0f} remaining; awaiting full exit before judging stop"
                                    if remaining_qty > 0
                                    else f"Latest stored status is {status_raw or 'unknown'} with remaining qty {remaining_qty:.0f}"
                                )
                            )
                        )
                    ),
                    "analysis_basis": "trade_plan_snapshot_local_only",
                    "plan_status": status_raw,
                    "plan_days": last_pos.get("_days"),
                    "plan_rem": last_pos.get("_rem"),
                    "plan_current_sl": last_pos.get("_currentSL"),
                    "plan_moved_be": bool(last_pos.get("movedBE")),
                    "plan_trail_override": last_pos.get("trailOverride"),
                    "realized_qty": round(float(realized_qty), 6),
                    "realized_value": round(float(realized_value), 2),
                    "remaining_qty": round(float(remaining_qty), 6),
                }
            )

        campaigns.sort(key=lambda item: (item.get("entry_date") or "", item.get("symbol") or ""))
        honored_campaigns = [item for item in campaigns if item.get("honored")]
        violated_campaigns = [item for item in campaigns if item.get("status") == "VIOLATED"]
        open_campaigns = [item for item in campaigns if item.get("status") == "OPEN"]
        resolved_campaigns = honored_campaigns + violated_campaigns
        actual_gain_returns = [float(item.get("actual_return_pct")) for item in resolved_campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) >= 0]
        actual_loss_returns = [float(item.get("actual_return_pct")) for item in resolved_campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) < 0]
        actual_breakeven_returns = [float(item.get("actual_return_pct")) for item in resolved_campaigns if item.get("actual_return_pct") is not None and round(float(item.get("actual_return_pct")), 2) == 0.0]
        actual_pnl_total = round(sum(float(item.get("actual_pnl") or 0.0) for item in resolved_campaigns), 2)
        planned_pnl_total = round(sum(float(item.get("target_pnl") or item.get("actual_pnl") or 0.0) for item in resolved_campaigns), 2)
        money_left_on_table = round(planned_pnl_total - actual_pnl_total, 2)
        stop_pnl_total = round(sum(float(item.get("stop_pnl") or 0.0) for item in resolved_campaigns), 2)

        current_streak = 0
        longest_streak = 0
        for item in campaigns:
            if item.get("honored"):
                current_streak += 1
                longest_streak = max(longest_streak, current_streak)
            else:
                current_streak = 0

        win_count = len([item for item in campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) >= 0])
        loss_count = len([item for item in campaigns if item.get("actual_return_pct") is not None and float(item.get("actual_return_pct")) < 0])
        closed_count = len(resolved_campaigns)
        honored_count = len(honored_campaigns)
        violated_count = len(violated_campaigns)
        all_time_honored_count, all_time_trade_count = _count_honored_trades(plan_dates)
        return {
            "ok": True,
            "tradebook_path": "",
            "latest_trade_date": latest_plan_date.isoformat(),
            "history_start_date": selected_dates[0] if selected_dates else "",
            "history_window_label": history_window_label,
            "history_window_count": len(selected_dates),
            "stop_loss_pct": round(default_stop_loss_pct, 2),
            "summary": {
                "closed_campaigns": len(resolved_campaigns),
                "open_campaigns": len(open_campaigns),
                "honored_campaigns": honored_count,
                "violated_campaigns": violated_count,
                "all_time_honored_count": all_time_honored_count,
                "all_time_trade_count": all_time_trade_count,
                "all_time_honor_rate": round((all_time_honored_count / all_time_trade_count) * 100.0, 1) if all_time_trade_count else None,
                "stop_touched_campaigns": violated_count,
                "breach_count": violated_count,
                "honor_rate": round((honored_count / len(resolved_campaigns)) * 100.0, 1) if resolved_campaigns else None,
                "violation_rate": round((violated_count / len(resolved_campaigns)) * 100.0, 1) if resolved_campaigns else None,
                "win_rate": round((win_count / closed_count) * 100.0, 1) if closed_count else None,
                "loss_rate": round((loss_count / closed_count) * 100.0, 1) if closed_count else None,
                "longest_honor_streak": longest_streak,
                "current_honor_streak": current_streak,
                "actual_win_count": win_count,
                "actual_loss_count": loss_count,
                "actual_win_rate": round((win_count / closed_count) * 100.0, 1) if closed_count else None,
                "actual_loss_rate": round((loss_count / closed_count) * 100.0, 1) if closed_count else None,
                "actual_avg_gain_pct": round(sum(actual_gain_returns) / len(actual_gain_returns), 2) if actual_gain_returns else None,
                "actual_avg_loss_pct": round(sum(actual_loss_returns) / len(actual_loss_returns), 2) if actual_loss_returns else None,
                "actual_breakeven_count": len(actual_breakeven_returns),
                "actual_breakeven_rate": round((len(actual_breakeven_returns) / closed_count) * 100.0, 1) if closed_count else None,
                "actual_pnl_total": actual_pnl_total,
                "planned_pnl_total": planned_pnl_total,
                "stop_pnl_total": stop_pnl_total,
                "money_left_on_table": money_left_on_table,
                "counterfactual_win_rate": None,
                "counterfactual_loss_rate": None,
                "counterfactual_avg_gain_pct": None,
                "counterfactual_avg_loss_pct": None,
                "counterfactual_decisive_count": 0,
                "counterfactual_breakeven_count": 0,
                "counterfactual_breakeven_rate": None,
                "breaches_prevented": 0,
                "best_return_pct": round(max((float(item.get("return_pct")) for item in campaigns if item.get("return_pct") is not None), default=0.0), 2) if campaigns else None,
                "worst_return_pct": round(min((float(item.get("return_pct")) for item in campaigns if item.get("return_pct") is not None), default=0.0), 2) if campaigns else None,
            },
            "campaigns": campaigns,
            "closed_campaigns": honored_campaigns + violated_campaigns,
            "open_campaigns_list": open_campaigns,
            "note": "HONORED means the execution block exited at or below the active stop. VIOLATED means the execution block exited above the active stop or stayed open without respecting it. OPEN means the trade is still unresolved.",
            "debug_log_path": str(self.debug_log_path),
        }

    def _build_roundtrip_campaigns(self, rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
        grouped: Dict[str, List[Dict[str, object]]] = {}
        for row in rows:
            symbol = normalize_symbol(str(row.get("symbol", "")))
            if not symbol:
                continue
            grouped.setdefault(symbol, []).append(row)

        campaigns: List[Dict[str, object]] = []
        for symbol, symbol_rows in grouped.items():
            symbol_rows = sorted(
                symbol_rows,
                key=lambda row: (
                    self._parse_tradebook_time(row.get("order_execution_time")) or datetime.min,
                    str(row.get("trade_date") or ""),
                    str(row.get("trade_id") or ""),
                    str(row.get("order_id") or ""),
                ),
            )

            campaign_idx = 0
            current: Optional[Dict[str, object]] = None
            for row in symbol_rows:
                trade_type = str(row.get("trade_type") or "").strip().lower()
                qty = self._as_float(row.get("quantity"))
                price = self._as_float(row.get("price"))
                if qty <= 0 or price <= 0:
                    continue
                row_time = self._parse_tradebook_time(row.get("order_execution_time")) or self._parse_tradebook_time(row.get("trade_date")) or datetime.min

                if trade_type == "buy":
                    if current is None:
                        campaign_idx += 1
                        current = {
                            "campaign_id": f"{symbol}:{campaign_idx}",
                            "symbol": symbol,
                            "start_time": row_time,
                            "end_time": None,
                            "buy_qty": 0.0,
                            "sell_qty": 0.0,
                            "buy_value": 0.0,
                            "sell_value": 0.0,
                            "net_qty": 0.0,
                        }
                    current["buy_qty"] = float(current["buy_qty"]) + qty
                    current["buy_value"] = float(current["buy_value"]) + (qty * price)
                    current["net_qty"] = float(current["net_qty"]) + qty
                    current["end_time"] = row_time
                elif trade_type == "sell":
                    if current is None:
                        continue
                    current["sell_qty"] = float(current["sell_qty"]) + qty
                    current["sell_value"] = float(current["sell_value"]) + (qty * price)
                    current["net_qty"] = float(current["net_qty"]) - qty
                    current["end_time"] = row_time

                if current is not None and float(current["net_qty"]) <= 1e-9 and float(current["buy_qty"]) > 0:
                    buy_qty = float(current["buy_qty"])
                    sell_qty = float(current["sell_qty"])
                    buy_value = float(current["buy_value"])
                    sell_value = float(current["sell_value"])
                    entry_price = round(buy_value / buy_qty, 2) if buy_qty else None
                    exit_price = round(sell_value / sell_qty, 2) if sell_qty else None
                    campaigns.append(
                        {
                            "campaign_id": current["campaign_id"],
                            "symbol": symbol,
                            "start_time": current["start_time"].isoformat() if isinstance(current["start_time"], datetime) else "",
                            "end_time": current["end_time"].isoformat() if isinstance(current["end_time"], datetime) else "",
                            "status": "closed",
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "buy_qty": round(buy_qty, 6),
                            "sell_qty": round(sell_qty, 6),
                        }
                    )
                    current = None

            if current is not None and float(current["buy_qty"]) > 0:
                buy_qty = float(current["buy_qty"])
                buy_value = float(current["buy_value"])
                entry_price = round(buy_value / buy_qty, 2) if buy_qty else None
                campaigns.append(
                    {
                        "campaign_id": current["campaign_id"],
                        "symbol": symbol,
                        "start_time": current["start_time"].isoformat() if isinstance(current["start_time"], datetime) else "",
                        "end_time": "",
                        "status": "open",
                        "entry_price": entry_price,
                        "exit_price": None,
                        "buy_qty": round(buy_qty, 6),
                        "sell_qty": round(float(current["sell_qty"]), 6),
                    }
                )

        campaigns.sort(key=lambda item: (item.get("start_time") or "", item.get("symbol") or ""))
        return campaigns

    def build_stop_loss_streak(self, repo: BhavRepository) -> Dict[str, object]:
        tradebook_path = self._latest_tradebook_path()
        settings = self.load_settings()
        stop_loss_pct = self._as_float(settings.get("stop_loss_pct"))
        if stop_loss_pct <= 0:
            stop_loss_pct = 2.0
        history_start_date = date(2026, 4, 17)
        self._log_debug(
            "stop-loss streak request: stop_loss_pct=%.2f tradebook_path=%s history_start_date=%s"
            % (stop_loss_pct, str(tradebook_path) if tradebook_path else "", history_start_date.isoformat())
        )
        if not tradebook_path or not tradebook_path.exists():
            self._log_debug("stop-loss streak aborted: tradebook csv not found")
            return {
                "ok": False,
                "message": "No tradebook CSV found in the trades folder.",
                "stop_loss_pct": round(stop_loss_pct, 2),
                "history_start_date": history_start_date.isoformat(),
                "summary": {},
                "campaigns": [],
                "closed_campaigns": [],
                "open_campaigns_list": [],
                "debug_log_path": str(self.debug_log_path),
            }

        try:
            self._log_debug(f"stop-loss streak reading csv: {tradebook_path}")
            with tradebook_path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                rows = [row for row in reader if row.get("symbol")]
            self._log_debug(f"stop-loss streak csv read complete: row_count={len(rows)}")
        except Exception as exc:
            self._log_debug(f"stop-loss streak csv read failed: {exc}")
            return {
                "ok": False,
                "message": f"Failed to read tradebook CSV: {exc}",
                "stop_loss_pct": round(stop_loss_pct, 2),
                "history_start_date": history_start_date.isoformat(),
                "summary": {},
                "campaigns": [],
                "closed_campaigns": [],
                "open_campaigns_list": [],
                "debug_log_path": str(self.debug_log_path),
            }

        report = self._build_stop_loss_campaigns(rows, repo, stop_loss_pct, history_start_date=history_start_date)
        try:
            summary = report.get("summary") if isinstance(report, dict) else {}
            closed_count = summary.get("closed_campaigns") if isinstance(summary, dict) else None
            open_count = summary.get("open_campaigns") if isinstance(summary, dict) else None
            honored_count = summary.get("honored_campaigns") if isinstance(summary, dict) else None
            self._log_debug(
                "stop-loss streak report ready: closed=%s open=%s honored=%s campaigns=%s"
                % (
                    closed_count,
                    open_count,
                    honored_count,
                    len(report.get("campaigns", [])) if isinstance(report, dict) else 0,
                )
            )
        except Exception:
            pass
        if isinstance(report, dict):
            report["debug_log_path"] = str(self.debug_log_path)
            report["tradebook_path"] = str(tradebook_path)
            report["history_start_date"] = history_start_date.isoformat()
        return report

    def _simulation_qty(self, entry_price: Optional[float], budget: float = 300000.0) -> int:
        price = self._as_float(entry_price)
        if price <= 0:
            return 0
        return max(1, int(math.ceil(budget / price)))

    def build_portfolio_simulation(self, repo: BhavRepository) -> Dict[str, object]:
        tradebook_path = self._latest_tradebook_path()
        starting_capital = 3000000.0
        per_position_budget = 300000.0
        self._log_debug(
            "portfolio sim request: starting_capital=%.2f per_position_budget=%.2f tradebook_path=%s"
            % (starting_capital, per_position_budget, str(tradebook_path) if tradebook_path else "")
        )
        if not tradebook_path or not tradebook_path.exists():
            return {
                "ok": False,
                "message": "No tradebook CSV found in the trades folder.",
                "starting_capital": starting_capital,
                "per_position_budget": per_position_budget,
                "summary": {},
                "daily": [],
                "positions": [],
                "open_positions": [],
                "campaigns": [],
                "tradebook_path": "",
                "debug_log_path": str(self.debug_log_path),
            }

        try:
            with tradebook_path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                rows = [row for row in reader if row.get("symbol")]
        except Exception as exc:
            self._log_debug(f"portfolio sim csv read failed: {exc}")
            return {
                "ok": False,
                "message": f"Failed to read tradebook CSV: {exc}",
                "starting_capital": starting_capital,
                "per_position_budget": per_position_budget,
                "summary": {},
                "daily": [],
                "positions": [],
                "open_positions": [],
                "campaigns": [],
                "tradebook_path": str(tradebook_path),
                "debug_log_path": str(self.debug_log_path),
            }

        campaigns = self._build_roundtrip_campaigns(rows)
        if not campaigns:
            return {
                "ok": True,
                "starting_capital": starting_capital,
                "per_position_budget": per_position_budget,
                "summary": {
                    "funded_campaigns": 0,
                    "skipped_campaigns": 0,
                    "closed_campaigns": 0,
                    "open_campaigns": 0,
                    "portfolio_value": starting_capital,
                    "invested_amount": 0.0,
                    "cash": starting_capital,
                    "market_value": 0.0,
                    "open_positions_count": 0,
                    "portfolio_return_pct": 0.0,
                },
                "daily": [],
                "positions": [],
                "open_positions": [],
                "campaigns": [],
                "tradebook_path": str(tradebook_path),
                "latest_market_date": repo.latest_market_date().isoformat() if repo.latest_market_date() else "",
                "note": "No round-trip campaigns were detected in the latest tradebook.",
                "debug_log_path": str(self.debug_log_path),
            }

        latest_market_day = repo.latest_market_date() or date.today()
        settings = self.load_settings()
        stop_loss_pct = self._as_float(settings.get("stop_loss_pct"))
        if stop_loss_pct <= 0:
            stop_loss_pct = 2.0
        campaign_days: List[date] = []
        for item in campaigns:
            start_dt = self._parse_tradebook_time(item.get("start_time"))
            if start_dt:
                campaign_days.append(start_dt.date())
        earliest_day = min(campaign_days) if campaign_days else latest_market_day
        if earliest_day > latest_market_day:
            earliest_day = latest_market_day

        unique_symbols = sorted({normalize_symbol(str(item.get("symbol") or "")) for item in campaigns if normalize_symbol(str(item.get("symbol") or ""))})
        close_by_symbol: Dict[str, Dict[date, float]] = {}
        all_market_days: set[date] = set()
        for symbol in unique_symbols:
            bars = repo.fetch_daily_bars(symbol, earliest_day, latest_market_day)
            day_map: Dict[date, float] = {}
            for bar in bars:
                bar_date = bar.get("mktdate")
                close_value = self._as_float(bar.get("close"))
                if isinstance(bar_date, date) and close_value > 0:
                    day_map[bar_date] = close_value
                    all_market_days.add(bar_date)
            if day_map:
                close_by_symbol[symbol] = day_map

        if not all_market_days:
            return {
                "ok": False,
                "message": "No bhav daily data found for portfolio simulation.",
                "starting_capital": starting_capital,
                "per_position_budget": per_position_budget,
                "summary": {},
                "daily": [],
                "positions": [],
                "open_positions": [],
                "campaigns": campaigns,
                "tradebook_path": str(tradebook_path),
                "debug_log_path": str(self.debug_log_path),
            }

        market_days = sorted(day for day in all_market_days if earliest_day <= day <= latest_market_day)
        campaign_meta: Dict[str, Dict[str, object]] = {}
        campaign_records: Dict[str, Dict[str, object]] = {}
        events: List[Dict[str, object]] = []
        for item in campaigns:
            entry_dt = self._parse_tradebook_time(item.get("start_time"))
            entry_price = self._as_float(item.get("entry_price"))
            if not entry_dt or entry_price <= 0:
                continue
            symbol = normalize_symbol(str(item.get("symbol") or ""))
            entry_day = entry_dt.date()
            path = self._evaluate_stop_path(
                symbol=symbol,
                entry_dt=entry_dt,
                entry_day=entry_day,
                end_day=latest_market_day,
                entry_price=entry_price,
                stop_loss_pct=stop_loss_pct,
                repo=repo,
                exit_dt=self._parse_tradebook_time(item.get("end_time")) if item.get("end_time") else None,
                live_return_pct=None,
                simulation_end_day=latest_market_day,
            )
            stop_touched = bool(path.get("stop_touched"))
            stop_touch_raw = str(path.get("stop_touch_date") or "")
            stop_touch_dt = None
            if stop_touched and stop_touch_raw:
                parsed_touch = self._parse_tradebook_time(stop_touch_raw)
                if parsed_touch:
                    if len(stop_touch_raw) == 10:
                        stop_touch_dt = datetime.combine(parsed_touch.date(), dt_time(15, 30))
                    else:
                        stop_touch_dt = self._naive_dt(parsed_touch)
            stop_touch_price = self._as_float(path.get("stop_touch_price")) if stop_touched else None
            if stop_touched and stop_touch_price <= 0:
                stop_touch_price = entry_price
            key = str(item.get("campaign_id") or "")
            campaign_meta[key] = {
                "stop_touched": stop_touched,
                "stop_touch_date": stop_touch_raw,
                "stop_touch_stage": str(path.get("stop_touch_stage") or ""),
                "stop_touch_price": stop_touch_price,
                "analysis_basis": "symbol_roundtrip_day1_tactical_then_daily_close_with_day3_ema_trailing",
            }
            events.append(
                {
                    "ts": entry_dt,
                    "kind": "entry",
                    "campaign_id": key,
                    "symbol": symbol,
                    "entry_price": entry_price,
                }
            )
            if stop_touched and stop_touch_dt and stop_touch_price > 0:
                events.append(
                    {
                        "ts": stop_touch_dt,
                        "kind": "exit",
                        "campaign_id": key,
                        "symbol": symbol,
                        "exit_price": round(stop_touch_price, 2),
                        "stop_touch_date": stop_touch_raw,
                        "stop_touch_stage": str(path.get("stop_touch_stage") or ""),
                    }
                )

        events.sort(key=lambda ev: (ev["ts"], 0 if ev["kind"] == "entry" else 1, str(ev.get("campaign_id") or "")))

        cash = starting_capital
        open_positions: Dict[str, Dict[str, object]] = {}
        funded_campaigns = 0
        skipped_campaigns = 0
        daily: List[Dict[str, object]] = []
        event_idx = 0

        for market_day in market_days:
            while event_idx < len(events) and events[event_idx]["ts"].date() == market_day:
                ev = events[event_idx]
                key = str(ev.get("campaign_id") or "")
                if ev["kind"] == "entry":
                    entry_price = self._as_float(ev.get("entry_price"))
                    if entry_price <= 0:
                        skipped_campaigns += 1
                        event_idx += 1
                        continue
                    budget = min(per_position_budget, cash)
                    qty = 0
                    if entry_price > 0 and budget > 0:
                        target_qty = int(math.ceil(per_position_budget / entry_price))
                        target_cost = round(target_qty * entry_price, 2)
                        if cash >= per_position_budget and target_cost <= cash + 0.01:
                            qty = target_qty
                        else:
                            qty = int(budget // entry_price)
                    invested = round(qty * entry_price, 2)
                    if qty <= 0 or invested <= 0:
                        skipped_campaigns += 1
                        self._log_debug(
                            "portfolio sim skipped entry: date=%s symbol=%s entry_price=%.2f cash=%.2f budget=%.2f"
                            % (market_day.isoformat(), ev.get("symbol") or "", entry_price, cash, budget)
                        )
                    else:
                        cash = round(cash - invested, 2)
                        open_positions[key] = {
                            "campaign_id": key,
                            "symbol": ev.get("symbol"),
                            "qty": int(qty),
                            "entry_price": round(entry_price, 2),
                            "invested": invested,
                            "entry_time": ev["ts"].isoformat(timespec="seconds"),
                            "status": "open",
                        }
                        meta = campaign_meta.get(key, {})
                        campaign_records[key] = {
                            "campaign_id": key,
                            "symbol": ev.get("symbol"),
                            "entry_date": ev["ts"].date().isoformat(),
                            "entry_time": ev["ts"].isoformat(timespec="seconds"),
                            "exit_time": "",
                            "status": "open",
                            "sim_qty": int(qty),
                            "entry_price": round(entry_price, 2),
                            "current_price": round(entry_price, 2),
                            "invested": invested,
                            "current_value": invested,
                            "pnl_value": 0.0,
                            "pnl_pct": 0.0,
                            "simulated_exit_price": None,
                            "stop_touched": bool(meta.get("stop_touched")),
                            "stop_touch_date": str(meta.get("stop_touch_date") or ""),
                            "stop_touch_stage": str(meta.get("stop_touch_stage") or ""),
                            "stop_touch_price": meta.get("stop_touch_price"),
                            "analysis_basis": str(meta.get("analysis_basis") or ""),
                        }
                        if budget < per_position_budget or invested < per_position_budget:
                            self._log_debug(
                                "portfolio sim reduced allocation: date=%s symbol=%s budget=%.2f qty=%s invested=%.2f cash_left=%.2f"
                                % (
                                    market_day.isoformat(),
                                    ev.get("symbol") or "",
                                    budget,
                                    qty,
                                    invested,
                                    cash,
                                )
                            )
                        funded_campaigns += 1
                elif ev["kind"] == "exit":
                    pos = open_positions.pop(key, None)
                    if pos:
                        exit_price = round(float(ev.get("exit_price") or 0.0), 2)
                        qty = int(pos.get("qty") or 0)
                        proceeds = round(qty * exit_price, 2)
                        cash = round(cash + proceeds, 2)
                        pos["exit_time"] = ev["ts"].isoformat(timespec="seconds")
                        pos["exit_price"] = exit_price
                        pos["status"] = "closed"
                        rec = campaign_records.get(key)
                        if rec:
                            invested = float(rec.get("invested") or 0.0)
                            pnl_value = round(proceeds - invested, 2)
                            pnl_pct = round((pnl_value / invested) * 100.0, 2) if invested else None
                            rec.update(
                                {
                                    "exit_time": ev["ts"].isoformat(timespec="seconds"),
                                    "status": "closed",
                                    "current_price": exit_price,
                                    "current_value": round(proceeds, 2),
                                    "pnl_value": pnl_value,
                                    "pnl_pct": pnl_pct,
                                    "simulated_exit_price": exit_price,
                                    "stop_touched": True,
                                    "stop_touch_date": str(ev.get("stop_touch_date") or ev["ts"].date().isoformat()),
                                    "stop_touch_stage": str(ev.get("stop_touch_stage") or ""),
                                    "stop_touch_price": exit_price,
                                }
                            )
                event_idx += 1

            open_list: List[Dict[str, object]] = []
            invested_amount = 0.0
            market_value = 0.0
            for pos in open_positions.values():
                symbol = str(pos.get("symbol") or "")
                qty = int(pos.get("qty") or 0)
                entry_price = self._as_float(pos.get("entry_price"))
                close_map = close_by_symbol.get(symbol, {})
                close_price = close_map.get(market_day)
                if close_price is None and close_map:
                    prior_days = [d for d in close_map.keys() if d <= market_day]
                    if prior_days:
                        close_price = close_map[max(prior_days)]
                if close_price is None:
                    close_price = entry_price
                current_value = round(qty * float(close_price or 0.0), 2)
                pnl_value = round(current_value - float(pos.get("invested") or 0.0), 2)
                pnl_pct = round((pnl_value / float(pos.get("invested") or 1.0)) * 100.0, 2) if pos.get("invested") else None
                open_list.append(
                    {
                        "campaign_id": pos.get("campaign_id"),
                        "symbol": symbol,
                        "qty": qty,
                        "entry_price": entry_price,
                        "current_price": round(float(close_price or 0.0), 2),
                        "invested": round(float(pos.get("invested") or 0.0), 2),
                        "current_value": current_value,
                        "pnl_value": pnl_value,
                        "pnl_pct": pnl_pct,
                        "entry_time": pos.get("entry_time"),
                        "status": "open",
                    }
                )
                rec = campaign_records.get(str(pos.get("campaign_id") or ""))
                if rec:
                    invested = float(pos.get("invested") or 0.0)
                    pnl_value = round(current_value - invested, 2)
                    pnl_pct = round((pnl_value / invested) * 100.0, 2) if invested else None
                    rec.update(
                        {
                            "status": "open",
                            "current_price": round(float(close_price or 0.0), 2),
                            "current_value": current_value,
                            "pnl_value": pnl_value,
                            "pnl_pct": pnl_pct,
                        }
                    )
                invested_amount += float(pos.get("invested") or 0.0)
                market_value += current_value

            portfolio_value = round(cash + market_value, 2)
            portfolio_return_pct = round(((portfolio_value - starting_capital) / starting_capital) * 100.0, 2) if starting_capital > 0 else None
            daily.append(
                {
                    "date": market_day.isoformat(),
                    "cash": round(cash, 2),
                    "invested_amount": round(invested_amount, 2),
                    "market_value": round(market_value, 2),
                    "portfolio_value": portfolio_value,
                    "open_positions_count": len(open_list),
                    "portfolio_return_pct": portfolio_return_pct,
                    "open_positions": open_list,
                }
            )

        current_snapshot = daily[-1] if daily else {}
        current_open_positions = current_snapshot.get("open_positions", []) if isinstance(current_snapshot, dict) else []
        campaign_rows = sorted(
            campaign_records.values(),
            key=lambda item: (
                item.get("entry_time") or "",
                item.get("symbol") or "",
            ),
        )
        summary = {
            "funded_campaigns": funded_campaigns,
            "skipped_campaigns": skipped_campaigns,
            "closed_campaigns": sum(1 for item in campaign_rows if item.get("status") == "closed"),
            "open_campaigns": sum(1 for item in campaign_rows if item.get("status") == "open"),
            "portfolio_value": current_snapshot.get("portfolio_value", starting_capital) if isinstance(current_snapshot, dict) else starting_capital,
            "invested_amount": current_snapshot.get("invested_amount", 0.0) if isinstance(current_snapshot, dict) else 0.0,
            "cash": current_snapshot.get("cash", starting_capital) if isinstance(current_snapshot, dict) else starting_capital,
            "market_value": current_snapshot.get("market_value", 0.0) if isinstance(current_snapshot, dict) else 0.0,
            "open_positions_count": current_snapshot.get("open_positions_count", 0) if isinstance(current_snapshot, dict) else 0,
            "portfolio_return_pct": current_snapshot.get("portfolio_return_pct", 0.0) if isinstance(current_snapshot, dict) else 0.0,
        }
        self._log_debug(
            "portfolio sim ready: funded=%s skipped=%s open=%s portfolio=%.2f"
            % (funded_campaigns, skipped_campaigns, summary["open_positions_count"], float(summary["portfolio_value"] or 0.0))
        )
        return {
            "ok": True,
            "starting_capital": starting_capital,
            "per_position_budget": per_position_budget,
            "summary": summary,
            "daily": daily,
            "positions": current_open_positions,
            "open_positions": current_open_positions,
            "campaign_rows": campaign_rows,
            "campaigns": campaign_rows,
            "tradebook_path": str(tradebook_path),
            "latest_market_date": latest_market_day.isoformat(),
            "note": "Each funded campaign is sized using up to 3 lakhs at entry, or whatever cash is available if less remains. Portfolio cash flow now follows the simulated exit path after entry; open rows show mark-to-market value and closed rows show realized value.",
            "debug_log_path": str(self.debug_log_path),
        }

    def _remaining_qty(self, position: Dict[str, object]) -> Optional[int]:
        actual_qty = self._total_qty(position)
        if actual_qty is None:
            return None
        sold = 0
        for trim in position.get("trims", []):
            if trim.get("done") and trim.get("sq"):
                sold += int(trim.get("sq") or 0)
        return max(0, int(actual_qty) - sold)

    def _execution_summary(self, position: Dict[str, object]) -> Dict[str, object]:
        total_qty = self._total_qty(position) or 0.0
        realized_qty = 0.0
        realized_value = 0.0
        last_sell_price = None
        last_sell_qty = 0.0
        last_sell_date = ""
        for trim in position.get("trims", []) or []:
            if not isinstance(trim, dict) or not trim.get("done"):
                continue
            sq = self._as_float(trim.get("sq"))
            ap = self._as_float(trim.get("ap"))
            if sq <= 0 or ap <= 0:
                continue
            realized_qty += sq
            realized_value += sq * ap
            last_sell_price = ap
            last_sell_qty = sq
            trim_date = str(trim.get("dt") or "").strip()
            if trim_date:
                last_sell_date = trim_date
        remaining_qty = self._remaining_qty(position)
        if remaining_qty is None:
            remaining_qty = max(0.0, total_qty - realized_qty)
        return {
            "total_qty": round(total_qty, 6),
            "realized_qty": round(realized_qty, 6),
            "realized_value": round(realized_value, 2),
            "remaining_qty": round(float(remaining_qty), 6),
            "sell_price": round(realized_value / realized_qty, 2) if realized_qty > 0 else None,
            "last_sell_price": round(last_sell_price, 2) if last_sell_price is not None else None,
            "last_sell_qty": round(last_sell_qty, 6),
            "last_sell_date": last_sell_date,
        }

    def _execution_block_exit_price(self, position: Dict[str, object], exec_summary: Optional[Dict[str, object]] = None) -> Optional[float]:
        exit_price = None
        for trim in position.get("trims", []) or []:
            if not isinstance(trim, dict) or not trim.get("done"):
                continue
            ap = self._as_float(trim.get("ap"))
            if ap <= 0:
                continue
            if exit_price is None or ap < exit_price:
                exit_price = ap
        if exit_price is not None:
            return round(exit_price, 2)

        summary = exec_summary if isinstance(exec_summary, dict) else self._execution_summary(position)
        for key in ("last_sell_price", "sell_price"):
            price = self._as_float(summary.get(key))
            if price > 0:
                return round(price, 2)
        return None

    def _total_qty(self, position: Dict[str, object]) -> Optional[float]:
        core_qty = self._primary_core_qty(position)
        tactical_qty = self._primary_tactical_qty(position)
        actual_qty = self._as_float(position.get("actualQty"))
        if core_qty <= 0 and tactical_qty <= 0 and actual_qty <= 0:
            return None
        return core_qty + tactical_qty + actual_qty

    def _entry_value(self, position: Dict[str, object]) -> Optional[object]:
        core_qty = self._primary_core_qty(position)
        core_entry = self._primary_core_entry(position)
        tactical_qty = self._primary_tactical_qty(position)
        tactical_entry = self._primary_tactical_entry(position)
        actual_qty = self._as_float(position.get("actualQty"))
        actual_entry = self._as_float(position.get("actualEntry"))

        legs: List[Tuple[float, float]] = []
        if core_qty > 0 and core_entry > 0:
            legs.append((core_qty, core_entry))
        if tactical_qty > 0 and tactical_entry > 0:
            legs.append((tactical_qty, tactical_entry))
        if actual_qty > 0 and actual_entry > 0:
            legs.append((actual_qty, actual_entry))

        if legs:
            total_qty = sum(qty for qty, _ in legs)
            if total_qty > 0:
                weighted = sum(qty * price for qty, price in legs) / total_qty
                return round(weighted, 2)

        if actual_entry > 0:
            return round(actual_entry, 2)
        if core_entry > 0:
            return round(core_entry, 2)
        if tactical_entry > 0:
            return round(tactical_entry, 2)
        return None

    def _is_meaningful_position(self, position: Dict[str, object]) -> bool:
        if self._entry_value(position) is not None or position.get("actualQty") is not None:
            return True

        scalar_fields = [
            "symbol",
            "merits",
            "planEntry",
            "planSL",
            "coreEntry",
            "coreQty",
            "coreSL",
            "tacticalEntry",
            "tacticalQty",
            "tacticalSL",
            "riskAmount",
            "entryDate",
            "trailNote",
            "posHigh",
            "trailOverride",
        ]
        for field in scalar_fields:
            value = position.get(field)
            if isinstance(value, str):
                if value.strip():
                    return True
            elif value not in (None, ""):
                return True

        mgmt = position.get("mgmt") or {}
        if str(mgmt.get("note") or "").strip():
            return True
        if any(bool(mgmt.get(key)) for key in ("fe", "fsl", "ft", "fbe")):
            return True

        for trim in position.get("trims", []):
            if trim.get("ap") is not None or trim.get("sq") is not None or trim.get("done"):
                return True
        return False

    def _live_position_key(self, position: Dict[str, object], fallback_date: str) -> str:
        symbol = normalize_symbol(str(position.get("symbol", "")))
        entry_date = str(position.get("entryDate") or fallback_date)
        actual_entry = self._entry_value(position)
        return f"live:{symbol}:{entry_date}:{actual_entry}"

    def _as_float(self, value: object) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _primary_tactical_qty(self, position: Dict[str, object]) -> float:
        raw = position.get("tacticalQty")
        if raw not in (None, ""):
            return max(0.0, self._as_float(raw))
        return 0.0

    def _primary_core_qty(self, position: Dict[str, object]) -> float:
        raw = position.get("coreQty")
        if raw not in (None, ""):
            return max(0.0, self._as_float(raw))
        return 0.0

    def _primary_tactical_entry(self, position: Dict[str, object]) -> float:
        raw = position.get("tacticalEntry")
        if raw not in (None, ""):
            return self._as_float(raw)
        return 0.0

    def _primary_core_entry(self, position: Dict[str, object]) -> float:
        raw = position.get("coreEntry")
        if raw not in (None, ""):
            return self._as_float(raw)
        return 0.0

    def _primary_tactical_sl(self, position: Dict[str, object]) -> float:
        raw = position.get("tacticalSL")
        if raw not in (None, ""):
            return self._as_float(raw)
        return 0.0

    def _primary_core_sl(self, position: Dict[str, object]) -> float:
        raw = position.get("coreSL")
        if raw not in (None, ""):
            return self._as_float(raw)
        return 0.0

    def _actual_deployed_risk(self, position: Dict[str, object]) -> float:
        plan_sl = self._as_float(position.get("planSL"))
        risk = 0.0

        core_qty = self._primary_core_qty(position)
        core_entry = self._primary_core_entry(position)
        core_sl = self._primary_core_sl(position) or plan_sl
        if core_qty > 0 and core_entry > 0 and core_sl > 0:
            risk += max(0.0, (core_entry - core_sl) * core_qty)

        tactical_qty = self._primary_tactical_qty(position)
        tactical_entry = self._primary_tactical_entry(position)
        tactical_sl = self._primary_tactical_sl(position) or plan_sl
        if tactical_qty > 0 and tactical_entry > 0 and tactical_sl > 0:
            risk += max(0.0, (tactical_entry - tactical_sl) * tactical_qty)

        actual_qty = self._as_float(position.get("actualQty"))
        actual_entry = self._as_float(position.get("actualEntry"))
        day_sl = self._as_float(position.get("daySL")) or plan_sl
        if actual_qty > 0 and actual_entry > 0 and day_sl > 0:
            risk += max(0.0, (actual_entry - day_sl) * actual_qty)

        return round(risk, 2)

    def _collect_latest_positioned(self) -> List[Dict[str, object]]:
        latest_positions: Dict[str, Dict[str, object]] = {}
        for item_date in self.list_plan_dates():
            payload = self.load_plan(item_date)
            for raw_position in payload.get("positions", []):
                if self._entry_value(raw_position) is None:
                    continue
                position = copy.deepcopy(raw_position)
                if not position.get("entryDate"):
                    position["entryDate"] = item_date
                key = self._live_position_key(position, item_date)
                latest_positions[key] = position
        return list(latest_positions.values())

    def build_day_view(self, plan_date: str, repo: BhavRepository) -> Dict[str, object]:
        target_dt = datetime.strptime(plan_date, "%Y-%m-%d").date()
        dates = [d for d in self.list_plan_dates() if d <= plan_date]
        settings = self.load_settings()

        open_positions: Dict[str, Dict[str, object]] = {}
        closed_keys: set[str] = set()
        planning_for_day: List[Dict[str, object]] = []
        exited_today_positions: List[Dict[str, object]] = []

        for item_date in dates:
            payload = self.load_plan(item_date)
            for raw_position in payload.get("positions", []):
                position = copy.deepcopy(raw_position)
                if self._entry_value(position) is not None:
                    if not position.get("entryDate"):
                        position["entryDate"] = item_date
                    key = str(position.get("id") or "").strip() or self._live_position_key(position, item_date)
                    is_closed = self._remaining_qty(position) == 0 or position.get("_status") == "closed"
                    # Once a position key has been observed as closed, do not let a later stale snapshot
                    # reopen it on a future day.
                    if key in closed_keys and not is_closed:
                        continue
                    if item_date == plan_date and is_closed:
                        open_positions.pop(key, None)
                        exited_today_positions.append(position)
                        closed_keys.add(key)
                        continue
                    if is_closed:
                        open_positions.pop(key, None)
                        closed_keys.add(key)
                        continue
                    open_positions[key] = position
                elif item_date == plan_date and self._is_meaningful_position(position):
                    planning_for_day.append(position)

        carried_positions = sorted(
            open_positions.values(),
            key=lambda p: (str(p.get("entryDate") or plan_date), normalize_symbol(str(p.get("symbol", "")))),
        )
        exited_today_positions = sorted(
            exited_today_positions,
            key=lambda p: (str(p.get("entryDate") or plan_date), normalize_symbol(str(p.get("symbol", "")))),
        )
        day_positions = carried_positions + exited_today_positions + planning_for_day

        exposure = 0.0
        for position in carried_positions:
            symbol = str(position.get("symbol", ""))
            price_info = repo.lookup_last_close(symbol, target_dt) if symbol else None
            if price_info and price_info.get("cmp") is not None:
                position["cmp"] = price_info["cmp"]
            remaining_qty = self._remaining_qty(position)
            if remaining_qty and remaining_qty > 0:
                mark_price = position.get("cmp") or self._entry_value(position) or position.get("planEntry") or 0
                exposure += float(mark_price) * remaining_qty

        available_capital = settings.get("available_capital")
        exposure_pct = None
        try:
            if available_capital:
                exposure_pct = (float(exposure) / float(available_capital)) * 100.0
        except Exception:
            exposure_pct = None

        realized_pnl = 0.0
        win_count = 0
        loss_count = 0
        for position in exited_today_positions:
            pnl = self._as_float(position.get("_realPnl"))
            realized_pnl += pnl
            if pnl > 0:
                win_count += 1
            elif pnl < 0:
                loss_count += 1

        open_pnl = 0.0
        carried_risk_total = 0.0
        for position in carried_positions:
            open_pnl += self._as_float(position.get("_openPnl"))
            remaining_qty = self._remaining_qty(position)
            if remaining_qty is None or remaining_qty <= 0:
                continue
            entry = self._entry_value(position)
            if entry is None or entry <= 0:
                entry = (
                    self._as_float(position.get("actualEntry"))
                    or self._primary_core_entry(position)
                    or self._primary_tactical_entry(position)
                    or self._as_float(position.get("planEntry"))
                )
            sl = self._as_float(position.get("_currentSL"))
            if sl <= 0:
                sl = self._as_float(position.get("planSL"))
            if entry > 0 and sl > 0:
                carried_risk_total += max(0.0, (entry - sl) * remaining_qty)

        return {
            "ok": True,
            "date": plan_date,
            "positions": day_positions,
            "briefing": self.load_plan(plan_date).get("briefing", {}),
            "raw_path": str(self.plan_path(plan_date)),
            "open_positions_count": len(carried_positions),
            "exited_today_count": len(exited_today_positions),
            "planning_count": len(planning_for_day),
            "exposure": round(exposure, 2),
            "exposure_pct": round(exposure_pct, 2) if exposure_pct is not None else None,
            "realized_pnl": round(realized_pnl, 2),
            "open_pnl": round(open_pnl, 2),
            "closed_count": len(exited_today_positions),
            "win_count": win_count,
            "loss_count": loss_count,
            "carried_risk_total": round(carried_risk_total, 2),
            "settings": settings,
        }

    def build_dashboard(self, repo: BhavRepository) -> Dict[str, object]:
        settings = self.load_settings()
        items: List[Dict[str, object]] = []
        for plan_date in self.list_plan_dates():
            view = self.build_day_view(plan_date, repo)
            items.append(
                {
                    "date": plan_date,
                    "open_positions_count": view["open_positions_count"],
                    "planning_count": view["planning_count"],
                    "exposure": view["exposure"],
                    "exposure_pct": view["exposure_pct"],
                    "realized_pnl": view.get("realized_pnl", 0.0),
                    "open_pnl": view.get("open_pnl", 0.0),
                    "closed_count": view.get("closed_count", 0),
                    "win_count": view.get("win_count", 0),
                    "loss_count": view.get("loss_count", 0),
                    "carried_risk_total": view.get("carried_risk_total", 0.0),
                }
            )
        latest_date = items[-1]["date"] if items else date.today().isoformat()
        return {"ok": True, "items": items, "latest_date": latest_date, "settings": settings}

    def build_goal_tracker(self, repo: BhavRepository) -> Dict[str, object]:
        dashboard = self.build_dashboard(repo)
        settings = dashboard["settings"]
        daily_risk_budget = self._as_float(settings.get("daily_risk"))
        position_risk_budget = self._as_float(settings.get("per_position_risk"))

        risk_by_date: Dict[str, Dict[str, object]] = {}
        plan_by_date: Dict[str, Dict[str, object]] = {}

        for position in self._collect_latest_positioned():
            entry_date = str(position.get("entryDate") or "")
            if not entry_date:
                continue

            risk_amount = self._as_float(position.get("riskAmount"))
            risk_item = risk_by_date.setdefault(
                entry_date,
                {
                    "date": entry_date,
                    "executed_count": 0,
                    "allotted_risk": 0.0,
                    "actual_risk": 0.0,
                    "max_single_allotted_risk": 0.0,
                    "max_single_actual_risk": 0.0,
                },
            )
            actual_risk = self._actual_deployed_risk(position)
            risk_item["executed_count"] += 1
            risk_item["allotted_risk"] += risk_amount
            risk_item["actual_risk"] += actual_risk
            risk_item["max_single_allotted_risk"] = max(float(risk_item["max_single_allotted_risk"]), risk_amount)
            risk_item["max_single_actual_risk"] = max(float(risk_item["max_single_actual_risk"]), actual_risk)

            mgmt = position.get("mgmt") or {}
            plan_item = plan_by_date.setdefault(
                entry_date,
                {
                    "date": entry_date,
                    "trade_count": 0,
                    "checks_done": 0,
                    "checks_total": 0,
                    "followed_entry_count": 0,
                    "respected_sl_count": 0,
                    "executed_trims_count": 0,
                    "breakeven_count": 0,
                },
            )
            plan_item["trade_count"] += 1
            checks = {
                "fe": "followed_entry_count",
                "fsl": "respected_sl_count",
                "ft": "executed_trims_count",
                "fbe": "breakeven_count",
            }
            for source_key, target_key in checks.items():
                plan_item["checks_total"] += 1
                if bool(mgmt.get(source_key)):
                    plan_item[target_key] += 1
                    plan_item["checks_done"] += 1

        r_progress_items: List[Dict[str, object]] = []
        for item_date in sorted(risk_by_date):
            item = risk_by_date[item_date]
            executed_count = int(item["executed_count"] or 0)
            allotted_risk = float(item["allotted_risk"] or 0.0)
            actual_risk = float(item["actual_risk"] or 0.0)
            avg_allotted_risk = allotted_risk / executed_count if executed_count else 0.0
            avg_actual_risk = actual_risk / executed_count if executed_count else 0.0
            max_single_allotted_risk = float(item["max_single_allotted_risk"] or 0.0)
            max_single_actual_risk = float(item["max_single_actual_risk"] or 0.0)
            r_progress_items.append(
                {
                    "date": item_date,
                    "executed_count": executed_count,
                    "allotted_risk": round(allotted_risk, 2),
                    "actual_risk": round(actual_risk, 2),
                    "avg_allotted_risk": round(avg_allotted_risk, 2),
                    "avg_actual_risk": round(avg_actual_risk, 2),
                    "max_single_allotted_risk": round(max_single_allotted_risk, 2),
                    "max_single_actual_risk": round(max_single_actual_risk, 2),
                    "daily_allotted_risk_pct": round((allotted_risk / daily_risk_budget) * 100.0, 2)
                    if daily_risk_budget > 0
                    else None,
                    "daily_actual_risk_pct": round((actual_risk / daily_risk_budget) * 100.0, 2)
                    if daily_risk_budget > 0
                    else None,
                    "actual_vs_allotted_pct": round((actual_risk / allotted_risk) * 100.0, 2)
                    if allotted_risk > 0
                    else None,
                    "avg_allotted_risk_pct": round((avg_allotted_risk / position_risk_budget) * 100.0, 2)
                    if position_risk_budget > 0
                    else None,
                    "avg_actual_risk_pct": round((avg_actual_risk / position_risk_budget) * 100.0, 2)
                    if position_risk_budget > 0
                    else None,
                    "max_single_allotted_risk_pct": round((max_single_allotted_risk / position_risk_budget) * 100.0, 2)
                    if position_risk_budget > 0
                    else None,
                    "max_single_actual_risk_pct": round((max_single_actual_risk / position_risk_budget) * 100.0, 2)
                    if position_risk_budget > 0
                    else None,
                }
            )

        plan_stats_items: List[Dict[str, object]] = []
        for item_date in sorted(plan_by_date):
            item = plan_by_date[item_date]
            trade_count = int(item["trade_count"] or 0)
            checks_total = int(item["checks_total"] or 0)
            checks_done = int(item["checks_done"] or 0)

            def ratio(count_key: str) -> Optional[float]:
                if trade_count <= 0:
                    return None
                return round((int(item[count_key]) / trade_count) * 100.0, 2)

            plan_stats_items.append(
                {
                    "date": item_date,
                    "trade_count": trade_count,
                    "checks_done": checks_done,
                    "checks_total": checks_total,
                    "adherence_pct": round((checks_done / checks_total) * 100.0, 2) if checks_total else None,
                    "followed_entry_pct": ratio("followed_entry_count"),
                    "respected_sl_pct": ratio("respected_sl_count"),
                    "executed_trims_pct": ratio("executed_trims_count"),
                    "breakeven_pct": ratio("breakeven_count"),
                }
            )

        latest_date = dashboard["latest_date"]
        return {
            "ok": True,
            "latest_date": latest_date,
            "settings": settings,
            "exposure_items": dashboard["items"],
            "r_progress_items": r_progress_items,
            "plan_stats_items": plan_stats_items,
        }

    def load_plan(self, plan_date: str) -> Dict[str, object]:
        payload = self.load_plan_raw(plan_date)
        path = self.plan_path(plan_date)
        if not isinstance(payload, dict):
            payload = {"positions": []}
        normalized_positions = [self._strip_legacy_trade_fields(position) for position in payload.get("positions", []) if isinstance(position, dict)]
        payload["positions"] = self._apply_trim_date_hints(self._dedupe_positions(normalized_positions))
        payload["date"] = plan_date
        payload["path"] = str(path)
        payload["exists"] = True
        return payload

    def load_plan_raw(self, plan_date: str) -> Dict[str, object]:
        path = self.plan_path(plan_date)
        if not path.exists():
            return {"date": plan_date, "positions": [], "path": str(path), "exists": False}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            payload = {"positions": []}
        payload["date"] = plan_date
        payload["path"] = str(path)
        payload["exists"] = True
        return payload

    def save_plan(self, plan_date: str, positions: Sequence[dict]) -> Dict[str, object]:
        path = self.plan_path(plan_date)
        normalized_positions = [self._strip_legacy_trade_fields(position) for position in positions if isinstance(position, dict)]
        cleaned_positions = [position for position in self._apply_trim_date_hints(normalized_positions) if self._is_meaningful_position(position)]
        cleaned_positions = self._dedupe_positions(cleaned_positions)
        payload = {
            "date": plan_date,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "positions": cleaned_positions,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self._trim_date_hints_cache = None
        return {"ok": True, "date": plan_date, "path": str(path), "saved_at": payload["saved_at"]}

    def save_day_briefing(self, plan_date: str, briefing: dict) -> Dict[str, object]:
        path = self.plan_path(plan_date)
        payload = self.load_plan_raw(plan_date)
        if not isinstance(payload, dict):
            payload = {}
        existing_briefing = payload.get("briefing")
        merged_briefing = dict(existing_briefing) if isinstance(existing_briefing, dict) else {}
        if isinstance(briefing, dict):
            merged_briefing.update(briefing)
        payload["date"] = plan_date
        payload["saved_at"] = datetime.now().isoformat(timespec="seconds")
        payload["briefing"] = merged_briefing
        if "positions" not in payload or not isinstance(payload.get("positions"), list):
            payload["positions"] = []
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {"ok": True, "date": plan_date, "path": str(path), "saved_at": payload["saved_at"]}

    def _dedupe_positions(self, positions: Sequence[dict]) -> List[dict]:
        deduped: Dict[str, dict] = {}
        for position in positions:
            if not isinstance(position, dict):
                continue
            pid = self._trim_position_key(position)
            deduped[pid] = position
        return list(deduped.values())


class TradePlanHandler(BaseHTTPRequestHandler):
    repo: BhavRepository
    store: TradePlanStore

    def _log_debug(self, message: str) -> None:
        logger = getattr(self.store, "_log_debug", None)
        if callable(logger):
            logger(message)

    def _kite_scan_targets(self, body: dict) -> dict:
        raw_date = str(body.get("date") or "").strip()
        if raw_date:
            try:
                target_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError as exc:
                raise ValueError("Missing or invalid date.") from exc
        else:
            target_date = datetime.now().date()

        payload = self.store.load_plan(target_date.isoformat())
        positions = payload.get("positions", []) if isinstance(payload, dict) else []
        targets: List[Dict[str, object]] = []
        for position in positions:
            if not isinstance(position, dict):
                continue
            symbol = str(position.get("symbol") or "").strip().upper()
            status = str(position.get("_status") or "").strip().lower()
            qty = 0
            try:
                qty = int(float(position.get("_rem") if position.get("_rem") is not None else 0))
            except Exception:
                qty = 0

            if not symbol:
                continue
            if status == "closed":
                continue
            is_planned = status in {"planning", "planned"}
            is_carried = status in {"partial", "active", "overnight"} or qty > 0
            if not (is_planned or is_carried):
                continue

            trail = position.get("trailOverride")
            try:
                trail_value = float(trail)
            except (TypeError, ValueError):
                trail_value = 0.0

            targets.append(
                {
                    "symbol": symbol,
                    "status": status,
                }
            )

        return {
            "ok": True,
            "date": target_date.isoformat(),
            "targets": targets,
        }

    def _opening_bar_guard(self, body: dict) -> dict:
        kite_mod = importlib.import_module("place_kite_stop_loss_orders")

        raw_date = str(body.get("date") or "").strip()
        if raw_date:
            try:
                target_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            except ValueError as exc:
                raise ValueError("Missing or invalid date.") from exc
        else:
            target_date = datetime.now().date()

        raw_token = body.get("instrument_token")
        try:
            instrument_token = int(raw_token)
        except (TypeError, ValueError) as exc:
            raise ValueError("Missing or invalid instrument token.") from exc

        symbol = str(body.get("symbol") or "").strip().upper()
        interval = str(body.get("interval") or "5minute").strip() or "5minute"
        threshold = float(body.get("threshold_pct") or 2.0)
        token_file = Path(body.get("token_file") or kite_mod.DEFAULT_TOKEN_FILE)
        kite = kite_mod.get_kite_client(token_file)

        from_dt = datetime.combine(target_date, dt_time(9, 15))
        to_dt = datetime.combine(target_date, dt_time(9, 20))
        rows = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_dt,
            to_date=to_dt,
            interval=interval,
            continuous=False,
            oi=False,
        )
        if not rows:
            return {
                "ok": False,
                "reason": "no_candles",
                "symbol": symbol,
                "instrument_token": instrument_token,
                "date": target_date.isoformat(),
                "interval": interval,
                "threshold_pct": threshold,
            }

        first = rows[0]
        candle_time = first.get("date")
        open_value = float(first.get("open") or 0)
        high_value = float(first.get("high") or 0)
        low_value = float(first.get("low") or 0)
        close_value = float(first.get("close") or 0)
        if not open_value:
            raise ValueError("Opening candle has no open price.")

        range_pct = ((high_value - low_value) / open_value) * 100.0
        body_pct = (abs(close_value - open_value) / open_value) * 100.0
        session_label = candle_time.isoformat() if hasattr(candle_time, "isoformat") else str(candle_time)
        return {
            "ok": True,
            "symbol": symbol,
            "instrument_token": instrument_token,
            "date": target_date.isoformat(),
            "interval": interval,
            "threshold_pct": threshold,
            "opening": {
                "time": session_label,
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "rangePct": round(range_pct, 2),
                "bodyPct": round(body_pct, 2),
            },
        }

    def _place_kite_sl_order(self, body: dict) -> dict:
        kite_mod = importlib.import_module("place_kite_stop_loss_orders")

        plan_date = self._require_date(str(body.get("date") or ""))
        if plan_date is None:
            raise ValueError("Missing or invalid date.")

        position_id = str(body.get("position_id") or body.get("id") or "").strip()
        if not position_id:
            raise ValueError("Missing position id.")

        payload = self.store.load_plan(plan_date.isoformat())
        positions = payload.get("positions", []) if isinstance(payload, dict) else []
        position = None
        for item in positions:
            if isinstance(item, dict) and str(item.get("id") or "").strip() == position_id:
                position = item
                break
        if position is None:
            raise ValueError("Position not found in saved plan.")

        trail = kite_mod.trailing_stop_value(position)
        if trail is None:
            raise ValueError("No trailing stop is set for this position.")

        status = str(position.get("_status") or "").strip().lower()
        if status == "closed":
            raise ValueError("This position is already closed.")

        rem = kite_mod.remaining_qty(position)
        if rem <= 0:
            raise ValueError("No remaining quantity to protect.")

        symbol = kite_mod.infer_tradingsymbol(str(position.get("symbol") or ""))
        exchange = kite_mod.infer_exchange(str(position.get("symbol") or ""))
        tag_prefix = str(body.get("tag_prefix") or "TP-SL").strip() or "TP-SL"
        tag = f"{tag_prefix}-{plan_date.isoformat()}-{symbol}"
        product = str(body.get("product") or "CNC").strip().upper() or "CNC"
        if product not in {"CNC", "MIS"}:
            product = "CNC"

        token_file = Path(body.get("token_file") or kite_mod.DEFAULT_TOKEN_FILE)
        kite = kite_mod.get_kite_client(token_file)
        tick_size = kite_mod.resolve_tick_size(kite, symbol, exchange, fallback=float(body.get("tick_size") or 0.05))
        limit_price, trigger_price = kite_mod.stop_limit_prices(float(trail), tick_size)

        state_file = Path(body.get("state_file") or kite_mod.DEFAULT_STATE_FILE).expanduser().resolve()
        state = kite_mod.load_state(state_file)
        remembered = state.setdefault("orders", {})

        if tag in remembered:
            return {
                "ok": True,
                "skipped": True,
                "reason": "already_processed",
                "tag": tag,
                "order_id": remembered[tag].get("order_id", ""),
                "symbol": symbol,
                "quantity": rem,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
                "tick_size": tick_size,
            }

        existing_orders = kite_mod.get_orders_with_tag(kite, tag)
        if existing_orders:
            last_order = existing_orders[-1]
            order_id = str(last_order.get("order_id") or "")
            status_txt = str(last_order.get("status") or "")
            remembered[tag] = {
                "order_id": order_id,
                "status": status_txt,
                "symbol": symbol,
                "plan_date": plan_date.isoformat(),
                "quantity": rem,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
                "tick_size": tick_size,
            }
            state["last_run"] = {
                "plan_file": str(self.store.plan_path(plan_date.isoformat())),
                "plan_date": plan_date.isoformat(),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            kite_mod.save_state(state_file, state)
            return {
                "ok": True,
                "skipped": True,
                "reason": "kite_order_exists",
                "tag": tag,
                "order_id": order_id,
                "status": status_txt,
                "symbol": symbol,
                "quantity": rem,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
                "tick_size": tick_size,
            }

        preview_only = bool(body.get("preview_only"))
        if preview_only:
            return {
                "ok": True,
                "preview": True,
                "tag": tag,
                "symbol": symbol,
                "exchange": exchange,
                "quantity": rem,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
                "tick_size": tick_size,
            }

        try:
            order_id = kite.place_order(
                variety="regular",
                exchange=exchange,
                tradingsymbol=symbol,
                transaction_type="SELL",
                quantity=rem,
                product=product,
                order_type="SL",
                validity="DAY",
                price=limit_price,
                trigger_price=trigger_price,
                tag=tag,
            )
        except Exception as exc:
            code = getattr(exc, "code", None)
            detail = f"{exc.__class__.__name__}"
            if code is not None:
                detail += f" ({code})"
            detail += f": {exc}"
            self._log_debug(
                "kite SL order failed: " + detail + "\n" + traceback.format_exc()
            )
            raise
        remembered[tag] = {
            "order_id": order_id,
            "status": "submitted",
            "symbol": symbol,
            "plan_date": plan_date.isoformat(),
            "quantity": rem,
            "trigger_price": trigger_price,
            "limit_price": limit_price,
            "tick_size": tick_size,
        }
        state["last_run"] = {
            "plan_file": str(self.store.plan_path(plan_date.isoformat())),
            "plan_date": plan_date.isoformat(),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        kite_mod.save_state(state_file, state)
        return {
            "ok": True,
            "placed": True,
            "order_id": order_id,
            "tag": tag,
            "symbol": symbol,
            "exchange": exchange,
            "quantity": rem,
            "trigger_price": trigger_price,
            "limit_price": limit_price,
            "tick_size": tick_size,
        }


    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def log_message(self, fmt: str, *args) -> None:
        return

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path in {"/", "/index.html"}:
            if not self.store.html_path.exists():
                return self._send_error_json(HTTPStatus.NOT_FOUND, "HTML file not found.")
            blob = self.store.html_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)
            return

        if parsed.path == "/trade_plan_1.html":
            html_path = self.store.base_dir / "trade_plan_1.html"
            if not html_path.exists():
                return self._send_error_json(HTTPStatus.NOT_FOUND, "trade_plan_1.html not found.")
            blob = html_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)
            return

        if parsed.path == "/TRADEP_12_1.htm":
            html_path = self.store.base_dir / "TRADEP_12_1.htm"
            if not html_path.exists():
                return self._send_error_json(HTTPStatus.NOT_FOUND, "TRADEP_12_1.htm not found.")
            blob = html_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)
            return

        if parsed.path == "/api/health":
            return self._send_json(
                {
                    "ok": True,
                    "html_path": str(self.store.html_path),
                    "save_dir": str(self.store.save_dir),
                }
            )

        if parsed.path == "/static/trade_plan_app.js":
            script_path = self.store.base_dir / "trade_plan_app.js"
            if not script_path.exists():
                return self._send_error_json(HTTPStatus.NOT_FOUND, "trade_plan_app.js not found.")
            blob = script_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)
            return

        if parsed.path == "/api/storage-info":
            return self._send_json(
                {
                    "ok": True,
                    "html_path": str(self.store.html_path),
                    "save_dir": str(self.store.save_dir),
                    "public_ip": fetch_public_ip(),
                    "public_ip_v6": fetch_public_ipv6(),
                }
            )

        if parsed.path == "/api/settings":
            return self._send_json({"ok": True, **self.store.load_settings(), "path": str(self.store.settings_path)})

        if parsed.path == "/api/dashboard":
            return self._send_json(self.store.build_dashboard(self.repo))

        if parsed.path == "/api/goal-tracker":
            return self._send_json(self.store.build_goal_tracker(self.repo))

        if parsed.path == "/api/stop-loss-streak":
            if getattr(self.store, "_log_debug", None):
                try:
                    self.store._log_debug(
                        "stop-loss streak endpoint hit (%s) from %s:%s"
                        % (parsed.path, self.client_address[0], self.client_address[1])
                    )
                except Exception:
                    pass
            try:
                return self._send_json(self.store.build_stop_loss_streak(self.repo))
            except Exception as exc:
                if getattr(self.store, "_log_debug", None):
                    try:
                        self.store._log_debug(f"stop-loss streak endpoint failed: {exc}")
                    except Exception:
                        pass
                return self._send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, f"Stop-loss streak report failed: {exc}")

        if parsed.path == "/api/streaks":
            if getattr(self.store, "_log_debug", None):
                try:
                    self.store._log_debug(
                        "plan streaks endpoint hit from %s:%s"
                        % (self.client_address[0], self.client_address[1])
                    )
                except Exception:
                    pass
            try:
                return self._send_json(self.store._build_snapshot_streaks(self.repo, 20))
            except Exception as exc:
                if getattr(self.store, "_log_debug", None):
                    try:
                        self.store._log_debug(f"plan streaks endpoint failed: {exc}")
                    except Exception:
                        pass
                return self._send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, f"Plan streaks report failed: {exc}")

        if parsed.path == "/api/portfolio-sim":
            if getattr(self.store, "_log_debug", None):
                try:
                    self.store._log_debug(
                        "portfolio sim endpoint hit from %s:%s"
                        % (self.client_address[0], self.client_address[1])
                    )
                except Exception:
                    pass
            try:
                return self._send_json(self.store.build_portfolio_simulation(self.repo))
            except Exception as exc:
                if getattr(self.store, "_log_debug", None):
                    try:
                        self.store._log_debug(f"portfolio sim endpoint failed: {exc}")
                    except Exception:
                        pass
                return self._send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, f"Portfolio simulation failed: {exc}")

        if parsed.path == "/api/plan":
            plan_date = self._require_date(params.get("date", [""])[0])
            if plan_date is None:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Missing or invalid date.")
            return self._send_json(self.store.load_plan(plan_date.isoformat()))

        if parsed.path == "/api/day-view":
            plan_date = self._require_date(params.get("date", [""])[0])
            if plan_date is None:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Missing or invalid date.")
            return self._send_json(self.store.build_day_view(plan_date.isoformat(), self.repo))

        if parsed.path == "/api/symbols":
            term = params.get("term", [""])[0]
            limit = min(max(int(params.get("limit", ["10"])[0] or "10"), 1), 20)
            return self._send_json({"ok": True, "items": self.repo.suggest_symbols(term, limit)})

        if parsed.path == "/api/resolve-symbol":
            symbol = params.get("symbol", [""])[0]
            plan_date = self._require_date(params.get("date", [""])[0])
            if plan_date is None:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Missing or invalid date.")
            return self._send_json(self.repo.resolve_with_price(symbol, plan_date))

        return self._send_error_json(HTTPStatus.NOT_FOUND, "Route not found.")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in {"/api/plan", "/api/day-view", "/api/settings", "/api/kite/place-sl-order", "/api/kite/opening-bar", "/api/kite/scan-targets"}:
            return self._send_error_json(HTTPStatus.NOT_FOUND, "Route not found.")

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            return self._send_error_json(HTTPStatus.BAD_REQUEST, "Invalid JSON body.")

        if parsed.path == "/api/settings":
            if not isinstance(body, dict):
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Settings body must be an object.")
            return self._send_json(self.store.save_settings(body))

        if parsed.path == "/api/day-view":
            if not isinstance(body, dict):
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Request body must be an object.")
            plan_date = self._require_date(str(body.get("date") or ""))
            if plan_date is None:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Missing or invalid date.")
            briefing = body.get("briefing") or {}
            if not isinstance(briefing, dict):
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "`briefing` must be an object.")
            return self._send_json(self.store.save_day_briefing(plan_date.isoformat(), briefing))

        if parsed.path == "/api/kite/place-sl-order":
            if not isinstance(body, dict):
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Request body must be an object.")
            try:
                return self._send_json(self._place_kite_sl_order(body))
            except Exception as exc:
                code = getattr(exc, "code", None)
                message = f"{exc.__class__.__name__}"
                if code is not None:
                    message += f" ({code})"
                message += f": {exc}"
                return self._send_error_json(HTTPStatus.BAD_REQUEST, message)

        if parsed.path == "/api/kite/opening-bar":
            if not isinstance(body, dict):
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Request body must be an object.")
            try:
                return self._send_json(self._opening_bar_guard(body))
            except Exception as exc:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))

        if parsed.path == "/api/kite/scan-targets":
            if not isinstance(body, dict):
                return self._send_error_json(HTTPStatus.BAD_REQUEST, "Request body must be an object.")
            try:
                return self._send_json(self._kite_scan_targets(body))
            except Exception as exc:
                return self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))

        params = parse_qs(parsed.query)
        plan_date = self._require_date(params.get("date", [""])[0])
        if plan_date is None:
            return self._send_error_json(HTTPStatus.BAD_REQUEST, "Missing or invalid date.")

        positions = body.get("positions", [])
        if not isinstance(positions, list):
            return self._send_error_json(HTTPStatus.BAD_REQUEST, "`positions` must be a list.")

        return self._send_json(self.store.save_plan(plan_date.isoformat(), positions))

    def _require_date(self, raw_value: str) -> Optional[date]:
        try:
            return datetime.strptime(raw_value, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        blob = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def _send_error_json(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"ok": False, "message": message}, status=status)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trade plan local API for bhav-backed symbol lookup and saves.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--html-path", default=str(DEFAULT_HTML_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    html_path = Path(args.html_path).expanduser().resolve()

    repo = BhavRepository()
    store = TradePlanStore(html_path)
    TradePlanHandler.repo = repo
    TradePlanHandler.store = store

    server = ThreadingHTTPServer((args.host, args.port), TradePlanHandler)
    print(f"Trade plan API listening on http://{args.host}:{args.port}")
    print(f"HTML file : {html_path}")
    print(f"Save dir  : {store.save_dir}")
    server.serve_forever()


if __name__ == "__main__":
    main()
