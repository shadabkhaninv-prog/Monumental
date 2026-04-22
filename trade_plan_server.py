from __future__ import annotations

import argparse
import copy
import json
import re
from datetime import date, datetime
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

DEFAULT_HTML_PATH = Path(r"C:\Users\shada\Downloads\trade_plan_1.html")


def normalize_symbol(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").strip().upper())


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
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self._year_tables: Optional[List[int]] = None
        self._symbol_catalog: Optional[List[str]] = None
        self._symbol_set: set[str] = set()
        self._inactive_map: Dict[str, str] = {}
        self._company_names: Dict[str, str] = {}
        self._load_reference_data()

    def _load_reference_data(self) -> None:
        self._inactive_map = self._load_inactive_map()
        symbols = set(self._inactive_map.keys()) | {v for v in self._inactive_map.values() if v}
        self._company_names = {}

        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT UPPER(SYMBOL), COMPANY_NAME FROM nse_symbols")
            for symbol, company_name in cursor.fetchall():
                normalized = normalize_symbol(symbol)
                if not normalized:
                    continue
                symbols.add(normalized)
                if company_name:
                    self._company_names[normalized] = str(company_name)
        finally:
            cursor.close()

        latest_years = self.available_year_tables()[:3]
        cursor = self.conn.cursor()
        try:
            for year in latest_years:
                cursor.execute(f"SELECT DISTINCT UPPER(SYMBOL) FROM bhav{year}")
                for (symbol,) in cursor.fetchall():
                    normalized = normalize_symbol(symbol)
                    if normalized:
                        symbols.add(normalized)
        finally:
            cursor.close()

        self._symbol_catalog = sorted(symbols)
        self._symbol_set = set(self._symbol_catalog)

    def _load_inactive_map(self) -> Dict[str, str]:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                SELECT UPPER(symbol) AS symbol, UPPER(TRIM(new_symbol)) AS new_symbol
                FROM inactive_symbols
                WHERE new_symbol IS NOT NULL
                  AND TRIM(new_symbol) <> ''
                """
            )
            result: Dict[str, str] = {}
            for old_symbol, new_symbol in cursor.fetchall():
                old_norm = normalize_symbol(old_symbol)
                new_norm = normalize_symbol(new_symbol)
                if old_norm and new_norm:
                    result[old_norm] = new_norm
            return result
        finally:
            cursor.close()

    def available_year_tables(self) -> List[int]:
        if self._year_tables is None:
            cursor = self.conn.cursor()
            try:
                cursor.execute("SHOW TABLES LIKE 'bhav____'")
                years: List[int] = []
                for (table_name,) in cursor.fetchall():
                    suffix = table_name[4:]
                    if suffix.isdigit():
                        years.append(int(suffix))
                self._year_tables = sorted(years, reverse=True)
            finally:
                cursor.close()
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
        cursor = self.conn.cursor()
        try:
            for year in candidate_years:
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
                row = cursor.fetchone()
                if row:
                    actual_symbol, close_price, price_date = row
                    return {
                        "symbol": normalize_symbol(actual_symbol),
                        "cmp": float(close_price) if close_price is not None else None,
                        "price_date": price_date.isoformat(),
                        "table": f"bhav{year}",
                    }
        finally:
            cursor.close()
        return None

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
        self.save_dir = self.html_path.with_name(f"{self.html_path.stem}_data")
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.base_dir = Path(__file__).resolve().parent
        self.settings_path = self.save_dir / "settings.json"

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
            "checklist_groups": groups,
            **legacy,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        self.settings_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {"ok": True, "path": str(self.settings_path), **payload}

    def _remaining_qty(self, position: Dict[str, object]) -> Optional[int]:
        actual_qty = self._total_qty(position)
        if actual_qty is None:
            return None
        sold = 0
        for trim in position.get("trims", []):
            if trim.get("done") and trim.get("sq"):
                sold += int(trim.get("sq") or 0)
        return max(0, int(actual_qty) - sold)

    def _total_qty(self, position: Dict[str, object]) -> Optional[float]:
        overnight_qty = self._as_float(position.get("overnightQty"))
        actual_qty = self._as_float(position.get("actualQty"))
        if overnight_qty <= 0 and actual_qty <= 0:
            return None
        return overnight_qty + actual_qty

    def _entry_value(self, position: Dict[str, object]) -> Optional[object]:
        overnight_qty = self._as_float(position.get("overnightQty"))
        overnight_entry = self._as_float(position.get("overnightEntry"))
        actual_qty = self._as_float(position.get("actualQty"))
        actual_entry = self._as_float(position.get("actualEntry"))

        legs: List[Tuple[float, float]] = []
        if overnight_qty > 0 and overnight_entry > 0:
            legs.append((overnight_qty, overnight_entry))
        if actual_qty > 0 and actual_entry > 0:
            legs.append((actual_qty, actual_entry))

        if len(legs) == 2:
            total_qty = legs[0][0] + legs[1][0]
            if total_qty > 0:
                weighted = ((legs[0][0] * legs[0][1]) + (legs[1][0] * legs[1][1])) / total_qty
                return round(weighted, 2)
        if len(legs) == 1:
            return round(legs[0][1], 2)

        if actual_entry > 0:
            return round(actual_entry, 2)
        if overnight_entry > 0:
            return round(overnight_entry, 2)
        return None

    def _is_meaningful_position(self, position: Dict[str, object]) -> bool:
        if self._entry_value(position) is not None or position.get("actualQty") is not None:
            return True

        scalar_fields = [
            "symbol",
            "merits",
            "planEntry",
                "planSL",
                "intraSL",
                "riskAmount",
                "entryDate",
                "overnightEntry",
                "overnightQty",
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

    def _actual_deployed_risk(self, position: Dict[str, object]) -> float:
        plan_sl = self._as_float(position.get("planSL"))
        intra_sl = self._as_float(position.get("intraSL")) or plan_sl
        total_qty = self._total_qty(position)
        entry_value = self._entry_value(position)
        if not (total_qty and total_qty > 0 and entry_value and entry_value > 0 and plan_sl > 0):
            return 0.0

        intra_qty = round(total_qty * 0.30)
        core_qty = max(0.0, total_qty - intra_qty)
        risk = max(0.0, (entry_value - plan_sl) * core_qty)
        if intra_sl > 0:
            risk += max(0.0, (entry_value - intra_sl) * intra_qty)

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
        planning_for_day: List[Dict[str, object]] = []
        exited_today_positions: List[Dict[str, object]] = []

        for item_date in dates:
            payload = self.load_plan(item_date)
            for raw_position in payload.get("positions", []):
                position = copy.deepcopy(raw_position)
                if self._entry_value(position) is not None:
                    if not position.get("entryDate"):
                        position["entryDate"] = item_date
                    key = self._live_position_key(position, item_date)
                    is_closed = self._remaining_qty(position) == 0 or position.get("_status") == "closed"
                    if item_date == plan_date and is_closed:
                        open_positions.pop(key, None)
                        exited_today_positions.append(position)
                        continue
                    if is_closed:
                        open_positions.pop(key, None)
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

        return {
            "ok": True,
            "date": plan_date,
            "positions": day_positions,
            "raw_path": str(self.plan_path(plan_date)),
            "open_positions_count": len(carried_positions),
            "exited_today_count": len(exited_today_positions),
            "planning_count": len(planning_for_day),
            "exposure": round(exposure, 2),
            "exposure_pct": round(exposure_pct, 2) if exposure_pct is not None else None,
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
        path = self.plan_path(plan_date)
        if not path.exists():
            return {"date": plan_date, "positions": [], "path": str(path), "exists": False}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {"positions": []}
        payload["date"] = plan_date
        payload["path"] = str(path)
        payload["exists"] = True
        return payload

    def save_plan(self, plan_date: str, positions: Sequence[dict]) -> Dict[str, object]:
        path = self.plan_path(plan_date)
        cleaned_positions = [position for position in positions if self._is_meaningful_position(position)]
        payload = {
            "date": plan_date,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "positions": cleaned_positions,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {"ok": True, "date": plan_date, "path": str(path), "saved_at": payload["saved_at"]}


class TradePlanHandler(BaseHTTPRequestHandler):
    repo: BhavRepository
    store: TradePlanStore

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
                }
            )

        if parsed.path == "/api/settings":
            return self._send_json({"ok": True, **self.store.load_settings(), "path": str(self.store.settings_path)})

        if parsed.path == "/api/dashboard":
            return self._send_json(self.store.build_dashboard(self.repo))

        if parsed.path == "/api/goal-tracker":
            return self._send_json(self.store.build_goal_tracker(self.repo))

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
        if parsed.path not in {"/api/plan", "/api/settings"}:
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
