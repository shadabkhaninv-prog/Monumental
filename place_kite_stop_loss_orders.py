#!/usr/bin/env python3
"""
Place Kite stop-loss limit sell orders for saved trade-plan positions.

By default this runs in dry-run mode and only prints the orders it would place.
Pass --place to submit live orders.

Selection rule
--------------
Only positions with a real trailing stop set are eligible:
  - trailOverride must be present and > 0
  - the position must not be closed
  - remaining quantity must be > 0

Example
-------
python place_kite_stop_loss_orders.py --date 2026-04-24 --place
"""

from __future__ import annotations

import argparse
import os
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TOKEN_FILE = BASE_DIR / "kite_token.txt"
DEFAULT_SAVE_DIR = Path.home() / "Downloads" / "trade_plan_1_data"
DEFAULT_TAG_PREFIX = "TP-SL"
DEFAULT_STATE_FILE = DEFAULT_SAVE_DIR / ".kite_stop_loss_orders_state.json"


def read_kite_token_file(token_file: Path) -> Dict[str, str]:
    if not token_file.exists():
        raise SystemExit(f"Missing token file: {token_file}")

    values: Dict[str, str] = {}
    for raw_line in token_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip().upper()] = value.strip()

    if "API_KEY" not in values or "ACCESS_TOKEN" not in values:
        raise SystemExit(f"{token_file} must contain API_KEY and ACCESS_TOKEN.")
    return values


def get_kite_client(token_file: Path):
    try:
        from kiteconnect import KiteConnect
    except ImportError as exc:
        raise SystemExit("kiteconnect is not installed. Install it first.") from exc

    creds = read_kite_token_file(token_file)
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(key, None)
    kite = KiteConnect(api_key=creds["API_KEY"])
    kite.set_access_token(creds["ACCESS_TOKEN"])
    session = getattr(kite, "session", None)
    if session is not None:
        try:
            session.trust_env = False
        except Exception:
            pass
        try:
            session.proxies = {}
        except Exception:
            pass
    return kite


def friendly_kite_error(exc: Exception) -> str:
    msg = str(exc) or exc.__class__.__name__
    lower = msg.lower()
    if "no ips configured" in lower or "allowed ips" in lower or "ip" in lower and "configured" in lower:
        return (
            "Kite blocked the order because no allowed IPs are configured for this app. "
            "Add your current public IP in the Kite developer console, then retry."
        )
    return msg


def parse_plan_date_from_name(path: Path) -> Optional[date]:
    try:
        return datetime.strptime(path.stem, "%Y-%m-%d").date()
    except ValueError:
        return None


def load_latest_plan_path(save_dir: Path, plan_date: Optional[str] = None) -> Path:
    if plan_date:
        candidate = save_dir / f"{plan_date}.json"
        if not candidate.exists():
            raise SystemExit(f"Plan file not found: {candidate}")
        return candidate

    plan_files = sorted(
        (p for p in save_dir.glob("*.json") if parse_plan_date_from_name(p) is not None),
        key=lambda p: parse_plan_date_from_name(p) or date.min,
    )
    if not plan_files:
        raise SystemExit(f"No dated plan files found in {save_dir}")
    return plan_files[-1]


def as_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def total_qty(position: Dict[str, object]) -> int:
    overnight_qty = int(as_float(position.get("overnightQty")))
    actual_qty = int(as_float(position.get("actualQty")))
    return overnight_qty + actual_qty


def remaining_qty(position: Dict[str, object]) -> int:
    rem = position.get("_rem")
    if rem is not None:
        try:
            return max(0, int(float(rem)))
        except (TypeError, ValueError):
            pass

    qty = total_qty(position)
    if qty <= 0:
        return 0

    sold = 0
    for trim in position.get("trims", []) or []:
        if not isinstance(trim, dict):
            continue
        if trim.get("done") and trim.get("sq") is not None:
            try:
                sold += int(float(trim.get("sq")))
            except (TypeError, ValueError):
                continue
    return max(0, qty - sold)


def trailing_stop_value(position: Dict[str, object]) -> Optional[float]:
    raw = position.get("trailOverride")
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def infer_exchange(symbol: str) -> str:
    if ":" in symbol:
        prefix, _ = symbol.split(":", 1)
        if prefix.strip():
            return prefix.strip().upper()
    return "NSE"


def infer_tradingsymbol(symbol: str) -> str:
    symbol = (symbol or "").strip().upper()
    if ":" in symbol:
        symbol = symbol.split(":", 1)[1]
    return symbol


def round_to_tick(value: float, tick: float) -> float:
    tick = tick if tick and tick > 0 else 0.05
    return round(round(value / tick) * tick, 2)


def stop_limit_prices(trigger_price: float, tick: float) -> Tuple[float, float]:
    trigger = round_to_tick(trigger_price, tick)
    limit = round_to_tick(max(trigger - tick, tick), tick)
    if limit >= trigger:
        limit = round_to_tick(max(trigger - tick, tick), tick)
    return limit, trigger


def load_plan_positions(plan_path: Path) -> List[Dict[str, object]]:
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return []
    positions = payload.get("positions", [])
    return positions if isinstance(positions, list) else []


def load_state(state_file: Path) -> Dict[str, Any]:
    if not state_file.exists():
        return {}
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_state(state_file: Path, payload: Dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def get_orders_with_tag(kite, tag: str) -> List[Dict[str, object]]:
    try:
        orders = kite.orders()
    except Exception:
        return []

    matches: List[Dict[str, object]] = []
    for order in orders or []:
        if str(order.get("tag") or "") == tag:
            matches.append(order)
    return matches


def filter_positions_for_sl_orders(positions: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    selected: List[Dict[str, object]] = []
    for position in positions:
        if not isinstance(position, dict):
            continue
        symbol = str(position.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        trail = trailing_stop_value(position)
        if trail is None:
            continue
        status = str(position.get("_status") or "").strip().lower()
        if status == "closed":
            continue
        rem = remaining_qty(position)
        if rem <= 0:
            continue
        position = dict(position)
        position["_sl_trigger"] = trail
        position["_remaining_qty"] = rem
        selected.append(position)
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description="Place Kite stop-loss limit orders from saved trade-plan snapshots.")
    parser.add_argument("--date", default="", help="Plan date to read (YYYY-MM-DD). Default: latest dated plan.")
    parser.add_argument("--save-dir", type=Path, default=DEFAULT_SAVE_DIR, help="Directory containing dated plan JSON files.")
    parser.add_argument("--token-file", type=Path, default=DEFAULT_TOKEN_FILE, help="Path to kite_token.txt.")
    parser.add_argument("--tag-prefix", default=DEFAULT_TAG_PREFIX, help="Order tag prefix used for duplicate detection.")
    parser.add_argument("--tick-size", type=float, default=0.05, help="Price tick size used to derive the limit price.")
    parser.add_argument("--product", default="CNC", choices=["CNC", "MIS"], help="Kite product for the stop-loss order.")
    parser.add_argument("--place", action="store_true", help="Actually submit the orders. Without this flag the script only previews.")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE, help="Optional JSON file used to remember submitted orders.")
    args = parser.parse_args()

    save_dir = args.save_dir.expanduser().resolve()
    plan_path = load_latest_plan_path(save_dir, args.date.strip() or None)
    plan_date = parse_plan_date_from_name(plan_path)
    if plan_date is None:
        raise SystemExit(f"Invalid dated plan file: {plan_path.name}")

    positions = load_plan_positions(plan_path)
    selected = filter_positions_for_sl_orders(positions)
    if not selected:
        print(f"No eligible trailing-stop positions found in {plan_path.name}.")
        return 0

    kite = get_kite_client(args.token_file)
    state_file = args.state_file.expanduser().resolve()
    state = load_state(state_file)
    remembered = state.setdefault("orders", {})

    print(f"Plan file : {plan_path}")
    print(f"Mode      : {'LIVE PLACE' if args.place else 'DRY RUN'}")
    print(f"Eligible  : {len(selected)} position(s)")
    print(f"State     : {state_file}")
    print()

    placed = 0
    skipped = 0
    state_changed = False
    for position in selected:
        symbol = infer_tradingsymbol(str(position.get("symbol") or ""))
        exchange = infer_exchange(str(position.get("symbol") or ""))
        rem = int(position["_remaining_qty"])
        trigger = float(position["_sl_trigger"])
        limit_price, trigger_price = stop_limit_prices(trigger, args.tick_size)
        tag = f"{args.tag_prefix}-{plan_date.isoformat()}-{symbol}"

        if tag in remembered:
            record = remembered[tag]
            order_id = record.get("order_id", "")
            print(f"SKIP  {symbol:<12} already processed for tag {tag} order_id={order_id}")
            skipped += 1
            continue

        existing_orders = get_orders_with_tag(kite, tag)
        if existing_orders:
            last_order = existing_orders[-1]
            status = str(last_order.get("status") or "").strip()
            order_id = str(last_order.get("order_id") or "")
            print(f"SKIP  {symbol:<12} existing Kite order tag={tag} status={status} order_id={order_id}")
            remembered[tag] = {
                "order_id": order_id,
                "status": status,
                "symbol": symbol,
                "plan_date": plan_date.isoformat(),
                "quantity": rem,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
            }
            state_changed = True
            skipped += 1
            continue

        print(
            f"{'PLACE' if args.place else 'PREV '}  {symbol:<12} "
            f"qty={rem:<4} trigger={trigger_price:.2f} limit={limit_price:.2f} "
            f"exchange={exchange} product={args.product}"
        )

        if not args.place:
            continue

        try:
            order_id = kite.place_order(
                variety="regular",
                exchange=exchange,
                tradingsymbol=symbol,
                transaction_type="SELL",
                quantity=rem,
                product=args.product,
                order_type="SL",
                validity="DAY",
                price=limit_price,
                trigger_price=trigger_price,
                tag=tag,
            )
            print(f"  -> order_id={order_id}")
            remembered[tag] = {
                "order_id": order_id,
                "status": "submitted",
                "symbol": symbol,
                "plan_date": plan_date.isoformat(),
                "quantity": rem,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
            }
            state_changed = True
            placed += 1
        except Exception as exc:
            print(f"  !! failed: {friendly_kite_error(exc)}")

    if args.place and state_changed:
        state["last_run"] = {
            "plan_file": str(plan_path),
            "plan_date": plan_date.isoformat(),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        save_state(state_file, state)

    print()
    print(f"Placed: {placed}")
    print(f"Skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
