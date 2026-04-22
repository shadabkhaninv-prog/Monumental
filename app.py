"""
NSE BHAV Stock Viewer — standalone Flask app
Queries: bhav2024, bhav2025, bhav2026, mktdatecalendar  in localhost/bhav

Run:  python app.py
Open: http://localhost:5000
"""

from __future__ import annotations
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from flask import Flask, request, jsonify, render_template_string

import mysql.connector

# ── DB config (mirrors stock_rating.py) ─────────────────────────────────────
DB_CONFIG = dict(host="localhost", port=3306, user="root",
                 password="root", database="bhav")

app = Flask(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def to_date(v):
    """Coerce a value to datetime.date — handles both date objects and ISO strings."""
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return date.fromisoformat(v[:10])   # safe even with datetime strings
    return v


def safe(v):
    """Make a value JSON-serialisable (handle NaN / None / date)."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (date,)):
        return v.isoformat()
    return v


def get_latest_bhav_date(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(mktdate) AS d FROM mktdatecalendar")
    row = cursor.fetchone()
    cursor.close()
    return to_date(row[0]) if row else None


def fetch_bhav_archive_rows(conn, symbol, from_date, to_date):
    """Fetch BHAV rows for one symbol across the yearly bhav tables."""
    select_block = """
        SELECT
            mktdate, symbol, VOLATILITY,
            open, high, low, close, prevclose,
            volume, deliveryvolume, closeindictor,
            ROUND(((close - prevclose) / prevclose) * 100, 2) AS diff,
            ROUND(((high  - close)    / close)     * 100, 2) AS jag,
            ROUND((deliveryvolume     / volume)    * 100, 2) AS delper,
            `20DMA`, `10dma`, `50dma`, `5dma`
        FROM {table}
        WHERE UPPER(symbol) = %s
          AND mktdate >= %s
          AND mktdate <= %s
    """

    years_needed = list(range(from_date.year, to_date.year + 1))
    union_parts = [select_block.format(table=f"bhav{y}") for y in years_needed]
    archive_sql = "\n\nUNION ALL\n\n".join(union_parts) + "\n\nORDER BY mktdate DESC"
    params = []
    for _ in years_needed:
        params += [symbol, from_date, to_date]

    cursor = conn.cursor(dictionary=True)
    cursor.execute(archive_sql, params)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def avg_turnover_21d(rows):
    """Average 21-trading-day turnover using volume * close."""
    window = rows[-21:] if len(rows) >= 21 else rows
    turnovers = []
    for r in window:
        volume = r.get("volume")
        close = r.get("close")
        if volume is None or close is None:
            continue
        try:
            turnovers.append(float(volume) * float(close))
        except Exception:
            continue
    if not turnovers:
        return None
    return sum(turnovers) / len(turnovers)


def fetch_corporate_actions_map(conn, symbols):
    """Fetch split/bonus actions for the requested symbols."""
    symbol_list = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not symbol_list:
        return {}

    placeholders = ",".join(["%s"] * len(symbol_list))
    sql = f"""
        SELECT UPPER(Symbol) AS symbol, CompanyName, ExDate, A, B, Ratio, 'split' AS action_type
        FROM splits
        WHERE UPPER(Symbol) IN ({placeholders})
        UNION ALL
        SELECT UPPER(Symbol) AS symbol, CompanyName, ExDate, A, B, Ratio, 'bonus' AS action_type
        FROM bonus
        WHERE UPPER(Symbol) IN ({placeholders})
        ORDER BY symbol, STR_TO_DATE(ExDate, '%Y-%m-%d')
    """
    params = symbol_list + symbol_list
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, params)
    action_map = {}
    for row in cursor.fetchall():
        sym = row["symbol"]
        try:
            ratio = float(row.get("Ratio"))
        except Exception:
            ratio = None
        if not ratio or ratio <= 0:
            continue
        action_map.setdefault(sym, []).append({
            "symbol": sym,
            "company_name": row.get("CompanyName"),
            "exdate": to_date(row.get("ExDate")),
            "a": row.get("A"),
            "b": row.get("B"),
            "ratio": ratio,
            "action_type": row.get("action_type"),
        })
    cursor.close()
    return action_map


def apply_corporate_actions(ordered_rows, actions):
    """
    Back-adjust OHLC/EMA/volume using explicit split/bonus rows from the DB.
    """
    if not ordered_rows:
        return []
    if not actions:
        return ordered_rows

    normalized_actions = []
    for action in actions:
        exdate = to_date(action.get("exdate"))
        ratio = action.get("ratio")
        if exdate is None or ratio in (None, 0):
            continue
        try:
            ratio = float(ratio)
        except Exception:
            continue
        if ratio <= 0:
            continue
        normalized_actions.append({**action, "exdate": exdate, "ratio": ratio})
    normalized_actions.sort(key=lambda a: a["exdate"])
    if not normalized_actions:
        return ordered_rows

    adjusted_rows = []
    for row in ordered_rows:
        row_date = to_date(row.get("mktdate"))
        if row_date is None:
            continue
        factor = 1.0
        for action in normalized_actions:
            if action["exdate"] > row_date:
                factor *= 1.0 / action["ratio"]

        adj = dict(row)
        adj["_adj_factor"] = factor
        for field in ("open", "high", "low", "close", "5dma", "10dma", "20DMA", "50dma"):
            val = row.get(field)
            adj[f"_adj_{field}"] = safe(val * factor) if val is not None else None
        volume = row.get("volume")
        adj["_adj_volume"] = safe(volume / factor) if volume is not None and factor not in (None, 0) else safe(volume)
        adjusted_rows.append(adj)

    return adjusted_rows


def build_sector_chart_card(sym, sector, chart_from, latest_date, min_avg_turnover, corporate_actions=None):
    """Build one chart payload for a symbol using a dedicated connection."""
    conn = None
    try:
        conn = get_conn()
        rows = fetch_bhav_archive_rows(conn, sym, chart_from, latest_date)
        if not rows:
            return None

        turnover_21d = avg_turnover_21d(rows)
        if turnover_21d is None or turnover_21d < min_avg_turnover:
            return None

        ordered = sorted(rows, key=lambda r: r["mktdate"] or date.min)
        adjusted_rows = apply_corporate_actions(ordered, corporate_actions or [])
        candles = []
        volumes = []
        ema5 = []
        ema10 = []
        ema20 = []
        ema50 = []
        prior_close = None
        for r in adjusted_rows:
            d = to_date(r["mktdate"])
            if not d:
                continue
            iso = d.isoformat()
            close_val = safe(r.get("_adj_close"))
            prev_close = safe(r.get("_adj_prev_close"))
            if prev_close in (None, 0):
                prev_close = prior_close
            change_pct = None
            if close_val is not None and prev_close not in (None, 0):
                try:
                    change_pct = round(((float(close_val) - float(prev_close)) / float(prev_close)) * 100.0, 2)
                except Exception:
                    change_pct = None
            if close_val is not None:
                prior_close = close_val
            candles.append({
                "time": iso,
                "open": safe(r.get("_adj_open")),
                "high": safe(r.get("_adj_high")),
                "low": safe(r.get("_adj_low")),
                "close": close_val,
                "volume": safe(r.get("_adj_volume")),
                "change_pct": safe(change_pct),
                "adj_factor": safe(r.get("_adj_factor")),
            })
            volumes.append({
                "time": iso,
                "value": safe(r.get("_adj_volume")),
                "color": "#58b65b" if (close_val is not None and prev_close not in (None, 0) and close_val >= prev_close) else "#ef6a6a",
            })
            for series, key in [
                (ema5, "5dma"),
                (ema10, "10dma"),
                (ema20, "20DMA"),
                (ema50, "50dma"),
            ]:
                val = safe(r.get(f"_adj_{key}"))
                if val is not None:
                    series.append({"time": iso, "value": val})

        latest = ordered[-1]
        prev_close = latest.get("prevclose")
        latest_close = latest.get("close")
        move_pct = None
        if latest_close is not None and prev_close not in (None, 0):
            try:
                move_pct = round(((float(latest_close) - float(prev_close)) / float(prev_close)) * 100.0, 2)
            except Exception:
                move_pct = None

        return {
            "symbol": sym,
            "sector": sector,
            "has_data": bool(candles),
            "latest_date": safe(to_date(latest.get("mktdate"))),
            "end_close": safe(latest_close),
            "move_pct": safe(move_pct),
            "avg_turnover_21d": safe(round(turnover_21d, 2)),
            "candles": candles,
            "volume": volumes,
            "ema5": ema5,
            "ema10": ema10,
            "ema20": ema20,
            "ema50": ema50,
        }
    finally:
        if conn is not None:
            conn.close()


# ── routes ───────────────────────────────────────────────────────────────────

def render_page(page_mode: str = "stocks"):
    return render_template_string(HTML_PAGE, page_mode=page_mode)


@app.route("/")
def index():
    return render_page("stocks")


@app.route("/sectors")
def sectors_page():
    return render_page("sectors")


@app.route("/api/symbols")
def api_symbols():
    """Return all distinct symbols for autocomplete (union across year tables)."""
    q = request.args.get("q", "").strip().upper()
    try:
        conn = get_conn()
        cursor = conn.cursor()
        like = f"%{q}%" if q else "%"
        sql = """
            SELECT DISTINCT UPPER(symbol) AS sym FROM (
                SELECT symbol FROM bhav2024 WHERE UPPER(symbol) LIKE %s
                UNION
                SELECT symbol FROM bhav2025 WHERE UPPER(symbol) LIKE %s
                UNION
                SELECT symbol FROM bhav2026 WHERE UPPER(symbol) LIKE %s
            ) t ORDER BY sym LIMIT 50
        """
        cursor.execute(sql, (like, like, like))
        rows = [r[0] for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sectors")
def api_sectors():
    """Return all distinct sector names from sector1/sector2/sector3."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT sector_name
            FROM (
                SELECT sector1 AS sector_name FROM sectors WHERE sector1 IS NOT NULL AND sector1 <> ''
                UNION
                SELECT sector2 AS sector_name FROM sectors WHERE sector2 IS NOT NULL AND sector2 <> ''
                UNION
                SELECT sector3 AS sector_name FROM sectors WHERE sector3 IS NOT NULL AND sector3 <> ''
            ) s
            ORDER BY sector_name
        """)
        rows = [r[0] for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/top-sectors")
def api_top_sectors():
    """Return sectors with the most stocks across sector1/sector2/sector3."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sector_name, COUNT(DISTINCT symbol) AS stock_count
            FROM (
                SELECT symbol, UPPER(sector1) AS sector_name FROM sectors
                WHERE sector1 IS NOT NULL AND sector1 <> ''
                UNION ALL
                SELECT symbol, UPPER(sector2) AS sector_name FROM sectors
                WHERE sector2 IS NOT NULL AND sector2 <> ''
                UNION ALL
                SELECT symbol, UPPER(sector3) AS sector_name FROM sectors
                WHERE sector3 IS NOT NULL AND sector3 <> ''
            ) s
            WHERE sector_name IS NOT NULL AND sector_name <> ''
            GROUP BY sector_name
            ORDER BY stock_count DESC, sector_name
            LIMIT 15
        """)
        rows = [{"sector": r[0], "count": int(r[1])} for r in cursor.fetchall()]
        cursor.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sector-stocks")
def api_sector_stocks():
    """Return all stocks mapped to the requested sector."""
    sector = request.args.get("sector", "").strip().upper()
    if not sector:
        return jsonify({"error": "sector required"}), 400

    try:
        conn = get_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT symbol, sector1, sector2, sector3,
                   CASE WHEN UPPER(COALESCE(sector1, '')) = %s THEN 0 ELSE 1 END AS primary_rank
            FROM sectors
            WHERE UPPER(COALESCE(sector1, '')) = %s
               OR UPPER(COALESCE(sector2, '')) = %s
               OR UPPER(COALESCE(sector3, '')) = %s
            ORDER BY primary_rank, symbol
        """, (sector, sector, sector, sector))
        rows = cursor.fetchall()
        cursor.close(); conn.close()
        return jsonify({
            "sector": sector,
            "count": len(rows),
            "stocks": rows,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sector-charts")
def api_sector_charts():
    """Return a small chart board for the selected sector."""
    sector = request.args.get("sector", "").strip().upper()
    if not sector:
        return jsonify({"error": "sector required"}), 400

    min_avg_turnover = 10_000_000 * 10  # 10 crore

    try:
        limit = int(request.args.get("limit", 0))
    except ValueError:
        limit = 0
    limit = max(0, limit)

    try:
        conn = get_conn()
        latest_date = get_latest_bhav_date(conn)
        if latest_date is None:
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT UPPER(symbol) AS sym,
                   CASE WHEN UPPER(COALESCE(sector1, '')) = %s THEN 0 ELSE 1 END AS primary_rank
            FROM sectors
            WHERE UPPER(COALESCE(sector1, '')) = %s
               OR UPPER(COALESCE(sector2, '')) = %s
               OR UPPER(COALESCE(sector3, '')) = %s
            ORDER BY primary_rank, sym
        """, (sector, sector, sector, sector))
        symbols = [r[0] for r in cursor.fetchall()]
        cursor.close()

        if not symbols:
            conn.close()
            return jsonify({
                "sector": sector,
                "count": 0,
                "charts": [],
            })

        from datetime import timedelta
        chart_from = max(latest_date - timedelta(days=180), date(2000, 1, 1))
        if limit:
            symbols = symbols[:limit]

        action_map = fetch_corporate_actions_map(conn, symbols)

        charts = []
        max_workers = min(16, len(symbols)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(build_sector_chart_card, sym, sector, chart_from, latest_date, min_avg_turnover, action_map.get(sym, [])): i
                for i, sym in enumerate(symbols)
            }
            collected = []
            for fut in as_completed(futures):
                idx = futures[fut]
                card = fut.result()
                if card is not None:
                    collected.append((idx, card))

        charts = [card for _, card in sorted(collected, key=lambda item: item[0])]

        conn.close()
        return jsonify({
            "sector": sector,
            "count": len(symbols),
            "charts": charts,
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/stock")
def api_stock():
    """
    Replicates the full stockquery.sql logic for a given symbol.
    Optional query params: from_date, to_date (YYYY-MM-DD).
    Defaults: to_date = max(mktdate), from_date = to_date - 100 calendar days.
    Returns:
      - rows        : archiverollingperiod data (OHLCV + computed cols)
      - minvol      : {63d, 21d} min volatility
      - lowvolume   : {63d, 21d} min volume
      - yesterday   : reference date used (= to_date)
      - start_21d   : 21-trading-day lookback start
      - start_63d   : 63-trading-day lookback start
    """
    from datetime import timedelta
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol required"}), 400

    try:
        conn = get_conn()
        cursor = conn.cursor(dictionary=True)

        # ── 1. max(mktdate) → @yesterday (upper bound / "to" date) ──────────
        cursor.execute("SELECT MAX(mktdate) AS d FROM mktdatecalendar")
        row = cursor.fetchone()
        db_max = to_date(row["d"])
        if db_max is None:
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        # ── resolve to_date / from_date from request params ──────────────────
        to_date_param  = request.args.get("to_date",   "").strip()
        from_date_param = request.args.get("from_date", "").strip()

        yesterday = to_date(to_date_param)   if to_date_param   else db_max
        from_date = to_date(from_date_param) if from_date_param else (yesterday - timedelta(days=100))

        # ── 2. 21-trading-day lookback start ─────────────────────────────────
        cursor.execute("""
            SELECT mktdate FROM (
                SELECT DISTINCT mktdate FROM mktdatecalendar
                WHERE mktdate <= %s ORDER BY mktdate DESC LIMIT 21
            ) m ORDER BY mktdate ASC LIMIT 1
        """, (yesterday,))
        row21 = cursor.fetchone()
        start_21d = to_date(row21["mktdate"]) if row21 else yesterday

        # ── 3. 63-trading-day lookback start ─────────────────────────────────
        cursor.execute("""
            SELECT mktdate FROM (
                SELECT DISTINCT mktdate FROM mktdatecalendar
                WHERE mktdate <= %s ORDER BY mktdate DESC LIMIT 63
            ) m ORDER BY mktdate ASC LIMIT 1
        """, (yesterday,))
        row63 = cursor.fetchone()
        start_63d = to_date(row63["mktdate"]) if row63 else yesterday

        # ── 4. Build archiverollingperiod equivalent ──────────────────────────
        #   Only query the bhavYYYY tables that overlap with [from_date, yesterday].
        select_block = """
            SELECT
                mktdate, symbol, VOLATILITY,
                open, high, low, close, prevclose,
                volume, deliveryvolume, closeindictor,
                ROUND(((close - prevclose) / prevclose) * 100, 2) AS diff,
                ROUND(((high  - close)    / close)     * 100, 2) AS jag,
                ROUND((deliveryvolume     / volume)    * 100, 2) AS delper,
                `20DMA`, `10dma`, `50dma`, `5dma`
            FROM {table}
            WHERE UPPER(symbol) = %s
              AND mktdate >= %s
              AND mktdate <= %s"""

        years_needed = list(range(from_date.year, yesterday.year + 1))
        union_parts  = [select_block.format(table=f"bhav{y}") for y in years_needed]
        archive_sql  = "\n\nUNION ALL\n\n".join(union_parts) + "\n\nORDER BY mktdate DESC"

        params = []
        for _ in years_needed:
            params += [symbol, from_date, yesterday]

        cursor.execute(archive_sql, params)
        archive_rows = cursor.fetchall()

        if not archive_rows:
            cursor.close(); conn.close()
            return jsonify({"error": f"No data found for symbol '{symbol}'"}), 404

        # ── 5. minvol63: MIN(VOLATILITY) over 63-day window (excl. yesterday) ─
        #    minvol21: MIN(VOLATILITY) over 21-day window (excl. yesterday)
        #    (Exact same WHERE clauses as stockquery.sql)
        def minvol_from_archive(rows, start_date):
            vals = [r["VOLATILITY"] for r in rows
                    if r["mktdate"] is not None
                    and to_date(r["mktdate"]) >= start_date
                    and to_date(r["mktdate"]) <= yesterday
                    and r["VOLATILITY"] is not None]
            return min(vals) if vals else None

        minvol_63d = minvol_from_archive(archive_rows, start_63d)
        minvol_21d = minvol_from_archive(archive_rows, start_21d)

        # ── 6. lowvolume21/63  (note: names in original SQL are swapped vs dates)
        #    lowvolume21 uses @63daystart, lowvolume63 uses @21daystart
        #    Preserved exactly as written in stockquery.sql
        def minvolume_from_archive(rows, start_date):
            vals = [r["volume"] for r in rows
                    if r["mktdate"] is not None
                    and to_date(r["mktdate"]) >= start_date
                    and to_date(r["mktdate"]) <= yesterday
                    and r["volume"] is not None]
            return min(vals) if vals else None

        # lowvolume21 in original SQL uses @63daystart (preserved as-is)
        lowvol_table21 = minvolume_from_archive(archive_rows, start_63d)
        # lowvolume63 in original SQL uses @21daystart (preserved as-is)
        lowvol_table63 = minvolume_from_archive(archive_rows, start_21d)

        cursor.close(); conn.close()

        # ── 7. Serialise + tag highlight rows ────────────────────────────────
        clean_rows = []
        for r in archive_rows:
            d = {k: safe(v) for k, v in r.items()}
            rd = to_date(r["mktdate"]) if r["mktdate"] else None
            in_21d = (rd is not None
                      and rd >= start_21d
                      and rd <= yesterday)
            d["_hl_lowvol"]  = bool(in_21d and r["volume"]     is not None
                                    and r["volume"]     == lowvol_table63)
            d["_hl_lowvolatility"] = bool(in_21d and r["VOLATILITY"] is not None
                                          and r["VOLATILITY"] == minvol_21d)
            clean_rows.append(d)

        return jsonify({
            "symbol":      symbol,
            "yesterday":   safe(yesterday),
            "from_date":   safe(from_date),
            "start_21d":   safe(start_21d),
            "start_63d":   safe(start_63d),
            "rows":        clean_rows,
            "minvol": {
                "63d": safe(minvol_63d),
                "21d": safe(minvol_21d),
            },
            "lowvolume": {
                "table21_uses_63d_window": safe(lowvol_table21),
                "table63_uses_21d_window": safe(lowvol_table63),
            },
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ── HTML page (single-file, no external dependencies except CDN) ─────────────

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NSE BHAV Viewer</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:      #0f1117;
    --surface: #1a1d27;
    --card:    #21253a;
    --border:  #2e3450;
    --accent:  #4f8ef7;
    --accent2: #38d9a9;
    --warn:    #ffa94d;
    --red:     #ff6b6b;
    --green:   #51cf66;
    --text:    #e8eaf6;
    --muted:   #8892b0;
    --th-bg:   #1c2035;
    --row-alt: #1e2236;
    --shadow:  0 4px 24px rgba(0,0,0,.45);
  }

  /* full-height layout — header fixed, main scrolls */
  html, body { height: 100%; overflow: hidden; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif;
         font-size: 14px; display: flex; flex-direction: column; }

  /* ── header ── */
  header { background: var(--surface); border-bottom: 1px solid var(--border);
           padding: 14px 28px; display: flex; align-items: center; gap: 18px;
           flex-shrink: 0; z-index: 100; box-shadow: var(--shadow); }
  header h1 { font-size: 1.15rem; font-weight: 700; color: var(--accent);
              letter-spacing: .5px; white-space: nowrap; }
  header h1 span { color: var(--accent2); }

  /* ── search area ── */
  .search-wrap { flex: 1; max-width: 440px; position: relative; }
  .search-wrap input {
    width: 100%; padding: 9px 16px; border-radius: 8px;
    border: 1.5px solid var(--border); background: var(--card);
    color: var(--text); font-size: 15px; font-weight: 600; letter-spacing: 1px;
    outline: none; transition: border-color .2s;
  }
  .search-wrap input:focus { border-color: var(--accent); }
  .search-wrap input::placeholder { color: var(--muted); font-weight: 400; letter-spacing: 0; }

  .autocomplete-list {
    position: absolute; top: calc(100% + 4px); left: 0; right: 0;
    background: var(--card); border: 1px solid var(--border); border-radius: 8px;
    max-height: 260px; overflow-y: auto; z-index: 200; display: none;
    box-shadow: var(--shadow);
  }
  .autocomplete-list.open { display: block; }
  .ac-item { padding: 9px 16px; cursor: pointer; font-weight: 600;
             letter-spacing: .5px; transition: background .15s; }
  .ac-item:hover, .ac-item.active { background: var(--border); color: var(--accent); }

  /* ── date range inputs ── */
  .date-range { display: flex; align-items: center; gap: 6px; white-space: nowrap; }
  .date-range label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                      letter-spacing: .6px; }
  .date-range input[type="date"] {
    padding: 7px 10px; border-radius: 8px; border: 1.5px solid var(--border);
    background: var(--card); color: var(--text); font-size: 13px;
    outline: none; transition: border-color .2s; cursor: pointer;
    color-scheme: dark;
  }
  .date-range input[type="date"]:focus { border-color: var(--accent); }
  .date-sep { color: var(--muted); font-size: 12px; }

  /* ── clear dates btn ── */
  #clearDatesBtn {
    padding: 5px 10px; border-radius: 6px; border: 1px solid var(--border);
    background: transparent; color: var(--muted); font-size: 11px; cursor: pointer;
    transition: all .2s; white-space: nowrap;
  }
  #clearDatesBtn:hover { border-color: var(--accent); color: var(--accent); }

  /* ── load btn ── */
  #loadBtn {
    padding: 9px 22px; border-radius: 8px; border: none; cursor: pointer;
    background: var(--accent); color: #fff; font-weight: 700; font-size: 14px;
    transition: opacity .2s; white-space: nowrap;
  }
  #loadBtn:hover { opacity: .85; }
  #loadBtn:disabled { opacity: .4; cursor: default; }

  /* ── status bar ── */
  #statusBar {
    font-size: 12px; color: var(--muted); padding: 6px 28px;
    background: var(--surface); border-bottom: 1px solid var(--border);
    min-height: 28px; transition: color .2s; flex-shrink: 0;
  }
  #statusBar.err { color: var(--red); }
  #statusBar.ok  { color: var(--accent2); }

  /* ── main layout — this is the scroll container ── */
  main { padding: 20px 28px; flex: 1; overflow-y: auto; }

  /* ── summary cards ── */
  .cards { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 22px; }
  .card {
    background: var(--card); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 20px; min-width: 170px; flex: 1 1 170px;
    box-shadow: var(--shadow);
  }
  .card-label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                letter-spacing: .8px; margin-bottom: 6px; }
  .card-value { font-size: 1.45rem; font-weight: 700; color: var(--accent); }
  .card-value.green { color: var(--green); }
  .card-value.warn  { color: var(--warn);  }
  .card-sub { font-size: 11px; color: var(--muted); margin-top: 4px; }

  .top-links { display: flex; gap: 10px; align-items: center; }
  .top-links a {
    text-decoration: none; color: var(--muted); border: 1px solid var(--border);
    background: rgba(0,0,0,.12); padding: 8px 12px; border-radius: 8px;
    font-size: 13px; font-weight: 700; transition: all .15s ease;
  }
  .top-links a:hover { color: var(--text); border-color: var(--accent); }
  .top-links a.active { color: #fff; background: var(--accent); border-color: var(--accent); }

  .header-controls { display: flex; align-items: center; gap: 18px; flex: 1; justify-content: flex-end; }
  .page-sectors #stockControls { display: none; }
  .page-stocks #sectorPage { display: none; }
  .page-sectors .search-wrap,
  .page-sectors .date-range,
  .page-sectors #clearDatesBtn,
  .page-sectors #loadBtn,
  .page-sectors #metaInfo,
  .page-sectors #emptyState,
  .page-sectors #spinner,
  .page-sectors #contentArea { display: none !important; }

  .mode-card {
    background: linear-gradient(180deg, rgba(33,37,58,.95), rgba(26,29,39,.98));
    border: 1px solid var(--border); border-radius: 14px; box-shadow: var(--shadow);
    padding: 18px;
  }
  .mode-grid {
    display: grid; grid-template-columns: minmax(210px, 275px) 1fr;
    gap: 14px; align-items: start;
  }
  @media (max-width: 900px) {
    .mode-grid { grid-template-columns: 1fr; }
  }

  .sector-controls { display: flex; flex-direction: column; gap: 12px; }
  .sector-controls label {
    font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px;
  }
  .sector-controls select {
    width: 100%; padding: 10px 12px; border-radius: 8px; border: 1.5px solid var(--border);
    background: var(--card); color: var(--text); font-size: 14px; outline: none;
  }
  .sector-controls select:focus { border-color: var(--accent); }
  .sector-actions { display: flex; gap: 10px; flex-wrap: wrap; }
  .sector-actions button {
    padding: 9px 14px; border-radius: 8px; border: none; cursor: pointer;
    background: var(--accent); color: #fff; font-weight: 700; font-size: 13px;
  }
  .sector-actions button.secondary {
    background: transparent; border: 1px solid var(--border); color: var(--muted);
  }
  .sector-actions button.secondary:hover { color: var(--text); border-color: var(--accent); }

  .sector-shortcuts-wrap {
    margin-top: 12px;
    background: rgba(0,0,0,.10);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 12px;
  }
  .sector-shortcuts-head {
    display: flex; justify-content: space-between; align-items: baseline; gap: 12px;
    margin-bottom: 10px;
  }
  .sector-shortcuts-note { font-size: 12px; color: var(--muted); }
  .sector-shortcuts {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 10px;
  }
  .sector-shortcut {
    display: flex; flex-direction: column; gap: 4px;
    padding: 10px 12px;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--card);
    color: var(--text);
    cursor: pointer;
    text-align: left;
    transition: transform .12s ease, border-color .12s ease, background .12s ease;
  }
  .sector-shortcut:hover {
    transform: translateY(-1px);
    border-color: var(--accent);
    background: rgba(79, 142, 247, 0.08);
  }
  .sector-shortcut.active {
    border-color: var(--accent2);
    background: rgba(56, 217, 169, 0.12);
  }
  .sector-shortcut-name {
    font-size: 13px; font-weight: 800; letter-spacing: .4px; text-transform: uppercase;
  }
  .sector-shortcut-count {
    font-size: 11px; color: var(--muted);
  }

  .sector-list {
    margin-top: 14px; overflow: hidden; border-radius: 10px; border: 1px solid var(--border);
  }
  .sector-row {
    display: grid; grid-template-columns: 160px repeat(3, minmax(0, 1fr));
    gap: 0; border-bottom: 1px solid var(--border); background: var(--card);
  }
  .sector-row:nth-child(even) { background: var(--row-alt); }
  .sector-row.header {
    background: var(--th-bg); font-size: 11px; text-transform: uppercase; letter-spacing: .6px;
    color: var(--muted); font-weight: 700;
  }
  .sector-row > div {
    padding: 10px 12px; border-right: 1px solid var(--border); word-break: break-word;
  }
  .sector-row > div:last-child { border-right: none; }
  .sector-row .symbol { font-weight: 700; color: var(--accent2); }
  .sector-empty {
    padding: 26px; color: var(--muted); text-align: center;
  }
  .sector-spinner {
    width: 34px; height: 34px; border: 3px solid var(--border); border-top-color: var(--accent);
    border-radius: 50%; animation: spin .7s linear infinite; margin: 20px 0; display: none;
  }

  .sector-board {
    margin-top: 18px;
  }
  .sector-board-head {
    display: flex; justify-content: space-between; align-items: baseline; gap: 12px;
    margin-bottom: 10px;
  }
  .sector-board-note {
    font-size: 12px; color: var(--muted);
  }
  .sector-chart-grid {
    display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 14px;
  }
  @media (max-width: 1200px) {
    .sector-chart-grid { grid-template-columns: 1fr; }
  }
  @media (max-width: 760px) {
    .sector-chart-grid { grid-template-columns: 1fr; }
  }
  .sector-chart-card {
    background: var(--card); border: 1px solid var(--border); border-radius: 14px;
    box-shadow: var(--shadow); padding: 12px 12px 10px;
  }
  .sector-chart-head {
    display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;
    margin-bottom: 8px;
  }
  .sector-chart-title {
    font-size: 15px; font-weight: 800; color: var(--text); letter-spacing: .3px;
  }
  .sector-chart-sector {
    font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px;
    margin-top: 2px;
  }
  .sector-chart-meta {
    font-size: 12px; font-weight: 700; color: var(--accent2); white-space: nowrap;
    display: flex; align-items: center; gap: 8px;
  }
  .sector-chart-expand {
    border: 1px solid var(--border); background: transparent; color: var(--muted);
    border-radius: 8px; padding: 4px 8px; font-size: 11px; font-weight: 700;
    cursor: pointer; transition: all .15s ease;
  }
  .sector-chart-expand:hover { color: var(--text); border-color: var(--accent); }
  .sector-zoom-overlay {
    position: fixed; inset: 0; background: rgba(8, 10, 16, 0.88); backdrop-filter: blur(8px);
    z-index: 2000; display: none; padding: 22px;
  }
  .sector-zoom-overlay.open { display: block; }
  .sector-zoom-card {
    height: 100%; max-width: 1400px; margin: 0 auto; background: var(--surface);
    border: 1px solid var(--border); border-radius: 16px; box-shadow: var(--shadow);
    display: flex; flex-direction: column; overflow: hidden;
  }
  .sector-zoom-head {
    display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;
    padding: 14px 16px; border-bottom: 1px solid var(--border); background: rgba(0,0,0,.1);
  }
  .sector-zoom-title {
    font-size: 18px; font-weight: 900; color: var(--text);
  }
  .sector-zoom-subtitle {
    font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px;
    margin-top: 2px;
  }
  .sector-zoom-meta {
    font-size: 13px; font-weight: 800; color: var(--accent2); white-space: nowrap;
  }
  .sector-zoom-nav {
    display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
  }
  .sector-zoom-step {
    border: 1px solid var(--border); background: transparent; color: var(--text);
    border-radius: 10px; padding: 8px 12px; font-weight: 700; cursor: pointer;
  }
  .sector-zoom-step:hover { border-color: var(--accent); }
  .sector-zoom-step:disabled {
    opacity: .45; cursor: not-allowed;
  }
  .sector-zoom-counter {
    font-size: 12px; color: var(--muted); min-width: 84px; text-align: center;
  }
  .sector-zoom-close {
    border: 1px solid var(--border); background: transparent; color: var(--text);
    border-radius: 10px; padding: 8px 12px; font-weight: 700; cursor: pointer;
  }
  .sector-zoom-close:hover { border-color: var(--accent); }
  .sector-zoom-chart {
    flex: 1; min-height: 0; padding: 12px;
  }
  .sector-zoom-chart-inner {
    width: 100%; height: 100%; min-height: 520px; border-radius: 12px; overflow: hidden;
    position: relative;
  }
  .sector-mini-chart {
    width: 100%; height: 260px; border-radius: 10px; overflow: hidden;
    position: relative;
  }
  .sector-chart-hud {
    position: absolute; left: 10px; top: 10px; z-index: 25;
    min-width: 190px; max-width: 260px;
    padding: 4px 6px; border-radius: 6px;
    border: none;
    background: transparent;
    box-shadow: none;
    color: var(--text); font-size: 10px; line-height: 1.2;
    text-shadow: 0 1px 2px rgba(0,0,0,.35);
    pointer-events: none;
  }
  .sector-chart-hud.is-zoom {
    top: 12px; left: 12px;
    min-width: 220px; max-width: 300px;
    font-size: 11px;
    line-height: 1.25;
  }
  .sector-chart-hud .hud-grid {
    display: grid;
    grid-template-columns: repeat(3, max-content);
    gap: 4px 12px;
    align-items: center;
  }
  .sector-chart-hud .hud-item {
    display: inline-flex; align-items: center; gap: 4px;
    white-space: nowrap;
  }
  .sector-chart-hud .hud-key { color: var(--muted); font-weight: 700; }
  .sector-chart-hud .hud-val { font-weight: 800; color: var(--text); }
  .sector-chart-hud.is-zoom .hud-key,
  .sector-chart-hud.is-zoom .hud-val {
    font-size: 11px;
  }
  .sector-chart-hud .hud-pos { color: var(--accent2); }
  .sector-chart-hud .hud-neg { color: #ef6a6a; }
  .sector-chart-hud .hud-empty {
    color: var(--muted); font-size: 10px; letter-spacing: .3px;
  }
  .sector-board-empty {
    padding: 24px; text-align: center; color: var(--muted); border: 1px dashed var(--border);
    border-radius: 12px; background: rgba(0,0,0,.08);
  }

  /* ── section title ── */
  .section-title {
    font-size: 12px; font-weight: 700; color: var(--muted); text-transform: uppercase;
    letter-spacing: 1px; margin-bottom: 10px; border-left: 3px solid var(--accent);
    padding-left: 10px;
  }

  /* ── table container ── */
  .tbl-wrap {
    overflow-x: auto; border-radius: 10px; border: 1px solid var(--border);
    box-shadow: var(--shadow);
  }
  table { border-collapse: collapse; width: 100%; min-width: 1100px; }
  thead tr { background: var(--th-bg); position: sticky; top: 0; z-index: 50; }
  thead th {
    padding: 10px 12px; text-align: right; font-size: 11px; font-weight: 700;
    color: var(--muted); text-transform: uppercase; letter-spacing: .6px;
    border-bottom: 2px solid var(--border); white-space: nowrap; cursor: pointer;
    user-select: none;
  }
  thead th:first-child,
  thead th:nth-child(2) { text-align: left; }
  thead th:hover { color: var(--accent); }
  thead th .sort-arrow { opacity: .4; font-size: 10px; margin-left: 3px; }
  thead th.sorted .sort-arrow { opacity: 1; color: var(--accent); }

  tbody tr { border-bottom: 1px solid var(--border); transition: background .1s; }
  tbody tr:nth-child(even) { background: var(--row-alt); }
  tbody tr:hover { background: var(--border); }
  td { padding: 7px 12px; text-align: right; font-size: 13px; white-space: nowrap; }
  td:first-child { text-align: left; font-weight: 600; color: var(--muted); }
  td:nth-child(2) { text-align: left; font-weight: 700; font-size: 13px; }

  /* colour cells */
  .pos { color: var(--green); font-weight: 600; }
  .neg { color: var(--red);   font-weight: 600; }
  .close-hi { color: var(--accent2); font-weight: 700; }
  .badge {
    display: inline-block; padding: 1px 8px; border-radius: 4px; font-size: 11px;
    font-weight: 700; letter-spacing: .4px;
  }
  .badge-n { background: #1a2e20; color: var(--green); }
  .badge-y { background: #2e1a1a; color: var(--red); }

  /* ── empty / loader ── */
  #emptyState { text-align: center; padding: 80px 20px; color: var(--muted); display: flex;
                flex-direction: column; align-items: center; gap: 14px; }
  #emptyState .big-icon { font-size: 3.5rem; }
  #emptyState p { font-size: 1rem; }
  #emptyState small { font-size: .85rem; }

  .spinner { width: 40px; height: 40px; border: 3px solid var(--border);
             border-top-color: var(--accent); border-radius: 50%;
             animation: spin .7s linear infinite; margin: 60px auto; display: none; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── highlight rows ── */
  tr.hl-lowvol td {
    background: rgba(81, 207, 102, 0.13) !important;
  }
  tr.hl-lowvol td.hl-cell-vol {
    background: rgba(81, 207, 102, 0.45) !important;
    color: #a8f5b8 !important; font-weight: 800;
    box-shadow: inset 0 0 0 1px #51cf66;
  }
  tr.hl-lowvol td:first-child {
    border-left: 4px solid #51cf66;
    color: #51cf66 !important; font-weight: 700;
  }
  tr.hl-lowvol td:first-child::after { content: ' 📦'; font-size: 11px; }

  tr.hl-lowvolatility td {
    background: rgba(79, 142, 247, 0.13) !important;
  }
  tr.hl-lowvolatility td.hl-cell-vol {
    background: rgba(79, 142, 247, 0.45) !important;
    color: #a8cbff !important; font-weight: 800;
    box-shadow: inset 0 0 0 1px #4f8ef7;
  }
  tr.hl-lowvolatility td:first-child {
    border-left: 4px solid #4f8ef7;
    color: #4f8ef7 !important; font-weight: 700;
  }
  tr.hl-lowvolatility td:first-child::after { content: ' 🧊'; font-size: 11px; }

  /* both at once — purple override */
  tr.hl-lowvol.hl-lowvolatility td {
    background: rgba(204, 119, 255, 0.15) !important;
  }
  tr.hl-lowvol.hl-lowvolatility td:first-child {
    border-left: 4px solid #cc77ff;
    color: #cc77ff !important;
  }
  tr.hl-lowvol.hl-lowvolatility td:first-child::after { content: ' 📦🧊'; font-size: 11px; }

  /* legend */
  .hl-legend { display: flex; gap: 18px; margin-bottom: 10px; flex-wrap: wrap; }
  .hl-legend-item { display: flex; align-items: center; gap: 7px; font-size: 12px;
                    color: var(--muted); }
  .hl-dot { width: 12px; height: 12px; border-radius: 3px; flex-shrink: 0; }

  /* ── scrollbar ── */
  ::-webkit-scrollbar { width: 7px; height: 7px; }
  ::-webkit-scrollbar-track { background: var(--surface); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--accent); }
</style>
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body class="page-{{ page_mode }}">

<header>
  <h1>NSE <span>BHAV</span> Viewer</h1>

  <div class="top-links">
    <a href="/" class="{{ 'active' if page_mode == 'stocks' else '' }}">Stocks</a>
    <a href="/sectors" class="{{ 'active' if page_mode == 'sectors' else '' }}">Sectors</a>
  </div>

  <div class="search-wrap">
    <input id="symInput" type="text" placeholder="Type symbol e.g. KTKBANK"
           autocomplete="off" spellcheck="false">
    <div class="autocomplete-list" id="acList"></div>
  </div>

  <div class="date-range">
    <label>From</label>
    <input type="date" id="fromDate" title="From date (leave blank for last 100 days)">
    <span class="date-sep">→</span>
    <label>To</label>
    <input type="date" id="toDate" title="To date (leave blank for latest available)">
    <button id="clearDatesBtn" onclick="clearDates()" title="Reset to default (last 100 days)">✕ Clear</button>
  </div>

  <button id="loadBtn" onclick="loadStock()">Load</button>

  <div id="metaInfo" style="font-size:12px;color:var(--muted);white-space:nowrap;"></div>
</header>

<div id="statusBar"></div>
<div id="sectorZoomOverlay" class="sector-zoom-overlay" onclick="closeSectorZoom(event)">
  <div class="sector-zoom-card">
    <div class="sector-zoom-head">
      <div>
        <div class="sector-zoom-title" id="sectorZoomTitle"></div>
        <div class="sector-zoom-subtitle" id="sectorZoomSubtitle"></div>
      </div>
      <div class="sector-zoom-nav">
        <button class="sector-zoom-step" type="button" onclick="stepSectorZoom(-1)">Prev</button>
        <div class="sector-zoom-counter" id="sectorZoomCounter">0 / 0</div>
        <button class="sector-zoom-step" type="button" onclick="stepSectorZoom(1)">Next</button>
        <div class="sector-zoom-meta" id="sectorZoomMeta"></div>
        <button class="sector-zoom-close" type="button" onclick="closeSectorZoom()">Close</button>
      </div>
    </div>
    <div class="sector-zoom-chart">
      <div id="sectorZoomChart" class="sector-zoom-chart-inner"></div>
    </div>
  </div>
</div>

<main>
  <section id="sectorPage" style="display:none;">
    <div class="mode-card">
      <div class="mode-grid">
        <div class="sector-controls">
          <div>
            <label for="sectorSelect">Choose a sector</label>
            <select id="sectorSelect">
              <option value="">Loading sectors...</option>
            </select>
          </div>
          <div class="sector-actions">
            <button id="sectorLoadBtn" onclick="loadSectorCharts()">Load Charts</button>
          </div>
          <div class="sector-shortcuts-wrap">
            <div class="sector-shortcuts-head">
              <div class="section-title" style="margin:0;">Top 15 Sectors</div>
              <div class="sector-shortcuts-note">Click a sector to load its charts.</div>
            </div>
            <div class="sector-shortcuts" id="sectorShortcuts">
              <div class="sector-shortcuts-note">Loading shortcuts...</div>
            </div>
          </div>
        </div>
        <div>
          <div id="sectorSpinner" class="sector-spinner"></div>
          <div class="sector-board">
            <div class="sector-board-head">
              <div class="section-title" style="margin:0;">Sector Chart Board</div>
              <div class="sector-board-note" id="sectorBoardNote">Choose a sector to load chart cards.</div>
            </div>
            <div id="sectorBoardEmpty" class="sector-board-empty">No sector charts loaded yet.</div>
            <div id="sectorBoardGrid" class="sector-chart-grid" style="display:none;"></div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <div id="emptyState">
    <div class="big-icon">📊</div>
    <p>Enter a stock symbol to view BHAV data</p>
    <small>Default: last 100 days · Use From/To dates to customise range · bhav2024 · bhav2025 · bhav2026</small>
  </div>

  <div class="spinner" id="spinner"></div>

  <div id="contentArea" style="display:none;">
    <div class="cards" id="summaryCards"></div>
    <div class="section-title">OHLCV · DMAs · Computed Metrics</div>
    <div class="hl-legend">
      <div class="hl-legend-item">
        <div class="hl-dot" style="background:#51cf66"></div>
        <span>📦 Lowest volume in 21-day window</span>
      </div>
      <div class="hl-legend-item">
        <div class="hl-dot" style="background:#4f8ef7"></div>
        <span>🧊 Least volatile in 21-day window</span>
      </div>
      <div class="hl-legend-item">
        <div class="hl-dot" style="background:#cc77ff"></div>
        <span>📦🧊 Both</span>
      </div>
    </div>
    <div class="tbl-wrap">
      <table id="mainTable">
        <thead>
          <tr>
            <th data-col="mktdate">Date <span class="sort-arrow">▼</span></th>
            <th data-col="symbol">Symbol <span class="sort-arrow"></span></th>
            <th data-col="close">Close <span class="sort-arrow"></span></th>
            <th data-col="open">Open <span class="sort-arrow"></span></th>
            <th data-col="high">High <span class="sort-arrow"></span></th>
            <th data-col="low">Low <span class="sort-arrow"></span></th>
            <th data-col="prevclose">Prev Close <span class="sort-arrow"></span></th>
            <th data-col="diff">Diff % <span class="sort-arrow"></span></th>
            <th data-col="volume">Volume <span class="sort-arrow"></span></th>
            <th data-col="deliveryvolume">Del Vol <span class="sort-arrow"></span></th>
            <th data-col="delper">Del % <span class="sort-arrow"></span></th>
            <th data-col="VOLATILITY">Volatility <span class="sort-arrow"></span></th>
            <th data-col="jag">Jag % <span class="sort-arrow"></span></th>
            <th data-col="closeindictor">Close Ind <span class="sort-arrow"></span></th>
            <th data-col="5dma">5 DMA <span class="sort-arrow"></span></th>
            <th data-col="10dma">10 DMA <span class="sort-arrow"></span></th>
            <th data-col="20DMA">20 DMA <span class="sort-arrow"></span></th>
            <th data-col="50dma">50 DMA <span class="sort-arrow"></span></th>
          </tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
  </div>
</main>

<script>
// ── state ──────────────────────────────────────────────────────────────────
const PAGE_MODE = {{ page_mode|tojson }};
let allRows = [];
let sortCol = 'mktdate';
let sortAsc = false;   // default: newest first
let acSelected = -1;

// ── autocomplete ────────────────────────────────────────────────────────────
const symInput = document.getElementById('symInput');
const acList   = document.getElementById('acList');
let acTimeout;

symInput.addEventListener('input', () => {
  clearTimeout(acTimeout);
  const q = symInput.value.trim();
  acTimeout = setTimeout(() => fetchAC(q), 180);
});

symInput.addEventListener('keydown', e => {
  const items = acList.querySelectorAll('.ac-item');
  if (e.key === 'ArrowDown') {
    acSelected = Math.min(acSelected + 1, items.length - 1);
    highlightAC(items);
  } else if (e.key === 'ArrowUp') {
    acSelected = Math.max(acSelected - 1, 0);
    highlightAC(items);
  } else if (e.key === 'Enter') {
    if (acSelected >= 0 && items[acSelected]) {
      symInput.value = items[acSelected].textContent;
      closeAC();
    }
    loadStock();
  } else if (e.key === 'Escape') {
    closeAC();
  }
});

document.addEventListener('click', e => {
  if (!symInput.contains(e.target) && !acList.contains(e.target)) closeAC();
});

function highlightAC(items) {
  items.forEach((el, i) => el.classList.toggle('active', i === acSelected));
  if (acSelected >= 0 && items[acSelected]) {
    items[acSelected].scrollIntoView({ block: 'nearest' });
  }
}

async function fetchAC(q) {
  if (!q) { closeAC(); return; }
  try {
    const res = await fetch(`/api/symbols?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    if (data.error || !data.length) { closeAC(); return; }
    acSelected = -1;
    acList.innerHTML = data.map(s =>
      `<div class="ac-item" onclick="pickAC('${s}')">${s}</div>`
    ).join('');
    acList.classList.add('open');
  } catch { closeAC(); }
}

function pickAC(sym) {
  symInput.value = sym;
  closeAC();
  loadStock();
}

function closeAC() {
  acList.classList.remove('open');
  acList.innerHTML = '';
}

// ── clear dates ──────────────────────────────────────────────────────────────
// â”€â”€ sector browser â”€â”€
const sectorSelect = document.getElementById('sectorSelect');
const sectorSpinner = document.getElementById('sectorSpinner');
const sectorPage = document.getElementById('sectorPage');
const sectorShortcuts = document.getElementById('sectorShortcuts');
const sectorBoardEmpty = document.getElementById('sectorBoardEmpty');
const sectorBoardGrid = document.getElementById('sectorBoardGrid');
const sectorBoardNote = document.getElementById('sectorBoardNote');
const sectorZoomOverlay = document.getElementById('sectorZoomOverlay');
const sectorZoomTitle = document.getElementById('sectorZoomTitle');
const sectorZoomSubtitle = document.getElementById('sectorZoomSubtitle');
const sectorZoomMeta = document.getElementById('sectorZoomMeta');
const sectorZoomCounter = document.getElementById('sectorZoomCounter');
const sectorZoomChartEl = document.getElementById('sectorZoomChart');
let sectorChartInstances = [];
let sectorChartObservers = [];
let sectorBoardObserver = null;
let sectorBoardPayload = [];
let sectorZoomChart = null;
let sectorZoomObserver = null;
let sectorZoomIndex = -1;

sectorSelect.addEventListener('change', () => {
  if (sectorSelect.value) loadSectorCharts();
  else clearSectorBoard();
});

if (PAGE_MODE === 'sectors' && sectorPage) {
  sectorPage.style.display = 'block';
  clearSectorBoard();
  loadSectors();
  loadTopSectors();
}

async function reloadSectors() {
  await Promise.all([loadSectors(true), loadTopSectors()]);
}

async function loadSectors(resetSelection = false) {
  sectorSelect.disabled = true;
  setSectorLoading(true);
  try {
    const res = await fetch('/api/sectors');
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    sectorSelect.innerHTML = ['<option value="">-- choose a sector --</option>']
      .concat(data.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`))
      .join('');
    if (resetSelection) {
      sectorSelect.value = '';
      clearSectorBoard();
    }
    setStatus(`Loaded ${data.length} sectors`, 'ok');
  } catch (e) {
    sectorSelect.innerHTML = '<option value="">Unable to load sectors</option>';
    setStatus(`Error: ${e.message}`, 'err');
  } finally {
    sectorSelect.disabled = false;
    setSectorLoading(false);
  }
}

async function loadTopSectors() {
  try {
    const res = await fetch('/api/top-sectors');
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    sectorShortcuts.innerHTML = data.map(item => `
      <button class="sector-shortcut" type="button" onclick="pickSector('${escapeJs(item.sector)}')" title="${escapeHtml(item.sector)}">
        <div class="sector-shortcut-name">${escapeHtml(item.sector)}</div>
        <div class="sector-shortcut-count">${item.count} stocks</div>
      </button>
    `).join('');
    highlightSectorShortcut(sectorSelect.value);
  } catch (e) {
    sectorShortcuts.innerHTML = `<div class="sector-shortcuts-note">Shortcut load failed: ${escapeHtml(e.message)}</div>`;
  }
}

async function loadSectorCharts() {
  const sector = sectorSelect.value.trim().toUpperCase();
  if (!sector) {
    setStatus('Please choose a sector', 'err');
    return;
  }

  setSectorLoading(true);
  highlightSectorShortcut(sector);
  try {
    setStatus(`Loading charts for ${sector}...`, '');
    await loadSectorChartBoard(sector);
  } catch (e) {
    setStatus(`Error: ${e.message}`, 'err');
    clearSectorBoard(`No sector charts loaded for ${sector}.`);
  } finally {
    setSectorLoading(false);
  }
}

function pickSector(sector) {
  sectorSelect.value = sector;
  loadSectorCharts();
}

function highlightSectorShortcut(sector) {
  if (!sectorShortcuts) return;
  const target = String(sector || '').trim().toUpperCase();
  sectorShortcuts.querySelectorAll('.sector-shortcut').forEach(btn => {
    const label = (btn.querySelector('.sector-shortcut-name')?.textContent || '').trim().toUpperCase();
    btn.classList.toggle('active', label === target);
  });
}

function setSectorLoading(on) {
  sectorSpinner.style.display = on ? 'block' : 'none';
}

function sectorChartOptions(chartEl, isZoom = false) {
  return {
    width: Math.max(chartEl.clientWidth, 320),
    height: Math.max(chartEl.clientHeight, isZoom ? 560 : 260),
    layout: {
      background: { type: 'solid', color: '#1f2329' },
      textColor: '#94a3b8',
      fontFamily: 'Segoe UI, Tahoma, sans-serif',
      fontSize: isZoom ? 11 : 10,
    },
    grid: {
      vertLines: { color: '#303744' },
      horzLines: { color: '#303744' },
    },
    rightPriceScale: {
      borderColor: '#3a4250',
      scaleMargins: isZoom ? { top: 0.06, bottom: 0.22 } : { top: 0.08, bottom: 0.22 },
    },
    leftPriceScale: { visible: false },
    timeScale: {
      borderColor: '#3a4250',
      timeVisible: false,
      secondsVisible: false,
      fixLeftEdge: true,
      fixRightEdge: true,
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
    handleScroll: isZoom,
    handleScale: isZoom,
  };
}

function normalizeChartTime(time) {
  if (!time) return '';
  if (typeof time === 'string') return time;
  if (typeof time === 'object' && time.year && time.month && time.day) {
    const mm = String(time.month).padStart(2, '0');
    const dd = String(time.day).padStart(2, '0');
    return `${time.year}-${mm}-${dd}`;
  }
  return String(time);
}

function formatSectorTooltipDate(time) {
  const raw = normalizeChartTime(time);
  if (!raw) return '';
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleDateString('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  });
}

function createSectorHud(chartEl, isZoom = false) {
  const hud = document.createElement('div');
  hud.className = `sector-chart-hud${isZoom ? ' is-zoom' : ''}`;
  hud.innerHTML = `
    <div class="hud-grid">
      <div class="hud-item"><span class="hud-key">O:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">H:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">V:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">C:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">L:</span><span class="hud-val">–</span></div>
      <div class="hud-item"><span class="hud-key">%</span><span class="hud-val">–</span></div>
    </div>
  `;
  chartEl.appendChild(hud);
  return hud;
}

function renderSectorHud(hud, candleData) {
  if (!hud || !candleData) return;
  const change = candleData.change_pct;
  const changeClass = change == null ? '' : (change >= 0 ? 'hud-pos' : 'hud-neg');
  hud.innerHTML = `
    <div class="hud-grid">
      <div class="hud-item"><span class="hud-key">O:</span><span class="hud-val">${fmt(candleData.open, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">H:</span><span class="hud-val">${fmt(candleData.high, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">V:</span><span class="hud-val">${fmtVol(candleData.volume)}</span></div>
      <div class="hud-item"><span class="hud-key">C:</span><span class="hud-val">${fmt(candleData.close, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">L:</span><span class="hud-val">${fmt(candleData.low, 1)}</span></div>
      <div class="hud-item"><span class="hud-key">%</span><span class="hud-val ${changeClass}">${fmtPct(change)}</span></div>
    </div>
  `;
}

function drawSectorChart(chartEl, card, isZoom = false) {
  const chart = LightweightCharts.createChart(chartEl, sectorChartOptions(chartEl, isZoom));
  const candleMeta = new Map((card.candles || []).map(c => [normalizeChartTime(c.time), c]));
  const candleSeries = chart.addCandlestickSeries({
    upColor: '#58b65b',
    downColor: '#ef6a6a',
    borderUpColor: '#58b65b',
    borderDownColor: '#ef6a6a',
    wickUpColor: '#58b65b',
    wickDownColor: '#ef6a6a',
    priceLineVisible: false,
    lastValueVisible: false,
  });
  candleSeries.setData(card.candles || []);

  const volumeSeries = chart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    priceScaleId: '',
    lastValueVisible: false,
    priceLineVisible: false,
  });
  volumeSeries.priceScale().applyOptions({
    scaleMargins: isZoom ? { top: 0.82, bottom: 0.02 } : { top: 0.80, bottom: 0.02 },
  });
  volumeSeries.setData(card.volume || []);

  chart.addLineSeries({
    color: '#f59e0b',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema5 || []);

  chart.addLineSeries({
    color: '#2563eb',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema10 || []);

  chart.addLineSeries({
    color: '#10b981',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema20 || []);

  chart.addLineSeries({
    color: '#8b5cf6',
    lineWidth: 2,
    lastValueVisible: false,
    priceLineVisible: false,
    crosshairMarkerVisible: false,
  }).setData(card.ema50 || []);

  const hud = createSectorHud(chartEl, isZoom);
  const latestKey = (card.candles && card.candles.length)
    ? normalizeChartTime(card.candles[card.candles.length - 1].time)
    : null;
  if (latestKey && candleMeta.has(latestKey)) {
    renderSectorHud(hud, candleMeta.get(latestKey));
  }

  chart.subscribeCrosshairMove((param) => {
    if (!param || !param.time) {
      if (latestKey && candleMeta.has(latestKey)) {
        renderSectorHud(hud, candleMeta.get(latestKey));
      }
      return;
    }
    const candleData = candleMeta.get(normalizeChartTime(param.time));
    if (!candleData) {
      return;
    }
    renderSectorHud(hud, candleData);
  });

  chart.timeScale().fitContent();
  return chart;
}

function updateSectorZoomNav() {
  const total = sectorBoardPayload.length;
  const prevBtn = document.querySelector('.sector-zoom-step[onclick="stepSectorZoom(-1)"]');
  const nextBtn = document.querySelector('.sector-zoom-step[onclick="stepSectorZoom(1)"]');
  if (sectorZoomCounter) {
    sectorZoomCounter.textContent = total ? `${sectorZoomIndex + 1} / ${total}` : '0 / 0';
  }
  if (prevBtn) prevBtn.disabled = sectorZoomIndex <= 0;
  if (nextBtn) nextBtn.disabled = sectorZoomIndex < 0 || sectorZoomIndex >= total - 1;
}

function renderSectorZoom(idx) {
  const card = sectorBoardPayload[idx];
  if (!card || !card.has_data || !window.LightweightCharts) return;
  sectorZoomIndex = idx;
  sectorZoomTitle.textContent = card.symbol || '';
  sectorZoomSubtitle.textContent = card.sector || '';
  sectorZoomMeta.textContent = `Close ${fmt(card.end_close, 2)} ${card.move_pct == null ? '' : `| ${fmtPct(card.move_pct)}`}`;
  updateSectorZoomNav();

  sectorZoomOverlay.classList.add('open');
  sectorZoomChartEl.innerHTML = '';
  if (sectorZoomObserver) {
    sectorZoomObserver.disconnect();
    sectorZoomObserver = null;
  }
  if (sectorZoomChart) {
    sectorZoomChart.remove();
    sectorZoomChart = null;
  }
  requestAnimationFrame(() => {
    sectorZoomChart = drawSectorChart(sectorZoomChartEl, card, true);
    sectorZoomObserver = new ResizeObserver(() => {
      if (!sectorZoomChart) return;
      sectorZoomChart.applyOptions({
        width: sectorZoomChartEl.clientWidth,
        height: sectorZoomChartEl.clientHeight,
      });
      sectorZoomChart.timeScale().fitContent();
    });
    sectorZoomObserver.observe(sectorZoomChartEl);
  });
}

function renderSectorChartCard(idx) {
  const card = sectorBoardPayload[idx];
  const chartEl = document.getElementById(`sectorChart_${idx}`);
  if (!card || !chartEl || chartEl.dataset.rendered === '1') return;

  chartEl.dataset.rendered = '1';
  if (!card.has_data || !window.LightweightCharts) {
    chartEl.innerHTML = '<div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">No chart data</div>';
    return;
  }

  chartEl.innerHTML = '';
  const chart = drawSectorChart(chartEl, card, false);
  sectorChartInstances[idx] = chart;

  const observer = new ResizeObserver(() => {
    chart.applyOptions({
      width: chartEl.clientWidth,
      height: chartEl.clientHeight,
    });
    chart.timeScale().fitContent();
  });
  observer.observe(chartEl);
  sectorChartObservers[idx] = observer;
}

function setupSectorBoardLazyCharts() {
  if (!sectorBoardGrid || !sectorBoardPayload.length) return;
  if (sectorBoardObserver) {
    sectorBoardObserver.disconnect();
    sectorBoardObserver = null;
  }

  const cards = Array.from(sectorBoardGrid.querySelectorAll('.sector-mini-chart'));
  if (!('IntersectionObserver' in window)) {
    cards.forEach((el) => {
      const idx = Number(el.dataset.chartIdx);
      renderSectorChartCard(idx);
    });
    return;
  }

  sectorBoardObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      const idx = Number(entry.target.dataset.chartIdx);
      renderSectorChartCard(idx);
      sectorBoardObserver.unobserve(entry.target);
    });
  }, {
    root: null,
    rootMargin: '250px 0px',
    threshold: 0.08,
  });

  cards.forEach((el, idx) => {
    if (idx < 4) {
      renderSectorChartCard(idx);
    } else {
      sectorBoardObserver.observe(el);
    }
  });
}

function openSectorZoom(idx) {
  renderSectorZoom(idx);
}

function stepSectorZoom(delta) {
  if (!sectorBoardPayload.length) return;
  const nextIdx = sectorZoomIndex + delta;
  if (nextIdx < 0 || nextIdx >= sectorBoardPayload.length) return;
  renderSectorZoom(nextIdx);
}

function closeSectorZoom(event) {
  if (event && event.target && event.target.id !== 'sectorZoomOverlay') return;
  sectorZoomOverlay.classList.remove('open');
  sectorZoomIndex = -1;
  updateSectorZoomNav();
  if (sectorZoomObserver) {
    sectorZoomObserver.disconnect();
    sectorZoomObserver = null;
  }
  if (sectorZoomChart) {
    sectorZoomChart.remove();
    sectorZoomChart = null;
  }
}

function clearSectorBoard(message = 'No sector charts loaded yet.') {
  closeSectorZoom();
  if (sectorBoardObserver) {
    sectorBoardObserver.disconnect();
    sectorBoardObserver = null;
  }
  sectorChartInstances.forEach(chart => {
    try { chart.remove(); } catch {}
  });
  sectorChartObservers.forEach(obs => {
    try { obs.disconnect(); } catch {}
  });
  sectorChartInstances = [];
  sectorChartObservers = [];
  sectorBoardPayload = [];
  sectorBoardGrid.innerHTML = '';
  sectorBoardGrid.style.display = 'none';
  sectorBoardEmpty.textContent = message;
  sectorBoardEmpty.style.display = 'block';
  sectorBoardNote.textContent = message;
}

document.addEventListener('keydown', (event) => {
  if (!sectorZoomOverlay.classList.contains('open')) return;
  if (event.key === 'Escape') {
    event.preventDefault();
    closeSectorZoom();
  } else if (event.key === 'ArrowLeft') {
    event.preventDefault();
    stepSectorZoom(-1);
  } else if (event.key === 'ArrowRight') {
    event.preventDefault();
    stepSectorZoom(1);
  }
});

async function loadSectorChartBoard(sector) {
    if (!sector) {
      clearSectorBoard();
      return;
    }

  sectorBoardNote.textContent = `Loading chart cards for ${sector}...`;
  clearSectorBoard(`Loading chart cards for ${sector}...`);
  try {
    const res = await fetch(`/api/sector-charts?sector=${encodeURIComponent(sector)}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    if (!data.charts || !data.charts.length) {
      clearSectorBoard(`No charts met the 21D avg turnover filter for ${sector}.`);
      return;
    }

    sectorBoardPayload = data.charts;
    sectorBoardGrid.innerHTML = data.charts.map((card, idx) => `
      <div class="sector-chart-card">
        <div class="sector-chart-head">
          <div>
            <div class="sector-chart-title">${escapeHtml(card.symbol || '')}</div>
            <div class="sector-chart-sector">${escapeHtml(card.sector || '')}</div>
          </div>
          <div class="sector-chart-meta">
            Close ${fmt(card.end_close, 2)} ${card.move_pct == null ? '' : `| ${fmtPct(card.move_pct)}`}
            <button class="sector-chart-expand" type="button" onclick="openSectorZoom(${idx})">Expand</button>
          </div>
        </div>
        <div id="sectorChart_${idx}" class="sector-mini-chart" data-chart-idx="${idx}">
          <div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">Loading chart...</div>
        </div>
      </div>
    `).join('');

    sectorBoardEmpty.style.display = 'none';
    sectorBoardGrid.style.display = 'grid';
    sectorBoardNote.textContent = `Showing ${data.charts.length} chart card(s) for ${sector}.`;
    sectorChartInstances = [];
    sectorChartObservers.forEach(obs => {
      try { obs.disconnect(); } catch {}
    });
    sectorChartObservers = [];
    setupSectorBoardLazyCharts();
  } catch (e) {
    clearSectorBoard(`Chart board error: ${e.message}`);
  }
}

function clearDates() {
  document.getElementById('fromDate').value = '';
  document.getElementById('toDate').value   = '';
}

// ── load stock ───────────────────────────────────────────────────────────────
async function loadStock() {
  const sym      = symInput.value.trim().toUpperCase();
  if (!sym) { setStatus('Please enter a symbol', 'err'); return; }
  closeAC();

  const fromVal = document.getElementById('fromDate').value.trim();
  const toVal   = document.getElementById('toDate').value.trim();

  const btn = document.getElementById('loadBtn');
  btn.disabled = true;
  setStatus(`Loading data for ${sym} …`, '');
  showSpinner(true);
  hideContent();

  try {
    let url = `/api/stock?symbol=${encodeURIComponent(sym)}`;
    if (fromVal) url += `&from_date=${encodeURIComponent(fromVal)}`;
    if (toVal)   url += `&to_date=${encodeURIComponent(toVal)}`;

    const res  = await fetch(url);
    const data = await res.json();

    if (data.error) {
      setStatus(`Error: ${data.error}`, 'err');
      showSpinner(false);
      showEmpty();
      btn.disabled = false;
      return;
    }

    allRows  = data.rows || [];
    sortCol  = 'mktdate';
    sortAsc  = false;

    const rangeLabel = `${data.from_date} → ${data.yesterday}`;
    document.getElementById('metaInfo').innerHTML =
      `<b style="color:var(--text)">${data.symbol}</b> &nbsp;|&nbsp; ` +
      `Range: <b>${rangeLabel}</b> &nbsp;|&nbsp; ` +
      `21D start: ${data.start_21d} &nbsp;|&nbsp; ` +
      `63D start: ${data.start_63d} &nbsp;|&nbsp; ` +
      `<b style="color:var(--accent2)">${allRows.length} rows</b>`;

    renderSummary(data, sym);
    renderTable();
    showContent();
    setStatus(`Loaded ${allRows.length} rows for ${sym} (${rangeLabel})`, 'ok');
  } catch (e) {
    setStatus(`Network error: ${e.message}`, 'err');
    showEmpty();
  }

  showSpinner(false);
  btn.disabled = false;
}

// ── summary cards ────────────────────────────────────────────────────────────
function renderSummary(data, sym) {
  const rows = data.rows;
  const mv   = data.minvol;
  const lv   = data.lowvolume;

  // quick stats from latest row
  const latest = rows[0] || {};
  const oldest = rows[rows.length - 1] || {};
  const hi52  = rows.reduce((a, r) => Math.max(a, r.high || 0), 0);
  const lo52  = rows.reduce((a, r) => Math.min(a, r.low  || Infinity), Infinity);
  const pctFH = latest.close && hi52 ? (((latest.close - hi52) / hi52) * 100).toFixed(2) : '–';
  const pctFL = latest.close && lo52 && lo52 < Infinity
    ? (((latest.close - lo52) / lo52) * 100).toFixed(2) : '–';

  const cards = [
    { label: 'Latest Close',     value: fmt(latest.close, 2),     sub: latest.mktdate, cls: 'close-hi' },
    { label: 'Latest Diff %',    value: fmtPct(latest.diff),      sub: 'vs prev close', cls: colClass(latest.diff) },
    { label: '52W High',         value: fmt(hi52, 2),             sub: `${pctFH}% from high`, cls: '' },
    { label: '52W Low',          value: fmt(lo52 < Infinity ? lo52 : null, 2), sub: `+${pctFL}% from low`, cls: 'green' },
    { label: 'Min Vol (63D win)',value: fmt(lv.table21_uses_63d_window, 0), sub: 'table lowvolume21 — 63D window', cls: 'warn' },
    { label: 'Min Vol (21D win)',value: fmt(lv.table63_uses_21d_window, 0), sub: 'table lowvolume63 — 21D window', cls: 'warn' },
    { label: 'Min Volatility 63D', value: fmt(mv['63d'], 2), sub: '63 trading-day window', cls: '' },
    { label: 'Min Volatility 21D', value: fmt(mv['21d'], 2), sub: '21 trading-day window', cls: '' },
  ];

  document.getElementById('summaryCards').innerHTML = cards.map(c => `
    <div class="card">
      <div class="card-label">${c.label}</div>
      <div class="card-value ${c.cls}">${c.value ?? '–'}</div>
      <div class="card-sub">${c.sub || ''}</div>
    </div>
  `).join('');
}

// ── table render ─────────────────────────────────────────────────────────────
function renderTable() {
  const sorted = [...allRows].sort((a, b) => {
    let av = a[sortCol], bv = b[sortCol];
    if (av == null) av = sortAsc ? Infinity  : -Infinity;
    if (bv == null) bv = sortAsc ? Infinity  : -Infinity;
    if (typeof av === 'string') return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    return sortAsc ? av - bv : bv - av;
  });

  // update sort arrows
  document.querySelectorAll('thead th').forEach(th => {
    const col = th.dataset.col;
    const arrow = th.querySelector('.sort-arrow');
    th.classList.toggle('sorted', col === sortCol);
    if (arrow) arrow.textContent = col === sortCol ? (sortAsc ? '▲' : '▼') : '';
  });

  document.getElementById('tableBody').innerHTML = sorted.map(r => {
    const diffCls  = colClass(r.diff);
    const volClass = r.VOLATILITY != null && r.VOLATILITY > 8 ? 'warn' : '';
    const ci = r.closeindictor === 'Y' || r.closeindictor === 'y'
               ? `<span class="badge badge-y">Y</span>`
               : `<span class="badge badge-n">N</span>`;
    const trCls = [
      r._hl_lowvol         ? 'hl-lowvol'         : '',
      r._hl_lowvolatility  ? 'hl-lowvolatility'  : '',
    ].filter(Boolean).join(' ');
    const volCellCls  = [volClass,  r._hl_lowvolatility ? 'hl-cell-vol' : ''].filter(Boolean).join(' ');
    const volCellVol  = r._hl_lowvol ? 'hl-cell-vol' : '';
    return `
      <tr class="${trCls}">
        <td>${r.mktdate || '–'}</td>
        <td>${r.symbol || '–'}</td>
        <td class="close-hi">${fmt(r.close, 2)}</td>
        <td>${fmt(r.open, 2)}</td>
        <td>${fmt(r.high, 2)}</td>
        <td>${fmt(r.low,  2)}</td>
        <td>${fmt(r.prevclose, 2)}</td>
        <td class="${diffCls}">${fmtPct(r.diff)}</td>
        <td class="${volCellVol}">${fmtVol(r.volume)}</td>
        <td>${fmtVol(r.deliveryvolume)}</td>
        <td>${fmt(r.delper, 2)}</td>
        <td class="${volCellCls}">${fmt(r.VOLATILITY, 2)}</td>
        <td class="${colClass(r.jag)}">${fmt(r.jag, 2)}</td>
        <td>${ci}</td>
        <td>${fmt(r['5dma'],  2)}</td>
        <td>${fmt(r['10dma'], 2)}</td>
        <td>${fmt(r['20DMA'], 2)}</td>
        <td>${fmt(r['50dma'], 2)}</td>
      </tr>`;
  }).join('');
}

// ── sort on header click ──────────────────────────────────────────────────────
document.querySelectorAll('thead th[data-col]').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (sortCol === col) sortAsc = !sortAsc;
    else { sortCol = col; sortAsc = false; }
    renderTable();
  });
});

// ── helpers ──────────────────────────────────────────────────────────────────
function fmt(v, d = 2)   { return v == null ? '–' : Number(v).toLocaleString('en-IN', { minimumFractionDigits: d, maximumFractionDigits: d }); }
function fmtPct(v)       { if (v == null) return '–'; return (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%'; }
function fmtVol(v)       { return v == null ? '–' : Number(v).toLocaleString('en-IN'); }
function escapeHtml(v) {
  return String(v)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
function escapeJs(v) {
  return String(v)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}
function colClass(v)     { if (v == null) return ''; return v > 0 ? 'pos' : v < 0 ? 'neg' : ''; }
function setStatus(m, t) { const s = document.getElementById('statusBar'); s.textContent = m; s.className = t; }
function showSpinner(on) { document.getElementById('spinner').style.display = on ? 'block' : 'none'; }
function showContent()   { document.getElementById('contentArea').style.display = 'block'; document.getElementById('emptyState').style.display = 'none'; }
function hideContent()   { document.getElementById('contentArea').style.display = 'none'; }
function showEmpty()     { document.getElementById('emptyState').style.display = 'flex'; }

// ── enter key on input ────────────────────────────────────────────────────────
symInput.addEventListener('keydown', e => { if (e.key === 'Enter') loadStock(); });

</script>
</body>
</html>
"""

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    print(f"\n  NSE BHAV Viewer  →  http://{args.host}:{args.port}\n")
    app.run(host=args.host, port=args.port, debug=args.debug)
