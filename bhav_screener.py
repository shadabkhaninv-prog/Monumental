"""
Bhav Screener — Low-Volume / Low-Volatility (10d) screener
==========================================================
Standalone Flask app. Uses the same MySQL `bhav` database as app.py
(NSE BHAV Stock Viewer) but runs on its own port and adds no
dependency on app.py.

Filters
-------
  * Liquidity:  21-trading-day average turnover (volume * close) >= 10 cr
  * Low-volume list:     today's volume     == MIN(volume)     over last 10 trading days
  * Low-volatility list: today's VOLATILITY == MIN(VOLATILITY) over last 10 trading days

The date picker defaults to the most recent mktdate in mktdatecalendar.

Run:   python bhav_screener.py
Open:  http://localhost:5001
"""
from __future__ import annotations

import argparse
import math
from datetime import date
from typing import Dict, List, Optional

import mysql.connector
from flask import Flask, jsonify, render_template_string, request


DB_CONFIG = dict(host="localhost", port=3306, user="root",
                 password="root", database="bhav")

# Liquidity threshold (in rupees). 10 crore = 10 * 1_00_00_000.
MIN_AVG_TURNOVER_DEFAULT = 10 * 1_00_00_000

LOOKBACK_DAYS_MIN = 10   # window for 10-day min-volume / min-volatility
LOOKBACK_DAYS_TURN = 21  # window for avg turnover

app = Flask(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def to_date(v):
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return date.fromisoformat(v[:10])
    return v


def safe(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, date):
        return v.isoformat()
    return v


def get_latest_bhav_date(conn) -> Optional[date]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(mktdate) FROM mktdatecalendar")
    row = cur.fetchone()
    cur.close()
    return to_date(row[0]) if row and row[0] else None


def trading_days_ending(conn, end_date: date, n: int) -> List[date]:
    """Return the last n trading dates in mktdatecalendar ending on end_date (inclusive)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT mktdate FROM (
            SELECT DISTINCT mktdate FROM mktdatecalendar
             WHERE mktdate <= %s
             ORDER BY mktdate DESC LIMIT %s
        ) m ORDER BY mktdate ASC
        """,
        (end_date, int(n)),
    )
    rows = [to_date(r[0]) for r in cur.fetchall()]
    cur.close()
    return [d for d in rows if d is not None]


def union_bhav_select(years: List[int]) -> str:
    """Build a UNION ALL across yearly bhav tables."""
    block = (
        "SELECT UPPER(symbol) AS symbol, mktdate, volume, VOLATILITY, close "
        "FROM bhav{year} WHERE mktdate BETWEEN %s AND %s"
    )
    parts = [block.format(year=y) for y in years]
    return "\nUNION ALL\n".join(parts)


def get_excluded_symbols(conn) -> set[str]:
    """Return ETF/index symbols that should not appear in the screener."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT UPPER(symbol)
        FROM sectors
        WHERE UPPER(COALESCE(sector1, '')) = 'ETF'
    """)
    excluded = {str(row[0]).strip().upper() for row in cur.fetchall() if row and row[0]}
    cur.close()
    return excluded


# ── routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML_PAGE)


@app.route("/api/latest-date")
def api_latest_date():
    try:
        conn = get_conn()
        d = get_latest_bhav_date(conn)
        conn.close()
        return jsonify({"latest_date": safe(d)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/screener")
def api_screener():
    """
    Query params:
        date            (YYYY-MM-DD, optional; defaults to max(mktdate))
        min_turnover    (rupees, optional; default = 10 cr)
        limit           (optional; default = 200)
    Returns:
        {
          "as_of": "YYYY-MM-DD",
          "window_10d": { "start": ..., "end": ... },
          "window_21d": { "start": ..., "end": ... },
          "min_turnover": <float>,
          "universe": <int>,   # stocks passing liquidity + having today's row
          "low_volume": [ {...}, ... ],
          "low_volatility": [ {...}, ... ]
        }
    """
    try:
        limit = int(request.args.get("limit", 200))
    except ValueError:
        limit = 200
    try:
        min_turnover = float(request.args.get("min_turnover", MIN_AVG_TURNOVER_DEFAULT))
    except ValueError:
        min_turnover = float(MIN_AVG_TURNOVER_DEFAULT)

    try:
        conn = get_conn()

        # ── 1. Resolve "as-of" date
        as_of_param = (request.args.get("date") or "").strip()
        db_max = get_latest_bhav_date(conn)
        if not db_max:
            return jsonify({"error": "mktdatecalendar is empty"}), 500

        try:
            as_of = to_date(as_of_param) if as_of_param else db_max
        except Exception:
            return jsonify({"error": f"bad date: {as_of_param!r}"}), 400

        # clamp to the real trading-day on/at-or-before the requested date
        # (user might pick a Saturday / holiday)
        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(mktdate) FROM mktdatecalendar WHERE mktdate <= %s",
            (as_of,),
        )
        row = cur.fetchone()
        cur.close()
        resolved = to_date(row[0]) if row and row[0] else None
        if resolved is None:
            return jsonify({"error": "no trading day on or before selected date"}), 404
        as_of = resolved

        # ── 2. Trading-day windows
        d10 = trading_days_ending(conn, as_of, LOOKBACK_DAYS_MIN)
        d21 = trading_days_ending(conn, as_of, LOOKBACK_DAYS_TURN)
        if not d10 or not d21:
            return jsonify({"error": "not enough trading days in calendar"}), 500

        start_10d, end_10d = d10[0], d10[-1]
        start_21d, end_21d = d21[0], d21[-1]

        # ── 3. Pull all bhav rows across the 21-day window via yearly tables
        years = sorted({start_21d.year, end_21d.year})
        sql = union_bhav_select(years)
        params: List = []
        for _ in years:
            params += [start_21d, end_21d]

        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        excluded_symbols = get_excluded_symbols(conn)
        conn.close()

        # ── 4. Group by symbol, compute aggregates in Python
        d10_set = set(d10)
        agg: Dict[str, Dict] = {}
        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym:
                continue
            if sym in excluded_symbols:
                continue
            mdate = to_date(r.get("mktdate"))
            if mdate is None:
                continue
            vol = r.get("volume")
            vlt = r.get("VOLATILITY")
            cls = r.get("close")

            a = agg.setdefault(sym, {
                "turn_sum": 0.0, "turn_cnt": 0,
                "vols_10d": [], "vlts_10d": [],
                "today_vol": None, "today_vlt": None, "today_close": None,
                "today_date": None,
            })

            # turnover uses the full 21d window
            try:
                if vol is not None and cls is not None:
                    a["turn_sum"] += float(vol) * float(cls)
                    a["turn_cnt"] += 1
            except Exception:
                pass

            # 10d window values
            if mdate in d10_set:
                if vol is not None:
                    try:
                        a["vols_10d"].append(float(vol))
                    except Exception:
                        pass
                if vlt is not None:
                    try:
                        a["vlts_10d"].append(float(vlt))
                    except Exception:
                        pass

            # today's row
            if mdate == as_of:
                a["today_vol"]   = float(vol) if vol is not None else None
                a["today_vlt"]   = float(vlt) if vlt is not None else None
                a["today_close"] = float(cls) if cls is not None else None
                a["today_date"]  = mdate

        # ── 5. Filter + separate into the two lists
        low_volume: List[Dict] = []
        low_volatility: List[Dict] = []

        for sym, a in agg.items():
            if a["turn_cnt"] == 0 or a["today_vol"] is None:
                continue
            avg_turnover = a["turn_sum"] / a["turn_cnt"]
            if avg_turnover < min_turnover:
                continue

            row_out = {
                "symbol":           sym,
                "as_of":            safe(a["today_date"]),
                "close":            safe(a["today_close"]),
                "volume":           safe(a["today_vol"]),
                "volatility":       safe(a["today_vlt"]),
                "min_vol_10d":      safe(min(a["vols_10d"])) if a["vols_10d"] else None,
                "min_vlt_10d":      safe(min(a["vlts_10d"])) if a["vlts_10d"] else None,
                "avg_turnover_21d": safe(round(avg_turnover, 2)),
                "sample_10d_vol":   len(a["vols_10d"]),
                "sample_10d_vlt":   len(a["vlts_10d"]),
            }

            # low-volume list
            if (row_out["min_vol_10d"] is not None
                    and row_out["volume"] is not None
                    and float(row_out["volume"]) <= float(row_out["min_vol_10d"])):
                low_volume.append(row_out)

            # low-volatility list
            if (row_out["min_vlt_10d"] is not None
                    and row_out["volatility"] is not None
                    and float(row_out["volatility"]) <= float(row_out["min_vlt_10d"])):
                low_volatility.append(row_out)

        # sort by volume / volatility ascending, then by symbol
        low_volume.sort(key=lambda r: (float(r["volume"] or 0), r["symbol"]))
        low_volatility.sort(key=lambda r: (float(r["volatility"] or 0), r["symbol"]))

        return jsonify({
            "as_of": safe(as_of),
            "window_10d":    {"start": safe(start_10d), "end": safe(end_10d)},
            "window_21d":    {"start": safe(start_21d), "end": safe(end_21d)},
            "min_turnover":  min_turnover,
            "universe":      sum(1 for a in agg.values()
                                 if a["today_vol"] is not None and a["turn_cnt"] > 0
                                 and (a["turn_sum"] / a["turn_cnt"]) >= min_turnover),
            "low_volume":       low_volume[:limit],
            "low_volatility":   low_volatility[:limit],
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


# ── HTML page ────────────────────────────────────────────────────────────────
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bhav Screener — Low Volume / Volatility</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg:#0f1117; --surface:#1a1d27; --card:#21253a; --border:#2e3450;
    --accent:#4f8ef7; --accent2:#38d9a9; --warn:#ffa94d; --red:#ff6b6b;
    --green:#51cf66; --text:#e8eaf6; --muted:#8892b0; --th-bg:#1c2035;
    --row-alt:#1e2236; --shadow:0 4px 24px rgba(0,0,0,.45);
  }
  html,body{height:100%;overflow:hidden}
  body{background:var(--bg);color:var(--text);
       font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;
       display:flex;flex-direction:column}

  header{background:var(--surface);border-bottom:1px solid var(--border);
         padding:14px 28px;display:flex;align-items:center;gap:18px;
         flex-shrink:0;z-index:100;box-shadow:var(--shadow)}
  header h1{font-size:1.15rem;font-weight:700;color:var(--accent);
            letter-spacing:.5px;white-space:nowrap}
  header h1 span{color:var(--accent2)}
  .controls{display:flex;align-items:center;gap:14px;margin-left:auto;flex-wrap:wrap}
  .controls label{font-size:11px;color:var(--muted);text-transform:uppercase;
                  letter-spacing:.6px}
  .controls input,.controls select{
    padding:7px 10px;border-radius:8px;border:1.5px solid var(--border);
    background:var(--card);color:var(--text);font-size:13px;outline:none;
    color-scheme:dark;transition:border-color .15s}
  .controls input:focus,.controls select:focus{border-color:var(--accent)}
  .controls button{padding:8px 18px;border-radius:8px;border:none;cursor:pointer;
        background:var(--accent);color:#fff;font-weight:700;font-size:13px;
        transition:opacity .15s}
  .controls button:hover{opacity:.85}
  .controls button.secondary{background:transparent;border:1px solid var(--border);
        color:var(--muted)}
  .controls button.secondary:hover{color:var(--text);border-color:var(--accent)}

  #statusBar{font-size:12px;color:var(--muted);padding:6px 28px;
             background:var(--surface);border-bottom:1px solid var(--border);
             min-height:28px;flex-shrink:0}
  #statusBar.err{color:var(--red)}
  #statusBar.ok{color:var(--accent2)}

  main{padding:20px 28px;flex:1;overflow-y:auto}

  .meta-strip{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:18px}
  .meta-chip{background:var(--card);border:1px solid var(--border);
             border-radius:8px;padding:8px 14px;font-size:12px;color:var(--muted)}
  .meta-chip b{color:var(--text);font-weight:700;margin-left:4px}

  .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}
  @media(max-width:1100px){.grid{grid-template-columns:1fr}}

  .panel{background:var(--card);border:1px solid var(--border);border-radius:12px;
         overflow:hidden;box-shadow:var(--shadow);display:flex;flex-direction:column}
  .panel-head{padding:13px 18px;border-bottom:1px solid var(--border);
              background:var(--th-bg);display:flex;align-items:baseline;gap:12px;
              justify-content:space-between}
  .panel-head h2{font-size:14px;font-weight:700;color:var(--accent2);
                 letter-spacing:.3px;text-transform:uppercase}
  .panel-head .count{font-size:12px;color:var(--muted)}
  .panel-head .count b{color:var(--text)}

  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:9px 12px;text-align:right;border-bottom:1px solid var(--border);
        white-space:nowrap}
  th{position:sticky;top:0;background:var(--th-bg);color:var(--muted);
     font-size:11px;text-transform:uppercase;letter-spacing:.6px;
     font-weight:700;z-index:2}
  th:first-child,td:first-child{text-align:left}
  tbody tr:nth-child(even){background:var(--row-alt)}
  tbody tr:hover{background:rgba(79,142,247,.08)}
  td.sym{font-weight:700;color:var(--accent2);letter-spacing:.3px}
  .sym-link{color:inherit;text-decoration:none}
  .sym-link:hover{text-decoration:underline}
  td.green{color:var(--green)}
  td.warn{color:var(--warn)}
  td.muted{color:var(--muted)}
  .empty{padding:30px 18px;text-align:center;color:var(--muted);font-size:13px}

  .spinner{width:30px;height:30px;border:3px solid var(--border);
           border-top-color:var(--accent);border-radius:50%;
           animation:spin .8s linear infinite;margin:30px auto;display:none}
  @keyframes spin{to{transform:rotate(360deg)}}
  .panel.loading .spinner{display:block}
  .panel.loading tbody,.panel.loading .empty{display:none}

  .footlink{margin-top:18px;text-align:center;font-size:12px;color:var(--muted)}
  .footlink a{color:var(--accent);text-decoration:none;margin:0 6px}
  .footlink a:hover{text-decoration:underline}
</style>
</head>
<body>

<header>
  <h1>Bhav <span>Screener</span></h1>
  <div class="controls">
    <label for="asOf">As of</label>
    <input id="asOf" type="date">
    <label for="minTurn">Min 21d turnover (cr)</label>
    <input id="minTurn" type="number" min="0" step="0.5" value="10" style="width:90px">
    <label for="lim">Limit</label>
    <input id="lim" type="number" min="10" max="1000" value="200" style="width:85px">
    <button id="runBtn">Run</button>
    <button id="resetBtn" class="secondary">Today</button>
  </div>
</header>

<div id="statusBar">Pick a date and click Run. (Defaults to the latest trading date in bhav.)</div>

<main>
  <div class="meta-strip" id="metaStrip"></div>

  <div class="grid">
    <section class="panel" id="panelVol">
      <div class="panel-head">
        <h2>Lowest Volume · 10 days</h2>
        <div class="count">matches <b id="cntVol">—</b></div>
      </div>
      <div class="spinner"></div>
      <div style="overflow:auto;max-height:calc(100vh - 280px)">
      <table>
        <thead><tr>
          <th>Symbol</th><th>Close</th>
          <th>Volume</th><th>10d Min</th>
          <th>21d Turnover (cr)</th>
        </tr></thead>
        <tbody id="tbodyVol"></tbody>
      </table>
      </div>
      <div class="empty" id="emptyVol" style="display:none">No stocks match.</div>
    </section>

    <section class="panel" id="panelVlt">
      <div class="panel-head">
        <h2>Lowest Volatility · 10 days</h2>
        <div class="count">matches <b id="cntVlt">—</b></div>
      </div>
      <div class="spinner"></div>
      <div style="overflow:auto;max-height:calc(100vh - 280px)">
      <table>
        <thead><tr>
          <th>Symbol</th><th>Close</th>
          <th>Volatility</th><th>10d Min</th>
          <th>21d Turnover (cr)</th>
        </tr></thead>
        <tbody id="tbodyVlt"></tbody>
      </table>
      </div>
      <div class="empty" id="emptyVlt" style="display:none">No stocks match.</div>
    </section>
  </div>

  <div class="footlink">
    <a href="http://localhost:5000" target="_blank">Open Bhav Viewer →</a>
    <a href="http://localhost:9000" target="_blank">Launchpad →</a>
  </div>
</main>

<script>
const $  = id => document.getElementById(id);
const asOfEl = $('asOf'), minTurnEl = $('minTurn'), limEl = $('lim');
const runBtn = $('runBtn'), resetBtn = $('resetBtn'), status = $('statusBar');
const tVol = $('tbodyVol'), tVlt = $('tbodyVlt');
const cntVol = $('cntVol'), cntVlt = $('cntVlt');
const emptyVol = $('emptyVol'), emptyVlt = $('emptyVlt');
const panelVol = $('panelVol'), panelVlt = $('panelVlt');
const metaStrip = $('metaStrip');

function setStatus(msg, kind){
  status.textContent = msg;
  status.className = kind || '';
}

function cr(n){ if(n==null||isNaN(n)) return '—'; return (n/1e7).toFixed(2); }
function num(n){ if(n==null||isNaN(n)) return '—'; return Number(n).toLocaleString('en-IN'); }
function fix2(n){ if(n==null||isNaN(n)) return '—'; return Number(n).toFixed(2); }
function fix4(n){ if(n==null||isNaN(n)) return '—'; return Number(n).toFixed(4); }

function renderRows(rows, tbody, valueKey, minKey, fmt){
  if(!rows.length){
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td class="sym"><a class="sym-link" href="http://localhost:5000/?symbol=${encodeURIComponent(r.symbol || '')}" target="_blank" rel="noopener">${r.symbol}</a></td>
      <td>${fix2(r.close)}</td>
      <td class="green">${fmt(r[valueKey])}</td>
      <td class="muted">${fmt(r[minKey])}</td>
      <td>${cr(r.avg_turnover_21d)}</td>
    </tr>`).join('');
  return rows.length;
}

function renderMeta(d){
  metaStrip.innerHTML = `
    <span class="meta-chip">As of<b>${d.as_of}</b></span>
    <span class="meta-chip">10d window<b>${d.window_10d.start} → ${d.window_10d.end}</b></span>
    <span class="meta-chip">21d window<b>${d.window_21d.start} → ${d.window_21d.end}</b></span>
    <span class="meta-chip">Min turnover<b>${cr(d.min_turnover)} cr</b></span>
    <span class="meta-chip">Universe<b>${d.universe}</b></span>
  `;
}

async function run(){
  setStatus('Running…'); runBtn.disabled = true;
  panelVol.classList.add('loading'); panelVlt.classList.add('loading');
  emptyVol.style.display = emptyVlt.style.display = 'none';
  try{
    const d  = asOfEl.value;
    const mt = (parseFloat(minTurnEl.value)||10) * 1e7;
    const lm = Math.max(10, parseInt(limEl.value,10)||200);
    const qs = new URLSearchParams({min_turnover: mt, limit: lm});
    if(d) qs.set('date', d);
    const r = await fetch('/api/screener?'+qs.toString());
    const j = await r.json();
    if(!r.ok){ throw new Error(j.error||r.statusText); }

    renderMeta(j);
    const nV = renderRows(j.low_volume,     tVol, 'volume',     'min_vol_10d', num);
    const nL = renderRows(j.low_volatility, tVlt, 'volatility', 'min_vlt_10d', fix4);
    cntVol.textContent = nV;
    cntVlt.textContent = nL;
    emptyVol.style.display = nV ? 'none' : 'block';
    emptyVlt.style.display = nL ? 'none' : 'block';
    setStatus(`OK — ${nV} low-volume · ${nL} low-volatility stocks on ${j.as_of}.`, 'ok');
    asOfEl.value = j.as_of;
  }catch(e){
    setStatus('Error: '+e.message, 'err');
  }finally{
    panelVol.classList.remove('loading'); panelVlt.classList.remove('loading');
    runBtn.disabled = false;
  }
}

async function initDefaults(){
  try{
    const r = await fetch('/api/latest-date');
    const j = await r.json();
    if(j.latest_date){ asOfEl.value = j.latest_date; }
    else              { asOfEl.valueAsDate = new Date(); }
  }catch(e){
    asOfEl.valueAsDate = new Date();
  }
}

runBtn.addEventListener('click', run);
resetBtn.addEventListener('click', async () => { await initDefaults(); run(); });
asOfEl.addEventListener('keydown', e => { if(e.key === 'Enter') run(); });

(async function(){ await initDefaults(); run(); })();
</script>
</body>
</html>
"""


# ── entry point ──────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Bhav Screener (low volume / volatility)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5001)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(f"[bhav-screener] http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
