#!/usr/bin/env python3
"""
Institutional Picks — Performance Tracker
Launch:  python -m streamlit run performance_tracker.py
"""
from __future__ import annotations
import re, time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import mysql.connector
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
REPORTS_DIR = BASE_DIR / "reports"
TOKEN_FILE  = BASE_DIR / "kite_token.txt"
DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME = "localhost", 3306, "root", "root", "bhav"
SLEEP_S     = 0.35
PERIODS: Dict[str, int] = {"1 Week": 7, "15 Days": 15, "1 Month": 30}
PERIOD_KEYS = list(PERIODS.keys())
TODAY       = date.today()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Picks Performance", page_icon="📈",
                   layout="wide", initial_sidebar_state="expanded")

# ── Design system ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

/* ── Kill Streamlit chrome ───────────────────────────────────── */
#stDecoration, [data-testid="stHeader"], [data-testid="stToolbar"],
[data-testid="stMainMenu"], [data-testid="stStatusWidget"],
[data-testid="stSidebarHeader"], .viewerBadge_container__r5tak,
footer, #MainMenu { display:none !important; }

/* ── Base ────────────────────────────────────────────────────── */
html, body { background:#070d18 !important; }
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.appview-container { background:#070d18 !important; }

/* ── Remove ALL top padding ──────────────────────────────────── */
.main .block-container,
section[data-testid="stMain"],
.appview-container .main section,
[data-testid="block-container"] {
    padding-top: 0 !important;
    background: #070d18 !important;
}
[data-testid="stSidebar"] > div:first-child { padding-top: 1.2rem !important; }

/* ── Sidebar ─────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #060c17 !important;
    border-right: 1px solid #0f1e30 !important;
}
[data-testid="stSidebar"] * { color: #c8dff0 !important; }

/* ── Selectbox control ───────────────────────────────────────── */
[data-baseweb="select"] > div:first-child {
    background: #0c1a2a !important;
    border: 1px solid #1a3550 !important;
    border-radius: 8px !important;
    min-height: 40px !important;
}
/* The actual text value shown */
[data-baseweb="select"] [data-testid="stMarkdown"] p,
[data-baseweb="select"] > div span,
[data-baseweb="select"] > div div,
[data-baseweb="select"] input { color: #d8eeff !important; font-weight:600 !important; font-size:0.9rem !important; }
[data-baseweb="select"] svg { fill:#3b6a9a !important; }

/* ── Dropdown popup ──────────────────────────────────────────── */
[data-baseweb="popover"], [data-baseweb="popover"] > div {
    background:#0a1622 !important;
    border:1px solid #1a3550 !important;
    border-radius:10px !important;
    box-shadow:0 12px 40px rgba(0,0,0,0.8) !important;
}
[data-baseweb="menu"] { background:#0a1622 !important; border-radius:10px !important; }
[data-baseweb="menu"] li,
[data-baseweb="menu"] [role="option"] {
    background:#0a1622 !important;
    color:#b8d8f8 !important;
    font-size:0.88rem !important;
    font-weight:600 !important;
    padding:10px 16px !important;
    border-bottom:1px solid #0f2035 !important;
}
[data-baseweb="menu"] li:hover,
[data-baseweb="menu"] [role="option"]:hover,
[data-baseweb="menu"] [aria-selected="true"] {
    background:#112640 !important;
    color:#ffffff !important;
}
[data-testid="stSidebar"] label {
    color:#4070a0 !important;
    font-size:0.62rem !important;
    font-weight:700 !important;
    text-transform:uppercase;
    letter-spacing:1.6px;
}

/* ── Run button ──────────────────────────────────────────────── */
.stButton > button[kind="primary"] {
    background:linear-gradient(135deg,#1a6b34,#238636) !important;
    border:none !important; border-radius:8px !important;
    color:#fff !important; font-weight:700 !important;
    font-size:0.88rem !important; letter-spacing:0.4px;
    height:40px !important;
    box-shadow:0 0 20px rgba(34,197,94,0.25) !important;
    transition:all 0.15s !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow:0 0 30px rgba(34,197,94,0.45) !important;
    transform:translateY(-1px);
}

/* ── Progress bar ────────────────────────────────────────────── */
[data-testid="stProgress"] > div > div {
    background:linear-gradient(90deg,#1a6b34,#3b82f6) !important;
}

/* ── Sort selectbox (main area) ──────────────────────────────── */
.sort-wrap [data-baseweb="select"] > div:first-child {
    background:#0c1829 !important;
    border:1px solid #162438 !important;
}

/* ── Tabs ────────────────────────────────────────────────────── */
[data-testid="stTabs"] [role="tab"] {
    color:#2a5a80 !important;
    font-weight:600 !important;
    font-size:0.82rem !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color:#7abcf0 !important;
    border-bottom:2px solid #3b82f6 !important;
}
[data-testid="stTabs"] [role="tablist"] {
    border-bottom:1px solid #0f2035 !important;
}

/* ── Custom metric cards ─────────────────────────────────────── */
.metric-row { display:flex; gap:12px; margin:0 0 20px 0; }
.mc {
    flex:1; background:#0c1829;
    border:1px solid #162438;
    border-radius:12px; padding:16px 20px;
    box-shadow:0 2px 16px rgba(0,0,0,0.4);
    transition:border-color 0.2s;
}
.mc:hover { border-color:#1e3a58; }
.mc-label {
    font-size:0.62rem; font-weight:700; text-transform:uppercase;
    letter-spacing:1.8px; color:#4a80a0; margin-bottom:8px;
}
.mc-value {
    font-size:1.8rem; font-weight:900; letter-spacing:-1px;
    line-height:1; margin-bottom:6px;
}
.mc-value.pos { color:#22c55e; }
.mc-value.neg { color:#f43f5e; }
.mc-value.neu { color:#7abcf0; }
.mc-sub { font-size:0.7rem; color:#3a6a88; font-weight:600; }
.mc-bm  { font-size:0.7rem; color:#2d7050; margin-top:3px; font-weight:600; }

/* ── Stat row (portfolio snapshot) ──────────────────────────── */
.stat-row { display:flex; gap:10px; margin:0 0 20px 0; flex-wrap:wrap; }
.stat {
    flex:1; min-width:140px;
    background:#0a1525; border:1px solid #10202f;
    border-radius:10px; padding:12px 16px;
}
.stat-label { font-size:0.6rem; font-weight:700; text-transform:uppercase;
              letter-spacing:1.5px; color:#4070a0; margin-bottom:5px; }
.stat-value { font-size:1.1rem; font-weight:800; color:#c8e4ff; }
.stat-delta { font-size:0.7rem; font-weight:600; margin-top:3px; }
.stat-delta.pos { color:#22c55e; }
.stat-delta.neg { color:#f43f5e; }

/* ── Section divider ─────────────────────────────────────────── */
.sec-label {
    font-size:0.6rem; font-weight:700; text-transform:uppercase;
    letter-spacing:2px; color:#3d6d8f;
    border-bottom:1px solid #0c1e30;
    padding-bottom:6px; margin:20px 0 12px 0;
}

/* ── Top bar ─────────────────────────────────────────────────── */
.topbar {
    display:flex; align-items:center; gap:16px;
    padding:10px 0 14px 0;
    border-bottom:1px solid #0c1e30;
    margin-bottom:18px;
}
.topbar-title {
    font-size:0.88rem; font-weight:800; color:#7ab8f0;
    text-transform:uppercase; letter-spacing:1px;
}
.topbar-pill {
    background:#0c1829; border:1px solid #162438;
    border-radius:20px; padding:3px 12px;
    font-size:0.7rem; font-weight:600; color:#4a80b8;
}
.topbar-pill b { color:#6aaae0; }

/* ── Performance table ───────────────────────────────────────── */
.pt-wrap {
    overflow-x:auto; border-radius:12px;
    border:1px solid #0f2035;
    box-shadow:0 4px 30px rgba(0,0,0,0.6);
}
table.pt {
    width:100%; border-collapse:collapse;
    font-family:'Inter','Segoe UI',system-ui,sans-serif;
    background:#080f1c;
}
table.pt thead th {
    background:#060c18; padding:11px 16px;
    font-size:0.62rem; font-weight:700;
    text-transform:uppercase; letter-spacing:1.4px;
    color:#4070a0; border-bottom:2px solid #0c1e30;
    white-space:nowrap; text-align:right;
    position:sticky; top:0;
}
table.pt thead th.left { text-align:left; }
table.pt thead th.center { text-align:center; }
table.pt tbody tr { border-bottom:1px solid #0a1828; }
table.pt tbody tr:nth-child(even) td { background:#060d1a; }
table.pt tbody tr:hover td { background:#0c1e30 !important; transition:background 0.1s; }
table.pt td {
    padding:9px 16px; text-align:right;
    font-size:0.83rem; font-weight:500;
    color:#3a6080; white-space:nowrap;
}
table.pt td.left  { text-align:left; }
table.pt td.center{ text-align:center; }
table.pt td.sym {
    color:#c8e4ff; font-weight:700; font-size:0.86rem;
    letter-spacing:0.4px; background:#060d18 !important;
}
table.pt td.base  { color:#2a5070; font-weight:600; }
table.pt td.close { color:#2a5070; }
table.pt td.num   { color:#1e3a50; font-size:0.72rem; }
table.pt td.ret   { font-weight:800; font-size:0.85rem; }

/* ── Skip chips ──────────────────────────────────────────────── */
.skip-chip {
    display:inline-block; background:#1a0808;
    border:1px solid #4a1010; border-radius:5px;
    padding:3px 10px; font-size:0.72rem; color:#f87171;
    margin:2px 4px 2px 0; font-family:monospace;
}

hr { border-color:#0c1e30 !important; margin:16px 0 !important; }
</style>
""", unsafe_allow_html=True)


# ── File helpers ──────────────────────────────────────────────────────────────
_DATE_PAT  = re.compile(r"institutional_picks_(\d{2})([a-zA-Z]{3})(\d{4})\.txt$", re.I)
_MONTH_MAP = {m:i+1 for i,m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}

def discover_picks_files() -> Dict[date, Path]:
    if not REPORTS_DIR.exists(): return {}
    result: Dict[date, Path] = {}
    for f in REPORTS_DIR.glob("institutional_picks_*.txt"):
        m = _DATE_PAT.match(f.name)
        if not m: continue
        dd, mon, yyyy = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        mon_n = _MONTH_MAP.get(mon)
        if mon_n:
            try: result[date(yyyy, mon_n, dd)] = f
            except ValueError: pass
    return dict(sorted(result.items(), reverse=True))

def parse_picks_file(path: Path) -> List[str]:
    out, seen = [], set()
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"): continue
        sym = line.split(":",1)[-1].strip().upper()
        if sym and sym not in seen:
            out.append(sym); seen.add(sym)
    return out

# ── Kite helpers ──────────────────────────────────────────────────────────────
def read_token_file(path: Path) -> Dict[str,str]:
    data: Dict[str,str] = {}
    if not path.exists(): return data
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k,v = line.split("=",1); data[k.strip().upper()] = v.strip()
    return data

@st.cache_resource(show_spinner=False)
def get_kite(api_key:str, access_token:str):
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def _norm(s:str)->str:
    return re.sub(r"[^A-Z0-9]","",s.upper().replace("&","AND"))

@st.cache_data(show_spinner=False, ttl=3600)
def load_nse_instruments(api_key:str, access_token:str)->Tuple[Dict,Dict]:
    kite = get_kite(api_key, access_token)
    rows = kite.instruments("NSE")
    exact:Dict[str,int]={}; normd:Dict[str,int]={}
    for r in rows:
        ts=str(r.get("tradingsymbol","")).strip().upper()
        tok=int(r.get("instrument_token",0))
        if not ts or not tok: continue
        itype=str(r.get("instrument_type","")).upper()
        seg=str(r.get("segment","")).upper()
        if itype=="INDEX" or "INDICES" in seg: continue
        exact.setdefault(ts,tok); normd.setdefault(_norm(ts),tok)
    return exact,normd

def resolve_token(exact:Dict,normd:Dict,sym:str)->Optional[int]:
    su=sym.upper().strip()
    return exact.get(su) or normd.get(_norm(su))

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_kite_history(api_key:str,access_token:str,token:int,from_d:date,to_d:date)->pd.DataFrame:
    kite=get_kite(api_key,access_token)
    rows=kite.historical_data(instrument_token=token,
        from_date=datetime.combine(from_d,datetime.min.time()),
        to_date=datetime.combine(to_d,datetime.min.time()),
        interval="day",continuous=False,oi=False)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows)
    df["date"]=pd.to_datetime(df["date"]).dt.date
    df["close"]=pd.to_numeric(df["close"],errors="coerce")
    return df.sort_values("date").reset_index(drop=True)

# ── MySQL / indexbhav ─────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_db_conn():
    return mysql.connector.connect(
        host=DB_HOST,port=DB_PORT,user=DB_USER,password=DB_PASS,
        database=DB_NAME,autocommit=True)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_smallcap250(from_d:date,to_d:date)->pd.DataFrame:
    sql_e="SELECT mktdate,close FROM indexbhav WHERE UPPER(symbol)='NIFTY SMALLCAP 250' AND mktdate BETWEEN %s AND %s ORDER BY mktdate"
    sql_l="SELECT mktdate,close FROM indexbhav WHERE UPPER(symbol) LIKE %s AND mktdate BETWEEN %s AND %s ORDER BY mktdate"
    try:
        conn=get_db_conn()
        df=pd.read_sql(sql_e,conn,params=[from_d,to_d])
        if df.empty:
            for pat in ("%SMALLCAP%250%","%NIFTY%SMALLCAP%"):
                df=pd.read_sql(sql_l,conn,params=[pat,from_d,to_d])
                if not df.empty: break
        if df.empty: return pd.DataFrame(columns=["mktdate","close"])
        df["mktdate"]=pd.to_datetime(df["mktdate"]).dt.date
        df["close"]=pd.to_numeric(df["close"],errors="coerce")
        return df.drop_duplicates("mktdate").sort_values("mktdate").reset_index(drop=True)
    except Exception as e:
        st.warning(f"indexbhav: {e}"); return pd.DataFrame(columns=["mktdate","close"])

# ── Price helpers ─────────────────────────────────────────────────────────────
def close_on_or_before(df:pd.DataFrame,d:date,dc="date",cc="close")->Optional[float]:
    sub=df[df[dc]<=d]; return float(sub.iloc[-1][cc]) if not sub.empty else None

def close_on_or_after(df:pd.DataFrame,d:date,dc="date",cc="close")->Optional[Tuple[float,date]]:
    sub=df[df[dc]>=d]
    if sub.empty: return None
    row=sub.iloc[0]; return float(row[cc]),row[dc]

def pct(base:float,end:float)->float:
    return (end-base)/base*100.0 if base else float("nan")

def _sign(v)->str:
    if v is None or (isinstance(v,float) and pd.isna(v)): return "—"
    return f"+{v:.2f}%" if v>=0 else f"{v:.2f}%"

# ── Core analyser ─────────────────────────────────────────────────────────────
def run_analysis(api_key,access_token,symbols,cutoff,progress):
    fetch_from=cutoff-timedelta(days=5)
    fetch_to=min(TODAY,cutoff+timedelta(days=40))

    bm_df=fetch_smallcap250(fetch_from,fetch_to)
    bm_ret:Dict[str,Optional[float]]={};  bm_asof:Dict[str,Optional[date]]={}
    bm_base=None
    if not bm_df.empty:
        bm_base=close_on_or_before(bm_df,cutoff,"mktdate","close")
        for label,days in PERIODS.items():
            target=cutoff+timedelta(days=days)
            if target>TODAY:
                sub=bm_df[bm_df["mktdate"]>cutoff]
                if not sub.empty:
                    row=sub.iloc[-1]
                    bm_ret[label]=pct(bm_base,float(row["close"])) if bm_base else None
                    bm_asof[label]=row["mktdate"]
                else: bm_ret[label]=bm_asof[label]=None
            else:
                result=close_on_or_after(bm_df,target,"mktdate","close")
                if result and bm_base:
                    bm_ret[label]=pct(bm_base,result[0]); bm_asof[label]=result[1]
                else: bm_ret[label]=bm_asof[label]=None
    else:
        for label in PERIODS: bm_ret[label]=bm_asof[label]=None

    exact,normd=load_nse_instruments(api_key,access_token)
    rows:List[dict]=[]; skipped:List[str]=[]; n=len(symbols)

    for i,sym in enumerate(symbols):
        progress.progress((i+1)/n, text=f"Fetching {sym}  ({i+1}/{n})")
        token=resolve_token(exact,normd,sym)
        if token is None: skipped.append(f"{sym} — not in NSE instruments"); continue
        df_h=fetch_kite_history(api_key,access_token,token,fetch_from,fetch_to)
        time.sleep(SLEEP_S)
        if df_h.empty: skipped.append(f"{sym} — no OHLC"); continue
        base=close_on_or_before(df_h,cutoff)
        if base is None: skipped.append(f"{sym} — no data on cutoff"); continue
        row:dict={"Symbol":sym,"Base Close":base}
        for label,days in PERIODS.items():
            target=cutoff+timedelta(days=days)
            if target>TODAY:
                sub=df_h[df_h["date"]>cutoff]
                if not sub.empty:
                    lr=sub.iloc[-1]
                    row[f"{label}|close"]=float(lr["close"]); row[f"{label}|date"]=lr["date"]
                    row[f"{label}|ret"]=pct(base,float(lr["close"]))
                    row[f"{label}|note"]=f"till {lr['date'].strftime('%d %b')}"
                else:
                    row[f"{label}|close"]=row[f"{label}|date"]=row[f"{label}|ret"]=row[f"{label}|note"]=None
            else:
                result=close_on_or_after(df_h,target)
                if result:
                    row[f"{label}|close"]=result[0]; row[f"{label}|date"]=result[1]
                    row[f"{label}|ret"]=pct(base,result[0]); row[f"{label}|note"]=None
                else:
                    row[f"{label}|close"]=row[f"{label}|date"]=row[f"{label}|ret"]=row[f"{label}|note"]=None
        rows.append(row)
    return pd.DataFrame(rows), bm_ret, bm_asof, skipped

# ── Return cell colour ────────────────────────────────────────────────────────
def _ret_bg(val, bm_val) -> Tuple[str,str]:
    """Returns (bg_color, fg_color) for a return value."""
    if val is None or (isinstance(val,float) and pd.isna(val)):
        return "#060d18","#1a3050"
    diff = abs(val-bm_val) if (bm_val is not None and not pd.isna(bm_val)) else abs(val)
    is_pos = val >= (bm_val if bm_val is not None else 0)
    # 6-tier scale
    G_bg = ["#071a0f","#0a2416","#0f3620","#164d2d","#1a6b35","#22543d"]
    G_fg = ["#22c55e","#22c55e","#4ade80","#ffffff","#ffffff","#ffffff"]
    R_bg = ["#1a0707","#290a0a","#3f1010","#5c1616","#7f1d1d","#991b1b"]
    R_fg = ["#f87171","#f87171","#fca5a5","#ffffff","#ffffff","#ffffff"]
    tier = 0 if diff<2 else 1 if diff<4 else 2 if diff<7 else 3 if diff<10 else 4 if diff<15 else 5
    if is_pos: return G_bg[tier], G_fg[tier]
    return R_bg[tier], R_fg[tier]

# ── HTML table ────────────────────────────────────────────────────────────────
def build_table(df:pd.DataFrame, bm_ret:Dict, cutoff:date) -> str:
    # Header
    def th(txt, cls=""):
        return f'<th class="{cls}">{txt}</th>'

    hdr = "<thead><tr>"
    hdr += th("#","center")
    hdr += th("Stock","left")
    hdr += th(f"Base Close<br><small>{cutoff.strftime('%d %b')}</small>","")
    for label in PERIOD_KEYS:
        target = cutoff + timedelta(days=PERIODS[label])
        partial = target > TODAY
        tag = f"{'⏳ ' if partial else ''}{target.strftime('%d %b')}"
        bm_val = bm_ret.get(label)
        bm_str = f"<small style='color:#0e4028'> BM {_sign(bm_val)}</small>" if bm_val is not None else ""
        hdr += th(f"{label}<br><small>{tag}</small>","")
        hdr += th(f"Ret%{bm_str}","")
    hdr += "</tr></thead>"

    # Body
    body = "<tbody>"
    for i,(_, r) in enumerate(df.iterrows()):
        body += "<tr>"
        body += f'<td class="num center">{i+1}</td>'
        body += f'<td class="sym left">{r["Symbol"]}</td>'
        base = r.get("Base Close")
        body += f'<td class="base">{f"{base:,.2f}" if base else "—"}</td>'
        for label in PERIOD_KEYS:
            c   = r.get(f"{label}|close")
            ret = r.get(f"{label}|ret")
            bm_val = bm_ret.get(label)
            body += f'<td class="close">{f"{c:,.2f}" if c is not None else "—"}</td>'
            bg, fg = _ret_bg(ret, bm_val)
            body += (f'<td class="ret" style="background:{bg};color:{fg};'
                     f'border-left:2px solid {bg}">{_sign(ret)}</td>')
        body += "</tr>"
    body += "</tbody>"
    return f'<div class="pt-wrap"><table class="pt">{hdr}{body}</table></div>'

# ── Custom metric card ────────────────────────────────────────────────────────
def metric_card(label:str, value:str, sub:str, bm_str:str="", is_pos:Optional[bool]=None) -> str:
    cls = "pos" if is_pos is True else ("neg" if is_pos is False else "neu")
    return (
        f'<div class="mc">'
        f'<div class="mc-label">{label}</div>'
        f'<div class="mc-value {cls}">{value}</div>'
        f'<div class="mc-sub">{sub}</div>'
        f'{"<div class=mc-bm>"+bm_str+"</div>" if bm_str else ""}'
        f'</div>'
    )

# ── Custom stat card ──────────────────────────────────────────────────────────
def stat_card(label:str, value:str, delta:str="", pos:Optional[bool]=None) -> str:
    dcls = "pos" if pos is True else ("neg" if pos is False else ""  )
    return (
        f'<div class="stat">'
        f'<div class="stat-label">{label}</div>'
        f'<div class="stat-value">{value}</div>'
        f'{"<div class=stat-delta "+dcls+">"+delta+"</div>" if delta else ""}'
        f'</div>'
    )

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    creds = read_token_file(TOKEN_FILE)
    api_key      = creds.get("API_KEY","")
    access_token = creds.get("ACCESS_TOKEN","")
    if not (api_key and access_token):
        st.error("kite_token.txt missing — run kite_get_access_token.py")
        st.stop()

    st.markdown("""
    <div style="margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #0c1e30;">
      <div style="font-size:1rem;font-weight:900;color:#4a9adf;letter-spacing:0.5px;">
        PICKS PERFORMANCE
      </div>
      <div style="font-size:0.65rem;color:#1a4060;font-weight:600;
           text-transform:uppercase;letter-spacing:2px;margin-top:4px;">
        Institutional · Nifty SC 250
      </div>
    </div>
    """, unsafe_allow_html=True)

    files_map = discover_picks_files()
    if not files_map:
        st.error(f"No institutional_picks_*.txt in {REPORTS_DIR}"); st.stop()

    dates     = list(files_map.keys())
    date_lbls = [d.strftime("%d %b %Y") for d in dates]

    st.markdown('<div style="font-size:0.6rem;font-weight:700;text-transform:uppercase;'
                'letter-spacing:1.8px;color:#1a4060;margin-bottom:6px;">Cutoff Date</div>',
                unsafe_allow_html=True)

    sel_idx   = st.selectbox("Picks date", range(len(dates)),
                              format_func=lambda i: date_lbls[i],
                              label_visibility="collapsed")
    cutoff    = dates[sel_idx]
    picks_path= files_map[cutoff]

    # Analysis windows
    st.markdown('<div style="font-size:0.6rem;font-weight:700;text-transform:uppercase;'
                'letter-spacing:1.8px;color:#1a4060;margin:16px 0 8px 0;">Windows</div>',
                unsafe_allow_html=True)
    for label, days in PERIODS.items():
        target = cutoff + timedelta(days=days)
        done   = target <= TODAY
        lag    = (TODAY - cutoff).days
        dot_c  = "#22c55e" if done else "#3b82f6"
        val_c  = "#4ab870" if done else "#5a90c0"
        sub    = target.strftime("%d %b %Y") + (" ✓" if done else f" ({lag}d)")
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:8px;padding:4px 0;">'
            f'<span style="width:6px;height:6px;border-radius:50%;background:{dot_c};'
            f'flex-shrink:0;margin-top:1px;display:inline-block;"></span>'
            f'<div><div style="font-size:0.78rem;font-weight:700;color:{val_c};">{label}</div>'
            f'<div style="font-size:0.67rem;color:#1a3a56;">{sub}</div></div>'
            f'</div>', unsafe_allow_html=True)

    st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
    run_btn = st.button("▶  Run Analysis", type="primary", use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN AREA
# ══════════════════════════════════════════════════════════════════════════════
symbols = parse_picks_file(picks_path)

# Top bar (always visible, compact)
st.markdown(
    f'<div class="topbar">'
    f'<span class="topbar-title">Institutional Picks</span>'
    f'<span class="topbar-pill">Cutoff <b>{cutoff.strftime("%d %b %Y")}</b></span>'
    f'<span class="topbar-pill"><b>{len(symbols)}</b> stocks</span>'
    f'<span class="topbar-pill">vs <b>Nifty SC 250</b></span>'
    f'</div>',
    unsafe_allow_html=True)

if not run_btn:
    st.markdown(
        '<div style="color:#1a3a58;font-size:0.85rem;padding:40px 0;text-align:center;">'
        '← Select a cutoff date and click <strong style="color:#3b82f6">▶ Run Analysis</strong>'
        '</div>', unsafe_allow_html=True)
    with st.expander(f"📋 {len(symbols)} stocks in this file"):
        cols = st.columns(5)
        for i,sym in enumerate(symbols): cols[i%5].markdown(f"`{sym}`")
    st.stop()

# ── Run ───────────────────────────────────────────────────────────────────────
prog = st.progress(0, text="Initialising…")
df, bm_ret, bm_asof, skipped = run_analysis(api_key, access_token, symbols, cutoff, prog)
prog.empty()

if df.empty:
    st.error("No data returned. Check Kite token validity."); st.stop()

# ── Benchmark cards ───────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">Nifty Smallcap 250 Benchmark</div>', unsafe_allow_html=True)
bm_html = '<div class="metric-row">'
for label in PERIOD_KEYS:
    val  = bm_ret.get(label)
    asof = bm_asof.get(label)
    target = cutoff + timedelta(days=PERIODS[label])
    partial = target > TODAY
    asof_str = f"{'⏳ ' if partial else ''}as of {asof.strftime('%d %b')}" if asof else "N/A"
    is_pos = (val > 0) if val is not None else None
    bm_html += metric_card(label, _sign(val), asof_str, "", is_pos)
bm_html += "</div>"
st.markdown(bm_html, unsafe_allow_html=True)

# ── Portfolio snapshot ────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">Portfolio Snapshot</div>', unsafe_allow_html=True)
stat_html = '<div class="stat-row">'
for label in PERIOD_KEYS:
    ret_col = f"{label}|ret"
    if ret_col not in df.columns: continue
    series  = df[ret_col].dropna()
    bm_val  = bm_ret.get(label)
    n       = len(series)
    avg     = series.mean() if n else None
    excess  = (avg - bm_val) if avg is not None and bm_val is not None else None
    n_beat  = int((series > bm_val).sum()) if bm_val is not None and n else 0
    pct_b   = n_beat/n*100 if n else 0

    stat_html += stat_card(
        f"{label} Avg",
        _sign(avg) if avg is not None else "—",
        (f"{excess:+.2f}% vs BM" if excess is not None else ""),
        pos=(excess>=0) if excess is not None else None)
    stat_html += stat_card(
        f"Beat BM ({label})",
        f"{n_beat} / {n}",
        f"{pct_b:.0f}% of picks",
        pos=(pct_b>=50))
stat_html += "</div>"
st.markdown(stat_html, unsafe_allow_html=True)

# ── Stock table ───────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label">Stock Returns</div>', unsafe_allow_html=True)

sort_opts = {"1 Month Ret%":"1 Month|ret","15 Days Ret%":"15 Days|ret",
             "1 Week Ret%":"1 Week|ret","Symbol (A→Z)":"Symbol"}

c1, c2 = st.columns([1, 5])
with c1:
    sort_choice = st.selectbox("Sort", list(sort_opts.keys()), index=0,
                                label_visibility="collapsed")
sort_col = sort_opts[sort_choice]
asc = sort_choice == "Symbol (A→Z)"
df_sorted = df.sort_values(sort_col, ascending=asc, na_position="last").reset_index(drop=True)
st.markdown(build_table(df_sorted, bm_ret, cutoff), unsafe_allow_html=True)

# ── Charts ────────────────────────────────────────────────────────────────────
st.markdown('<div class="sec-label" style="margin-top:24px;">Charts</div>', unsafe_allow_html=True)
tab1, tab2 = st.tabs(["📊 Return Bars", "🔵 1W vs 1M"])

PLOT_BG = "#060c18"
GRID_C  = "#0c1e30"

with tab1:
    for label in PERIOD_KEYS:
        ret_col = f"{label}|ret"
        if ret_col not in df.columns: continue
        bm_val  = bm_ret.get(label)
        partial = cutoff + timedelta(days=PERIODS[label]) > TODAY
        cdf = df[["Symbol", ret_col]].dropna().copy()
        cdf.columns = ["Symbol","Return%"]
        cdf = cdf.sort_values("Return%", ascending=False)
        cdf["col"] = cdf["Return%"].apply(
            lambda v: "#22c55e" if (bm_val is None or v>=bm_val) else "#ef4444")
        fig = go.Figure(go.Bar(
            x=cdf["Symbol"], y=cdf["Return%"],
            marker_color=cdf["col"], marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>%{y:.2f}%<extra></extra>"))
        if bm_val is not None:
            asof_l = bm_asof.get(label)
            fig.add_hline(y=bm_val, line_dash="dot", line_color="#f59e0b", line_width=1.5,
                annotation_text=f"  SC250 {bm_val:+.2f}%{' '+asof_l.strftime('%d %b') if asof_l else ''}",
                annotation_font_color="#f59e0b", annotation_position="top left")
        fig.update_layout(
            title=dict(text=f"{label} Returns{'  ⏳' if partial else ''}", font_size=13,
                       font_color="#2a5070", x=0),
            plot_bgcolor=PLOT_BG, paper_bgcolor=PLOT_BG, font_color="#4a7090",
            xaxis=dict(tickangle=-45, gridcolor=GRID_C, tickfont_size=11, tickfont_color="#2a5070"),
            yaxis=dict(gridcolor=GRID_C, zeroline=True, zerolinecolor="#102030",
                       ticksuffix="%", tickfont_color="#2a5070"),
            margin=dict(t=40,b=100,l=50,r=20), height=340, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    if "1 Week|ret" in df.columns and "1 Month|ret" in df.columns:
        sc = df[["Symbol","1 Week|ret","1 Month|ret"]].dropna().copy()
        sc.columns = ["Symbol","1W","1M"]
        bm_1w = bm_ret.get("1 Week"); bm_1m = bm_ret.get("1 Month")
        colours = []
        for _,row in sc.iterrows():
            aw = bm_1w is None or row["1W"]>=bm_1w
            am = bm_1m is None or row["1M"]>=bm_1m
            colours.append("#22c55e" if aw and am else ("#ef4444" if not aw and not am else "#f59e0b"))
        fig2 = go.Figure(go.Scatter(
            x=sc["1W"], y=sc["1M"], mode="markers+text", text=sc["Symbol"],
            textposition="top center", textfont=dict(size=9, color="#2a5070"),
            marker=dict(color=colours, size=10, line=dict(width=1, color="#0c1e30")),
            hovertemplate="<b>%{text}</b><br>1W %{x:.2f}%  1M %{y:.2f}%<extra></extra>"))
        if bm_1w is not None:
            fig2.add_vline(x=bm_1w, line_dash="dot", line_color="#f59e0b", line_width=1.2,
                annotation_text=" BM 1W", annotation_font_color="#f59e0b")
        if bm_1m is not None:
            fig2.add_hline(y=bm_1m, line_dash="dot", line_color="#f59e0b", line_width=1.2,
                annotation_text=" BM 1M", annotation_font_color="#f59e0b")
        fig2.update_layout(
            plot_bgcolor=PLOT_BG, paper_bgcolor=PLOT_BG, font_color="#4a7090",
            xaxis=dict(title="1 Week %", gridcolor=GRID_C, zeroline=True,
                       zerolinecolor="#102030", tickfont_color="#2a5070"),
            yaxis=dict(title="1 Month %", gridcolor=GRID_C, zeroline=True,
                       zerolinecolor="#102030", tickfont_color="#2a5070"),
            height=480, margin=dict(t=20,b=60,l=60,r=20))
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('<div style="font-size:0.7rem;color:#1a3a56;text-align:center;">'
                    '🟢 Beat both · 🟡 Mixed · 🔴 Lagged both · '
                    '<span style="color:#f59e0b">dotted</span> = benchmark</div>',
                    unsafe_allow_html=True)

# ── Skipped ───────────────────────────────────────────────────────────────────
if skipped:
    with st.expander(f"⚠️ {len(skipped)} stocks skipped"):
        st.markdown(" ".join(f'<span class="skip-chip">{s}</span>' for s in skipped),
                    unsafe_allow_html=True)

# ── Export ────────────────────────────────────────────────────────────────────
st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
exp = df.copy().rename(columns={"Symbol":"Stock"})
for label in PERIOD_KEYS:
    bm_val=bm_ret.get(label); rc=f"{label}|ret"
    if rc in exp.columns and bm_val is not None:
        exp[f"{label}|vs_bm"]=exp[rc]-bm_val
st.download_button("📥 Download CSV", exp.to_csv(index=False).encode(),
    file_name=f"perf_{cutoff.strftime('%d%b%Y').lower()}.csv", mime="text/csv")
