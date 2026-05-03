"""
Localhost Apps Launchpad
========================
A small Flask dashboard that lets you start / stop / open each of your
localhost apps from a single browser page. It never modifies any of the
application scripts - it only spawns the exact same commands you would
type by hand.

Run:
    python launchpad_server.py
Open:
    http://localhost:9000
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

from flask import Flask, jsonify, render_template_string, request

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent   # Monumental/
SETTINGS_FP = Path(__file__).resolve().parent / "launchpad_settings.json"
PY          = sys.executable                           # current python
LAUNCHPAD_PORT = int(os.environ.get("LAUNCHPAD_PORT", "9000"))

# Per-user settings that persist between runs (editable from the UI)
DEFAULT_SETTINGS: Dict = {
    "trade_plan_html": r"C:\Users\shada\Monumental\TRADEP_12_1.htm",
    "trade_plan_port": 8765,
    "ip_fire_port":    8501,
    "bhav_port":       5000,
    "perf_port":       8502,
    "screener_port":   5001,
}


def load_settings() -> Dict:
    if SETTINGS_FP.exists():
        try:
            data = json.loads(SETTINGS_FP.read_text(encoding="utf-8"))
            merged = {**DEFAULT_SETTINGS, **data}
            return merged
        except Exception:
            pass
    return dict(DEFAULT_SETTINGS)


def save_settings(data: Dict) -> None:
    SETTINGS_FP.write_text(json.dumps(data, indent=2), encoding="utf-8")


def build_apps(settings: Dict) -> Dict[str, Dict]:
    return {
        "trade_plan": {
            "name": "Trade Plan Server",
            "desc": "Local API for bhav-backed symbol lookup and saves.",
            "port": int(settings["trade_plan_port"]),
            "cmd":  [PY, "trade_plan_server.py",
                     "--html-path", str(settings["trade_plan_html"]),
                     "--port",      str(settings["trade_plan_port"])],
            "cwd":  str(BASE_DIR),
            "icon": "TP",
        },
        "ip_fire": {
            "name": "IP Fire Dashboard",
            "desc": "Institutional Picks - Fire status (Streamlit).",
            "port": int(settings["ip_fire_port"]),
            "cmd":  [PY, "-m", "streamlit", "run", "ip_dashboard.py",
                     "--server.port", str(settings["ip_fire_port"]),
                     "--server.headless", "true",
                     "--browser.gatherUsageStats", "false"],
            "cwd":  str(BASE_DIR),
            "icon": "IP",
        },
        "bhav_viewer": {
            "name": "Bhav Viewer",
            "desc": "NSE BHAV Stock Viewer (Flask).",
            "port": int(settings["bhav_port"]),
            "cmd":  [PY, "app.py"],
            "cwd":  str(BASE_DIR),
            "icon": "BV",
        },
        "bhav_screener": {
            "name": "Bhav Screener",
            "desc": "Low-volume / low-volatility screener (Flask).",
            "port": int(settings["screener_port"]),
            "cmd":  [PY, "bhav_screener.py", "--port", str(settings["screener_port"])],
            "cwd":  str(BASE_DIR),
            "icon": "BS",
        },
        "perf_tracker": {
            "name": "Performance Tracker",
            "desc": "Picks Performance tracker (Streamlit).",
            "port": int(settings["perf_port"]),
            "cmd":  [PY, "-m", "streamlit", "run", "performance_tracker.py",
                     "--server.port", str(settings["perf_port"]),
                     "--server.headless", "true",
                     "--browser.gatherUsageStats", "false"],
            "cwd":  str(BASE_DIR),
            "icon": "PT",
        },
    }


# running subprocess handles we started (for the 'managed' tag)
procs: Dict[str, subprocess.Popen] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def is_port_listening(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.25)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False


def find_pids_on_port(port: int) -> list[str]:
    """Windows-only: uses netstat to find PIDs listening on a port."""
    pids: list[str] = []
    if os.name != "nt":
        return pids
    try:
        out = subprocess.check_output(["netstat", "-ano"], text=True, errors="ignore")
    except Exception:
        return pids
    for line in out.splitlines():
        line = line.strip()
        if "LISTENING" not in line:
            continue
        # match :<port> with word boundary
        if f":{port} " not in line and not line.endswith(f":{port}"):
            # loose match: look for ':PORT' followed by whitespace
            parts = line.split()
            match = False
            for p in parts:
                if p.endswith(f":{port}"):
                    match = True
                    break
            if not match:
                continue
        parts = line.split()
        pid = parts[-1]
        if pid.isdigit():
            pids.append(pid)
    return pids


def kill_pid(pid: str) -> bool:
    try:
        if os.name == "nt":
            subprocess.call(["taskkill", "/F", "/PID", str(pid)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.call(["kill", "-9", str(pid)])
        return True
    except Exception:
        return False


def spawn(cfg: Dict) -> subprocess.Popen:
    kwargs = {"cwd": cfg["cwd"]}
    if os.name == "nt":
        # New console so the user sees the app's logs
        CREATE_NEW_CONSOLE = 0x00000010
        kwargs["creationflags"] = CREATE_NEW_CONSOLE
    return subprocess.Popen(cfg["cmd"], **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# Flask app
# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Localhost Launchpad</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root{
    --bg:#070d18; --panel:#0f1826; --panel2:#152238; --border:#1f2d46;
    --text:#e6edf7; --muted:#8ea0bd; --accent:#4f8cff; --accent2:#6aa6ff;
    --ok:#2bd576; --bad:#ff5d6c; --warn:#ffbf47;
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0;background:var(--bg);color:var(--text);
            font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,Roboto,sans-serif;}
  .wrap{max-width:1200px;margin:0 auto;padding:28px 24px 64px}
  header{display:flex;align-items:center;justify-content:space-between;gap:16px;
         padding-bottom:20px;border-bottom:1px solid var(--border);margin-bottom:28px}
  h1{font-size:22px;margin:0;font-weight:700;letter-spacing:.2px}
  h1 .dot{display:inline-block;width:10px;height:10px;border-radius:50%;
          background:var(--accent);box-shadow:0 0 12px var(--accent);margin-right:10px;vertical-align:middle}
  .sub{color:var(--muted);font-size:13px;margin-top:4px}
  .refresh{color:var(--muted);font-size:12px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:18px}
  .card{background:linear-gradient(180deg,var(--panel) 0%,var(--panel2) 100%);
        border:1px solid var(--border);border-radius:14px;padding:18px 18px 16px;
        display:flex;flex-direction:column;gap:12px;transition:transform .15s,border-color .15s;}
  .card:hover{border-color:#2b3d5c;transform:translateY(-1px)}
  .row{display:flex;align-items:center;gap:12px;justify-content:space-between}
  .title{display:flex;align-items:center;gap:12px;min-width:0}
  .icon{width:40px;height:40px;border-radius:10px;background:#1a2842;
        display:flex;align-items:center;justify-content:center;font-weight:700;
        color:var(--accent2);font-size:14px;letter-spacing:.5px;flex-shrink:0}
  .name{font-weight:700;font-size:16px;line-height:1.2}
  .meta{color:var(--muted);font-size:12px;margin-top:2px}
  .desc{color:#b9c5db;font-size:13px;line-height:1.45;min-height:36px}
  .status{display:inline-flex;align-items:center;gap:8px;font-size:12px;
          padding:5px 10px;border-radius:999px;background:#111c30;
          border:1px solid var(--border);color:var(--muted);white-space:nowrap}
  .status .pulse{width:8px;height:8px;border-radius:50%;background:var(--muted)}
  .status.ok{color:var(--ok);border-color:#1e4d38}
  .status.ok .pulse{background:var(--ok);box-shadow:0 0 8px var(--ok);animation:pulse 1.8s ease-in-out infinite}
  .status.bad{color:#e6a0a8;border-color:#4a2530}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .actions{display:flex;gap:8px;flex-wrap:wrap}
  button,a.btn{appearance:none;border:1px solid var(--border);background:#182642;
        color:var(--text);padding:8px 14px;border-radius:10px;font-size:13px;
        font-weight:600;cursor:pointer;text-decoration:none;transition:all .15s;
        display:inline-flex;align-items:center;gap:6px}
  button:hover,a.btn:hover{background:#1f3156;border-color:#32476c}
  button.primary{background:var(--accent);border-color:var(--accent);color:#fff}
  button.primary:hover{background:var(--accent2);border-color:var(--accent2)}
  button.danger{background:#3a1722;border-color:#5b2330;color:#ffb5bf}
  button.danger:hover{background:#4b1d2c;border-color:#6d2b3a}
  button:disabled{opacity:.4;cursor:not-allowed}
  .settings{background:#0b1222;border:1px solid var(--border);border-radius:14px;
            padding:16px 18px;margin-bottom:24px}
  .settings summary{cursor:pointer;color:var(--accent2);font-weight:600;font-size:14px}
  .settings .grid-set{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-top:14px}
  .settings label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--muted)}
  .settings input{background:#0a1424;border:1px solid var(--border);color:var(--text);
                  padding:8px 10px;border-radius:8px;font-size:13px;font-family:inherit}
  .settings input:focus{outline:none;border-color:var(--accent)}
  .settings .save-row{margin-top:12px;display:flex;align-items:center;gap:12px}
  .toast{position:fixed;right:20px;bottom:20px;background:#17253f;color:#fff;
         padding:10px 16px;border-radius:10px;border:1px solid var(--border);
         font-size:13px;opacity:0;transform:translateY(10px);transition:all .25s;pointer-events:none;z-index:9}
  .toast.show{opacity:1;transform:translateY(0)}
  .toast.ok{border-color:#1e4d38}
  .toast.err{border-color:#5b2330;background:#3a1722}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1><span class="dot"></span>Localhost Launchpad</h1>
      <div class="sub">Start, stop, and open your localhost apps from one place.</div>
    </div>
    <div class="refresh">auto-refresh: 2s</div>
  </header>

  <details class="settings">
    <summary>Settings (ports & trade-plan HTML path)</summary>
    <div class="grid-set">
      <label>Trade Plan HTML
        <input id="s_tp_html" type="text">
      </label>
      <label>Trade Plan port
        <input id="s_tp_port" type="number" min="1024" max="65535">
      </label>
      <label>IP Fire port
        <input id="s_ip_port" type="number" min="1024" max="65535">
      </label>
      <label>Bhav Viewer port
        <input id="s_bhav_port" type="number" min="1024" max="65535">
      </label>
      <label>Performance port
        <input id="s_perf_port" type="number" min="1024" max="65535">
      </label>
      <label>Bhav Screener port
        <input id="s_screener_port" type="number" min="1024" max="65535">
      </label>
    </div>
    <div class="save-row">
      <button class="primary" onclick="saveSettings()">Save</button>
      <span class="sub" id="s_hint">Changes apply to the next Start. Running apps keep their current port.</span>
    </div>
  </details>

  <div id="grid" class="grid"></div>
</div>

<div id="toast" class="toast"></div>

<script>
const state = { apps: {}, settings: {} };

async function api(path, opts={}){
  const r = await fetch(path, opts);
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

function toast(msg, kind='ok'){
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show ' + kind;
  clearTimeout(toast._t);
  toast._t = setTimeout(()=>{ t.className='toast ' + kind; }, 2200);
}

function render(){
  const grid = document.getElementById('grid');
  const apps = state.apps;
  const keys = Object.keys(apps);
  grid.innerHTML = keys.map(k => {
    const a = apps[k];
    const running = a.running;
    const stClass = running ? 'ok' : 'bad';
    const stText  = running ? 'Running' : 'Stopped';
    return `
      <div class="card" id="card-${k}">
        <div class="row">
          <div class="title">
            <div class="icon">${a.icon}</div>
            <div>
              <div class="name">${a.name}</div>
              <div class="meta">port ${a.port} · <a style="color:var(--accent2);text-decoration:none" href="${a.url}" target="_blank">${a.url}</a></div>
            </div>
          </div>
          <div class="status ${stClass}"><span class="pulse"></span>${stText}</div>
        </div>
        <div class="desc">${a.desc}</div>
        <div class="actions">
          <button class="primary" onclick="doStart('${k}')" ${running?'disabled':''}>Start</button>
          <button class="danger"  onclick="doStop('${k}')"  ${running?'':'disabled'}>Stop</button>
          <a class="btn" href="${a.url}" target="_blank" ${running?'':'style="opacity:.45;pointer-events:none"'}>Open ↗</a>
        </div>
      </div>`;
  }).join('');
}

async function refresh(){
  try{
    const data = await api('/api/status');
    state.apps = data.apps;
    render();
  }catch(e){ /* silent */ }
}

async function doStart(id){
  try{
    toast('Starting '+state.apps[id].name+'…');
    const r = await api('/api/start/'+id, {method:'POST'});
    if(r.ok){ toast(state.apps[id].name+' starting (pid '+(r.pid||'?')+')'); }
    else    { toast('Start failed: '+(r.error||'unknown'), 'err'); }
  }catch(e){ toast('Start failed: '+e.message, 'err'); }
  setTimeout(refresh, 500);
}

async function doStop(id){
  try{
    toast('Stopping '+state.apps[id].name+'…');
    const r = await api('/api/stop/'+id, {method:'POST'});
    if(r.ok){ toast(state.apps[id].name+' stopped'); }
    else    { toast('Stop failed: '+(r.error||'unknown'), 'err'); }
  }catch(e){ toast('Stop failed: '+e.message, 'err'); }
  setTimeout(refresh, 400);
}

async function loadSettings(){
  const s = await api('/api/settings');
  state.settings = s;
  document.getElementById('s_tp_html').value   = s.trade_plan_html;
  document.getElementById('s_tp_port').value   = s.trade_plan_port;
  document.getElementById('s_ip_port').value   = s.ip_fire_port;
  document.getElementById('s_bhav_port').value = s.bhav_port;
  document.getElementById('s_perf_port').value = s.perf_port;
  document.getElementById('s_screener_port').value = s.screener_port;
}

async function saveSettings(){
  const body = {
    trade_plan_html: document.getElementById('s_tp_html').value,
    trade_plan_port: +document.getElementById('s_tp_port').value,
    ip_fire_port:    +document.getElementById('s_ip_port').value,
    bhav_port:       +document.getElementById('s_bhav_port').value,
    perf_port:       +document.getElementById('s_perf_port').value,
    screener_port:   +document.getElementById('s_screener_port').value,
  };
  try{
    const r = await api('/api/settings', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    if(r.ok){ toast('Settings saved'); refresh(); }
    else    { toast('Save failed: '+(r.error||''), 'err'); }
  }catch(e){ toast('Save failed: '+e.message, 'err'); }
}

loadSettings();
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/status")
def api_status():
    settings = load_settings()
    cfg = build_apps(settings)
    out = {}
    for k, v in cfg.items():
        running = is_port_listening(v["port"])
        # clean up dead managed procs
        p = procs.get(k)
        if p and p.poll() is not None:
            procs.pop(k, None)
        out[k] = {
            "name":    v["name"],
            "desc":    v["desc"],
            "port":    v["port"],
            "url":     f"http://localhost:{v['port']}",
            "running": running,
            "managed": k in procs,
            "icon":    v["icon"],
        }
    return jsonify({"apps": out})


@app.route("/api/start/<app_id>", methods=["POST"])
def api_start(app_id):
    settings = load_settings()
    cfg = build_apps(settings)
    if app_id not in cfg:
        return jsonify({"ok": False, "error": "unknown app"}), 404
    if is_port_listening(cfg[app_id]["port"]):
        return jsonify({"ok": True, "msg": "already running"})
    try:
        p = spawn(cfg[app_id])
        procs[app_id] = p
        return jsonify({"ok": True, "pid": p.pid})
    except FileNotFoundError as e:
        return jsonify({"ok": False, "error": f"command not found: {e}"}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/stop/<app_id>", methods=["POST"])
def api_stop(app_id):
    settings = load_settings()
    cfg = build_apps(settings)
    if app_id not in cfg:
        return jsonify({"ok": False, "error": "unknown app"}), 404
    port = cfg[app_id]["port"]

    # 1) terminate our managed process if any
    p = procs.pop(app_id, None)
    if p and p.poll() is None:
        try:
            p.terminate()
        except Exception:
            pass

    # 2) force-kill anything listening on that port (covers externally-started)
    killed = []
    for pid in find_pids_on_port(port):
        if kill_pid(pid):
            killed.append(pid)

    return jsonify({"ok": True, "killed": killed})


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    if request.method == "GET":
        return jsonify(load_settings())
    data = request.get_json(silent=True) or {}
    current = load_settings()
    # merge only known keys
    for k in DEFAULT_SETTINGS.keys():
        if k in data:
            current[k] = data[k]
    try:
        save_settings(current)
        return jsonify({"ok": True, "settings": current})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    print(f"[launchpad] serving on http://localhost:{LAUNCHPAD_PORT}")
    print(f"[launchpad] project folder: {BASE_DIR}")
    # Keep the launchpad in a single process so the in-memory process table
    # stays stable for start/stop/status checks.
    app.run(host="127.0.0.1", port=LAUNCHPAD_PORT, debug=False, use_reloader=False)
