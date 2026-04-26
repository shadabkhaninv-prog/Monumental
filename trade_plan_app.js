const API_BASE = "http://127.0.0.1:8765/api";
let positions = [];
let planDate = todayStr();
let saveTimer = null;
let savePath = "";
let storageOnline = false;
let saveSeq = 0;
let dirty = false;
let currentView = "dashboard";
let selectedPositionId = null;
let dashboardItems = [];
let latestDashboardDate = todayStr();
let dayMeta = { open_positions_count: 0, exited_today_count: 0, planning_count: 0, exposure: 0, exposure_pct: null };
let settings = {
  available_capital: null,
  daily_risk: null,
  per_position_risk: null,
  stop_loss_pct: 2.0,
  checklist_groups: []
};
let goalTracker = { exposure_items: [], r_progress_items: [], plan_stats_items: [], latest_date: todayStr() };
let streakReport = {
  summary: {},
  campaigns: [],
  closed_campaigns: [],
  open_campaigns_list: [],
  latest_trade_date: todayStr(),
  stop_loss_pct: 2.0,
  tradebook_path: ""
};
let planStreakReport = {
  summary: {},
  campaigns: [],
  closed_campaigns: [],
  open_campaigns_list: [],
  latest_trade_date: todayStr(),
  history_start_date: "2026-04-17",
  stop_loss_pct: 2.0,
  tradebook_path: ""
};
let portfolioSimReport = {
  summary: {},
  daily: [],
  positions: [],
  open_positions: [],
  campaigns: [],
  starting_capital: 3000000,
  per_position_budget: 300000,
  latest_market_date: todayStr(),
  tradebook_path: ""
};
let goalTrackerTab = "exposure";
let kitePublicIp = "";
const QUIET = new Set(["symbol", "merits", "trailNote", "mgmt.fe", "mgmt.fsl", "mgmt.ft", "mgmt.fbe", "mgmt.note"]);

function todayStr() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}
function pkey(d) { return "tp_v3_" + d; }
function normalizeSymbol(v) { return (v || "").toUpperCase().replace(/[^A-Z0-9]/g, ""); }
function fi(v) { return Math.abs(Number(v || 0)).toLocaleString("en-IN", { maximumFractionDigits: 0 }); }
function money(v) { return v == null ? "-" : "Rs " + fi(v); }
function price2(v) { return v == null ? "-" : "Rs " + Number(v).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
function pricePlain(v) { return v == null ? "-" : Number(v).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
function priceSL(v) {
  if (v == null || v === "") return "-";
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString("en-IN", { minimumFractionDigits: 0, maximumFractionDigits: 2 });
}
function roundToTick(value, tickSize = 0.05) {
  const tick = Number(tickSize);
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isFinite(tick) || tick <= 0) return null;
  return Math.round(Math.round(n / tick) * tick * 100) / 100;
}
function stopLimitPreview(triggerPrice, tickSize = 0.05) {
  const trigger = roundToTick(triggerPrice, tickSize);
  if (trigger == null) return null;
  const tick = Number(tickSize);
  const limit = roundToTick(Math.max(trigger - tick, tick), tick);
  if (limit == null) return null;
  return { trigger, limit };
}
function pct(v) { return v == null ? "-" : Number(v).toFixed(1) + "%"; }
function sgn(v) { return v >= 0 ? "+" : "-"; }
function wait(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function e(id) { return document.getElementById(id); }
function fmtDateLabel(value) {
  if (!value) return "";
  const [y, m, d] = value.split("-");
  if (!y || !m || !d) return value;
  return `${d}-${m}-${y}`;
}

function normalizeIsoDate(value, fallback = "") {
  const raw = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const year = Number(raw.slice(0, 4));
    const month = Number(raw.slice(5, 7));
    const day = Number(raw.slice(8, 10));
    if (year >= 2000 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return raw;
    }
  }
  if (/^\d{2}-\d{2}-\d{4}$/.test(raw)) {
    const [day, month, year] = raw.split("-");
    const iso = `${year}-${month}-${day}`;
    const y = Number(year), m = Number(month), d = Number(day);
    if (y >= 2000 && y <= 2100 && m >= 1 && m <= 12 && d >= 1 && d <= 31) {
      return iso;
    }
  }
  return String(fallback || "").trim();
}

function escHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fmtThoughtStamp(value) {
  const ts = value ? new Date(value) : new Date();
  if (Number.isNaN(ts.getTime())) return "";
  return ts.toLocaleString([], { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function normalizeThoughtLog(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map(item => {
    const text = String(item && item.text != null ? item.text : "").trim();
    if (!text) return null;
    return {
      ts: item && item.ts ? item.ts : new Date().toISOString(),
      tag: String(item && item.tag ? item.tag : "NOTE").toUpperCase(),
      text
    };
  }).filter(Boolean).slice(-100);
}

function getThoughtLog(p) {
  return normalizeThoughtLog(p && p.thoughtLog);
}

function defaultChecklistGroups() {
  return [
    {
      title: "Entry",
      count: 2,
      items: [
        "Did not chase - entered at plan",
        "Sized to risk, not conviction",
        ""
      ]
    },
    {
      title: "Holding",
      count: 2,
      items: [
        "Held winners according to plan",
        "Did not interfere with structure",
        ""
      ]
    },
    {
      title: "Exit",
      count: 2,
      items: [
        "Moved or honored SL on plan",
        "Moved SL to breakeven when earned",
        ""
      ]
    }
  ];
}

function normalizeChecklistGroups(raw) {
  const defaults = defaultChecklistGroups();
  if (!Array.isArray(raw)) {
    return defaults;
  }
  return defaults.map((fallback, idx) => {
    const source = raw[idx] && typeof raw[idx] === "object" ? raw[idx] : {};
    const title = String(source.title || fallback.title || "").trim() || fallback.title;
    const items = Array.from({ length: 3 }, (_v, itemIdx) => {
      return String(Array.isArray(source.items) ? (source.items[itemIdx] || "") : "").trim();
    });
    let count = Number.parseInt(source.count, 10);
    if (!Number.isFinite(count)) {
      count = items.filter(Boolean).length;
    }
    count = Math.max(0, Math.min(3, count));
    return { title, count, items };
  });
}

function getChecklistGroups() {
  return normalizeChecklistGroups(settings.checklist_groups);
}

function defaultChecklistStateMatrix() {
  return defaultChecklistGroups().map(() => [false, false, false]);
}

function normalizeChecklistStateMatrix(raw) {
  const defaults = defaultChecklistStateMatrix();
  if (!Array.isArray(raw)) return defaults;
  return defaults.map((row, gi) => {
    const source = Array.isArray(raw[gi]) ? raw[gi] : [];
    return [0, 1, 2].map(ii => Boolean(source[ii]));
  });
}

function legacyChecklistStateMatrix(rawMgmt) {
  const matrix = defaultChecklistStateMatrix();
  if (!rawMgmt || typeof rawMgmt !== "object") return matrix;
  matrix[0][0] = Boolean(rawMgmt.fe);
  matrix[0][1] = Boolean(rawMgmt.ft);
  matrix[1][0] = Boolean(rawMgmt.fsl);
  matrix[2][0] = Boolean(rawMgmt.fbe);
  return matrix;
}

function legacyChecklistFieldsFromGroups(groups) {
  const safeGroups = normalizeChecklistGroups(groups);
  const entry = safeGroups[0]?.items || ["", "", ""];
  const holding = safeGroups[1]?.items || ["", "", ""];
  const exit = safeGroups[2]?.items || ["", "", ""];
  return {
    checklist_entry_1: String(entry[0] || ""),
    checklist_entry_2: String(entry[1] || ""),
    checklist_risk_1: String(holding[0] || ""),
    checklist_risk_2: String(exit[0] || "")
  };
}

function newPos() {
  return {
    id: "p" + Date.now() + Math.random().toString(36).slice(2, 6),
    symbol: "",
    merits: "",
    conviction: 3,
    cmp: null,
    planEntry: null,
    planSL: null,
    intraSL: null,
    intraRiskPct: 30,
    riskAmount: settings.per_position_risk != null ? Number(settings.per_position_risk) : null,
    actualEntry: null,
    overnightEntry: null,
    overnightQty: null,
    overnightSL: null,
    actualQty: null,
    intraQty: null,
    entryDate: "",
    posHigh: null,
    trailOverride: null,
    trailNote: "",
    thoughtTag: "NOTE",
    thoughtLog: [],
    movedBE: false,
    trims: [
      { pct: 3, type: "fixed", ap: null, sq: null, dt: "", done: false },
      { pct: 10, type: "trail", ap: null, sq: null, dt: "", done: false },
      { pct: 15, type: "fixed", ap: null, sq: null, dt: "", done: false },
      { pct: 25, type: "fixed", ap: null, sq: null, dt: "", done: false }
    ],
    mgmt: { checklist: defaultChecklistStateMatrix(), note: "" },
    collapsed: false
  };
}

function hydratePos(raw) {
  const base = newPos();
  const pos = Object.assign({}, base, raw || {});
  pos.mgmt = Object.assign({}, base.mgmt, (raw && raw.mgmt) || {});
  pos.mgmt.checklist = normalizeChecklistStateMatrix(pos.mgmt.checklist || legacyChecklistStateMatrix(pos.mgmt));
  pos.entryDate = normalizeIsoDate(pos.entryDate, "");
  pos.trims = base.trims.map((t, i) => {
    const src = Object.assign({}, t, ((raw && raw.trims) || [])[i] || {});
    src.dt = normalizeIsoDate(src.dt, i === 0 ? pos.entryDate : "");
    return src;
  });
  pos.thoughtTag = String(pos.thoughtTag || "NOTE").toUpperCase();
  pos.thoughtLog = normalizeThoughtLog(pos.thoughtLog);
  pos.symbol = normalizeSymbol(pos.symbol);
  return pos;
}

function dedupePositionsById(list) {
  const seen = new Map();
  (Array.isArray(list) ? list : []).forEach(item => {
    if (!item || typeof item !== "object") return;
    const key = String(item.id || "").trim() || `${normalizeSymbol(item.symbol)}|${String(item.entryDate || "")}|${String(item.actualEntry || item.overnightEntry || "")}`;
    seen.set(key, item);
  });
  return Array.from(seen.values());
}

async function api(path, options) {
  const response = await fetch(API_BASE + path, Object.assign({
    headers: { "Content-Type": "application/json" },
    cache: "no-store"
  }, options || {}));
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.message || ("HTTP " + response.status));
  return payload;
}

function setSaveState(text, kind, title) {
  const el = e("save-st");
  if (!el) return;
  el.textContent = text;
  el.className = "save-st" + (kind === "saved" ? " saved" : "");
  el.title = title || text;
}

function setNav(view) {
  [["nav-dashboard", "dashboard"], ["nav-day", "day"], ["nav-exposure", "exposure"], ["nav-simulation", "simulation"], ["nav-streaks", "streaks"], ["nav-portfolio", "portfolio"], ["nav-settings", "settings"]].forEach(([id, key]) => {
    const node = e(id);
    if (node) node.className = "nav-btn" + (view === key ? " on" : "");
  });
}

function ensureSimulationNav() {
  if (e("nav-simulation")) return;
  const nav = document.querySelector(".topnav");
  if (!nav) return;
  const btn = document.createElement("button");
  btn.className = "nav-btn";
  btn.id = "nav-simulation";
  btn.type = "button";
  btn.textContent = "Simulation";
  btn.onclick = () => goSimulation();
  const settingsBtn = e("nav-settings");
  if (settingsBtn && settingsBtn.parentNode === nav) nav.insertBefore(btn, settingsBtn);
  else nav.appendChild(btn);
}

function ensureStreaksNav() {
  if (e("nav-streaks")) return;
  const nav = document.querySelector(".topnav");
  if (!nav) return;
  const btn = document.createElement("button");
  btn.className = "nav-btn";
  btn.id = "nav-streaks";
  btn.type = "button";
  btn.textContent = "Streaks";
  btn.onclick = () => goStreaks();
  const settingsBtn = e("nav-settings");
  if (settingsBtn && settingsBtn.parentNode === nav) nav.insertBefore(btn, settingsBtn);
  else nav.appendChild(btn);
}

function ensurePortfolioNav() {
  if (e("nav-portfolio")) return;
  const nav = document.querySelector(".topnav");
  if (!nav) return;
  const btn = document.createElement("button");
  btn.className = "nav-btn";
  btn.id = "nav-portfolio";
  btn.type = "button";
  btn.textContent = "Portfolio";
  btn.onclick = () => goPortfolio();
  const settingsBtn = e("nav-settings");
  if (settingsBtn && settingsBtn.parentNode === nav) nav.insertBefore(btn, settingsBtn);
  else nav.appendChild(btn);
}

function syncChrome() {
  const showDay = currentView === "day";
  if (e("date-row")) e("date-row").style.display = showDay ? "" : "none";
  if (e("btn-add")) e("btn-add").style.display = showDay ? "" : "none";
  if (e("btn-collapse-all")) e("btn-collapse-all").style.display = "none";
  if (e("btn-add")) e("btn-add").disabled = showDay ? positions.length >= 5 : true;
  if (document.querySelector(".sumbar")) document.querySelector(".sumbar").style.display = showDay ? "flex" : "none";
  if (document.querySelector(".savebar")) document.querySelector(".savebar").style.display = showDay ? "flex" : "none";
  setNav(currentView);
}

async function loadStorageInfo() {
  try {
    const info = await api("/storage-info");
    storageOnline = true;
    savePath = info.save_dir || "";
    kitePublicIp = info.public_ip || "";
    if (currentView === "day") setSaveState("Ready", "saved", "Autosave active");
  } catch (err) {
    storageOnline = false;
    kitePublicIp = "";
    if (currentView === "day") setSaveState("Server offline", "", String(err.message || err));
  }
}

async function loadSettings() {
  if (!storageOnline) await loadStorageInfo();
  if (!storageOnline) return settings;
  try {
    const payload = await api("/settings");
    settings = {
      available_capital: payload.available_capital,
      daily_risk: payload.daily_risk,
      per_position_risk: payload.per_position_risk,
      stop_loss_pct: payload.stop_loss_pct != null ? payload.stop_loss_pct : 2.0,
      checklist_groups: normalizeChecklistGroups(payload.checklist_groups || [
        {
          title: payload.checklist_group_1_title || "Entry",
          count: 2,
          items: [payload.checklist_entry_1 || "", payload.checklist_entry_2 || "", ""]
        },
        {
          title: payload.checklist_group_2_title || "Holding",
          count: 1,
          items: [payload.checklist_risk_1 || "", "", ""]
        },
        {
          title: payload.checklist_group_3_title || "Exit",
          count: 1,
          items: [payload.checklist_risk_2 || "", "", ""]
        }
      ])
    };
  } catch (_err) {}
  return settings;
}

async function saveSettings() {
  const checklistGroups = readChecklistGroupsFromDOM();
  const legacy = legacyChecklistFieldsFromGroups(checklistGroups);
  const payload = {
    available_capital: parseFloat(e("set-capital")?.value || "") || null,
    daily_risk: parseFloat(e("set-daily-risk")?.value || "") || null,
    per_position_risk: parseFloat(e("set-position-risk")?.value || "") || null,
    stop_loss_pct: parseFloat(e("set-stop-loss")?.value || "") || null,
    checklist_groups: checklistGroups,
    ...legacy
  };
  if (!storageOnline) await loadStorageInfo();
  if (!storageOnline) {
    alert("Server is offline. Start trade_plan_server.py first.");
    return;
  }
  const result = await api("/settings", { method: "POST", body: JSON.stringify(payload) });
  settings = {
    available_capital: result.available_capital,
    daily_risk: result.daily_risk,
    per_position_risk: result.per_position_risk,
    stop_loss_pct: result.stop_loss_pct != null ? result.stop_loss_pct : 2.0,
    checklist_groups: normalizeChecklistGroups(result.checklist_groups || checklistGroups)
  };
  setSaveState("Settings saved", "saved", "Autosave active");
  await loadDashboard();
  renderApp();
}

async function save(opts) {
  const options = Object.assign({ silent: false }, opts || {});
  const seq = ++saveSeq;
  localStorage.setItem(pkey(planDate), JSON.stringify(positions));
  if (!storageOnline) await loadStorageInfo();
  if (!storageOnline) {
    if (!options.silent) setSaveState("Save failed. API server is offline.", "", "Run trade_plan_server.py");
    return false;
  }
  if (!options.silent) setSaveState("Saving...", "", "Autosave active");
  try {
    const payload = await api("/plan?date=" + encodeURIComponent(planDate), {
      method: "POST",
      body: JSON.stringify({ positions })
    });
    if (seq !== saveSeq) return true;
    savePath = payload.path || savePath;
    const stamp = new Date().toLocaleTimeString();
    dirty = false;
    if (!options.silent) setSaveState("Saved " + stamp, "saved", "Autosave active");
    await loadDashboard();
    return true;
  } catch (err) {
    if (!options.silent) setSaveState("Save failed", "", String(err.message || err));
    return false;
  }
}

function autoSave() {
  if (currentView !== "day") return;
  dirty = true;
  setSaveState("Saving...", "", "Autosave active");
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => { save(); }, 900);
}

async function flushPendingSave() {
  if (currentView !== "day") return;
  if (!dirty) return;
  clearTimeout(saveTimer);
  await save({ silent: true });
}

async function loadDashboard() {
  if (!storageOnline) await loadStorageInfo();
  await loadSettings();
  if (!storageOnline) {
    dashboardItems = [];
    latestDashboardDate = todayStr();
    setSaveState("Server offline", "", "Run trade_plan_server.py");
    return;
  }
  try {
    const payload = await api("/dashboard");
    dashboardItems = Array.isArray(payload.items) ? payload.items : [];
    latestDashboardDate = payload.latest_date || todayStr();
    if (payload.settings) settings = payload.settings;
    if (!planDate) planDate = latestDashboardDate;
    setSaveState("Ready", "saved", "Autosave active");
  } catch (_err) {
    dashboardItems = [];
    latestDashboardDate = todayStr();
    setSaveState("Dashboard load failed", "", "API unavailable");
  }
}

async function loadGoalTracker() {
  if (!storageOnline) await loadStorageInfo();
  await loadSettings();
  if (!storageOnline) {
    goalTracker = { exposure_items: [], r_progress_items: [], plan_stats_items: [], latest_date: todayStr() };
    return;
  }
  try {
    const payload = await api("/goal-tracker");
    goalTracker = {
      exposure_items: Array.isArray(payload.exposure_items) ? payload.exposure_items : [],
      r_progress_items: Array.isArray(payload.r_progress_items) ? payload.r_progress_items : [],
      plan_stats_items: Array.isArray(payload.plan_stats_items) ? payload.plan_stats_items : [],
      latest_date: payload.latest_date || todayStr()
    };
    if (payload.settings) settings = payload.settings;
    dashboardItems = goalTracker.exposure_items.slice();
    latestDashboardDate = goalTracker.latest_date || latestDashboardDate;
  } catch (_err) {
    goalTracker = { exposure_items: [], r_progress_items: [], plan_stats_items: [], latest_date: todayStr() };
  }
}

async function loadDayView(date) {
  await flushPendingSave();
  planDate = date;
  clearTimeout(saveTimer);
  if (e("plan-date")) e("plan-date").value = date;
  let loaded = null;
  if (!storageOnline) await loadStorageInfo();
  if (storageOnline) {
    try {
      loaded = await api("/day-view?date=" + encodeURIComponent(date));
      savePath = loaded.raw_path || savePath;
    } catch (err) {
      setSaveState("Load failed. " + err.message, "", String(err.message || err));
    }
  }
  if (!loaded) {
    const raw = localStorage.getItem(pkey(date));
    loaded = {
      positions: raw ? JSON.parse(raw) : [],
      raw_path: savePath,
      open_positions_count: 0,
      exited_today_count: 0,
      planning_count: 0,
      exposure: 0,
      exposure_pct: null
    };
  }
  positions = dedupePositionsById(Array.isArray(loaded.positions) ? loaded.positions.map(hydratePos) : []);
  selectedPositionId = null;
  dayMeta = {
    open_positions_count: loaded.open_positions_count || 0,
    exited_today_count: loaded.exited_today_count || 0,
    planning_count: loaded.planning_count || 0,
    exposure: loaded.exposure || 0,
    exposure_pct: loaded.exposure_pct
  };
  renderApp();
  await refreshAllSymbols({ silent: true, persist: positions.length > 0 });
  setSaveState("Loaded " + positions.length + " position(s) for " + fmtDateLabel(date), "saved", loaded.raw_path || savePath);
}

async function changeDate(v) {
  if (v) await openDay(v);
}

async function shiftDate(delta) {
  const dt = new Date(planDate + "T12:00:00");
  dt.setDate(dt.getDate() + delta);
  const next = dt.toISOString().split("T")[0];
  await openDay(next);
}

function compute(p) {
  const totalRisk = Number(p.riskAmount || 0);
  const coreRps = (p.planEntry && p.planSL) ? (p.planEntry - p.planSL) : null;
  const intraRps = (p.planEntry && p.intraSL) ? (p.planEntry - p.intraSL) : coreRps;
  const blendRps = (coreRps && intraRps) ? (coreRps * 0.70 + intraRps * 0.30) : coreRps;
  const entry = getEffectiveEntry(p);
  p._coreRps = coreRps;
  p._intraRps = intraRps;
  p._blendRps = blendRps;
  p._rps = blendRps && coreRps && intraRps
    ? `Core Rs ${coreRps.toFixed(2)} | Intra Rs ${intraRps.toFixed(2)} | Blend Rs ${blendRps.toFixed(2)}`
    : (coreRps ? "Rs " + coreRps.toFixed(2) : null);
  const coreQty = coreRps && coreRps > 0 && totalRisk > 0 ? Math.floor(totalRisk / coreRps) : 0;
  p._suggestedIntraQty = coreQty > 0 ? Math.max(1, Math.round(coreQty * 0.30)) : null;
  if (p.intraQty == null && p._suggestedIntraQty != null) p.intraQty = p._suggestedIntraQty;
  const manualIntraQty = p.intraQty != null ? Math.max(0, parseInt(p.intraQty, 10) || 0) : null;
  const effectiveIntraQty = manualIntraQty != null ? manualIntraQty : 0;
  const totalQty = coreQty + effectiveIntraQty;
  p._qty = totalQty;
  p._intraQty = effectiveIntraQty;
  p._predQty = coreQty;
  p._predRisk = p._predQty > 0 && coreRps ? +(p._predQty * coreRps).toFixed(2) : 0;
  p._intraRisk = p._intraQty > 0 && intraRps ? +(p._intraQty * intraRps).toFixed(2) : 0;
  p._planRisk = +(p._predRisk + p._intraRisk).toFixed(2);
  const ae = entry || p.planEntry;
  p.trims[0]._sug = ae ? +(ae * 1.03).toFixed(2) : null;
  p.trims[1]._sug = ae ? +(ae * 1.07).toFixed(2) : null;
  p.trims[2]._sug = ae ? +(ae * 1.15).toFixed(2) : null;
  p.trims[3]._sug = ae ? +(ae * 1.25).toFixed(2) : null;
  const carriedQty = getTotalQty(p);
  p._suggestedQty = carriedQty != null ? Math.max(1, Math.round(carriedQty * 0.30)) : null;
  const totalSold = p.trims.reduce((sum, t) => sum + (t.done && t.sq ? t.sq : 0), 0);
  p._rem = carriedQty != null ? Math.max(0, carriedQty - totalSold) : null;
  p._realPnl = 0;
  p.trims.forEach(t => {
    t._pnl = (t.done && t.sq && t.ap && p.actualEntry) ? +((t.ap - p.actualEntry) * t.sq).toFixed(0) : null;
    if (t._pnl != null) p._realPnl += t._pnl;
  });
  p._openPnl = (p.cmp && entry && p._rem > 0) ? +((p.cmp - entry) * p._rem).toFixed(0) : null;
  p._currentSL = p.trailOverride || p.planSL || null;
  p._days = null;
  p._beSug = false;
  if (p.entryDate && entry) {
    const diff = Math.floor((Date.now() - new Date(p.entryDate + "T00:00:00")) / 86400000);
    p._days = diff;
    if (diff >= 2 && !p.movedBE) p._beSug = true;
  }
  const allSold = p._rem === 0;
  const anyTrim = p.trims.some(t => t.done);
  p._status = !entry ? "planning" : allSold ? "closed" : anyTrim ? "partial" : "active";
}

function getTotalQty(p) {
  const overnightQty = p.overnightQty != null ? Number(p.overnightQty) : null;
  const actualQty = p.actualQty != null ? Number(p.actualQty) : null;
  if (overnightQty == null && actualQty == null) return null;
  return (overnightQty || 0) + (actualQty || 0);
}
function getTrailStopOrderPreview(p, tickSize = 0.05) {
  const trigger = p.trailOverride != null ? Number(p.trailOverride) : null;
  const qty = p._rem != null ? Number(p._rem) : getTotalQty(p);
  const status = String(p._status || "").toLowerCase();
  if (!(trigger > 0 && qty > 0 && status !== "closed")) return null;
  const prices = stopLimitPreview(trigger, tickSize);
  if (!prices) return null;
  return { qty: Math.max(0, Math.floor(qty)), trigger: prices.trigger, limit: prices.limit };
}

function getExecutionRisk(p) {
  const planSL = p.planSL != null ? Number(p.planSL) : null;
  const intraSL = p.intraSL != null ? Number(p.intraSL) : null;
  const overnightSL = p.overnightSL != null ? Number(p.overnightSL) : null;
  const totalQty = getTotalQty(p);
  const entry = getEffectiveEntry(p);
  if (!(totalQty > 0 && entry > 0 && planSL > 0)) return null;
  const intraQty = Math.max(0, Math.round(totalQty * 0.30));
  const ovQty = Math.max(0, Number(p.overnightQty || 0));
  const actualQty = Math.max(0, Number(p.actualQty || 0));
  const coreQty = Math.max(0, totalQty - intraQty);
  const overnightPortion = ovQty > 0 ? Math.min(coreQty, ovQty) : 0;
  const actualPortion = Math.max(0, coreQty - overnightPortion);
  const overnightRisk = Math.max(0, (entry - (overnightSL > 0 ? overnightSL : planSL)) * overnightPortion);
  const coreRisk = Math.max(0, (entry - planSL) * actualPortion);
  const intraRisk = Math.max(0, (entry - (intraSL > 0 ? intraSL : planSL)) * intraQty);
  return +(overnightRisk + coreRisk + intraRisk).toFixed(2);
}

function getPlannedRisk(p) {
  const planned = p._planRisk != null ? Number(p._planRisk) : null;
  return planned != null && planned > 0 ? +planned.toFixed(2) : null;
}

function getLiveRisk(p) {
  const entry = getEffectiveEntry(p);
  const sl = p._currentSL != null ? Number(p._currentSL) : (p.planSL != null ? Number(p.planSL) : null);
  const remainingQty = p._rem != null ? Number(p._rem) : getTotalQty(p);
  if (!(entry > 0 && sl > 0 && remainingQty > 0)) return null;
  return +Math.max(0, (entry - sl) * remainingQty).toFixed(2);
}

function hasExecutionDetails(p) {
  return getTotalQty(p) != null && getEffectiveEntry(p) != null;
}

function getDisplayedRisk(p) {
  if (hasExecutionDetails(p)) return getLiveRisk(p);
  return getPlannedRisk(p);
}

function getDisplayedRiskLabel(p) {
  return hasExecutionDetails(p) ? "Live risk" : "Planned risk";
}

function getEffectiveEntry(p) {
  const legs = [];
  const overnightQty = p.overnightQty != null ? Number(p.overnightQty) : null;
  const overnightEntry = p.overnightEntry != null ? Number(p.overnightEntry) : null;
  const actualQty = p.actualQty != null ? Number(p.actualQty) : null;
  const actualEntry = p.actualEntry != null ? Number(p.actualEntry) : null;
  if (overnightQty != null && overnightQty > 0 && overnightEntry != null && overnightEntry > 0) {
    legs.push({ qty: overnightQty, price: overnightEntry });
  }
  if (actualQty != null && actualQty > 0 && actualEntry != null && actualEntry > 0) {
    legs.push({ qty: actualQty, price: actualEntry });
  }
  if (legs.length === 2) {
    const totalQty = legs[0].qty + legs[1].qty;
    if (totalQty > 0) {
      const weighted = ((legs[0].qty * legs[0].price) + (legs[1].qty * legs[1].price)) / totalQty;
      return +weighted.toFixed(2);
    }
  }
  if (legs.length === 1) {
    return +legs[0].price.toFixed(2);
  }
  return actualEntry != null ? actualEntry : (overnightEntry != null ? overnightEntry : null);
}
function isPositioned(p) { return !!getEffectiveEntry(p); }
function isOpenPosition(p) { return p._status === "active" || p._status === "partial"; }
function getBucketKey(p) {
  if (!isPositioned(p)) return "planning";
  if (p._status === "closed") return "exited";
  if (p.entryDate && p.entryDate < planDate) return "overnight";
  return "current";
}
function getPrimaryBadge(p) { return isPositioned(p) ? "positioned" : "planning"; }
function getSecondaryBadge(p) {
  if (!isPositioned(p)) return "";
  if (p._status === "partial") return "partial";
  if (p._status === "closed") return "closed";
  return getBucketKey(p);
}
function getHeaderMeta(p, bucketKey) {
  const entry = getEffectiveEntry(p);
  const sl = p._currentSL != null ? p._currentSL : p.planSL;
  const slHtml = sl != null ? `<strong>SL Rs ${priceSL(sl)}</strong>` : `<strong>SL</strong>`;
  if (isPositioned(p)) {
    const openQty = p._rem != null ? p._rem : (p.actualQty != null ? p.actualQty : (p.overnightQty || 0));
    const entryTxt = entry ? `Entry Rs ${priceSL(entry)}` : "";
    if (bucketKey === "overnight") return `${entryTxt} | ${slHtml} | Open qty ${openQty}`;
    if (p._status === "closed") return `${entryTxt} | ${slHtml} | Flat by close`;
    return `${entryTxt} | ${slHtml} | Open qty ${openQty}`;
  }
  const priceTxt = p.planEntry ? `Buy near Rs ${priceSL(p.planEntry)}` : "Buy price pending";
  const qtyTxt = p._qty > 0 ? `Qty ${p._qty}` : "Qty pending";
  return `${priceTxt} | ${qtyTxt}`;
}
function getSectionTitle(key) {
  if (key === "overnight") return "Overnight Positions";
  if (key === "current") return "Current Positions";
  if (key === "exited") return "Exited Today";
  return "Planning Queue";
}
function getSectionNote(key, count) {
  if (key === "overnight") return `${count} carried position${count === 1 ? "" : "s"}`;
  if (key === "current") return `${count} live for ${planDate}`;
  if (key === "exited") return `${count} trade${count === 1 ? "" : "s"} completed today and not carried overnight`;
  return `${count} planned setup${count === 1 ? "" : "s"} not executed yet`;
}

function getDisplayBuckets() {
  return ["planning", "current", "exited", "overnight"];
}

function ensureSelectedPosition() {
  if (!positions.length) {
    selectedPositionId = null;
    return null;
  }
  if (!positions.some(p => p.id === selectedPositionId)) {
    const byBucket = {};
    positions.forEach(p => {
      compute(p);
      const bucket = getBucketKey(p);
      if (!byBucket[bucket]) byBucket[bucket] = p.id;
    });
    selectedPositionId = byBucket.planning || byBucket.current || byBucket.exited || byBucket.overnight || positions[0].id;
  }
  return positions.find(p => p.id === selectedPositionId) || positions[0];
}

function selectPos(id) {
  if (!positions.some(p => p.id === id)) return;
  selectedPositionId = id;
  renderDayView();
}

function upd(id, field, value) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  if (field.startsWith("mgmt.")) p.mgmt[field.slice(5)] = value;
  else p[field] = value;
  autoSave();
  if (field.startsWith("mgmt.") || field === "thoughtTag") {
    paint(id, p);
    return;
  }
  if (!QUIET.has(field)) {
    compute(p);
    paint(id, p);
  }
}

function updTrim(id, ti, field, value) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  p.trims[ti][field] = value;
  const row = e("tr-" + ti + "-" + id);
  if (row && field === "done") row.className = value ? "t-done" : "";
  compute(p);
  paint(id, p);
  autoSave();
}

function setConv(id, value) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  p.conviction = value;
  const c = e("conv-" + id);
  if (c) c.innerHTML = [1, 2, 3, 4, 5].map(i => `<span class="cdot ${i <= value ? "on" : ""}" onclick="setConv('${id}',${i})"></span>`).join("");
  autoSave();
}

function moveBE(id) {
  const p = positions.find(x => x.id === id);
  const entry = getEffectiveEntry(p);
  if (!p || !entry) return;
  p.movedBE = true;
  p.trailOverride = entry;
  if (!p.trailNote) p.trailNote = "SL moved to breakeven (Rs " + entry + ")";
  const note = e("tni-" + id);
  const over = e("toi-" + id);
  if (note) note.value = p.trailNote;
  if (over) over.value = entry;
  compute(p);
  paint(id, p);
  autoSave();
}

function readChecklistGroupsFromDOM() {
  const groups = [];
  for (let gi = 0; gi < 3; gi += 1) {
    const title = String(e(`cg-title-${gi}`)?.value || "").trim();
    const countRaw = parseInt(e(`cg-count-${gi}`)?.value || "0", 10);
    const count = Number.isFinite(countRaw) ? Math.max(0, Math.min(3, countRaw)) : 0;
    const items = [];
    for (let ii = 0; ii < 3; ii += 1) {
      const value = String(e(`cg-item-${gi}-${ii}`)?.value || "").trim();
      items.push(value);
    }
    groups.push({
      title: title || defaultChecklistGroups()[gi].title,
      count,
      items,
    });
  }
  return normalizeChecklistGroups(groups);
}

function renderChecklistGroupCards() {
  const groups = getChecklistGroups();
  return groups.map((group, gi) => {
    const countOptions = [0, 1, 2, 3].map(n => `<option value="${n}" ${group.count === n ? "selected" : ""}>${n}</option>`).join("");
    const rows = [0, 1, 2].map(ii => {
      const visible = ii < group.count;
      const item = group.items[ii] || "";
      return `
        <div class="check-item-row" style="${visible ? "" : "display:none;"}">
          <input type="text" class="fin" id="cg-item-${gi}-${ii}" placeholder="Item ${ii + 1}" value="${escHtml(item)}">
          <button class="btn-sec" type="button" onclick="removeChecklistItem(${gi}, ${ii})">Delete</button>
        </div>
      `;
    }).join("");
    return `
      <section class="check-group">
        <div class="check-group-head">
          <div class="field">
            <div class="flabel">Group heading</div>
            <input type="text" class="fin" id="cg-title-${gi}" value="${escHtml(group.title || "")}" placeholder="Group title">
          </div>
          <div class="field">
            <div class="flabel">Entries</div>
            <select class="fin" id="cg-count-${gi}" onchange="updateChecklistGroupCount(${gi}, this.value)">
              ${countOptions}
            </select>
          </div>
        </div>
        <div class="check-items">
          ${rows}
        </div>
        <button class="btn-sec check-item-add" type="button" onclick="addChecklistItem(${gi})" ${group.count >= 3 ? "disabled" : ""}>+ Add entry</button>
      </section>
    `;
  }).join("");
}

function updateChecklistGroupCount(groupIdx, value) {
  const groups = readChecklistGroupsFromDOM();
  const idx = Number(groupIdx);
  if (!groups[idx]) return;
  const count = Math.max(0, Math.min(3, parseInt(value, 10) || 0));
  groups[idx].count = count;
  for (let i = count; i < 3; i += 1) {
    groups[idx].items[i] = "";
  }
  settings.checklist_groups = groups;
  renderSettingsView();
}

function addChecklistItem(groupIdx) {
  const groups = readChecklistGroupsFromDOM();
  const idx = Number(groupIdx);
  if (!groups[idx]) return;
  if (groups[idx].count >= 3) return;
  groups[idx].count = Math.min(3, groups[idx].count + 1);
  settings.checklist_groups = groups;
  renderSettingsView();
}

function removeChecklistItem(groupIdx, itemIdx) {
  const groups = readChecklistGroupsFromDOM();
  const idx = Number(groupIdx);
  const row = Number(itemIdx);
  if (!groups[idx]) return;
  groups[idx].items.splice(row, 1);
  while (groups[idx].items.length < 3) groups[idx].items.push("");
  groups[idx].count = Math.min(3, groups[idx].items.filter(Boolean).length);
  settings.checklist_groups = groups;
  renderSettingsView();
}

function toggleChecklistItem(id, groupIdx, itemIdx, checked) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  const matrix = normalizeChecklistStateMatrix(p.mgmt && p.mgmt.checklist ? p.mgmt.checklist : legacyChecklistStateMatrix(p.mgmt));
  const gi = Number(groupIdx);
  const ii = Number(itemIdx);
  if (!matrix[gi] || matrix[gi][ii] == null) return;
  matrix[gi][ii] = Boolean(checked);
  p.mgmt.checklist = matrix;
  const rail = e("live-rail");
  if (rail) rail.outerHTML = renderRightRail(p);
  autoSave();
}

function updateTrailSL(id) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  const over = e("toi-" + id);
  if (!over) return;
  const value = parseFloat(over.value);
  if (!Number.isFinite(value) || value <= 0) return;
  p.trailOverride = value;
  if (!p.trailNote) p.trailNote = "Trail SL updated to Rs " + value;
  const note = e("tni-" + id);
  if (note && !note.value) note.value = p.trailNote;
  compute(p);
  paint(id, p);
  autoSave();
}

async function pushStopLossOrder(id) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  const preview = getTrailStopOrderPreview(p);
  if (!preview) {
    alert("Set a trailing stop and keep the position open before pushing the Kite SL order.");
    return;
  }
  const symbol = p.symbol || "this position";
  const ok = confirm(
    `Place Kite SL order for ${symbol}?\n\nQty: ${preview.qty}\nTrigger: Rs ${priceSL(preview.trigger)}\nLimit: Rs ${priceSL(preview.limit)}`
  );
  if (!ok) return;
  try {
    const result = await api("/kite/place-sl-order", {
      method: "POST",
      body: JSON.stringify({
        date: planDate,
        position_id: id
      })
    });
    const msg = result.placed
      ? `Placed Kite SL order for ${symbol}.`
      : `Kite SL order already exists for ${symbol}.`;
    setSaveState(msg, "saved", result.order_id ? "Order ID " + result.order_id : "Kite SL");
    alert(
      `${msg}\n\nQty: ${result.quantity}\nTrigger: Rs ${priceSL(result.trigger_price)}\nLimit: Rs ${priceSL(result.limit_price)}${result.order_id ? `\nOrder ID: ${result.order_id}` : ""}`
    );
  } catch (err) {
    const msg = String(err.message || err || "");
    const hint = msg.includes("Route not found")
      ? "\n\nRestart trade_plan_server.py so the new Kite route is loaded."
      : "";
    alert("Failed to place Kite SL order: " + msg + hint);
  }
}

async function fetchSuggestions(term) {
  const list = e("symbol-suggestions");
  if (!list) return;
  list.innerHTML = "";
  const clean = normalizeSymbol(term);
  if (!clean || !storageOnline) return;
  try {
    const payload = await api("/symbols?term=" + encodeURIComponent(clean) + "&limit=8");
    list.innerHTML = (payload.items || []).map(item => `<option value="${item.symbol}">${item.company_name || ""}</option>`).join("");
  } catch (_err) {}
}

function handleSymbolInput(id, input) {
  const clean = normalizeSymbol(input.value);
  input.value = clean;
  upd(id, "symbol", clean);
  fetchSuggestions(clean);
}

async function resolveSymbolAndCmp(id, rawValue, opts) {
  const options = Object.assign({ input: null, silent: false }, opts || {});
  const p = positions.find(x => x.id === id);
  if (!p) return false;
  const symbol = normalizeSymbol(rawValue);
  if (!symbol) {
    p.symbol = "";
    p.cmp = null;
    compute(p);
    paint(id, p);
    autoSave();
    return false;
  }
  if (!storageOnline) await loadStorageInfo();
  if (!storageOnline) {
    if (!options.silent) setSaveState("Server offline. Cannot resolve symbol/CMP.", "", "Run trade_plan_server.py");
    return false;
  }
  try {
    const result = await api("/resolve-symbol?symbol=" + encodeURIComponent(symbol) + "&date=" + encodeURIComponent(planDate));
    if (!result.ok) {
      p.symbol = symbol;
      p.cmp = null;
      compute(p);
      paint(id, p);
      const suffix = (result.suggestions || []).length ? " Suggestions: " + result.suggestions.join(", ") : "";
      if (!options.silent) setSaveState(result.message + suffix, "", result.message + suffix);
      autoSave();
      return false;
    }
    p.symbol = result.canonical_symbol;
    p.cmp = result.cmp;
    if (options.input) options.input.value = result.canonical_symbol;
    compute(p);
    paint(id, p);
    if (!options.silent) {
      const via = result.matched_via && result.matched_via !== "exact" ? " [" + result.matched_via + "]" : "";
      setSaveState("Symbol " + symbol + " -> " + result.canonical_symbol + via + ", CMP " + result.cmp + " from " + result.table + " (" + result.price_date + ")", "saved");
    }
    autoSave();
    return true;
  } catch (err) {
    if (!options.silent) setSaveState("Symbol lookup failed. " + err.message, "", String(err.message || err));
    return false;
  }
}

async function commitSymbol(id, input) { await resolveSymbolAndCmp(id, input.value, { input }); }
async function refreshAllSymbols(opts) {
  const options = Object.assign({ silent: true, persist: false }, opts || {});
  let changed = false;
  for (const p of positions) {
    if (!p.symbol) continue;
    const beforeSymbol = p.symbol;
    const beforeCmp = p.cmp;
    await resolveSymbolAndCmp(p.id, p.symbol, { silent: options.silent });
    if (p.symbol !== beforeSymbol || p.cmp !== beforeCmp) changed = true;
    await wait(40);
  }
  if (changed && options.persist) await save({ silent: true });
}

function paint(id, p) {
  const set = (nodeId, text, cls) => {
    const node = e(nodeId);
    if (!node) return;
    if (text != null) node.textContent = text;
    if (cls != null) node.className = cls;
  };
  const cmpInput = e("cmp-" + id);
  const symInput = e("sym-" + id);
  if (symInput && symInput.value !== p.symbol) symInput.value = p.symbol || "";
  if (cmpInput) cmpInput.value = p.cmp != null ? p.cmp : "";
  const card = e("card-" + id);
  if (card) card.className = "pcard st-" + p._status;
  const list = e("list-" + id);
  if (list) list.className = "plist-item st-" + p._status + (selectedPositionId === id ? " on" : "");
  set("list-sym-" + id, p.symbol || "SYMBOL");
  const listMeta = e("list-meta-" + id);
  if (listMeta) listMeta.innerHTML = getHeaderMeta(p, getBucketKey(p));
  const listPnl = e("list-pnl-" + id);
  if (listPnl) {
    const total = (p._realPnl || 0) + (p._openPnl || 0);
    listPnl.textContent = total !== 0 ? sgn(total) + "Rs " + fi(total) : "-";
    listPnl.className = "plist-pnl " + (total >= 0 ? "pos" : "neg");
  }
  const lp = e("list-pbadge-" + id);
  if (lp) lp.className = "badge badge-" + getPrimaryBadge(p);
  set("list-pbadge-" + id, getPrimaryBadge(p).toUpperCase());
  const ls = e("list-sbadge-" + id);
  if (ls) {
    const secondary = getSecondaryBadge(p);
    ls.textContent = secondary.toUpperCase();
    ls.className = "badge badge-" + (secondary || "ghost");
    ls.style.display = secondary ? "" : "none";
  }
  set("pbadge-" + id, getPrimaryBadge(p).toUpperCase(), "badge badge-" + getPrimaryBadge(p));
  const sb = e("sbadge-" + id);
  if (sb) {
    const secondary = getSecondaryBadge(p);
    sb.textContent = secondary.toUpperCase();
    sb.className = "badge badge-" + (secondary || "ghost");
    sb.style.display = secondary ? "" : "none";
  }
  const hm = e("hmeta-" + id);
  if (hm) hm.innerHTML = getHeaderMeta(p, getBucketKey(p));
  const qtyEl = e("qty-" + id);
  if (qtyEl) {
    qtyEl.textContent = p._qty > 0 ? p._qty : "-";
    qtyEl.style.color = p._qty > 0 ? "var(--green)" : "var(--t3)";
    qtyEl.style.fontSize = p._qty > 0 ? "22px" : "16px";
  }
  set("rps-val-" + id, p._rps ? p._rps : "-");
  set("prisk-" + id, p._planRisk != null && p._planRisk > 0 ? price2(p._planRisk) : "-");
  const carriedQty = getTotalQty(p);
  set("totq-" + id, carriedQty != null ? carriedQty : "-");
  set("epx-" + id, getEffectiveEntry(p) != null ? price2(getEffectiveEntry(p)) : "-");
  const execRisk = getExecutionRisk(p);
  set("erisk-" + id, execRisk != null ? price2(execRisk) : "-");
  set("rem-" + id, p._rem != null ? p._rem : (carriedQty != null ? carriedQty : "-"));
  const qty = carriedQty || p._qty || 0;
  const splits = [Math.ceil(qty * 0.33), Math.ceil(qty * 0.25), Math.floor(qty * 0.25), Math.floor(qty * 0.17)];
  ["sp0", "sp1", "sp2", "sp3"].forEach((name, i) => set(name + "-" + id, qty > 0 ? splits[i] : "-"));
  let running = carriedQty || 0;
  p.trims.forEach((t, i) => {
    const sugEl = e("ts" + i + "-" + id);
    if (sugEl) {
      sugEl.textContent = t._sug ? "Rs " + t._sug : (i === 1 ? "Enter pos high" : "-");
      sugEl.style.color = t._sug ? "var(--amb)" : "var(--t3)";
      sugEl.className = t._sug ? "t-target" : "t-na";
    }
    const pnlEl = e("tp" + i + "-" + id);
    if (pnlEl) {
      if (t._pnl != null) {
        pnlEl.textContent = (t._pnl >= 0 ? "+" : "-") + "Rs " + fi(t._pnl);
        pnlEl.className = "t-pl " + (t._pnl >= 0 ? "pos" : "neg");
      } else {
        pnlEl.textContent = "-";
        pnlEl.className = "t-pl";
      }
    }
    running -= (t.done && t.sq ? t.sq : 0);
    set("tr" + i + "-" + id, carriedQty != null ? running + " rem" : "");
  });
  const sumEl = e("pnlsum-" + id);
  if (sumEl) {
    const hasTrim = p.trims.some(t => t.done);
    sumEl.style.display = hasTrim ? "flex" : "none";
    if (hasTrim) {
      const net = (p._realPnl || 0) + (p._openPnl || 0);
      const realEl = e("psr-" + id);
      const netEl = e("psn-" + id);
      if (realEl) {
        realEl.textContent = sgn(p._realPnl) + "Rs " + fi(p._realPnl);
        realEl.style.color = p._realPnl >= 0 ? "var(--green)" : "var(--red)";
      }
      if (netEl) {
        netEl.textContent = sgn(net) + "Rs " + fi(net);
        netEl.style.color = net >= 0 ? "var(--green)" : "var(--red)";
      }
    }
  }
  set("tsl-" + id, "Rs " + priceSL(p._currentSL));
  set("days-" + id, p._days != null ? "Day " + p._days + " in trade" : "");
  const beBanner = e("beb-" + id);
  if (beBanner) beBanner.style.display = p._beSug ? "flex" : "none";
  const beConf = e("bec-" + id);
  if (beConf) {
    beConf.style.display = p.movedBE ? "block" : "none";
    beConf.textContent = p.movedBE ? "Rs " + p.actualEntry : "";
  }
  const headRisk = e("hrisk-" + id);
  if (headRisk) {
    const displayRisk = getDisplayedRisk(p);
    const displayRiskLabel = getDisplayedRiskLabel(p);
    headRisk.style.display = displayRisk != null ? "" : "none";
    headRisk.textContent = displayRisk != null ? `${displayRiskLabel} ${price2(displayRisk)}` : "";
    headRisk.className = "head-pnl neg";
  }
  const thoughtInputDraft = e("thought-input-" + id)?.value || "";
  const mgmtNoteDraft = e("mgmt-note-" + id)?.value || "";
  const liveRail = e("live-rail");
  if (liveRail && selectedPositionId === id) {
    liveRail.outerHTML = renderRightRail(p);
    const thoughtInput = e("thought-input-" + id);
    if (thoughtInput && thoughtInputDraft) thoughtInput.value = thoughtInputDraft;
    const mgmtNote = e("mgmt-note-" + id);
    if (mgmtNote && mgmtNoteDraft) mgmtNote.value = mgmtNoteDraft;
  }
  updateSummary();
}

function updateSummary() {
  const openCount = positions.filter(p => {
    compute(p);
    return isOpenPosition(p);
  }).length;
  let risk = 0;
  let real = 0;
  let open = 0;
  positions.forEach(p => {
    compute(p);
    const displayRisk = getDisplayedRisk(p);
    if (displayRisk != null) risk += displayRisk;
    real += (p._realPnl || 0);
    if (isOpenPosition(p) && p._openPnl != null) open += p._openPnl;
  });
  const net = real + open;
  const sign = v => v > 0 ? "+" : v < 0 ? "-" : "";
  const set = (id, text, cls) => {
    const node = e(id);
    if (node) {
      node.textContent = text;
      node.className = "sv " + cls;
    }
  };
  set("s-pos", String(openCount), "neu");
  set("s-exp", dayMeta.exposure ? "Rs " + fi(dayMeta.exposure) : "-", "neu");
  set("s-expct", dayMeta.exposure_pct != null ? pct(dayMeta.exposure_pct) : "-", "neu");
  set("s-risk", risk > 0 ? "Rs " + fi(risk) : "-", "neu");
  set("s-real", real !== 0 ? sign(real) + "Rs " + fi(real) : "-", real >= 0 ? "pos" : "neg");
  set("s-open", open !== 0 ? sign(open) + "Rs " + fi(open) : "-", open >= 0 ? "pos" : "neg");
  set("s-net", net !== 0 ? sign(net) + "Rs " + fi(net) : "-", net >= 0 ? "pos" : "neg");
}

function trackerBars(items, options) {
  const cfg = Object.assign({
    valueKey: "value",
    pctKey: null,
    label: item => money(item[cfg.valueKey]),
    currentDate: planDate,
    fillClass: "",
    emptyText: "No data yet."
  }, options || {});
  if (!items.length) return `<div class="view-note">${cfg.emptyText}</div>`;
  const maxValue = Math.max(...items.map(item => Number(item[cfg.valueKey] || 0)), 1);
  return items.map(item => {
    const rawValue = Number(item[cfg.valueKey] || 0);
    const height = cfg.pctKey && item[cfg.pctKey] != null
      ? Math.max(8, Math.min(100, Number(item[cfg.pctKey] || 0)))
      : Math.max(8, Math.round((rawValue / maxValue) * 100));
    return `<div class="exp-col ${item.date === cfg.currentDate ? "current" : ""}"><div class="exp-top">${cfg.label(item)}</div><div class="exp-bar"><div class="exp-fill ${cfg.fillClass}" style="height:${height}%"></div></div><div class="exp-date">${fmtDateLabel(item.date)}</div></div>`;
  }).join("");
}

function renderWormChart(items, options) {
  const cfg = Object.assign({
    title: "Daily Worm Graph",
    note: "A glossy directional line for deployment progress, so the goal reads like momentum instead of blocks.",
    valueKey: "exposure_pct",
    fallbackValueKey: "exposure",
    latestLabel: "Latest",
    peakLabel: "Peak",
    formatter: value => pct(value),
    emptyText: "Worm graph needs saved dates to show direction."
  }, options || {});
  if (!items.length) return `<div class="view-note">${cfg.emptyText}</div>`;
  const series = items.slice(-10);
  const width = 960;
  const height = 220;
  const padL = 26;
  const padR = 18;
  const padT = 18;
  const padB = 28;
  const values = series.map(item => item[cfg.valueKey] != null ? Number(item[cfg.valueKey]) : Number(item[cfg.fallbackValueKey] || 0));
  const maxVal = Math.max(...values, 1);
  const minValRaw = Math.min(...values, 0);
  const minVal = minValRaw === maxVal ? Math.max(0, maxVal - 1) : minValRaw;
  const usableW = width - padL - padR;
  const usableH = height - padT - padB;
  const stepX = series.length > 1 ? usableW / (series.length - 1) : 0;
  const points = series.map((item, index) => {
    const value = values[index];
    const x = padL + (stepX * index);
    const y = padT + usableH - (((value - minVal) / (maxVal - minVal || 1)) * usableH);
    return { x, y, value, date: item.date };
  });
  let line = `M ${points[0].x.toFixed(1)} ${points[0].y.toFixed(1)}`;
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1];
    const curr = points[i];
    const cx = ((prev.x + curr.x) / 2).toFixed(1);
    line += ` C ${cx} ${prev.y.toFixed(1)}, ${cx} ${curr.y.toFixed(1)}, ${curr.x.toFixed(1)} ${curr.y.toFixed(1)}`;
  }
  const area = `${line} L ${points[points.length - 1].x.toFixed(1)} ${height - padB} L ${points[0].x.toFixed(1)} ${height - padB} Z`;
  const grid = [0, 0.5, 1].map(ratio => {
    const y = padT + (usableH * ratio);
    return `<line x1="${padL}" y1="${y.toFixed(1)}" x2="${width - padR}" y2="${y.toFixed(1)}" stroke="rgba(255,255,255,.06)" stroke-dasharray="4 6"/>`;
  }).join("");
  const dots = points.map((point, index) => {
    const active = index === points.length - 1;
    return `<circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="${active ? 5 : 3.5}" fill="${active ? "#9fe7ff" : "#c8d8ff"}" stroke="${active ? "#27d3ff" : "rgba(255,255,255,.45)"}" stroke-width="${active ? 2 : 1.2}"/>`;
  }).join("");
  const labels = points.map(point => `<text x="${point.x.toFixed(1)}" y="${height - 8}" text-anchor="middle" fill="rgba(183,184,209,.78)" font-size="10">${fmtDateLabel(point.date)}</text>`).join("");
  const first = values[0] || 0;
  const last = values[values.length - 1] || 0;
  const delta = last - first;
  const direction = delta > 0 ? "Uptrend" : delta < 0 ? "Downtrend" : "Flat";
  const peak = Math.max(...values, 0);
  const latest = series[series.length - 1];
  return `
    <div class="worm-card">
      <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">${cfg.title}</div>
      <div class="view-note">${cfg.note}</div>
      <div class="worm-frame">
        <svg class="worm-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Daily worm graph for exposure trend">
          <defs>
            <linearGradient id="wormStroke" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stop-color="#41d1ff"/>
              <stop offset="55%" stop-color="#6e8cff"/>
              <stop offset="100%" stop-color="#7cf0c7"/>
            </linearGradient>
            <linearGradient id="wormFill" x1="0%" y1="0%" x2="0%" y2="100%">
              <stop offset="0%" stop-color="rgba(94,167,255,.38)"/>
              <stop offset="75%" stop-color="rgba(20,26,43,.08)"/>
              <stop offset="100%" stop-color="rgba(20,26,43,0)"/>
            </linearGradient>
            <filter id="wormGlow" x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur stdDeviation="4.5" result="blur"/>
              <feMerge>
                <feMergeNode in="blur"/>
                <feMergeNode in="SourceGraphic"/>
              </feMerge>
            </filter>
          </defs>
          ${grid}
          <path d="${area}" fill="url(#wormFill)"></path>
          <path d="${line}" fill="none" stroke="rgba(65,209,255,.18)" stroke-width="10" stroke-linecap="round" filter="url(#wormGlow)"></path>
          <path d="${line}" fill="none" stroke="url(#wormStroke)" stroke-width="4.5" stroke-linecap="round"></path>
          ${dots}
          ${labels}
        </svg>
      </div>
      <div class="worm-meta">
        <div class="worm-pill"><div class="dash-k">Direction</div><div class="dash-v small">${direction}</div></div>
        <div class="worm-pill"><div class="dash-k">Change</div><div class="dash-v small">${delta > 0 ? "+" : delta < 0 ? "-" : ""}${cfg.formatter(Math.abs(delta))}</div></div>
        <div class="worm-pill"><div class="dash-k">${cfg.latestLabel}</div><div class="dash-v small">${cfg.formatter(latest?.[cfg.valueKey] != null ? latest[cfg.valueKey] : latest?.[cfg.fallbackValueKey])}</div></div>
        <div class="worm-pill"><div class="dash-k">${cfg.peakLabel}</div><div class="dash-v small">${cfg.formatter(peak)}</div></div>
      </div>
    </div>
  `;
}

function setGoalTrackerTab(tab) {
  goalTrackerTab = tab;
  if (currentView === "exposure") renderExposureView();
}

function renderGoalTrackerTabs() {
  const tabs = [
    ["exposure", "Exposure"],
    ["r-progress", "R Progress"],
    ["plan-stats", "Sticking To Plan"]
  ];
  return `<div class="tracker-tabs">${tabs.map(([key, label]) => `<button class="tracker-tab ${goalTrackerTab === key ? "on" : ""}" onclick="setGoalTrackerTab('${key}')">${label}</button>`).join("")}</div>`;
}

function renderExposureTab() {
  const items = goalTracker.exposure_items.length ? goalTracker.exposure_items : dashboardItems;
  const capital = Number(settings.available_capital || 0);
  const activeDate = planDate || goalTracker.latest_date || latestDashboardDate || todayStr();
  const selectedItem = items.find(item => item.date === activeDate) || items[items.length - 1] || null;
  const currentExposure = Number(selectedItem?.exposure || 0);
  const currentPct = selectedItem?.exposure_pct != null ? Number(selectedItem.exposure_pct) : (capital > 0 ? (currentExposure / capital) * 100 : null);
  const openPositionsCount = Number(selectedItem?.open_positions_count || 0);
  const gap = capital > 0 ? Math.max(0, capital - currentExposure) : null;
  const exposureBars = trackerBars(items.slice(-8), {
    valueKey: "exposure",
    pctKey: capital > 0 ? "exposure_pct" : null,
    currentDate: activeDate,
    label: item => capital > 0 ? pct(item.exposure_pct) : money(item.exposure),
    emptyText: "Save a few trade dates and your deployment bars will appear here."
  });
  const wormGraph = renderWormChart(items, {
    title: "Daily Worm Graph",
    note: "A glossy directional line for deployment progress, so the goal reads like momentum instead of blocks.",
    valueKey: "exposure_pct",
    fallbackValueKey: "exposure",
    latestLabel: "Latest",
    peakLabel: "Peak",
    formatter: value => pct(value)
  });
  const progress = currentPct != null ? Math.max(0, Math.min(100, currentPct)) : 0;
  const goalText = capital > 0
    ? `Capital left to deploy: ${money(gap)}. This is the screen to judge whether your market read is translating into real deployment.`
    : "Set available capital in Settings to turn this into a true deployment tracker.";
  return `
    <section class="tracker-stack">
      <section class="settings-card exp-graph">
        <div>
          <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Exposure</div>
          <div class="view-note">How much capital was actually in the market across your saved days.</div>
          <div class="exp-bars">${exposureBars}</div>
        </div>
        <div class="goal-card">
          <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Goal Tracker</div>
          <div class="goal-big">${currentPct != null ? pct(currentPct) : "-"}</div>
          <div class="goal-copy">${goalText}</div>
          <div class="prog-track"><div class="prog-fill" style="width:${progress}%"></div></div>
          <div class="goal-meta">
            <div><div class="dash-k">Current exposure</div><div class="dash-v small">${money(currentExposure)}</div></div>
            <div><div class="dash-k">Available capital</div><div class="dash-v small">${money(settings.available_capital)}</div></div>
            <div><div class="dash-k">Open positions</div><div class="dash-v small">${openPositionsCount}</div></div>
            <div><div class="dash-k">Capital left</div><div class="dash-v small">${gap != null ? money(gap) : "Set in Settings"}</div></div>
          </div>
        </div>
      </section>
      ${wormGraph}
      <section class="tracker-grid">
        <div class="metric-card"><div class="dash-k">Planned setups on active date</div><div class="metric-big">${selectedItem ? (selectedItem.planning_count || 0) : 0}</div><div class="metric-copy">Keep this beside deployment so you can see if planning is actually converting into exposure.</div></div>
        <div class="metric-card"><div class="dash-k">Open positions on active date</div><div class="metric-big">${openPositionsCount}</div><div class="metric-copy">This is your real deployed count, not just names on a watchlist.</div></div>
        <div class="metric-card"><div class="dash-k">Goal pressure</div><div class="metric-big">${gap != null ? money(gap) : "-"}</div><div class="metric-copy">The remaining distance to full capital deployment when market conditions deserve aggression.</div></div>
      </section>
    </section>
  `;
}

function renderRProgressTab() {
  const items = goalTracker.r_progress_items || [];
  const latest = items[items.length - 1] || null;
  const budget = Number(settings.daily_risk || 0);
  const perTrade = Number(settings.per_position_risk || 0);
  const bars = trackerBars(items.slice(-8), {
    valueKey: "actual_risk",
    pctKey: budget > 0 ? "daily_actual_risk_pct" : null,
    currentDate: latest?.date || "",
    label: item => budget > 0 ? pct(item.daily_actual_risk_pct) : money(item.actual_risk),
    fillClass: "warm",
    emptyText: "As you execute real trades, actual deployed risk will show up here."
  });
  const wormGraph = renderWormChart(items, {
    title: "R Deployment Worm Graph",
    note: "This tracks how much of your allotted risk was actually deployed in the trade, so under-sized executions stand out immediately.",
    valueKey: "actual_vs_allotted_pct",
    fallbackValueKey: "daily_actual_risk_pct",
    latestLabel: "Actual vs allotted",
    peakLabel: "Best deployment",
    formatter: value => pct(value),
    emptyText: "Worm graph needs executed trades with both allotted and actual risk."
  });
  return `
    <section class="tracker-stack">
      <section class="settings-card exp-graph">
        <div>
          <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">R Progress</div>
          <div class="view-note">How much real risk you actually put on, not just how much you allowed yourself on paper.</div>
          <div class="exp-bars">${bars}</div>
        </div>
        <div class="goal-card">
          <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Capacity Snapshot</div>
          <div class="goal-big">${latest?.actual_vs_allotted_pct != null ? pct(latest.actual_vs_allotted_pct) : "-"}</div>
          <div class="goal-copy">This compares actual risk deployed against allotted risk. If this stays low, you are approving risk but not really putting it to work.</div>
          <div class="prog-track"><div class="prog-fill" style="width:${Math.max(0, Math.min(100, Number(latest?.actual_vs_allotted_pct || 0)))}%"></div></div>
          <div class="goal-meta">
            <div><div class="dash-k">Actual risk deployed</div><div class="dash-v small">${money(latest?.actual_risk)}</div></div>
            <div><div class="dash-k">Allotted risk</div><div class="dash-v small">${money(latest?.allotted_risk)}</div></div>
            <div><div class="dash-k">Daily risk budget</div><div class="dash-v small">${budget > 0 ? money(budget) : "Set in Settings"}</div></div>
            <div><div class="dash-k">Daily actual vs budget</div><div class="dash-v small">${latest?.daily_actual_risk_pct != null ? pct(latest.daily_actual_risk_pct) : "-"}</div></div>
          </div>
        </div>
      </section>
      ${wormGraph}
      <section class="tracker-grid">
        <div class="metric-card"><div class="dash-k">Executed trades on latest entry day</div><div class="metric-big">${latest?.executed_count ?? 0}</div><div class="metric-copy">This helps separate real risk-taking from simple watchlist expansion.</div></div>
        <div class="metric-card"><div class="dash-k">Avg actual risk vs target</div><div class="metric-big">${latest?.avg_actual_risk_pct != null ? pct(latest.avg_actual_risk_pct) : "-"}</div><div class="metric-copy">Use this to judge whether you are still trading too small versus your intended per-trade size.</div></div>
        <div class="metric-card"><div class="dash-k">Avg allotted risk</div><div class="metric-big">${money(latest?.avg_allotted_risk)}</div><div class="metric-copy">What you told yourself the average trade was allowed to use.</div></div>
        <div class="metric-card"><div class="dash-k">Max actual single-trade risk</div><div class="metric-big">${money(latest?.max_single_actual_risk)}</div><div class="metric-copy">The biggest real bet you actually placed, after position size and stop distance.</div></div>
        <div class="metric-card"><div class="dash-k">Per-trade target</div><div class="metric-big">${perTrade > 0 ? money(perTrade) : "-"}</div><div class="metric-copy">Your benchmark for whether actual position size is rising with confidence and execution quality.</div></div>
      </section>
    </section>
  `;
}

function renderPlanStatsTab() {
  const items = goalTracker.plan_stats_items || [];
  const latest = items[items.length - 1] || null;
  const bars = trackerBars(items.slice(-8), {
    valueKey: "adherence_pct",
    pctKey: "adherence_pct",
    currentDate: latest?.date || "",
    label: item => pct(item.adherence_pct),
    fillClass: "",
    emptyText: "Tick the management review checkboxes on executed trades and the plan-discipline graph will populate."
  });
  const scoreClass = Number(latest?.adherence_pct || 0) >= 70 ? "hot" : Number(latest?.adherence_pct || 0) >= 40 ? "warm" : "cold";
  return `
    <section class="tracker-stack">
      <section class="settings-card exp-graph">
        <div>
          <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Sticking To Plan</div>
          <div class="view-note">Your discipline score comes from the execution checklist on real positioned trades.</div>
          <div class="exp-bars">${bars}</div>
        </div>
        <div class="goal-card">
          <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Discipline Snapshot</div>
          <div class="goal-big">${latest?.adherence_pct != null ? pct(latest.adherence_pct) : "-"}</div>
          <div class="goal-copy">A high score means you are not only spotting setups, but also trading them with the behavior you intended.</div>
          <div class="prog-track"><div class="prog-fill" style="width:${Math.max(0, Math.min(100, Number(latest?.adherence_pct || 0)))}%"></div></div>
          <div class="goal-meta">
            <div><div class="dash-k">Tracked trades</div><div class="dash-v small">${latest?.trade_count ?? 0}</div></div>
            <div><div class="dash-k">Checks completed</div><div class="dash-v small">${latest ? `${latest.checks_done}/${latest.checks_total}` : "-"}</div></div>
            <div><div class="dash-k">Followed entry</div><div class="dash-v small">${latest?.followed_entry_pct != null ? pct(latest.followed_entry_pct) : "-"}</div></div>
            <div><div class="dash-k">Respected SL</div><div class="dash-v small">${latest?.respected_sl_pct != null ? pct(latest.respected_sl_pct) : "-"}</div></div>
          </div>
        </div>
      </section>
      <section class="warm-grid">
        <div class="warm-card ${scoreClass}"><div class="warm-date">Followed Entry</div><div class="warm-score">${latest?.followed_entry_pct != null ? pct(latest.followed_entry_pct) : "-"}</div><div class="warm-meta">Did you enter as planned instead of chasing strength?</div></div>
        <div class="warm-card ${scoreClass}"><div class="warm-date">Respected SL</div><div class="warm-score">${latest?.respected_sl_pct != null ? pct(latest.respected_sl_pct) : "-"}</div><div class="warm-meta">Did you honor the stop instead of negotiating with it?</div></div>
        <div class="warm-card ${scoreClass}"><div class="warm-date">Executed Trims</div><div class="warm-score">${latest?.executed_trims_pct != null ? pct(latest.executed_trims_pct) : "-"}</div><div class="warm-meta">Were profits booked near the intended levels?</div></div>
        <div class="warm-card ${scoreClass}"><div class="warm-date">Breakeven Discipline</div><div class="warm-score">${latest?.breakeven_pct != null ? pct(latest.breakeven_pct) : "-"}</div><div class="warm-meta">Did you protect capital once the trade had room to mature?</div></div>
      </section>
    </section>
  `;
}

function renderExposureView() {
  syncChrome();
  const main = e("main");
  if (!main) return;
  let body = renderExposureTab();
  if (goalTrackerTab === "r-progress") body = renderRProgressTab();
  else if (goalTrackerTab === "plan-stats") body = renderPlanStatsTab();
  main.innerHTML = `
    <section class="hero">
      <h2>Goal Tracker</h2>
      <p>Track three things separately: deployment progress, your ability to scale R responsibly, and whether your real execution is staying faithful to the plan.</p>
      ${renderGoalTrackerTabs()}
    </section>
    ${body}
  `;
}

function addPos() {
  if (currentView !== "day" || positions.length >= 5) return;
  const fresh = newPos();
  positions.push(fresh);
  selectedPositionId = fresh.id;
  renderDayView();
  const card = e("detail-pane");
  const input = e("sym-" + fresh.id);
  if (card) card.scrollIntoView({ behavior: "smooth", block: "start" });
  if (input) setTimeout(() => input.focus(), 80);
  autoSave();
}

function removePos(id) {
  if (!confirm("Remove this position?")) return;
  const currentIndex = positions.findIndex(p => p.id === id);
  positions = positions.filter(p => p.id !== id);
  if (selectedPositionId === id) {
    const fallback = positions[Math.max(0, Math.min(currentIndex, positions.length - 1))];
    selectedPositionId = fallback ? fallback.id : null;
  }
  renderDayView();
  autoSave();
}

function clearPlanFields(id) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  if (!confirm("Clear the plan-level fields for this position?")) return;
  p.planEntry = null;
  p.planSL = null;
  p.intraSL = null;
  p.riskAmount = null;
  p.intraQty = null;
  renderDayView();
  autoSave();
}

function clearExecutionFields(id) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  if (!confirm("Clear the execution fields for this position?")) return;
  p.actualEntry = null;
  p.overnightEntry = null;
  p.overnightQty = null;
  p.overnightSL = null;
  p.actualQty = null;
  p.entryDate = "";
  p.posHigh = null;
  p.trailOverride = null;
  p.trailNote = "";
  p.movedBE = false;
  p.trims = p.trims.map(t => ({ pct: t.pct, type: t.type, ap: null, sq: null, dt: "", done: false }));
  renderDayView();
  autoSave();
}

function setThoughtTag(id, tag) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  p.thoughtTag = String(tag || "NOTE").toUpperCase();
  paint(id, p);
  autoSave();
}

function addThoughtLog(id) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  const input = e("thought-input-" + id);
  if (!input) return;
  const text = String(input.value || "").trim();
  if (!text) return;
  const log = normalizeThoughtLog(p.thoughtLog);
  log.push({
    ts: new Date().toISOString(),
    tag: String(p.thoughtTag || "NOTE").toUpperCase(),
    text
  });
  p.thoughtLog = log.slice(-100);
  input.value = "";
  paint(id, p);
  autoSave();
}

function toggleCollapse(id) {
  const p = positions.find(x => x.id === id);
  if (!p) return;
  p.collapsed = !p.collapsed;
  const body = e("pb-" + id);
  const chev = e("chv-" + id);
  if (body) body.className = "pbody" + (p.collapsed ? " hide" : "");
  if (chev) chev.className = "chev" + (!p.collapsed ? " open" : "");
}

function collapseAll() {
  return;
}

function renderDashboardView() {
  syncChrome();
  const main = e("main");
  if (!main) return;
  const cards = dashboardItems.length ? dashboardItems.map(item => `
    <div class="dash-card" onclick="openDay('${item.date}')">
      <div class="dash-date">${fmtDateLabel(item.date)}</div>
      <div class="dash-sub">Open positions carried to this date and plan saved for that day</div>
      <div class="dash-stats">
        <div><div class="dash-k">Open positions</div><div class="dash-v">${item.open_positions_count || 0}</div></div>
        <div><div class="dash-k">Planned trades</div><div class="dash-v">${item.planning_count || 0}</div></div>
        <div><div class="dash-k">Exposure</div><div class="dash-v small">${money(item.exposure)}</div></div>
        <div><div class="dash-k">% Exposure</div><div class="dash-v small">${pct(item.exposure_pct)}</div></div>
      </div>
    </div>
  `).join("") : `<div class="settings-card"><div class="view-note">No saved trade dates yet. Open the Day view and save your first plan, then the grid will appear here.</div></div>`;
  main.innerHTML = `
    <section class="hero">
      <h2>Date Dashboard</h2>
    </section>
    <section class="settings-card">
      <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Global Snapshot</div>
      <div class="dash-stats">
        <div><div class="dash-k">Available capital</div><div class="dash-v small">${money(settings.available_capital)}</div></div>
        <div><div class="dash-k">Daily risk</div><div class="dash-v small">${money(settings.daily_risk)}</div></div>
        <div><div class="dash-k">Per-position risk</div><div class="dash-v small">${money(settings.per_position_risk)}</div></div>
        <div><div class="dash-k">Quick open</div><div class="dash-v small">${fmtDateLabel(latestDashboardDate || todayStr())}</div></div>
      </div>
    </section>
    <section class="dash-grid">${cards}</section>
  `;
}

function renderSettingsView() {
  syncChrome();
  const main = e("main");
  if (!main) return;
  main.innerHTML = `
    <section class="settings-card">
      <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Global Settings</div>
      <div class="view-note">These defaults feed the dashboard exposure numbers, new trade cards, checklist copy, and simulation analysis.</div>
      <div class="settings-grid" style="margin-top:14px">
        <div class="field">
          <div class="flabel">Available capital</div>
          <input type="number" class="fin num" id="set-capital" placeholder="Total trading capital" value="${settings.available_capital != null ? settings.available_capital : ""}">
        </div>
        <div class="field">
          <div class="flabel">Daily risk</div>
          <input type="number" class="fin num" id="set-daily-risk" placeholder="Max risk for the day" value="${settings.daily_risk != null ? settings.daily_risk : ""}">
        </div>
        <div class="field">
          <div class="flabel">Per-position risk</div>
          <input type="number" class="fin num" id="set-position-risk" placeholder="Default risk per trade" value="${settings.per_position_risk != null ? settings.per_position_risk : ""}">
        </div>
        <div class="field">
          <div class="flabel">Stop loss %</div>
          <input type="number" step="0.1" class="fin num" id="set-stop-loss" placeholder="2.0" value="${settings.stop_loss_pct != null ? settings.stop_loss_pct : 2.0}">
        </div>
      </div>
      <div class="settings-card" style="margin-top:14px">
        <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Checklist Copy</div>
        <div class="view-note">Edit the three checklist groups shown in the Day view. Each group can hold up to three items.</div>
        <div class="checklist-editor" style="margin-top:14px">
          ${renderChecklistGroupCards()}
        </div>
      </div>
      <div class="settings-actions">
        <button class="btn-save" onclick="saveSettings()">Save settings</button>
        <button class="btn-sec" onclick="goDashboard()">Back to dashboard</button>
      </div>
    </section>
  `;
}

function renderDayView() {
  syncChrome();
  const main = e("main");
  if (!main) return;
  main.innerHTML = "";
  const empty = document.createElement("div");
  empty.className = "empty";
  empty.id = "empty";
  empty.style.display = positions.length === 0 ? "block" : "none";
  empty.innerHTML = `<h3>No positions for ${fmtDateLabel(planDate)}</h3><p>Add a trade plan or let older open positions carry into this date.</p>`;
  main.appendChild(empty);
  if (e("btn-add")) e("btn-add").disabled = positions.length >= 5;
  const buckets = { overnight: [], current: [], exited: [], planning: [] };
  positions.forEach(p => {
    compute(p);
    buckets[getBucketKey(p)].push(p);
  });
  const selected = ensureSelectedPosition();
  if (selected) selected.collapsed = false;
  let num = 1;
  let selectedNum = 1;
  let listHtml = "";
  getDisplayBuckets().forEach(key => {
    if (!buckets[key].length) return;
    listHtml += `<section class="list-group"><div class="group"><div class="group-h"><div><div class="group-t">${getSectionTitle(key)}</div><div class="group-n">${getSectionNote(key, buckets[key].length)}</div></div></div></div>`;
    buckets[key].forEach(p => {
      if (selected && p.id === selected.id) selectedNum = num;
      const total = (p._realPnl || 0) + (p._openPnl || 0);
      const primary = getPrimaryBadge(p);
      const secondary = getSecondaryBadge(p);
      listHtml += `<button class="plist-item st-${p._status}${selected && p.id === selected.id ? " on" : ""}" id="list-${p.id}" onclick="selectPos('${p.id}')"><div class="plist-top"><div class="plist-main"><div class="plist-title"><span class="plist-dot">${num}</span><span class="plist-sym" id="list-sym-${p.id}">${p.symbol || "SYMBOL"}</span></div><div class="plist-meta" id="list-meta-${p.id}">${getHeaderMeta(p, key)}</div><div class="plist-badges"><span class="badge badge-${primary}" id="list-pbadge-${p.id}">${primary.toUpperCase()}</span><span class="badge badge-${secondary || "ghost"}" id="list-sbadge-${p.id}" style="${secondary ? "" : "display:none"}">${secondary.toUpperCase()}</span></div></div><div class="plist-pnl ${total >= 0 ? "pos" : "neg"}" id="list-pnl-${p.id}">${total !== 0 ? sgn(total) + "Rs " + fi(total) : "-"}</div></div></button>`;
      num += 1;
    });
    listHtml += `</section>`;
  });
  const layout = document.createElement("section");
  layout.className = "day-layout";
  const detailHtml = selected
    ? buildCard(selected, selectedNum)
    : `<div class="plist-empty">Select a position from the left to edit it here.</div>`;
  layout.innerHTML = `<div class="day-list">${listHtml || `<div class="plist-empty">No positions saved for this date yet.</div>`}</div><div class="day-detail" id="detail-pane">${selected ? `<div class="detail-shell">${detailHtml}</div>` : detailHtml}</div>${renderRightRail(selected)}`;
  main.appendChild(layout);
  if (selected) {
    const detailHead = layout.querySelector(".detail-shell .phead");
    if (detailHead) detailHead.removeAttribute("onclick");
  }
  updateSummary();
}

function renderRightRail(p) {
  if (!p) {
    return `<aside class="day-rail" id="live-rail"><div class="rail-empty">Select a position from the left to manage it live.</div></aside>`;
  }
  const thoughtEntries = getThoughtLog(p).slice().reverse();
  const checklistGroups = getChecklistGroups();
  const checklistState = normalizeChecklistStateMatrix(p.mgmt && p.mgmt.checklist ? p.mgmt.checklist : legacyChecklistStateMatrix(p.mgmt));
  const thoughtHtml = thoughtEntries.length ? thoughtEntries.map(item => {
    const tag = String(item.tag || "NOTE").toUpperCase();
    const tagClass = tag.toLowerCase().replace(/[^a-z0-9]+/g, "-");
    return `<article class="thought-item"><div class="thought-head"><span class="thought-time">${escHtml(fmtThoughtStamp(item.ts))}</span><span class="thought-tag tag-${tagClass}">${escHtml(tag)}</span></div><div class="thought-text">${escHtml(item.text)}</div></article>`;
  }).join("") : `<div class="rail-empty small">No thought log entries yet for this trade.</div>`;
  const tags = ["ENTRY", "TRIM", "SL MOVED", "EXIT", "CAUTION", "LESSON"];
  const tagHtml = tags.map(tag => {
    const on = String(p.thoughtTag || "NOTE").toUpperCase() === tag;
    const tagClass = tag.toLowerCase().replace(/[^a-z0-9]+/g, "-");
    return `<button class="tag-chip ${on ? "on" : ""} tag-${tagClass}" type="button" onclick="setThoughtTag('${p.id}','${tag.replace(/'/g, "\\'")}')">${tag}</button>`;
  }).join("");
  return `
    <aside class="day-rail" id="live-rail">
      <section class="rail-card">
        <div class="sec-title">Checklist <span class="sec-note">fill as you manage the trade</span></div>
        <div class="rail-clusters">
          ${checklistGroups.map((group, gi) => {
            const count = Math.max(0, Math.min(3, Number(group.count) || 0));
            const rows = (group.items || []).slice(0, count).map((text, ii) => {
              if (!text) return "";
              const checked = Boolean(checklistState[gi] && checklistState[gi][ii]);
              return `<label class="chk-row"><input type="checkbox" id="ck-${p.id}-${gi}-${ii}" ${checked ? "checked" : ""} onchange="toggleChecklistItem('${p.id}', ${gi}, ${ii}, this.checked)"><span>${escHtml(text)}</span></label>`;
            }).filter(Boolean).join("");
            return `
              <div class="rail-cluster">
                <div class="rail-cluster-h">${escHtml(group.title || `Group ${gi + 1}`)}</div>
                ${rows || `<div class="rail-empty small">No checklist items configured.</div>`}
              </div>
            `;
          }).join("")}
        </div>
      </section>
      <section class="rail-card rail-head">
        <div class="sec-title">Thought Log <span class="sec-note">${thoughtEntries.length} entries</span></div>
        <div class="tag-row">${tagHtml}</div>
        <textarea class="dev-note" id="thought-input-${p.id}" placeholder="What are you seeing, doing, or waiting for in real time?"></textarea>
        <div class="rail-log-wrap"><button class="btn-save rail-log-btn" type="button" onclick="addThoughtLog('${p.id}')">Log</button></div>
        <div class="thought-list">${thoughtHtml}</div>
      </section>
    </aside>`;
}

function renderApp() {
  if (currentView === "dashboard") renderDashboardView();
  else if (currentView === "exposure") renderExposureView();
  else if (currentView === "simulation") renderStreakView();
  else if (currentView === "streaks") renderPlanStreakView();
  else if (currentView === "portfolio") renderPortfolioView();
  else if (currentView === "settings") renderSettingsView();
  else renderDayView();
}

async function goDashboard() {
  await flushPendingSave();
  currentView = "dashboard";
  await loadDashboard();
  renderApp();
}

async function openDay(date) {
  currentView = "day";
  await loadDayView(date);
}

async function goDay() {
  const target = planDate || latestDashboardDate || todayStr();
  await openDay(target);
}

async function goSettings() {
  await flushPendingSave();
  currentView = "settings";
  await loadSettings();
  renderApp();
}

async function goExposure() {
  await flushPendingSave();
  currentView = "exposure";
  await loadGoalTracker();
  renderApp();
}

async function goPortfolio() {
  await flushPendingSave();
  currentView = "portfolio";
  await loadPortfolioSim();
  renderApp();
}

async function loadPortfolioSim() {
  if (!storageOnline) await loadStorageInfo();
  await loadSettings();
  try {
    const payload = await api("/portfolio-sim");
    portfolioSimReport = {
      ok: payload.ok !== false,
      message: payload.message || "",
      summary: payload.summary || {},
      daily: Array.isArray(payload.daily) ? payload.daily : [],
      positions: Array.isArray(payload.positions) ? payload.positions : [],
      open_positions: Array.isArray(payload.open_positions) ? payload.open_positions : [],
      campaign_rows: Array.isArray(payload.campaign_rows) ? payload.campaign_rows : [],
      campaigns: Array.isArray(payload.campaigns) ? payload.campaigns : [],
      starting_capital: payload.starting_capital != null ? Number(payload.starting_capital) : 3000000,
      per_position_budget: payload.per_position_budget != null ? Number(payload.per_position_budget) : 300000,
      latest_market_date: payload.latest_market_date || todayStr(),
      tradebook_path: payload.tradebook_path || "",
      note: payload.note || "",
      debug_log_path: payload.debug_log_path || ""
    };
  } catch (_err) {
    portfolioSimReport = {
      ok: false,
      message: "Unable to load portfolio simulation from the local server.",
      summary: {},
      daily: [],
      positions: [],
      open_positions: [],
      campaign_rows: [],
      campaigns: [],
      starting_capital: 3000000,
      per_position_budget: 300000,
      latest_market_date: todayStr(),
      tradebook_path: "",
      note: "",
      debug_log_path: ""
    };
  }
}

function renderPortfolioView() {
  syncChrome();
  const main = e("main");
  if (!main) return;
  const report = portfolioSimReport || {};
  const summary = report.summary || {};
  const daily = Array.isArray(report.daily) ? report.daily : [];
  const positions = Array.isArray(report.positions) ? report.positions : [];
  const openPositions = Array.isArray(report.open_positions) ? report.open_positions : [];
  const campaignRows = Array.isArray(report.campaign_rows) ? report.campaign_rows : [];
  const startCapital = Number(report.starting_capital || 3000000);
  const perPosition = Number(report.per_position_budget || 300000);
  const latestDay = daily.length ? daily[daily.length - 1] : null;
  const currentValue = summary.portfolio_value != null ? Number(summary.portfolio_value) : (latestDay && latestDay.portfolio_value != null ? Number(latestDay.portfolio_value) : startCapital);
  const profitAmount = summary.portfolio_value != null ? Number(summary.portfolio_value) - startCapital : (latestDay && latestDay.portfolio_value != null ? Number(latestDay.portfolio_value) - startCapital : 0);
  const investedAmount = summary.invested_amount != null ? Number(summary.invested_amount) : (latestDay && latestDay.invested_amount != null ? Number(latestDay.invested_amount) : 0);
  const cashLeft = summary.cash != null ? Number(summary.cash) : (latestDay && latestDay.cash != null ? Number(latestDay.cash) : startCapital);
  const openCount = summary.open_positions_count != null ? Number(summary.open_positions_count) : (latestDay && latestDay.open_positions_count != null ? Number(latestDay.open_positions_count) : openPositions.length);
  const returnPct = summary.portfolio_return_pct != null ? Number(summary.portfolio_return_pct) : (latestDay && latestDay.portfolio_return_pct != null ? Number(latestDay.portfolio_return_pct) : null);
  const fmtNum = v => v == null ? "-" : Number(v).toLocaleString("en-IN", { maximumFractionDigits: 2 });
  const fmtPct = v => v == null ? "-" : `${Number(v).toFixed(2)}%`;
  const reportAlert = report.ok === false && report.message ? `<div class="view-note" style="margin-top:10px;color:#ffb4b4">${escHtml(report.message)}${report.debug_log_path ? ` Debug log: ${escHtml(report.debug_log_path)}` : ""}</div>` : "";
  const dailyRows = daily.length ? daily.map((row, idx) => {
    const val = row.portfolio_value != null ? Number(row.portfolio_value) : null;
    const inv = row.invested_amount != null ? Number(row.invested_amount) : null;
    const cash = row.cash != null ? Number(row.cash) : null;
    const open = row.open_positions_count != null ? Number(row.open_positions_count) : 0;
    const ret = row.portfolio_return_pct != null ? Number(row.portfolio_return_pct) : null;
    return `<tr><td>${idx + 1}</td><td>${escHtml(fmtDateLabel(row.date || ""))}</td><td>${open}</td><td>${fmtNum(inv)}</td><td>${fmtNum(cash)}</td><td>${fmtNum(val)}</td><td class="t-pl ${ret != null && ret >= 0 ? "pos" : "neg"}">${fmtPct(ret)}</td></tr>`;
  }).join("") : `<tr><td colspan="7"><div class="view-note">No daily portfolio snapshots available.</div></td></tr>`;
  const positionRows = campaignRows.length ? campaignRows.map((item, idx) => {
    const simQty = item.sim_qty != null ? Number(item.sim_qty) : null;
    const entry = item.entry_price != null ? Number(item.entry_price) : null;
    const cur = item.current_price != null ? Number(item.current_price) : null;
    const inv = item.invested != null ? Number(item.invested) : null;
    const curVal = item.current_value != null ? Number(item.current_value) : null;
    const pnlPct = item.pnl_pct != null ? Number(item.pnl_pct) : null;
    const isOpen = String(item.status || "").toLowerCase() === "open";
    const statusBadge = `<span class="badge ${isOpen ? "badge-partial" : "badge-closed"}">${isOpen ? "OPEN" : "CLOSED"}</span>`;
    return `<tr><td>${idx + 1}</td><td><strong>${escHtml(item.symbol || "-")}</strong></td><td>${escHtml(fmtDateLabel(item.entry_date || ""))}</td><td>${simQty != null ? simQty : "-"}</td><td>${fmtNum(inv)}</td><td>${fmtNum(curVal)}</td><td class="t-pl ${pnlPct != null && pnlPct >= 0 ? "pos" : "neg"}">${fmtPct(pnlPct)}</td><td>${statusBadge}</td><td>${escHtml(item.exit_time || item.entry_time || "")}</td></tr>`;
  }).join("") : `<tr><td colspan="9"><div class="view-note">No funded simulation campaigns were available.</div></td></tr>`;
  main.innerHTML = `
    <section class="hero">
      <h2>Portfolio</h2>
      <p>Real-money capital simulation using a 30 lakh starting pool and a 3 lakh target per position. Qty is rounded to whole shares, and if free cash drops below the target the simulation uses whatever is left in whole shares. Open positions are marked to bhav closes at each EOD.</p>
    </section>
    <section class="settings-card">
      <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Latest snapshot</div>
        <div class="view-note">Starting capital: ${fmtNum(startCapital)}. Per-position budget target: ${fmtNum(perPosition)}. Latest market date: ${escHtml(report.latest_market_date || "-")}. ${escHtml(report.note || "")}</div>
      ${reportAlert}
      <div class="tracker-grid" style="margin-top:14px">
        <div class="metric-card"><div class="dash-k">Portfolio value</div><div class="metric-big">${fmtNum(currentValue)}</div><div class="metric-copy">Cash plus current market value of open positions.</div></div>
        <div class="metric-card"><div class="dash-k">Profit amount</div><div class="metric-big ${profitAmount >= 0 ? "pos" : "neg"}">${fmtNum(profitAmount)}</div><div class="metric-copy">Portfolio value minus the 30 lakh starting capital.</div></div>
        <div class="metric-card"><div class="dash-k">Invested amount</div><div class="metric-big">${fmtNum(investedAmount)}</div><div class="metric-copy">Capital tied up in open positions at the latest EOD.</div></div>
        <div class="metric-card"><div class="dash-k">Cash left</div><div class="metric-big">${fmtNum(cashLeft)}</div><div class="metric-copy">Unallocated capital still sitting in cash.</div></div>
        <div class="metric-card"><div class="dash-k">Open positions</div><div class="metric-big">${openCount}</div><div class="metric-copy">Positions still open in the simulation at the latest EOD.</div></div>
        <div class="metric-card"><div class="dash-k">Portfolio return</div><div class="metric-big ${returnPct != null && returnPct >= 0 ? "pos" : "neg"}">${fmtPct(returnPct)}</div><div class="metric-copy">Gain or loss versus the 30 lakh starting capital.</div></div>
        <div class="metric-card"><div class="dash-k">Campaigns funded</div><div class="metric-big">${summary.funded_campaigns != null ? summary.funded_campaigns : 0}</div><div class="metric-copy">${summary.skipped_campaigns != null ? summary.skipped_campaigns : 0} campaign(s) were skipped if capital was unavailable.</div></div>
      </div>
      <div class="view-note" style="margin-top:12px">Each campaign targets 3 lakhs at entry. If free cash is lower at the time of entry, the simulation deploys whatever is available in whole shares.</div>
    </section>
    <section class="settings-card" style="margin-top:14px">
      <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Daily portfolio value</div>
      <div class="view-note">EOD snapshots show how much of the 30 lakh pool is deployed, how many positions remain open, and what the whole book is worth at the close of each day.</div>
      <div style="overflow:auto;margin-top:12px">
        <table class="ttbl">
          <thead><tr><th>#</th><th>Date</th><th>Open</th><th>Invested</th><th>Cash</th><th>Value</th><th>Return</th></tr></thead>
          <tbody>${dailyRows}</tbody>
        </table>
      </div>
    </section>
    <section class="settings-card" style="margin-top:14px">
      <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">Campaign simulation</div>
        <div class="view-note">Each campaign targets 3 lakhs at entry. Open rows show mark-to-market value; closed rows show realized value.</div>
      <div style="overflow:auto;margin-top:12px">
        <table class="ttbl">
          <thead><tr><th>#</th><th>Symbol</th><th>Entry</th><th>Sim qty</th><th>Invested</th><th>Current / Realized</th><th>P/L</th><th>Status</th><th>Closed / last update</th></tr></thead>
          <tbody>${positionRows}</tbody>
        </table>
      </div>
    </section>
  `;
}

async function loadStreakReport() {
  if (!storageOnline) await loadStorageInfo();
  await loadSettings();
  try {
    const payload = await api("/stop-loss-streak");
    streakReport = {
      ok: payload.ok !== false,
      message: payload.message || "",
      summary: payload.summary || {},
      campaigns: Array.isArray(payload.campaigns) ? payload.campaigns : [],
      closed_campaigns: Array.isArray(payload.closed_campaigns) ? payload.closed_campaigns : [],
      open_campaigns_list: Array.isArray(payload.open_campaigns_list) ? payload.open_campaigns_list : [],
      latest_trade_date: payload.latest_trade_date || todayStr(),
      stop_loss_pct: payload.stop_loss_pct != null ? payload.stop_loss_pct : Number(settings.stop_loss_pct || 2.0),
      note: payload.note || "",
      tradebook_path: payload.tradebook_path || "",
      history_start_date: "",
      debug_log_path: payload.debug_log_path || ""
    };
    } catch (_err) {
      streakReport = {
        ok: false,
        message: "Unable to load simulation report from the local server.",
        summary: {},
        campaigns: [],
        closed_campaigns: [],
      open_campaigns_list: [],
      latest_trade_date: todayStr(),
      stop_loss_pct: Number(settings.stop_loss_pct || 2.0),
      tradebook_path: "",
      history_start_date: "",
      debug_log_path: ""
    };
  }
}

async function loadPlanStreakReport() {
  if (!storageOnline) await loadStorageInfo();
  await loadSettings();
  try {
    const payload = await api("/streaks");
    planStreakReport = {
      ok: payload.ok !== false,
      message: payload.message || "",
      summary: payload.summary || {},
      campaigns: Array.isArray(payload.campaigns) ? payload.campaigns : [],
      closed_campaigns: Array.isArray(payload.closed_campaigns) ? payload.closed_campaigns : [],
      open_campaigns_list: Array.isArray(payload.open_campaigns_list) ? payload.open_campaigns_list : [],
      latest_trade_date: payload.latest_trade_date || todayStr(),
      history_start_date: payload.history_start_date || "2026-04-17",
      stop_loss_pct: payload.stop_loss_pct != null ? payload.stop_loss_pct : Number(settings.stop_loss_pct || 2.0),
      note: payload.note || "",
      tradebook_path: payload.tradebook_path || "",
      debug_log_path: payload.debug_log_path || ""
    };
  } catch (_err) {
    planStreakReport = {
      ok: false,
      message: "Unable to load streaks report from the local server.",
      summary: {},
      campaigns: [],
      closed_campaigns: [],
      open_campaigns_list: [],
      latest_trade_date: todayStr(),
      history_start_date: "2026-04-17",
      stop_loss_pct: Number(settings.stop_loss_pct || 2.0),
      tradebook_path: "",
      debug_log_path: ""
    };
  }
}

function renderExecutionRibbon(campaigns) {
  const items = Array.isArray(campaigns)
    ? campaigns
        .filter(item => {
          const status = String(item.status || "").toUpperCase();
          return status === "HONORED" || status === "VIOLATED";
        })
        .slice(-10)
    : [];
  if (!items.length) return "";
  const meta = {
    HONORED: { fg: "#22c55e", bg: "rgba(34,197,94,.12)", ring: "rgba(34,197,94,.28)" },
    VIOLATED: { fg: "#ef4444", bg: "rgba(239,68,68,.12)", ring: "rgba(239,68,68,.28)" }
  };
  const total = items.length;
  const honored = items.filter(item => String(item.status || "").toUpperCase() === "HONORED").length;
  const violated = items.filter(item => String(item.status || "").toUpperCase() === "VIOLATED").length;
  const chips = items.map(item => {
    const status = String(item.status || "OPEN").toUpperCase();
    const m = meta[status] || meta.HONORED;
    const label = escHtml(status);
    const symbol = escHtml(item.symbol || "-");
    const date = escHtml(fmtDateLabel(item.entry_date || item.price_date || ""));
    return `
      <div style="min-width:124px;flex:0 0 auto;padding:10px 12px;border-radius:14px;border:1px solid ${m.ring};background:${m.bg};color:${m.fg};box-shadow:inset 0 1px 0 rgba(255,255,255,.03)">
        <div style="display:flex;justify-content:space-between;gap:10px;align-items:center;font-size:12px;letter-spacing:.02em">
          <strong style="font-size:13px">${symbol}</strong>
          <span style="font-size:10px;font-weight:800;opacity:.9">${label}</span>
        </div>
        <div style="margin-top:4px;font-size:10px;opacity:.78">${date}</div>
      </div>
    `;
  }).join("");
  return `
    <div class="streak-rail" style="margin-top:14px;padding:14px 14px 12px;border-radius:18px;border:1px solid rgba(148,163,184,.16);background:linear-gradient(180deg,rgba(10,14,25,.92),rgba(12,16,28,.72));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)">
      <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:14px;flex-wrap:wrap">
        <div>
          <div class="sec-title" style="margin:0 0 4px 0;border:none;padding:0">Trade ribbon</div>
          <div class="view-note" style="margin:0">Only honored and violated trades are shown.</div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <span class="badge" style="background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.25)">HONORED ${honored}</span>
          <span class="badge" style="background:rgba(239,68,68,.12);color:#ef4444;border:1px solid rgba(239,68,68,.25)">VIOLATED ${violated}</span>
          <span class="badge" style="background:rgba(148,163,184,.10);color:#cbd5e1;border:1px solid rgba(148,163,184,.16)">TRACKED ${total}</span>
        </div>
      </div>
      <div style="display:flex;gap:10px;overflow:auto;margin-top:12px;padding-bottom:2px">
        ${chips}
      </div>
    </div>
  `;
}

function getStreakAffirmation(summary) {
  const honored = Number(summary?.honored_campaigns || 0);
  const violated = Number(summary?.violated_campaigns || 0);
  const open = Number(summary?.open_campaigns || 0);
  const current = Number(summary?.current_honor_streak || 0);
  const best = Number(summary?.longest_honor_streak || 0);
  const total = honored + violated;
  const rate = total > 0 ? (honored / total) * 100 : null;
  if (total === 0) return { lead: "Start the chain.", body: "One honored stop starts momentum." };
  if (open > 0 && current > 0) return { lead: "Guard the live trade.", body: `${current} clean reps in a row. Don't hand back the edge.` };
  if (violated > honored) return { lead: "Reset fast.", body: "Tight stops. Clean exits. No excuses." };
  if (current >= 3 || best >= 5 || (rate != null && rate >= 75)) return { lead: "You're in control.", body: "Clean trades are stacking. Keep the promise." };
  return { lead: "Stay sharp.", body: "Respect the stop and let the streak compound." };
}

// STREAK FRAME: habit-style execution progress block for the Streaks tab.
function renderStreakFrame(campaigns, summary) {
  const items = Array.isArray(campaigns)
    ? [...campaigns]
        .sort((a, b) => String(b.entry_date || b.price_date || "").localeCompare(String(a.entry_date || a.price_date || "")))
        .filter(item => {
          const status = String(item.status || "").toUpperCase();
          return status === "HONORED" || status === "VIOLATED";
        })
        .slice(0, 14)
    : [];
  if (!items.length) return "";
  const current = Number(summary?.current_honor_streak || 0);
  const best = Number(summary?.longest_honor_streak || 0);
  const honoredCount = Number(summary?.honored_campaigns || 0);
  const violatedCount = Number(summary?.violated_campaigns || 0);
  const recent = items.slice(0, 6);
  const recentHonored = recent.filter(item => String(item.status || "").toUpperCase() === "HONORED").length;
  const recentTotal = recent.length;
  const recentViolated = Math.max(0, recentTotal - recentHonored);
  const recentLead = recentTotal
    ? `${recentHonored} of the last ${recentTotal} trades honored your stop.`
    : "Build the first clean rep.";
  const recentBody = recentTotal
    ? (recentHonored >= recentViolated
      ? "Small wins. Strong habits. Big edge."
      : "Reset fast. Tight stops. Clean execution.")
    : "One honored stop starts momentum.";
  const actualPnlTotal = summary?.actual_pnl_total != null ? Number(summary.actual_pnl_total) : null;
  const plannedPnlTotal = summary?.planned_pnl_total != null ? Number(summary.planned_pnl_total) : null;
  const stopPnlTotal = summary?.stop_pnl_total != null ? Number(summary.stop_pnl_total) : null;
  const moneyLeftOnTable = summary?.money_left_on_table != null ? Number(summary.money_left_on_table) : null;
  const allTimeHonoredCount = summary?.all_time_honored_count != null ? Number(summary.all_time_honored_count) : null;
  const allTimeTradeCount = summary?.all_time_trade_count != null ? Number(summary.all_time_trade_count) : null;
  const allTimeHonorRate = summary?.all_time_honor_rate != null ? Number(summary.all_time_honor_rate) : null;
  const statusMeta = {
    HONORED: { fg: "#4ade80", bg: "rgba(34,197,94,.16)", ring: "rgba(34,197,94,.30)", icon: "&#10003;" },
    VIOLATED: { fg: "#ef4444", bg: "rgba(239,68,68,.16)", ring: "rgba(239,68,68,.30)", icon: "&#215;" }
  };
  const iconByStatus = status => status === "HONORED" ? "&#10003;" : "&#215;";
  const detailFor = item => {
    const status = String(item.status || "").toUpperCase();
    const isTrail = Number(item.plan_trail_override || 0) > 0 || Number(item.plan_current_sl || 0) > Number(item.buy_price || 0) * 0.985 + 0.01 || /trail/i.test(String(item.status_reason || ""));
    if (status === "HONORED") {
      return isTrail
        ? { headline: "Trailing stop hit", body: "Discipline protects." }
        : { headline: "Initial stop hit", body: "Plan. Protect. Perform." };
    }
    return { headline: "Exited above stop", body: "Review. Improve. Repeat." };
  };
  const tiles = items.map(item => {
    const status = String(item.status || "HONORED").toUpperCase();
    const meta = statusMeta[status] || statusMeta.HONORED;
    const label = escHtml(item.symbol || "-");
    const date = escHtml(fmtDateLabel(item.entry_date || item.price_date || ""));
    const detail = detailFor(item);
    const leftOnTable = Number(item.target_miss_pnl != null ? item.target_miss_pnl : 0);
    const statusRow = status === "VIOLATED" && Number.isFinite(leftOnTable) && leftOnTable > 1000
      ? `<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:8px"><div style="display:inline-flex;align-items:center;gap:6px;padding:2px 9px;border-radius:999px;border:1px solid ${meta.ring};background:${meta.bg};color:${meta.fg};font-size:10px;font-weight:900;letter-spacing:.02em">${status}</div><div style="display:inline-flex;align-items:center;padding:2px 10px;border-radius:999px;border:1px solid rgba(245,158,11,.38);background:linear-gradient(180deg,rgba(245,158,11,.26),rgba(245,158,11,.14));color:#ffd875;font-size:10px;font-weight:950;letter-spacing:.02em;text-shadow:0 0 10px rgba(255,191,64,.28)">${pricePlain(leftOnTable)}</div></div>`
      : "";
    return `
        <div style="min-width:176px;flex:0 0 auto;padding:12px 12px 10px;border-radius:16px;border:1px solid ${meta.ring};background:linear-gradient(180deg,rgba(16,20,30,.94),rgba(10,14,24,.86));box-shadow:inset 0 1px 0 rgba(255,255,255,.03);text-align:left">
          <div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start">
            <div style="font-size:13px;font-weight:900;letter-spacing:.01em;color:#f8fafc">${label}</div>
            <div style="font-size:10px;color:#cbd5e1">${date}</div>
          </div>
          ${statusRow || `<div style="display:inline-flex;align-items:center;gap:6px;margin-top:8px;padding:2px 9px;border-radius:999px;border:1px solid ${meta.ring};background:${meta.bg};color:${meta.fg};font-size:10px;font-weight:900;letter-spacing:.02em">${status}</div>`}
          <div style="display:flex;align-items:center;justify-content:center;margin:12px 0 8px 0">
            <div style="width:58px;height:58px;border-radius:999px;border:1px solid ${meta.ring};background:rgba(255,255,255,.03);display:flex;align-items:center;justify-content:center;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)">
              <div style="font-size:30px;line-height:1;color:${meta.fg};font-weight:900">${iconByStatus(status)}</div>
            </div>
          </div>
          <div style="height:3px;border-radius:999px;background:${status === "HONORED" ? "linear-gradient(90deg, rgba(34,197,94,.92), rgba(74,222,128,.72))" : "linear-gradient(90deg, rgba(239,68,68,.92), rgba(248,113,113,.72))"};box-shadow:0 0 0 1px ${meta.ring} inset"></div>
          <div style="margin-top:10px;font-size:13px;line-height:1.2;font-weight:800;color:#f8fafc">${detail.headline}</div>
          <div style="margin-top:3px;font-size:11px;line-height:1.3;color:#b8c4d6">${detail.body}</div>
        </div>
      `;
  }).join("");
  return `
    <div style="margin-top:14px;padding:14px;border-radius:20px;border:1px solid rgba(148,163,184,.16);background:linear-gradient(180deg,rgba(10,14,25,.96),rgba(12,16,28,.78));box-shadow:0 18px 50px rgba(0,0,0,.26),inset 0 1px 0 rgba(255,255,255,.03)">
      <div style="display:flex;justify-content:space-between;gap:14px;align-items:flex-start;flex-wrap:wrap;margin-bottom:12px">
        <div style="font-size:20px;line-height:1.05;font-weight:950;letter-spacing:.02em;color:#f8fafc">HONOR THY STOP</div>
        <div style="max-width:520px;text-align:right;font-size:12px;line-height:1.35;font-weight:800;color:#ffd875;letter-spacing:.01em">STOPLOSS IS A PROMISE . WHAT GOOD IS A MAN WHO DOES NOT KEEP HIS PROMISE</div>
      </div>
      <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:stretch">
        <div style="flex:1;min-width:190px;padding:14px 16px;border-radius:16px;border:1px solid rgba(148,163,184,.12);background:rgba(255,255,255,.02)">
          <div class="dash-k" style="margin-bottom:6px;color:#43d17c">CURRENT STREAK</div>
          <div style="display:flex;align-items:flex-end;gap:12px">
            <div style="font-size:50px;line-height:0.9;font-weight:950;color:#5ee38d">${current}</div>
            <div style="font-size:28px;line-height:1;color:#5ee38d">&#128293;</div>
          </div>
          <div style="margin-top:6px;font-size:14px;color:#f8fafc">${current === 1 ? "execution honored" : "executions honored"}</div>
        </div>
        <div style="flex:1;min-width:190px;padding:14px 16px;border-radius:16px;border:1px solid rgba(148,163,184,.12);background:rgba(255,255,255,.02)">
          <div class="dash-k" style="margin-bottom:6px;color:#cbd5e1">BEST STREAK</div>
          <div style="display:flex;align-items:flex-end;gap:12px">
            <div style="font-size:50px;line-height:0.9;font-weight:950;color:#f8fafc">${best}</div>
            <div style="font-size:28px;line-height:1;color:#5ee38d">&#9734;</div>
          </div>
          <div style="margin-top:6px;font-size:14px;color:#cbd5e1">in this window</div>
        </div>
        <div style="flex:1;min-width:190px;padding:14px 16px;border-radius:16px;border:1px solid rgba(148,163,184,.12);background:rgba(255,255,255,.02)">
          <div class="dash-k" style="margin-bottom:6px;color:#cbd5e1">TOTAL HONORED</div>
          <div style="display:flex;align-items:flex-end;gap:10px">
            <div style="font-size:50px;line-height:0.9;font-weight:950;color:#f8fafc">${allTimeHonoredCount != null ? allTimeHonoredCount : "-"}</div>
            <div style="font-size:18px;line-height:1;color:#cbd5e1">/ ${allTimeTradeCount != null ? allTimeTradeCount : "-"}</div>
          </div>
          <div style="margin-top:6px;font-size:14px;color:#cbd5e1">${allTimeHonorRate != null ? `${allTimeHonorRate.toFixed(1)}% honored to date` : "all trades to date"}</div>
        </div>
        <div style="flex:1.1;min-width:220px;padding:14px 16px;border-radius:16px;border:1px solid rgba(148,163,184,.12);background:rgba(255,255,255,.02)">
          <div class="dash-k" style="margin-bottom:6px;color:#f0f4ff">MONEY LEFT ON THE TABLE</div>
          <div style="font-size:24px;line-height:1.05;font-weight:950;color:${moneyLeftOnTable != null && moneyLeftOnTable >= 0 ? "#5ee38d" : "#ef4444"}">${moneyLeftOnTable != null ? `${moneyLeftOnTable >= 0 ? "+" : "-"}${pricePlain(Math.abs(moneyLeftOnTable))}` : "-"}</div>
          <div class="view-note" style="margin-top:6px;font-size:12px;line-height:1.35">If every target had been hit: ${plannedPnlTotal != null ? `${plannedPnlTotal >= 0 ? "+" : "-"}${pricePlain(Math.abs(plannedPnlTotal))}` : "-"} vs actual ${actualPnlTotal != null ? `${actualPnlTotal >= 0 ? "+" : "-"}${pricePlain(Math.abs(actualPnlTotal))}` : "-"}</div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px">
            <span class="badge" style="background:rgba(34,197,94,.14);color:#22c55e;border:1px solid rgba(34,197,94,.25)">HONORED ${honoredCount}</span>
            <span class="badge" style="background:rgba(239,68,68,.14);color:#ef4444;border:1px solid rgba(239,68,68,.25)">VIOLATED ${violatedCount}</span>
          </div>
        </div>
      </div>
      <div style="margin:6px 0 10px 0;font-size:16px;line-height:1.2;font-weight:900;color:#5ee38d">${escHtml(recentLead)}</div>
      <div style="margin:-4px 0 10px 0;font-size:12px;line-height:1.35;color:#94a3b8">${escHtml(recentBody)}</div>
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:18px;margin-bottom:12px">
        <div style="font-size:16px;font-weight:900;color:#f8fafc">Recent executions <span style="margin-left:8px;padding:2px 10px;border-radius:999px;border:1px solid rgba(148,163,184,.16);background:rgba(255,255,255,.04);font-size:11px;color:#cbd5e1">${recent.length}</span></div>
      </div>
      <div style="display:flex;gap:14px;overflow:auto;padding-bottom:6px">
        ${tiles}
      </div>
    </div>
  `;
}

function renderStreakView() {
  syncChrome();
  const main = e("main");
  if (!main) return;
  const isPlanMode = currentView === "streaks";
  const activeReport = isPlanMode ? planStreakReport : streakReport;
  const summary = activeReport.summary || {};
  const stop = Number(activeReport.stop_loss_pct || settings.stop_loss_pct || 2.0);
  const closed = Array.isArray(activeReport.closed_campaigns) ? activeReport.closed_campaigns : [];
  const open = Array.isArray(activeReport.open_campaigns_list) ? activeReport.open_campaigns_list : [];
  const totalClosed = summary.closed_campaigns != null ? summary.closed_campaigns : closed.length;
  const honorRate = summary.honor_rate != null ? summary.honor_rate : (totalClosed ? (summary.honored_campaigns || 0) / totalClosed * 100 : null);
  const violationRate = summary.violation_rate != null ? summary.violation_rate : (totalClosed && honorRate != null ? Math.max(0, 100 - honorRate) : null);
    const actualWinRate = summary.actual_win_rate != null ? summary.actual_win_rate : summary.win_rate;
    const actualLossRate = summary.actual_loss_rate != null ? summary.actual_loss_rate : summary.loss_rate;
    const counterWinRate = summary.counterfactual_win_rate != null ? summary.counterfactual_win_rate : actualWinRate;
    const counterLossRate = summary.counterfactual_loss_rate != null ? summary.counterfactual_loss_rate : actualLossRate;
    const actualAvgGain = summary.actual_avg_gain_pct != null ? summary.actual_avg_gain_pct : null;
    const actualAvgLoss = summary.actual_avg_loss_pct != null ? summary.actual_avg_loss_pct : null;
    const counterAvgGain = summary.counterfactual_avg_gain_pct != null ? summary.counterfactual_avg_gain_pct : null;
    const counterAvgLoss = summary.counterfactual_avg_loss_pct != null ? summary.counterfactual_avg_loss_pct : null;
    const actualBeCount = summary.actual_breakeven_count != null ? summary.actual_breakeven_count : 0;
    const counterBeCount = summary.counterfactual_breakeven_count != null ? summary.counterfactual_breakeven_count : 0;
    const fmtRate = v => v == null ? "-" : `${Number(v).toFixed(1)}%`;
    const fmtPct = v => v == null ? "-" : `${Number(v).toFixed(2)}%`;
  const tradebookName = String(activeReport.tradebook_path || "").split(/[\\/]/).pop() || "not found";
  const sourceName = isPlanMode ? "saved trade-plan snapshots" : tradebookName;
  const reportAlert = activeReport.ok === false && activeReport.message ? `<div class="view-note" style="margin-top:10px;color:#ffb4b4">${escHtml(activeReport.message)}${activeReport.debug_log_path ? ` Debug log: ${escHtml(activeReport.debug_log_path)}` : ""}</div>` : "";
  const frameHtml = isPlanMode ? renderStreakFrame(activeReport.campaigns || [...closed, ...open], summary) : "";
  const closedVisual = isPlanMode ? [...closed].sort((a, b) => String(b.entry_date || b.price_date || "").localeCompare(String(a.entry_date || a.price_date || ""))) : closed;
  const openVisual = isPlanMode ? [...open].sort((a, b) => String(b.entry_date || b.price_date || "").localeCompare(String(a.entry_date || a.price_date || ""))) : open;
  const rowHtml = (arr, emptyMsg) => {
    const rows = [];
    let totalQty = 0;
    let totalInvested = 0;
    let totalValue = 0;
    let totalPnl = 0;
    let totalSimReturn = 0;
    let simReturnCount = 0;
    for (const item of arr) {
      const qty = isPlanMode
        ? (item.actual_qty != null ? Number(item.actual_qty) : (item.buy_qty != null ? Number(item.buy_qty) : null))
        : (item.sim_qty != null ? Number(item.sim_qty) : null);
      const buyPrice = item.buy_price != null
        ? Number(item.buy_price)
        : (item.entry_price != null ? Number(item.entry_price) : null);
      const stopPrice = item.stop_price != null ? Number(item.stop_price) : null;
      const completedTrimCount = isPlanMode ? Number(item.completed_trim_count || 0) : 0;
      const sellPrice = isPlanMode
        ? (item.executed_sell_price != null ? Number(item.executed_sell_price) : (item.current_cmp != null ? Number(item.current_cmp) : null))
        : (item.simulated_exit_price != null ? Number(item.simulated_exit_price) : null);
      const value = isPlanMode
        ? (item.actual_value != null ? Number(item.actual_value) : null)
        : (item.sim_value != null ? Number(item.sim_value) : null);
      const actualPct = item.actual_return_pct != null ? Number(item.actual_return_pct) : (item.return_pct != null ? Number(item.return_pct) : null);
      const stopPnl = isPlanMode && item.target_pnl != null
        ? Number(item.target_pnl)
        : (isPlanMode && completedTrimCount > 0 && qty != null && buyPrice != null && stopPrice != null ? (qty * (stopPrice - buyPrice)) : null);
      const simPct = item.counterfactual_return_pct != null ? Number(item.counterfactual_return_pct) : null;
      const statusValue = String(item.status || item.plan_status || (isPlanMode ? "" : (item.stop_touched ? "closed" : "open"))).toLowerCase();
      const statusLabel = statusValue ? statusValue.toUpperCase() : (isPlanMode ? "OPEN" : (!Boolean(item.stop_touched) ? "OPEN" : "CLOSED"));
      const statusBadge = isPlanMode
        ? (statusLabel === "HONORED"
          ? `<span class="badge" style="background:rgba(34,197,94,.14);color:#22c55e;border:1px solid rgba(34,197,94,.30)">${escHtml(statusLabel)}</span>`
          : statusLabel === "OPEN"
          ? `<span class="badge" style="background:rgba(245,158,11,.14);color:#f59e0b;border:1px solid rgba(245,158,11,.30)">${escHtml(statusLabel)}</span>`
          : `<span class="badge" style="background:rgba(239,68,68,.14);color:#ef4444;border:1px solid rgba(239,68,68,.30)">${escHtml(statusLabel)}</span>`)
        : (statusLabel === "CLOSED" || statusLabel === "DONE"
          ? `<span class="badge" style="background:rgba(148,163,184,.12);color:#94a3b8;border:1px solid rgba(148,163,184,.25)">${escHtml(statusLabel)}</span>`
          : `<span class="badge" style="background:rgba(34,197,94,.14);color:#22c55e;border:1px solid rgba(34,197,94,.30)">${escHtml(statusLabel)}</span>`);
      const invested = qty != null && buyPrice != null ? qty * buyPrice : null;
      const pnl = value != null && invested != null ? value - invested : null;
      if (qty != null) totalQty += qty;
      if (invested != null) totalInvested += invested;
      if (value != null) totalValue += value;
      if (pnl != null) totalPnl += pnl;
      if (simPct != null) {
        totalSimReturn += simPct;
        simReturnCount += 1;
      }
      const sellTxt = sellPrice != null ? pricePlain(sellPrice) : "-";
      const stopPnlTxt = stopPnl != null ? (Math.abs(stopPnl) < 0.005 ? "0.00" : `${stopPnl >= 0 ? "+" : "-"}${pricePlain(Math.abs(stopPnl))}`) : "-";
      const leftOnTable = isPlanMode && item.target_miss_pnl != null
        ? Number(item.target_miss_pnl)
        : (isPlanMode && completedTrimCount > 0 && stopPnl != null && pnl != null ? (stopPnl - pnl) : null);
      const leftOnTableTxt = leftOnTable != null ? (Math.abs(leftOnTable) < 0.005 ? "0.00" : `${leftOnTable >= 0 ? "+" : "-"}${pricePlain(Math.abs(leftOnTable))}`) : "-";
      const valueTxt = value != null ? pricePlain(value) : "-";
      const pnlTxt = pnl != null ? `${pnl >= 0 ? "+" : "-"}${pricePlain(Math.abs(pnl))}` : "-";
      const pctCell = isPlanMode
        ? `<td class="t-pl ${actualPct != null && actualPct >= 0 ? "pos" : "neg"}">${actualPct != null ? `${actualPct >= 0 ? "+" : ""}${actualPct.toFixed(2)}%` : "-"}</td>`
        : `<td class="t-pl ${actualPct != null && actualPct >= 0 ? "pos" : "neg"}">${actualPct != null ? `${actualPct >= 0 ? "+" : ""}${actualPct.toFixed(2)}%` : "-"}</td><td class="t-pl ${simPct != null && simPct >= 0 ? "pos" : "neg"}">${simPct != null ? `${simPct >= 0 ? "+" : ""}${simPct.toFixed(2)}%` : "-"}</td>`;
      rows.push(`<tr><td>${rows.length + 1}</td><td><strong>${escHtml(item.symbol || "-")}</strong></td><td>${escHtml(fmtDateLabel(item.entry_date || item.start_time || ""))}</td><td>${qty != null ? qty : "-"}</td><td>${buyPrice != null ? pricePlain(buyPrice) : "-"}</td><td>${sellTxt}</td><td class="t-pl ${stopPnl != null && stopPnl >= 0 ? "pos" : "neg"}">${stopPnlTxt}</td><td class="t-pl ${leftOnTable != null && leftOnTable >= 0 ? "pos" : "neg"}">${leftOnTableTxt}</td><td>${valueTxt}</td><td class="t-pl ${pnl != null && pnl >= 0 ? "pos" : "neg"}">${pnlTxt}</td>${pctCell}<td>${statusBadge}</td></tr>`);
    }
    const totalSimAvg = simReturnCount ? totalSimReturn / simReturnCount : null;
    const totalPnlTxt = `${totalPnl >= 0 ? "+" : "-"}${pricePlain(Math.abs(totalPnl))}`;
    const totalRow = rows.length
      ? isPlanMode
        ? `<tr class="total-row"><td></td><td><strong>TOTAL</strong></td><td></td><td>${totalQty || "-"}</td><td>${totalInvested ? pricePlain(totalInvested / Math.max(totalQty, 1)) : "-"}</td><td></td><td class="t-pl ${summary?.planned_pnl_total != null && Number(summary.planned_pnl_total) >= 0 ? "pos" : "neg"}">${summary?.planned_pnl_total != null ? `${Number(summary.planned_pnl_total) >= 0 ? "+" : "-"}${pricePlain(Math.abs(Number(summary.planned_pnl_total)))}` : "-"}</td><td class="t-pl ${summary?.money_left_on_table != null && Number(summary.money_left_on_table) >= 0 ? "pos" : "neg"}">${summary?.money_left_on_table != null ? `${Number(summary.money_left_on_table) >= 0 ? "+" : "-"}${pricePlain(Math.abs(Number(summary.money_left_on_table)))}` : "-"}</td><td>${pricePlain(totalValue)}</td><td class="t-pl ${totalPnl >= 0 ? "pos" : "neg"}">${totalPnlTxt}</td><td></td><td></td></tr>`
        : `<tr class="total-row"><td></td><td><strong>TOTAL</strong></td><td></td><td>${totalQty || "-"}</td><td>${totalInvested ? pricePlain(totalInvested / Math.max(totalQty, 1)) : "-"}</td><td></td><td>${pricePlain(totalValue)}</td><td class="t-pl ${totalPnl >= 0 ? "pos" : "neg"}">${totalPnlTxt}</td><td></td><td class="t-pl ${totalSimAvg != null && totalSimAvg >= 0 ? "pos" : "neg"}">${totalSimAvg != null ? `${totalSimAvg >= 0 ? "+" : ""}${totalSimAvg.toFixed(2)}%` : "-"}</td><td></td></tr>`
      : "";
    return { body: rows.join(""), totalRow };
  };
  const closedEmptyMsg = isPlanMode ? "No honored trades were found in the history window." : "No closed campaigns found in the latest tradebook.";
  const openEmptyMsg = isPlanMode ? "No violated trades were present in the history window." : "No open campaigns were present in the latest tradebook.";
  const closedTable = rowHtml(closedVisual, closedEmptyMsg);
  const openTable = rowHtml(openVisual, openEmptyMsg);
   const tableCols = isPlanMode ? 12 : 11;
   const tableHeader = isPlanMode
     ? "<tr><th>#</th><th>Symbol</th><th>Entry</th><th>Qty</th><th>Buy</th><th>Sell / CMP</th><th>Target P/L</th><th>Money left on table</th><th>Value</th><th>P/L</th><th>Actual %</th><th>Status</th></tr>"
     : "<tr><th>#</th><th>Symbol</th><th>Entry</th><th>Sim qty</th><th>Buy</th><th>Sim sell / CMP</th><th>Sim value</th><th>P/L</th><th>Actual %</th><th>Sim %</th><th>Status</th></tr>";
  main.innerHTML = `
    ${isPlanMode ? "" : `
    <section class="hero">
      <h2>Simulation</h2>
      <p>Measures whether each closed campaign stayed inside your selected stop loss. This is percentage-only and focuses on discipline, not rupees.</p>
    </section>`}
    ${isPlanMode
      ? `<div style="margin-top:0">${reportAlert}${frameHtml}</div>`
      : `<section class="settings-card">
          <div class="sec-title" style="margin-bottom:12px;border:none;padding-bottom:0">Latest snapshot</div>
          <div class="view-note">Stop loss used for this analysis: ${pct(stop)}. Latest tradebook: ${escHtml(tradebookName)}. The table compares actual live return with the stop-plan path: day 1 checks intraday after the actual buy time, day 2 checks the daily chart close against the original 2% stop, and from day 3 onward the stop trails to breakeven or the prior 5-day EMA, whichever is higher.</div>
          ${reportAlert}
          ${frameHtml}
          <div class="view-note" style="margin-top:12px">${escHtml(activeReport.note || "No additional note.")}</div>
        </section>`}
      <section class="settings-card" style="margin-top:14px">
        <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">${isPlanMode ? "Open trades" : "Open trades"}</div>
         <div class="view-note">${isPlanMode ? "These trades are still open. The table shows the latest state only." : "These are still open in the latest tradebook. Their return is marked to the latest bhav close when available."}</div>
        <div style="overflow:auto;margin-top:12px">
            <table class="ttbl">
            <thead>${tableHeader}</thead>
            <tbody>${openTable.body || `<tr><td colspan="${tableCols}"><div class="view-note">${escHtml(openEmptyMsg)}</div></td></tr>`}</tbody>
            </table>
          </div>
      </section>
      <section class="settings-card" style="margin-top:14px">
        <div class="sec-title" style="margin-bottom:12px;border-bottom:none;padding-bottom:0">${isPlanMode ? "Closed trades" : "Closed campaigns"}</div>
         <div class="view-note">${isPlanMode ? "Each row shows actual P&L, target P&L, and the gap between them using only the saved snapshot. HONORED means the exit was at or below the active stop; VIOLATED means it exited above the stop." : "Each row is one round-trip campaign grouped by symbol and execution order. Actual return is the live trade outcome; the stop-plan column shows what the trade would have returned if your stop plan had been followed against real market data after the buy."}</div>
        <div style="overflow:auto;margin-top:12px">
          <table class="ttbl">
            <thead>${tableHeader}</thead>
            <tbody>${closedTable.body || `<tr><td colspan="${tableCols}"><div class="view-note">${escHtml(closedEmptyMsg)}</div></td></tr>`}</tbody>
            ${closedTable.totalRow ? `<tfoot>${closedTable.totalRow}</tfoot>` : ""}
          </table>
        </div>
      </section>
    `;
}

function renderPlanStreakView() {
  renderStreakView();
}

async function goSimulation() {
  await flushPendingSave();
  currentView = "simulation";
  await loadStreakReport();
  renderApp();
}

async function goStreaks() {
  await flushPendingSave();
  currentView = "streaks";
  await loadPlanStreakReport();
  renderApp();
}

function buildCard(p, num) {
  const convHtml = [1, 2, 3, 4, 5].map(i => `<span class="cdot ${i <= p.conviction ? "on" : ""}" onclick="setConv('${p.id}',${i})"></span>`).join("");
  const primaryBadge = getPrimaryBadge(p);
  const secondaryBadge = getSecondaryBadge(p);
  const headerMeta = getHeaderMeta(p, getBucketKey(p));
  const entry = getEffectiveEntry(p);
  const closeNoteValue = p.mgmt && p.mgmt.note ? p.mgmt.note : "";
  const carriedQty = getTotalQty(p);
  const displayRisk = getDisplayedRisk(p);
  const displayRiskLabel = getDisplayedRiskLabel(p);
  const slOrder = getTrailStopOrderPreview(p);
  const publicIpNote = kitePublicIp ? `Kite/public egress IP: ${kitePublicIp}` : "Public IP not detected yet.";
  const slOrderButton = slOrder
    ? `<button class="be-btn" type="button" onclick="pushStopLossOrder('${p.id}')">Push SL Order (${slOrder.qty} @ Rs ${priceSL(slOrder.limit)})</button>`
    : "";
  const slOrderNote = slOrder
    ? `Kite order preview: qty ${slOrder.qty}, trigger Rs ${priceSL(slOrder.trigger)}, limit Rs ${priceSL(slOrder.limit)}`
    : "Set a trail override to enable the Kite SL push button.";
  const compactNumStyle = 'style="max-width:118px;width:100%"';
  const compactTrimStyle = 'style="max-width:148px;width:100%"';
  const qty = carriedQty != null ? carriedQty : (p._qty || 0);
  const splits = [Math.ceil(qty * 0.33), Math.ceil(qty * 0.25), Math.floor(qty * 0.25), Math.floor(qty * 0.17)];
  const splitHtml = `<div class="qty-splits"><div class="qs-box"><div class="qs-lbl">T1 - lock</div><div class="qs-val" id="sp0-${p.id}">${qty > 0 ? splits[0] : "-"}</div><div class="qs-pct">33%</div></div><div class="qs-box"><div class="qs-lbl">T2 - target</div><div class="qs-val" id="sp1-${p.id}">${qty > 0 ? splits[1] : "-"}</div><div class="qs-pct">25%</div></div><div class="qs-box"><div class="qs-lbl">T3 - run</div><div class="qs-val" id="sp2-${p.id}">${qty > 0 ? splits[2] : "-"}</div><div class="qs-pct">25%</div></div><div class="qs-box"><div class="qs-lbl">T4 - runner</div><div class="qs-val" id="sp3-${p.id}">${qty > 0 ? splits[3] : "-"}</div><div class="qs-pct">17%</div></div></div>`;
  const trimLabels = ["+3% lock", "+7% target", "+15% run", "+25% runner"];
  const trimColors = ["var(--amb)", "var(--blu)", "var(--green)", "var(--green)"];
  let running = carriedQty != null ? carriedQty : 0;
  let trimRows = "";
  p.trims.forEach((t, i) => {
    const sug = t._sug ? "Rs " + t._sug : (i === 1 ? "7% target" : "-");
    const pnlTxt = t._pnl != null ? (t._pnl >= 0 ? "+" : "-") + "Rs " + fi(t._pnl) : "-";
    running -= (t.done && t.sq ? t.sq : 0);
    trimRows += `<tr id="tr-${i}-${p.id}" class="${t.done ? "t-done" : ""}"><td><span class="t-label">T${i + 1}</span><span class="t-pct" style="color:${trimColors[i]}">${trimLabels[i]}</span></td><td><input type="date" class="t-in t-date" ${compactTrimStyle} value="${t.dt || ""}" oninput="updTrim('${p.id}',${i},'dt',this.value)"></td><td><span id="ts${i}-${p.id}" class="${t._sug ? "t-target" : "t-na"}">${sug}</span></td><td><input type="number" class="t-in" placeholder="Rs sell" ${compactTrimStyle} value="${t.ap != null ? t.ap : ""}" oninput="updTrim('${p.id}',${i},'ap',parseFloat(this.value)||null)"></td><td><input type="number" class="t-in" placeholder="qty" ${compactTrimStyle} value="${t.sq != null ? t.sq : ""}" oninput="updTrim('${p.id}',${i},'sq',parseInt(this.value)||null)"><span class="t-rem" id="tr${i}-${p.id}">${carriedQty != null ? running + " rem" : ""}</span></td><td><span id="tp${i}-${p.id}" class="${t._pnl != null ? "t-pl " + (t._pnl >= 0 ? "pos" : "neg") : "t-pl"}">${pnlTxt}</span></td><td style="text-align:center"><input type="checkbox" class="t-chk" ${t.done ? "checked" : ""} onchange="updTrim('${p.id}',${i},'done',this.checked)"></td></tr>`;
  });
  const hasTrim = p.trims.some(t => t.done);
  const net = (p._realPnl || 0) + (p._openPnl || 0);
  const pnlSumHtml = `<div class="pnl-sum" id="pnlsum-${p.id}" style="display:${hasTrim ? "flex" : "none"}"><div class="ps-item"><span>Realized:</span><strong id="psr-${p.id}" style="color:${(p._realPnl || 0) >= 0 ? "var(--green)" : "var(--red)"}">${sgn(p._realPnl || 0)}Rs ${fi(p._realPnl || 0)}</strong></div><div class="ps-item"><span>Net:</span><strong id="psn-${p.id}" style="color:${net >= 0 ? "var(--green)" : "var(--red)"}">${sgn(net)}Rs ${fi(net)}</strong></div></div>`;
  const beBanner = `<div class="be-banner" id="beb-${p.id}" style="display:${p._beSug ? "flex" : "none"}"><span class="be-text">Day ${p._days || 0} since entry - move SL to breakeven (Rs ${entry || "-"})?</span><button class="be-btn" onclick="moveBE('${p.id}')">Apply</button></div>`;
  return `
  <div class="pcard st-${p._status}" id="card-${p.id}">
    <div class="phead" onclick="toggleCollapse('${p.id}')">
      <div class="phead-l">
        <div class="pos-num">${num}</div>
        <div class="phead-core">
          <input type="text" class="sym-in" id="sym-${p.id}" list="symbol-suggestions" placeholder="SYMBOL" value="${p.symbol}" oninput="handleSymbolInput('${p.id}',this)" onblur="commitSymbol('${p.id}',this)" onclick="event.stopPropagation()">
          <div class="hmeta" id="hmeta-${p.id}">${headerMeta}</div>
        </div>
        <span class="badge badge-${primaryBadge}" id="pbadge-${p.id}">${primaryBadge.toUpperCase()}</span>
        <span class="badge badge-${secondaryBadge || "ghost"}" id="sbadge-${p.id}" style="${secondaryBadge ? "" : "display:none"}">${secondaryBadge.toUpperCase()}</span>
        <div class="conv"><span class="conv-label">conviction</span><div class="cdots" id="conv-${p.id}">${convHtml}</div></div>
      </div>
      <div class="phead-r">
        ${displayRisk != null ? `<span class="head-pnl neg" id="hrisk-${p.id}">${displayRiskLabel} ${price2(displayRisk)}</span>` : `<span class="head-pnl" id="hrisk-${p.id}" style="display:none"></span>`}
        <button class="btn-rm" onclick="event.stopPropagation();removePos('${p.id}')" title="Remove">x</button>
        <span class="chev ${p.collapsed ? "" : "open"}" id="chv-${p.id}">v</span>
      </div>
    </div>
    <div class="pbody ${p.collapsed ? "hide" : ""}" id="pb-${p.id}">
      <div class="sec"><div class="sec-title">Merits &amp; thesis</div><textarea class="fin" rows="2" placeholder="Why this stock? RS rank, catalyst, setup pattern..." oninput="upd('${p.id}','merits',this.value)">${p.merits || ""}</textarea></div>
      <div class="sec">
        <div class="sec-title">Plan <span class="sec-note">core qty uses planned SL, tactical qty is set manually</span><button class="btn-sec" type="button" style="padding:3px 8px;font-size:10px" onclick="event.stopPropagation();clearPlanFields('${p.id}')">Clear plan</button></div>
        <div class="g5">
          <div class="field"><div class="flabel">CMP <span class="flabel-tag">bhav close</span></div><input type="number" class="fin num" id="cmp-${p.id}" placeholder="auto" value="${p.cmp != null ? p.cmp : ""}" readonly></div>
          <div class="field"><div class="flabel">Plan entry <span style="color:var(--blu);font-size:9px">Rs</span></div><input type="number" class="fin num en-f" placeholder="0.00" value="${p.planEntry != null ? p.planEntry : ""}" oninput="upd('${p.id}','planEntry',parseFloat(this.value)||null)"></div>
          <div class="field"><div class="flabel">Predetermined SL <span style="color:var(--red);font-size:9px">Rs</span></div><input type="number" class="fin num sl-f" placeholder="0.00" value="${p.planSL != null ? p.planSL : ""}" oninput="upd('${p.id}','planSL',parseFloat(this.value)||null)"></div>
          <div class="field"><div class="flabel">Intraday SL <span style="color:var(--amb);font-size:9px">Rs</span></div><input type="number" class="fin num" placeholder="set on entry" value="${p.intraSL != null ? p.intraSL : ""}" oninput="upd('${p.id}','intraSL',parseFloat(this.value)||null)"></div>
          <div class="field"><div class="flabel">Risk willing <span class="flabel-tag">Rs</span></div><input type="number" class="fin num" placeholder="5000" value="${p.riskAmount != null ? p.riskAmount : ""}" oninput="upd('${p.id}','riskAmount',parseFloat(this.value)||null)"></div>
        </div>
        <div class="g4" style="margin-top:10px;gap:14px;align-items:start;">
          <div class="field"><div class="flabel">Sizing math <span class="flabel-tag">computed</span></div><div class="fin num" id="rps-val-${p.id}" style="color:var(--t2);font-weight:600;cursor:default;">${p._rps ? p._rps : "-"}</div></div>
          <div class="field"><div class="flabel">Planned qty <span class="flabel-tag">computed</span></div><div class="fcomp" id="qty-${p.id}" style="color:${p._qty > 0 ? "var(--green)" : "var(--t3)"};">${p._qty > 0 ? p._qty : "-"}</div></div>
          <div class="field"><div class="flabel">Intraday qty <span class="flabel-tag">manual</span></div><input type="number" class="fin num" placeholder="qty" value="${p.intraQty != null ? p.intraQty : (p._suggestedIntraQty != null ? p._suggestedIntraQty : "")}" oninput="upd('${p.id}','intraQty',parseInt(this.value)||0)"></div>
          <div class="field"><div class="flabel">Planned risk <span class="flabel-tag">computed</span></div><div class="fcomp" id="prisk-${p.id}" style="color:var(--amb)">${p._planRisk != null && p._planRisk > 0 ? price2(p._planRisk) : "-"}</div></div>
        </div>
      </div>
      <div class="div"></div>
        <div class="sec-title">Execution <span class="sec-note">actual filled qty is split 30% intraday SL and 70% original SL</span><button class="btn-sec" type="button" style="padding:3px 8px;font-size:10px" onclick="event.stopPropagation();clearExecutionFields('${p.id}')">Clear exec</button></div>
        <div class="g4" style="grid-template-columns:repeat(5,minmax(0,1fr));gap:14px;align-items:start;">
          <div class="field"><div class="flabel">OV QTY</div><input type="number" class="fin num" ${compactNumStyle} placeholder="qty" value="${p.overnightQty != null ? p.overnightQty : ""}" oninput="upd('${p.id}','overnightQty',parseInt(this.value)||null)"></div>
          <div class="field"><div class="flabel">OV PRICE</div><input type="number" class="fin num en-f" ${compactNumStyle} placeholder="overnight fill" value="${p.overnightEntry != null ? p.overnightEntry : ""}" oninput="upd('${p.id}','overnightEntry',parseFloat(this.value)||null)"></div>
          <div class="field"><div class="flabel">OV SL</div><input type="number" class="fin num sl-f" ${compactNumStyle} placeholder="overnight sl" value="${p.overnightSL != null ? p.overnightSL : ""}" oninput="upd('${p.id}','overnightSL',parseFloat(this.value)||null)"></div>
          <div class="field"><div class="flabel">QTY</div><input type="number" class="fin num" ${compactNumStyle} placeholder="qty" value="${p.actualQty != null ? p.actualQty : ""}" oninput="upd('${p.id}','actualQty',parseInt(this.value)||null)"></div>
          <div class="field"><div class="flabel">PRICE</div><input type="number" class="fin num en-f" ${compactNumStyle} placeholder="same-day fill" value="${p.actualEntry != null ? p.actualEntry : ""}" oninput="upd('${p.id}','actualEntry',parseFloat(this.value)||null)"></div>
        </div>
        <div class="g4" style="margin-top:10px;gap:14px;align-items:start;">
          <div class="field"><div class="flabel">TOTAL QTY <span class="flabel-tag">computed</span></div><div class="fcomp" id="totq-${p.id}" style="font-size:20px;color:var(--t1);">${getTotalQty(p) != null ? getTotalQty(p) : "-"}</div></div>
          <div class="field"><div class="flabel">ENTRY PRICE <span class="flabel-tag">weighted avg</span></div><div class="fcomp" id="epx-${p.id}" style="font-size:20px;color:var(--t1);">${entry != null ? price2(entry) : "-"}</div></div>
          <div class="field"><div class="flabel">ENTRY DATE</div><input type="date" class="fin" ${compactTrimStyle} value="${p.entryDate || ""}" onchange="upd('${p.id}','entryDate',this.value)"></div>
          <div class="field"><div class="flabel">EXEC RISK <span class="flabel-tag">qty x sl gap</span></div><div class="fcomp" id="erisk-${p.id}" style="font-size:20px;color:${getExecutionRisk(p) > 0 ? "var(--red)" : "var(--t1)"};">${getExecutionRisk(p) != null ? price2(getExecutionRisk(p)) : "-"}</div></div>
        </div>
        <div style="margin-top:10px;"><div style="font-size:10px;color:var(--t3);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">Suggested 4-part split</div>${splitHtml}</div>
      </div>
      <div class="div"></div>
      <div class="sec"><div class="sec-title">Trim plan &amp; execution <span class="sec-note">enter trim date, actual sell Rs + qty after each trim</span></div><table class="ttbl"><thead><tr><th>Trim</th><th>Date</th><th>Suggested Rs</th><th>Actual sell Rs</th><th>Qty sold</th><th>P&amp;L</th><th class="c">Done</th></tr></thead><tbody>${trimRows}</tbody></table>${pnlSumHtml}</div>
      <div class="div"></div>
      <div class="trail-box">
        <div class="trail-hdr"><span class="trail-title">Trail stop loss</span><span class="trail-days" id="days-${p.id}">${p._days != null ? "Day " + p._days + " in trade" : ""}</span></div>
        <div class="trail-grid">
          <div><div class="tc-label">Initial SL</div><div class="tc-val sl-c"><strong>Rs ${priceSL(p.planSL)}</strong></div></div>
          <div><div class="tc-label">Current SL</div><div class="tc-val sl-c" id="tsl-${p.id}"><strong>Rs ${priceSL(p._currentSL)}</strong></div></div>
          <div><div class="tc-label">Breakeven</div><div class="tc-val ${p.movedBE ? "ok-c" : ""}" id="bec-${p.id}" style="display:${p.movedBE ? "block" : "none"}">${p.movedBE ? "Rs " + priceSL(entry || p.actualEntry || p.overnightEntry) : ""}</div><div class="tc-val" ${p.movedBE ? 'style="display:none"' : ""}>${(entry || p.actualEntry || p.overnightEntry) ? "Rs " + priceSL(entry || p.actualEntry || p.overnightEntry) : "-"}</div></div>
          <div><div class="tc-label">Override SL</div><div style="display:flex;gap:6px;align-items:center;margin-top:4px;flex-wrap:wrap"><input type="number" class="t-in" id="toi-${p.id}" style="width:92px" placeholder="manual" value="${p.trailOverride != null ? p.trailOverride : ""}" oninput="upd('${p.id}','trailOverride',parseFloat(this.value)||null)"><button class="be-btn" type="button" onclick="updateTrailSL('${p.id}')">Update SL</button>${slOrderButton}</div></div>
        </div>
        <div class="view-note" style="margin-top:8px">${slOrderNote}<br>${publicIpNote}</div>
        ${beBanner}
        <div class="trail-note-wrap"><div class="trail-note-lbl">SL adjustment note</div><input type="text" class="t-note" id="tni-${p.id}" placeholder="e.g. moved SL to EMA20 at Rs 182 - structure holding..." value="${p.trailNote || ""}" oninput="upd('${p.id}','trailNote',this.value)"></div>
      </div>
      <div class="sec"><div class="sec-title">Close note <span class="sec-note">capture the trade at close</span></div><textarea class="dev-note" id="mgmt-note-${p.id}" placeholder="Deviation, lesson, or what to remember at close..." oninput="upd('${p.id}','mgmt.note',this.value)">${escHtml(closeNoteValue)}</textarea></div>
    </div>
  </div>`;
}

document.addEventListener("DOMContentLoaded", async () => {
  planDate = todayStr();
  if (e("plan-date")) e("plan-date").value = planDate;
  ensureSimulationNav();
  ensureStreaksNav();
  ensurePortfolioNav();
  await loadStorageInfo();
  await loadDashboard();
  renderApp();
});

