(function () {
  const THRESHOLD_PCT = 2.0;
  const ROOT_ID = "kite-opening-bar-guard";
  const PANEL_ID = "kite-opening-bar-guard-panel";
  const API_BASE = "http://127.0.0.1:8765/api";
  const completedKeys = new Set();
  let pending = false;
  let autoHideTimer = null;
  let styleInjected = false;

  function isKite() {
    return location.hostname === "kite.zerodha.com" || location.hostname.endsWith(".kite.zerodha.com");
  }

  function todayStr() {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  }

  function parseChartIdentity() {
    const parts = location.pathname.split("/").filter(Boolean);
    const instrumentToken = Number(parts[parts.length - 1]);
    const symbol = parts.length >= 2 ? String(parts[parts.length - 2] || "").toUpperCase() : "";
    if (!Number.isFinite(instrumentToken) || instrumentToken <= 0) return null;
    return { instrumentToken, symbol };
  }

  function chartKey(identity) {
    return `${identity.instrumentToken}:${todayStr()}`;
  }

  function ensureRoot() {
    let el = document.getElementById(ROOT_ID);
    if (el) return el;
    el = document.createElement("div");
    el.id = ROOT_ID;
    el.style.cssText = "position:fixed;inset:0;pointer-events:none;z-index:2147483647;";
    document.documentElement.appendChild(el);
    return el;
  }

  function ensureStyles() {
    if (styleInjected) return;
    styleInjected = true;
    const style = document.createElement("style");
    style.textContent = `
      @keyframes kiteOpeningBarFlash {
        0%   { transform: translateX(-50%) scale(0.98); opacity: 0.55; filter: saturate(0.95); }
        20%  { transform: translateX(-50%) scale(1.01); opacity: 1; filter: saturate(1.2); }
        40%  { transform: translateX(-50%) scale(0.99); opacity: 0.78; }
        60%  { transform: translateX(-50%) scale(1.015); opacity: 1; filter: saturate(1.25); }
        80%  { transform: translateX(-50%) scale(0.995); opacity: 0.86; }
        100% { transform: translateX(-50%) scale(1); opacity: 1; }
      }
    `;
    document.documentElement.appendChild(style);
  }

  function ensurePanel() {
    const root = ensureRoot();
    let el = document.getElementById(PANEL_ID);
    if (el) return el;
    el = document.createElement("div");
    el.id = PANEL_ID;
    el.style.cssText = [
      "position:fixed",
      "top:96px",
      "left:50%",
      "transform:translateX(-50%)",
      "min-width:340px",
      "max-width:540px",
      "padding:14px 18px",
      "border-radius:18px",
      "background:linear-gradient(180deg, rgba(255,255,255,0.18), rgba(255,255,255,0.08))",
      "backdrop-filter:blur(16px) saturate(145%)",
      "-webkit-backdrop-filter:blur(16px) saturate(145%)",
      "border:1px solid rgba(123,179,255,0.42)",
      "box-shadow:0 14px 36px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.24)",
      "color:#f5f7ff",
      "font-family:Inter,Segoe UI,Arial,sans-serif",
      "pointer-events:auto",
      "display:none"
    ].join(";");
    el.innerHTML = `
      <div style="display:flex;align-items:flex-start;gap:12px;">
        <div style="width:12px;height:12px;border-radius:999px;background:radial-gradient(circle at 30% 30%, #fff 0%, #f59e0b 26%, #ef4444 100%);box-shadow:0 0 16px rgba(245,158,11,.75);margin-top:4px;flex:0 0 auto;"></div>
        <div style="flex:1 1 auto;min-width:0;">
          <div id="kite-opening-bar-head" style="font-size:14px;font-weight:800;letter-spacing:.02em;margin-bottom:6px;">Opening Bar Guard</div>
          <div id="kite-opening-bar-text" style="font-size:13px;line-height:1.45;color:#e5eefc;">Waiting for chart...</div>
        </div>
        <button id="kite-opening-bar-close" style="border:0;background:rgba(255,255,255,0.06);color:#eef2ff;border-radius:999px;width:28px;height:28px;cursor:pointer;pointer-events:auto;box-shadow:inset 0 1px 0 rgba(255,255,255,0.14);">×</button>
      </div>
    `;
    root.appendChild(el);
    el.querySelector("#kite-opening-bar-close").addEventListener("click", () => {
      el.style.display = "none";
    });
    return el;
  }

  function showPanel(text, tone) {
    ensureStyles();
    const panel = ensurePanel();
    const node = panel.querySelector("#kite-opening-bar-text");
    const head = panel.querySelector("#kite-opening-bar-head");
    panel.style.display = "block";
    panel.style.animation = "kiteOpeningBarFlash 0.75s ease-in-out 3";
    node.textContent = text;
    if (tone === "bad") {
      panel.style.borderColor = "rgba(248,113,113,0.5)";
      panel.style.boxShadow = "0 14px 36px rgba(127,29,29,0.35), inset 0 1px 0 rgba(255,255,255,0.24)";
      head.textContent = "Avoid Trade";
    } else {
      panel.style.borderColor = "rgba(123,179,255,0.42)";
      panel.style.boxShadow = "0 14px 36px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.24)";
      head.textContent = "Opening Bar Guard";
    }
    window.clearTimeout(autoHideTimer);
    autoHideTimer = window.setTimeout(() => {
      hidePanel();
      panel.style.animation = "none";
    }, 10000);
  }

  function hidePanel() {
    const panel = ensurePanel();
    panel.style.display = "none";
    panel.style.animation = "none";
  }

  async function fetchJson(path, body) {
    const response = await fetch(`${API_BASE}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify(body)
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.message || payload.reason || `HTTP ${response.status}`);
    return payload;
  }

  async function fetchScanTargets() {
    const payload = await fetchJson("/kite/scan-targets", { date: todayStr() });
    return Array.isArray(payload.targets) ? payload.targets : [];
  }

  async function fetchOpeningBar(identity) {
    return fetchJson("/kite/opening-bar", {
      date: todayStr(),
      instrument_token: identity.instrumentToken,
      symbol: identity.symbol,
      threshold_pct: THRESHOLD_PCT
    });
  }

  async function runOnceForCurrentChart() {
    if (!isKite()) return;
    const identity = parseChartIdentity();
    if (!identity) {
      hidePanel();
      return;
    }

    const key = chartKey(identity);
    if (completedKeys.has(key) || pending) return;
    pending = true;

    try {
      const targets = await fetchScanTargets();
      const allowed = targets.some(item => String(item.symbol || "").toUpperCase() === identity.symbol);
      if (!allowed) {
        completedKeys.add(key);
        hidePanel();
        return;
      }

      const payload = await fetchOpeningBar(identity);
      const open = payload.opening;
      const range = Number(open.rangePct || 0);
      const threshold = Number(payload.threshold_pct || THRESHOLD_PCT);

      if (range > threshold) {
        showPanel(
          `${identity.symbol} avoid trade. 5 min opening bar is out of comfort zone (${range.toFixed(2)}%).`,
          "bad"
        );
        completedKeys.add(key);
      } else {
        hidePanel();
        completedKeys.add(key);
      }
    } catch (_err) {
      hidePanel();
    } finally {
      pending = false;
    }
  }

  if (!isKite()) return;
  ensurePanel();
  runOnceForCurrentChart();

  const observer = new MutationObserver(() => {
    const identity = parseChartIdentity();
    if (!identity) return;
    const key = chartKey(identity);
    if (!completedKeys.has(key)) runOnceForCurrentChart();
  });
  observer.observe(document.documentElement, { subtree: true, childList: true });

  window.addEventListener("popstate", runOnceForCurrentChart);
  window.addEventListener("focus", runOnceForCurrentChart);
})();
