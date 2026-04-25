(function () {
  const ROOT_ID = "kite-trade-helper-overlay";
  const API_BASE = "http://127.0.0.1:8765/api";

  function ensureOverlay() {
    let root = document.getElementById(ROOT_ID);
    if (root) return root;

    root = document.createElement("div");
    root.id = ROOT_ID;
    root.style.cssText = [
      "position:fixed",
      "top:12px",
      "right:12px",
      "z-index:2147483647",
      "max-width:360px",
      "font-family:Inter,Segoe UI,Arial,sans-serif",
      "background:rgba(15,18,26,0.96)",
      "color:#eef2ff",
      "border:1px solid rgba(90,120,255,0.35)",
      "border-radius:14px",
      "box-shadow:0 14px 32px rgba(0,0,0,0.35)",
      "padding:12px 14px",
      "line-height:1.35",
      "backdrop-filter:blur(10px)"
    ].join(";");

    root.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;">
        <div style="font-weight:700;font-size:13px;letter-spacing:.02em;color:#7dd3fc;">Kite Helper</div>
        <button id="kite-trade-helper-close" style="border:0;background:transparent;color:#9aa4bf;font-size:18px;cursor:pointer;padding:0;line-height:1;">&times;</button>
      </div>
      <div id="kite-trade-helper-body" style="margin-top:8px;font-size:12px;color:#d6dcf5;">
        Loading helper status...
      </div>
    `;

    document.documentElement.appendChild(root);
    root.querySelector("#kite-trade-helper-close").addEventListener("click", () => root.remove());
    return root;
  }

  async function loadStatus() {
    const root = ensureOverlay();
    const body = root.querySelector("#kite-trade-helper-body");
    try {
      const resp = await fetch(`${API_BASE}/storage-info`, { cache: "no-store" });
      const data = await resp.json();
      const publicIp = data.public_ip || "not detected";
      body.innerHTML = `
        <div style="margin-bottom:6px;">Local trade helper is active.</div>
        <div style="color:#93c5fd;">Public IP: <strong>${escapeHtml(publicIp)}</strong></div>
        <div style="margin-top:6px;color:#cbd5e1;">If Kite blocks SL placement, confirm this IP is whitelisted in the Kite app.</div>
      `;
    } catch (err) {
      body.innerHTML = `
        <div style="margin-bottom:6px;color:#fca5a5;">Local helper server unavailable.</div>
        <div style="color:#cbd5e1;">Start <strong>trade_plan_server.py</strong> to show status here.</div>
      `;
    }
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  ensureOverlay();
  loadStatus();
})();
