
// ── state ──────────────────────────────────────────────────────────────────
const PAGE_MODE = "reports";
const ADMIN_JOBS = [{"description": "Runs the date-based SQL batch and can optionally switch to GM-only mode.", "fields": [{"label": "Run date", "name": "date", "required": true, "type": "date"}, {"default": false, "label": "GM mode", "name": "gm", "type": "checkbox"}], "key": "bhav_sql_batch", "label": "Bhav SQL batch", "needs": ["date"]}, {"description": "Refreshes NIFTY Smallcap 100 / 250 rows in indexbhav.", "fields": [{"default": "date", "label": "Update mode", "name": "mode", "options": [{"label": "Range", "value": "date"}, {"label": "Single date", "value": "single"}], "type": "select"}, {"label": "Single date", "name": "date", "type": "date"}, {"label": "From date", "name": "from_date", "type": "date"}, {"label": "To date", "name": "to_date", "type": "date"}, {"default": false, "label": "Dry run", "name": "dry_run", "type": "checkbox"}], "key": "index_smallcaps", "label": "Index BHAV smallcaps", "needs": ["date_mode"]}, {"description": "Exports bhav.sectors to bse_master.csv for downstream rating jobs.", "fields": [], "key": "sector_csv", "label": "Sector CSV export", "needs": []}, {"description": "Reloads bhav.nse_symbols from the NSE equity listing CSV.", "fields": [], "key": "nse_symbols", "label": "NSE symbols load", "needs": []}, {"description": "Loads local IPO performance CSVs into bhav.ipobhav.", "fields": [{"label": "CSV file", "name": "file", "placeholder": "Optional path to a CSV file", "type": "text"}], "key": "ipo_csv", "label": "IPO CSV load", "needs": []}, {"description": "Scrapes and loads listed mainline IPO rows into bhav.ipobhav.", "fields": [{"label": "Start year", "name": "start_year", "placeholder": "e.g. 2022", "type": "number"}, {"label": "End year", "name": "end_year", "placeholder": "e.g. 2026", "type": "number"}], "key": "chittorgarh_ipo", "label": "Chittorgarh IPO load", "needs": ["year_range"]}, {"description": "Loads listed mainline IPO reference data into bhav.ipobhav.", "fields": [{"label": "Table", "name": "table", "placeholder": "Optional MySQL table name", "type": "text"}, {"label": "Limit", "name": "limit", "placeholder": "Optional row limit", "type": "number"}, {"label": "Minimum year", "name": "min_year", "placeholder": "Optional filter year", "type": "number"}], "key": "moneycontrol_ipo", "label": "Moneycontrol IPO load", "needs": []}];
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
function setReportsStatus(message, kind = '') {
  if (!reportsStatus) return;
  reportsStatus.textContent = message || '';
  reportsStatus.className = kind ? `reports-status ${kind}` : 'reports-status';
}

function setReportsConsole(text) {
  if (!reportsConsole) return;
  reportsConsole.value = String(text || '');
}

function appendReportsConsole(text) {
  if (!reportsConsole) return;
  const existing = reportsConsole.value || '';
  const next = existing ? `${existing}\n${text}` : String(text || '');
  reportsConsole.value = next.slice(-20000);
  reportsConsole.scrollTop = reportsConsole.scrollHeight;
}

function isoDateOffset(baseIso, daysDelta) {
  const base = baseIso ? new Date(`${baseIso}T00:00:00`) : new Date();
  base.setDate(base.getDate() + daysDelta);
  return base.toISOString().slice(0, 10);
}

async function initReportsDefaults() {
  if (!reportsCutoffDate || !reportsResetDate) return;
  try {
    const res = await fetch('/api/latest-date');
    const data = await res.json();
    const latest = data.latest_date || new Date().toISOString().slice(0, 10);
    reportsCutoffDate.value = latest;
    if (!reportsResetDate.value) {
      reportsResetDate.value = isoDateOffset(latest, -30);
    }
    setReportsStatus(`Ready. Latest trading date: ${latest}`, '');
  } catch (err) {
    const today = new Date().toISOString().slice(0, 10);
    if (!reportsCutoffDate.value) reportsCutoffDate.value = today;
    if (!reportsResetDate.value) reportsResetDate.value = isoDateOffset(today, -30);
    setReportsStatus(`Using local date defaults because the latest-date lookup failed: ${err.message}`, 'err');
  }
}

function applyReportsDownloadLinks(data) {
  if (reportsDownloadXlsx) {
    reportsDownloadXlsx.href = data.xlsx_url || '#';
    reportsDownloadXlsx.style.display = data.xlsx_url ? 'inline-flex' : 'none';
  }
  if (reportsOpenHtml) {
    reportsOpenHtml.href = data.report_url || '#';
    reportsOpenHtml.style.display = data.report_url ? 'inline-flex' : 'none';
  }
}

function showReportsPreview(data) {
  if (reportsFrame && data.report_url) {
    reportsFrame.src = data.report_url;
  }
  if (reportsActiveFile) {
    reportsActiveFile.textContent = data.filename || data.html_path || 'Report loaded';
  }
  applyReportsDownloadLinks(data);
}

async function loadLatestReport() {
  if (!reportsStatus) return;
  setReportsStatus('Looking for the latest generated HTML report...', '');
  try {
    const res = await fetch('/api/reports/liquid-momentum/latest');
    const data = await res.json();
    if (!data.found) {
      setReportsStatus('No generated liquid momentum HTML report found yet.', 'err');
      if (reportsActiveFile) reportsActiveFile.textContent = 'No report loaded yet.';
      return;
    }
    const xlsxUrl = `/download-generated/${encodeURIComponent(data.filename.replace(/\.html$/i, '.xlsx'))}`;
    showReportsPreview({ report_url: data.url, filename: data.filename, xlsx_url: xlsxUrl, html_path: data.filename });
    setReportsStatus(`Loaded ${data.filename}`, 'ok');
  } catch (err) {
    setReportsStatus(`Failed to load the latest report: ${err.message}`, 'err');
  }
}

async function generateLiquidMomentumReport() {
  if (!reportsGenerateBtn) return;
  const payload = {
    as_of_date: reportsCutoffDate ? reportsCutoffDate.value.trim() : '',
    reset_date: reportsResetDate ? reportsResetDate.value.trim() : '',
    source: reportsSource ? reportsSource.value.trim() : 'kite',
    debug_symbol: reportsDebugSymbol ? reportsDebugSymbol.value.trim() : '',
    full_mode: reportsFullMode ? reportsFullMode.checked : true,
    extended: reportsExtended ? reportsExtended.checked : false,
    run_fundamentals: reportsRunFundamentals ? reportsRunFundamentals.checked : false,
  };
  if (!payload.as_of_date || !payload.reset_date) {
    setReportsStatus('As-of date and reset date are required.', 'err');
    return;
  }
  reportsGenerateBtn.disabled = true;
  if (reportsLatestBtn) reportsLatestBtn.disabled = true;
  setReportsConsole('');
  setReportsStatus('Generating neo_liquid_momentum_scanner.py report...', '');
  appendReportsConsole('POST /api/reports/liquid-momentum/run');
  try {
    const res = await fetch('/api/reports/liquid-momentum/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (data.stdout) appendReportsConsole(data.stdout.trimEnd());
    if (data.stderr) appendReportsConsole(`STDERR:\n${data.stderr.trimEnd()}`);
    if (data.error) {
      setReportsStatus(`Error: ${data.error}`, 'err');
      return;
    }
    if (!data.ok) {
      setReportsStatus(`Report generation finished with exit code ${data.returncode}.`, 'err');
    } else {
      setReportsStatus(`Generated ${data.html_path}`, 'ok');
    }
    showReportsPreview({
      report_url: data.report_url,
      filename: data.html_path ? data.html_path.split(/[\\/]/).pop() : '',
      xlsx_url: data.xlsx_url,
      html_path: data.html_path,
    });
  } catch (err) {
    setReportsStatus(`Failed to generate the report: ${err.message}`, 'err');
  } finally {
    reportsGenerateBtn.disabled = false;
    if (reportsLatestBtn) reportsLatestBtn.disabled = false;
  }
}

function adminFieldValue(field, jobKey, currentValue = '') {
  const baseId = `admin-${jobKey}-${field.name}`;
  const label = escapeHtml(field.label || field.name);
  const required = field.required ? ' required' : '';
  const placeholder = field.placeholder ? ` placeholder="${escapeHtml(field.placeholder)}"` : '';
  const value = currentValue || field.default || '';

  if (field.type === 'checkbox') {
    const checked = value === true || value === 'true' || value === '1' ? ' checked' : '';
    return `
      <div class="admin-field">
        <div class="admin-checkbox">
          <input id="${baseId}" data-param="${escapeHtml(field.name)}" type="checkbox"${checked}>
          <label for="${baseId}" style="margin:0;text-transform:none;letter-spacing:0;font-size:13px;color:var(--text)">${label}</label>
        </div>
      </div>
    `;
  }

  if (field.type === 'select') {
    const options = (field.options || []).map(option => {
      const selected = String(option.value) === String(value) ? ' selected' : '';
      return `<option value="${escapeHtml(option.value)}"${selected}>${escapeHtml(option.label)}</option>`;
    }).join('');
    return `
      <div class="admin-field">
        <label for="${baseId}">${label}</label>
        <select id="${baseId}" data-param="${escapeHtml(field.name)}"${required}>
          ${options}
        </select>
      </div>
    `;
  }

  if (field.type === 'textarea') {
    return `
      <div class="admin-field full-width">
        <label for="${baseId}">${label}</label>
        <textarea id="${baseId}" data-param="${escapeHtml(field.name)}"${placeholder}${required}>${escapeHtml(value)}</textarea>
      </div>
    `;
  }

  return `
    <div class="admin-field">
      <label for="${baseId}">${label}</label>
      <input id="${baseId}" data-param="${escapeHtml(field.name)}" type="${escapeHtml(field.type || 'text')}" value="${escapeHtml(value)}"${placeholder}${required}>
    </div>
  `;
}

function renderAdminJobs() {
  if (!adminJobsContainer) return;
  if (!Array.isArray(ADMIN_JOBS)) {
    adminJobsContainer.innerHTML = '<div class="admin-job">No admin jobs configured.</div>';
    if (adminStatus) adminStatus.textContent = 'No jobs available.';
    return;
  }

  if (adminJobCount) adminJobCount.textContent = String(ADMIN_JOBS.length);
  adminJobsContainer.innerHTML = ADMIN_JOBS.map(job => {
    const fieldsHtml = (job.fields || []).map(field => adminFieldValue(field, job.key)).join('');
    return `
      <article class="admin-job" data-job-key="${escapeHtml(job.key)}">
        <div class="admin-job-head">
          <div>
            <div class="admin-job-title">${escapeHtml(job.label)}</div>
            <div class="admin-job-meta">${escapeHtml(job.description || '')}</div>
          </div>
          <div class="admin-job-meta">${escapeHtml(job.key)}</div>
        </div>
        <div class="admin-job-form">
          ${fieldsHtml || '<div class="admin-job-meta full-width">No parameters required.</div>'}
        </div>
        <div class="admin-job-actions">
          <button type="button" class="admin-run-btn">Run Job</button>
          <button type="button" class="secondary admin-clear-btn">Clear Log</button>
        </div>
        <div class="admin-job-status">Ready.</div>
        <textarea class="admin-log" readonly placeholder="Job output will appear here."></textarea>
      </article>
    `;
  }).join('');

  adminJobsContainer.querySelectorAll('.admin-job').forEach(card => {
    const runBtn = card.querySelector('.admin-run-btn');
    const clearBtn = card.querySelector('.admin-clear-btn');
    const statusEl = card.querySelector('.admin-job-status');
    const logEl = card.querySelector('.admin-log');
    const jobKey = card.getAttribute('data-job-key');

    if (clearBtn) {
      clearBtn.addEventListener('click', () => {
        if (logEl) logEl.value = '';
        if (statusEl) {
          statusEl.textContent = 'Cleared.';
          statusEl.className = 'admin-job-status';
        }
      });
    }

    if (runBtn) {
      runBtn.addEventListener('click', async () => {
        const payload = {};
        card.querySelectorAll('[data-param]').forEach(fieldEl => {
          const name = fieldEl.getAttribute('data-param');
          if (!name) return;
          if (fieldEl.type === 'checkbox') {
            payload[name] = fieldEl.checked;
          } else {
            payload[name] = String(fieldEl.value || '').trim();
          }
        });

        if (statusEl) {
          statusEl.textContent = `Running ${jobKey}...`;
          statusEl.className = 'admin-job-status';
        }
        if (logEl) logEl.value = '';
        runBtn.disabled = true;
        if (clearBtn) clearBtn.disabled = true;

        try {
          const res = await fetch(`/api/admin/jobs/${encodeURIComponent(jobKey)}/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await res.json();
          const parts = [];
          if (data.command) parts.push(`Command: ${Array.isArray(data.command) ? data.command.join(' ') : data.command}`);
          if (data.stdout) parts.push(`STDOUT:\n${data.stdout.trimEnd()}`);
          if (data.stderr) parts.push(`STDERR:\n${data.stderr.trimEnd()}`);
          if (logEl) logEl.value = parts.join('\n\n').trim();

          if (data.error) {
            if (statusEl) {
              statusEl.textContent = `Error: ${data.error}`;
              statusEl.className = 'admin-job-status err';
            }
            return;
          }

          if (statusEl) {
            const outcome = data.ok ? 'completed successfully' : `finished with exit code ${data.returncode}`;
            statusEl.textContent = `${jobKey} ${outcome}.`;
            statusEl.className = data.ok ? 'admin-job-status ok' : 'admin-job-status err';
          }
        } catch (err) {
          if (statusEl) {
            statusEl.textContent = `Failed to run job: ${err.message}`;
            statusEl.className = 'admin-job-status err';
          }
        } finally {
          runBtn.disabled = false;
          if (clearBtn) clearBtn.disabled = false;
        }
      });
    }
  });

  if (adminStatus) {
    adminStatus.textContent = `${ADMIN_JOBS.length} backend jobs ready.`;
    adminStatus.className = 'admin-status';
  }
}

if (adminRefreshBtn) {
  adminRefreshBtn.addEventListener('click', renderAdminJobs);
}

const sectorSelect = document.getElementById('sectorSelect');
const sectorSpinner = document.getElementById('sectorSpinner');
const sectorPage = document.getElementById('sectorPage');
const sectorPageTabs = document.getElementById('sectorPageTabs');
const sectorChartsPane = document.getElementById('sectorChartsPane');
const sectorEditorPane = document.getElementById('sectorEditorPane');
const sectorShortcuts = document.getElementById('sectorShortcuts');
const sectorBoardEmpty = document.getElementById('sectorBoardEmpty');
const sectorBoardGrid = document.getElementById('sectorBoardGrid');
const sectorBoardNote = document.getElementById('sectorBoardNote');
const sectorEditorQueryEl = document.getElementById('sectorEditorQuery');
const sectorEditorSearchBtn = document.getElementById('sectorEditorSearchBtn');
const sectorEditorResetBtn = document.getElementById('sectorEditorResetBtn');
const sectorEditorSaveBtn = document.getElementById('sectorEditorSaveBtn');
const sectorEditorClearBtn = document.getElementById('sectorEditorClearBtn');
const sectorEditorStatus = document.getElementById('sectorEditorStatus');
const sectorEditorCount = document.getElementById('sectorEditorCount');
const sectorEditorTbody = document.getElementById('sectorEditorTbody');
const sectorEditorEmpty = document.getElementById('sectorEditorEmpty');
const sectorEditorSymbol = document.getElementById('sectorEditorSymbol');
const sectorEditorSector1 = document.getElementById('sectorEditorSector1');
const sectorEditorSector2 = document.getElementById('sectorEditorSector2');
const sectorEditorSector3 = document.getElementById('sectorEditorSector3');
const sectorZoomOverlay = document.getElementById('sectorZoomOverlay');
const sectorZoomTitle = document.getElementById('sectorZoomTitle');
const sectorZoomSubtitle = document.getElementById('sectorZoomSubtitle');
const sectorZoomMeta = document.getElementById('sectorZoomMeta');
const sectorZoomCounter = document.getElementById('sectorZoomCounter');
const sectorZoomChartEl = document.getElementById('sectorZoomChart');
const screenerPreviewOverlay = document.getElementById('screenerPreviewOverlay');
const screenerPreviewTitle = document.getElementById('screenerPreviewTitle');
const screenerPreviewSubtitle = document.getElementById('screenerPreviewSubtitle');
const screenerPreviewMeta = document.getElementById('screenerPreviewMeta');
const screenerPreviewChartEl = document.getElementById('screenerPreviewChart');
const screenerPreviewCard = document.getElementById('screenerPreviewCard');
const screenerPage = document.getElementById('screenerPage');
const gmlistPage = document.getElementById('gmlistPage');
const gmlistAsOfEl = document.getElementById('gmlistAsOf');
const gmlistStrongStartAsOfEl = document.getElementById('gmlistStrongStartAsOf');
const gmlistRunBtn = document.getElementById('gmlistRunBtn');
const gmlistStrongStartRunBtn = document.getElementById('gmlistStrongStartRunBtn');
const gmlistTodayBtn = document.getElementById('gmlistTodayBtn');
const gmlistStatus = document.getElementById('gmlistStatus');
const gmlistMetaStrip = document.getElementById('gmlistMetaStrip');
const gmlistTabs = document.getElementById('gmlistTabs');
const gmlistCountTabLv21 = document.getElementById('gmlistCountTabLv21');
const gmlistCountTabLowvol21 = document.getElementById('gmlistCountTabLowvol21');
const gmlistCountTabInsideDays = document.getElementById('gmlistCountTabInsideDays');
const gmlistCountTabHd = document.getElementById('gmlistCountTabHd');
const gmlistCountTabLive = document.getElementById('gmlistCountTabLive');
const gmlistCountTabStrongStart = document.getElementById('gmlistCountTabStrongStart');
const gmlistCountPanelLv21 = document.getElementById('gmlistCountPanelLv21');
const gmlistCountPanelLowvol21 = document.getElementById('gmlistCountPanelLowvol21');
const gmlistCountPanelInsideDays = document.getElementById('gmlistCountPanelInsideDays');
const gmlistCountPanelHd = document.getElementById('gmlistCountPanelHd');
const gmlistCountPanelLive = document.getElementById('gmlistCountPanelLive');
const gmlistCountPanelLiveLv21 = document.getElementById('gmlistCountPanelLiveLv21');
const gmlistCountPanelLiveLowvol21 = document.getElementById('gmlistCountPanelLiveLowvol21');
const gmlistCountPanelLiveInsideDays = document.getElementById('gmlistCountPanelLiveInsideDays');
const gmlistCountPanelStrongStart = document.getElementById('gmlistCountPanelStrongStart');
const gmlistTbodyLv21 = document.getElementById('gmlistTbodyLv21');
const gmlistTbodyLowvol21 = document.getElementById('gmlistTbodyLowvol21');
const gmlistTbodyInsideDays = document.getElementById('gmlistTbodyInsideDays');
const gmlistTbodyHd = document.getElementById('gmlistTbodyHd');
const gmlistLiveAsOfEl = document.getElementById('gmlistLiveAsOf');
const gmlistLiveRunBtn = document.getElementById('gmlistLiveRunBtn');
const gmlistLiveTodayBtn = document.getElementById('gmlistLiveTodayBtn');
const gmlistLiveStatus = document.getElementById('gmlistLiveStatus');
const gmlistLiveMetaStrip = document.getElementById('gmlistLiveMetaStrip');
const gmlistLiveSubtabs = document.getElementById('gmlistLiveSubtabs');
const gmlistLiveTbodyLv21 = document.getElementById('gmlistLiveTbodyLv21');
const gmlistLiveTbodyLowvol21 = document.getElementById('gmlistLiveTbodyLowvol21');
const gmlistLiveTbodyInsideDays = document.getElementById('gmlistLiveTbodyInsideDays');
const gmlistLiveEmptyLv21 = document.getElementById('gmlistLiveEmptyLv21');
const gmlistLiveEmptyLowvol21 = document.getElementById('gmlistLiveEmptyLowvol21');
const gmlistLiveEmptyInsideDays = document.getElementById('gmlistLiveEmptyInsideDays');
const gmlistEmptyHd = document.getElementById('gmlistEmptyHd');
const gmlistTbodyStrongStart = document.getElementById('gmlistTbodyStrongStart');
const gmlistEmptyLv21 = document.getElementById('gmlistEmptyLv21');
const gmlistEmptyLowvol21 = document.getElementById('gmlistEmptyLowvol21');
const gmlistEmptyInsideDays = document.getElementById('gmlistEmptyInsideDays');
const gmlistEmptyStrongStart = document.getElementById('gmlistEmptyStrongStart');
const gmlistStrongStartNote = document.getElementById('gmlistStrongStartNote');
const gmlistStrongStartOverlay = document.getElementById('gmlistStrongStartOverlay');
const gmlistStrongStartCard = document.getElementById('gmlistStrongStartCard');
const gmlistStrongStartTitle = document.getElementById('gmlistStrongStartTitle');
const gmlistStrongStartSubtitle = document.getElementById('gmlistStrongStartSubtitle');
const gmlistStrongStartMeta = document.getElementById('gmlistStrongStartMeta');
const gmlistStrongStartChartEl = document.getElementById('gmlistStrongStartChart');
const screenerAsOfEl = document.getElementById('screenerAsOf');
const screenerTurnoverEl = document.getElementById('screenerTurnover');
const screenerLimitEl = document.getElementById('screenerLimit');
const screenerRunBtn = document.getElementById('screenerRunBtn');
const screenerTodayBtn = document.getElementById('screenerTodayBtn');
const screenerStatus = document.getElementById('screenerStatus');
const screenerMetaStrip = document.getElementById('screenerMetaStrip');
const screenerPanelVol = document.getElementById('screenerPanelVol');
const screenerPanelVlt = document.getElementById('screenerPanelVlt');
const screenerTbodyVol = document.getElementById('screenerTbodyVol');
const screenerTbodyVlt = document.getElementById('screenerTbodyVlt');
const screenerCountVol = document.getElementById('screenerCountVol');
const screenerCountVlt = document.getElementById('screenerCountVlt');
const screenerEmptyVol = document.getElementById('screenerEmptyVol');
const screenerEmptyVlt = document.getElementById('screenerEmptyVlt');
let sectorChartInstances = [];
let sectorChartObservers = [];
let sectorBoardObserver = null;
let sectorBoardPayload = [];
let sectorActivePane = 'charts';
let sectorEditorRows = [];
let sectorEditorLoadedQuery = '';
let sectorZoomChart = null;
let sectorZoomObserver = null;
let sectorZoomIndex = -1;
let screenerPreviewChart = null;
let screenerPreviewObserver = null;
let screenerPreviewSymbol = '';
let screenerPreviewTimer = null;
let screenerPreviewCloseTimer = null;
let screenerPreviewLoading = null;
const screenerPreviewCache = new Map();
let gmlistLivePreviewChart = null;
let gmlistLivePreviewObserver = null;
let gmlistLivePreviewSymbol = '';
let gmlistLivePreviewTimer = null;
let gmlistLivePreviewCloseTimer = null;
let gmlistLivePreviewLoading = null;
const gmlistLivePreviewCache = new Map();
let gmlistData = null;
let gmlistActiveTab = 'lv21';
let gmlistLiveActiveTab = 'lv21';
let gmlistLiveData = [];
let gmlistLiveLoadedDate = '';
let gmlistStrongStartData = [];
let gmlistStrongStartLoadedDate = '';
let gmlistStrongStartChart = null;
let gmlistStrongStartChartObserver = null;
let gmlistStrongStartHoverTimer = null;
let gmlistStrongStartHoverSymbol = '';
let gmlistStrongStartHoverDate = '';
let gmlistStrongStartHoverOpen = false;
let gmlistStrongStartCloseTimer = null;
const reportsPage = document.getElementById('reportsPage');
const reportsGenerateBtn = document.getElementById('reportsGenerateBtn');
const reportsLatestBtn = document.getElementById('reportsLatestBtn');
const reportsDownloadXlsx = document.getElementById('reportsDownloadXlsx');
const reportsOpenHtml = document.getElementById('reportsOpenHtml');
const reportsCutoffDate = document.getElementById('reportCutoffDate');
const reportsResetDate = document.getElementById('reportResetDate');
const reportsSource = document.getElementById('reportSource');
const reportsDebugSymbol = document.getElementById('reportDebugSymbol');
const reportsFullMode = document.getElementById('reportFullMode');
const reportsExtended = document.getElementById('reportExtended');
const reportsRunFundamentals = document.getElementById('reportRunFundamentals');
const reportsConsole = document.getElementById('reportConsole');
const reportsStatus = document.getElementById('reportsStatus');
const reportsFrame = document.getElementById('reportsFrame');
const reportsActiveFile = document.getElementById('reportsActiveFile');
let reportsLatestLoadedUrl = '';
const adminPage = document.getElementById('adminPage');
const adminJobsContainer = document.getElementById('adminJobsContainer');
const adminStatus = document.getElementById('adminStatus');
const adminRefreshBtn = document.getElementById('adminRefreshBtn');
const adminJobCount = document.getElementById('adminJobCount');

sectorSelect.addEventListener('change', () => {
  if (sectorSelect.value) loadSectorCharts();
  else clearSectorBoard();
});

if (screenerRunBtn) screenerRunBtn.addEventListener('click', loadScreener);
if (screenerTodayBtn) {
  screenerTodayBtn.addEventListener('click', async () => {
    await initScreenerDefaults();
    loadScreener();
  });
}
if (screenerAsOfEl) {
  screenerAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadScreener(); });
}
if (reportsGenerateBtn) {
  reportsGenerateBtn.addEventListener('click', generateLiquidMomentumReport);
}
if (reportsLatestBtn) {
  reportsLatestBtn.addEventListener('click', loadLatestReport);
}
if (reportsCutoffDate && reportsResetDate) {
  reportsCutoffDate.addEventListener('change', () => {
    if (!reportsCutoffDate.value) return;
    reportsResetDate.value = isoDateOffset(reportsCutoffDate.value, -30);
  });
}
if (reportsFrame) {
  reportsFrame.addEventListener('load', () => {
    if (reportsStatus && reportsFrame.src) {
      reportsStatus.textContent = `Preview loaded: ${reportsFrame.src}`;
    }
  });
}

if (gmlistRunBtn) gmlistRunBtn.addEventListener('click', loadGmList);
if (gmlistTodayBtn) {
  gmlistTodayBtn.addEventListener('click', async () => {
    await initGmListDefaults();
    loadGmList();
  });
}
if (gmlistAsOfEl) {
  gmlistAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadGmList(); });
}
if (gmlistStrongStartRunBtn) {
  gmlistStrongStartRunBtn.addEventListener('click', () => loadGmListStrongStart(true));
}
if (gmlistLiveRunBtn) {
  gmlistLiveRunBtn.addEventListener('click', () => loadGmListLive(true));
}
if (gmlistStrongStartAsOfEl) {
  gmlistStrongStartAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadGmListStrongStart(true); });
}
if (gmlistLiveTodayBtn) {
  gmlistLiveTodayBtn.addEventListener('click', async () => {
    if (gmlistLiveAsOfEl) gmlistLiveAsOfEl.value = localIsoDate();
    await loadGmListLive(true);
  });
}
if (gmlistLiveAsOfEl) {
  gmlistLiveAsOfEl.addEventListener('keydown', e => { if (e.key === 'Enter') loadGmListLive(true); });
}
if (gmlistTabs) {
  gmlistTabs.addEventListener('click', (event) => {
    const btn = event.target.closest('.gmlist-tab');
    if (!btn) return;
    switchGmListTab(btn.dataset.tab);
  });
}
if (gmlistLiveSubtabs) {
  gmlistLiveSubtabs.addEventListener('click', (event) => {
    const btn = event.target.closest('.gmlist-subtab');
    if (!btn) return;
    switchGmListLiveTab(btn.dataset.liveTab);
  });
}

if (sectorPageTabs) {
  sectorPageTabs.addEventListener('click', (event) => {
    const btn = event.target.closest('.sector-page-tab');
    if (!btn) return;
    switchSectorPane(btn.dataset.sectorPane);
  });
}
if (sectorEditorSearchBtn) {
  sectorEditorSearchBtn.addEventListener('click', () => loadSectorEditorRows());
}
if (sectorEditorResetBtn) {
  sectorEditorResetBtn.addEventListener('click', async () => {
    if (sectorEditorQueryEl) sectorEditorQueryEl.value = '';
    await loadSectorEditorRows('');
  });
}
if (sectorEditorSaveBtn) {
  sectorEditorSaveBtn.addEventListener('click', () => saveSectorEditorForm());
}
if (sectorEditorClearBtn) {
  sectorEditorClearBtn.addEventListener('click', () => {
    clearSectorEditorForm();
    setSectorEditorStatus('Ready to save a new row or update an existing symbol.', '');
  });
}
if (sectorEditorQueryEl) {
  sectorEditorQueryEl.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      loadSectorEditorRows();
    }
  });
}
if (sectorEditorSymbol) {
  sectorEditorSymbol.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      saveSectorEditorForm();
    }
  });
}
if (sectorEditorSector1) {
  sectorEditorSector1.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      saveSectorEditorForm();
    }
  });
}
if (sectorEditorSector2) {
  sectorEditorSector2.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      saveSectorEditorForm();
    }
  });
}
if (sectorEditorSector3) {
  sectorEditorSector3.addEventListener('keydown', e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      saveSectorEditorForm();
    }
  });
}

if (PAGE_MODE === 'sectors' && sectorPage) {
  sectorPage.style.display = 'block';
  clearSectorBoard();
  loadSectors();
  loadTopSectors();
  const initialSectorPane = new URLSearchParams(window.location.search).get('tab') === 'editor' ? 'editor' : 'charts';
  switchSectorPane(initialSectorPane);
}

if (PAGE_MODE === 'screener' && screenerPage) {
  screenerPage.style.display = 'block';
  initScreenerDefaults().then(loadScreener);
}

if (PAGE_MODE === 'gmlist' && gmlistPage) {
  gmlistPage.style.display = 'block';
  initGmListDefaults().then(() => {
    const initialTab = isNseMarketOpenNow() ? 'live' : 'lv21';
    switchGmListTab(initialTab);
    if (initialTab !== 'live') {
      loadGmList();
    }
  });
}

if (PAGE_MODE === 'admin' && adminPage) {
  adminPage.style.display = 'block';
  renderAdminJobs();
}

if (PAGE_MODE === 'reports' && reportsPage) {
  reportsPage.style.display = 'block';
  initReportsDefaults().then(loadLatestReport);
}

if (PAGE_MODE === 'stocks') {
  const initialParams = new URLSearchParams(window.location.search);
  const initialSymbol = (initialParams.get('symbol') || '').trim().toUpperCase();
  if (initialSymbol) {
    symInput.value = initialSymbol;
    const fromParam = (initialParams.get('from_date') || '').trim();
    const toParam = (initialParams.get('to_date') || '').trim();
    if (fromParam) document.getElementById('fromDate').value = fromParam;
    if (toParam) document.getElementById('toDate').value = toParam;
    setTimeout(() => loadStock(), 0);
  }
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

function switchSectorPane(pane) {
  const target = String(pane || 'charts').trim().toLowerCase() === 'editor' ? 'editor' : 'charts';
  sectorActivePane = target;
  if (sectorPageTabs) {
    sectorPageTabs.querySelectorAll('.sector-page-tab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.sectorPane === target);
    });
  }
  if (sectorChartsPane) sectorChartsPane.classList.toggle('active', target === 'charts');
  if (sectorEditorPane) sectorEditorPane.classList.toggle('active', target === 'editor');
  if (target === 'editor') {
    loadSectorEditorRows();
  }
}

function setSectorEditorStatus(msg, kind) {
  if (!sectorEditorStatus) return;
  sectorEditorStatus.textContent = msg;
  sectorEditorStatus.className = kind ? `sector-editor-status ${kind}` : 'sector-editor-status';
}

function clearSectorEditorForm() {
  if (sectorEditorSymbol) sectorEditorSymbol.value = '';
  if (sectorEditorSector1) sectorEditorSector1.value = '';
  if (sectorEditorSector2) sectorEditorSector2.value = '';
  if (sectorEditorSector3) sectorEditorSector3.value = '';
}

function fillSectorEditorForm(row) {
  if (!row) return;
  if (sectorEditorSymbol) sectorEditorSymbol.value = row.symbol || '';
  if (sectorEditorSector1) sectorEditorSector1.value = row.sector1 || '';
  if (sectorEditorSector2) sectorEditorSector2.value = row.sector2 || '';
  if (sectorEditorSector3) sectorEditorSector3.value = row.sector3 || '';
  if (sectorEditorSymbol) sectorEditorSymbol.focus();
  setSectorEditorStatus(`Loaded ${row.symbol || ''} into the form.`, 'ok');
}

function hasAnySectorValue(row) {
  return Boolean(
    String(row?.sector1 || '').trim() ||
    String(row?.sector2 || '').trim() ||
    String(row?.sector3 || '').trim()
  );
}

function renderSectorEditorRows(rows) {
  if (!sectorEditorTbody) return 0;
  if (!rows.length) {
    sectorEditorTbody.innerHTML = '';
    if (sectorEditorEmpty) sectorEditorEmpty.style.display = 'block';
    return 0;
  }
  if (sectorEditorEmpty) sectorEditorEmpty.style.display = 'none';
  sectorEditorTbody.innerHTML = rows.map(r => {
    const symbol = escapeHtml(r.symbol || '');
    const sector1 = escapeHtml(r.sector1 || '');
    const sector2 = escapeHtml(r.sector2 || '');
    const sector3 = escapeHtml(r.sector3 || '');
    return `
      <tr data-symbol="${symbol}">
        <td><button class="screener-sym sector-editor-symbol" type="button">${symbol}</button></td>
        <td><input type="text" maxlength="45" value="${sector1}" data-field="sector1"></td>
        <td><input type="text" maxlength="45" value="${sector2}" data-field="sector2"></td>
        <td><input type="text" maxlength="45" value="${sector3}" data-field="sector3"></td>
        <td>
          <div class="sector-editor-row-actions">
            <button class="sector-editor-row-load" type="button">Load</button>
            <button class="sector-editor-row-save" type="button">Save</button>
          </div>
        </td>
      </tr>
    `;
  }).join('');

  sectorEditorTbody.querySelectorAll('tr').forEach(tr => {
    const loadBtn = tr.querySelector('.sector-editor-row-load');
    const saveBtn = tr.querySelector('.sector-editor-row-save');
    const symbolBtn = tr.querySelector('.sector-editor-symbol');
    const getSnapshot = () => collectSectorEditorPayloadFromRow(tr);
    if (loadBtn) loadBtn.addEventListener('click', () => fillSectorEditorForm(getSnapshot()));
    if (symbolBtn) symbolBtn.addEventListener('click', () => fillSectorEditorForm(getSnapshot()));
    if (saveBtn) saveBtn.addEventListener('click', () => saveSectorEditorRow(tr));
    tr.querySelectorAll('input').forEach(inp => {
      inp.addEventListener('keydown', e => {
        if (e.key === 'Enter') {
          e.preventDefault();
          saveSectorEditorRow(tr);
        }
      });
    });
  });
  return rows.length;
}

async function loadSectorEditorRows(query = null) {
  if (!sectorEditorTbody || !sectorEditorQueryEl) return;
  const q = query === null ? sectorEditorQueryEl.value.trim() : String(query || '').trim();
  sectorEditorQueryEl.value = q;
  if (sectorEditorSearchBtn) sectorEditorSearchBtn.disabled = true;
  if (sectorEditorResetBtn) sectorEditorResetBtn.disabled = true;
  setSectorEditorStatus(q ? `Searching for "${q}"...` : 'Loading sector rows...', '');
  try {
    const qs = new URLSearchParams({ limit: '50' });
    if (q) qs.set('q', q);
    const res = await fetch('/api/sector-admin/search?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    sectorEditorRows = data.rows || [];
    sectorEditorLoadedQuery = q;
    const visibleRows = sectorEditorRows.filter(hasAnySectorValue);
    const exactMatch = q ? sectorEditorRows.find(r => String(r.symbol || '').trim().toUpperCase() === q.toUpperCase()) : null;
    if (exactMatch && !hasAnySectorValue(exactMatch)) {
      fillSectorEditorForm(exactMatch);
      setSectorEditorStatus(`${exactMatch.symbol} exists but has no sector values yet. Use the form above to add them.`, 'ok');
    }
    const count = renderSectorEditorRows(visibleRows);
    if (sectorEditorCount) sectorEditorCount.textContent = `${count} row${count === 1 ? '' : 's'}`;
    if (count) {
      setSectorEditorStatus(q ? `Showing ${count} row${count === 1 ? '' : 's'} for "${q}".` : `Showing first ${count} sector rows.`, 'ok');
    } else if (!exactMatch) {
      setSectorEditorStatus(q ? `No sector rows with values matched "${q}".` : 'No sector rows with values found.', '');
    }
    if (!count && sectorEditorEmpty) {
      sectorEditorEmpty.textContent = q
        ? `No sector rows with values matched "${q}".`
        : 'No sector rows with values available.';
    }
  } catch (e) {
    if (sectorEditorEmpty) sectorEditorEmpty.textContent = 'Search to load sector rows.';
    setSectorEditorStatus(`Error: ${e.message}`, 'err');
  } finally {
    if (sectorEditorSearchBtn) sectorEditorSearchBtn.disabled = false;
    if (sectorEditorResetBtn) sectorEditorResetBtn.disabled = false;
  }
}

function collectSectorEditorPayloadFromRow(tr) {
  if (!tr) return null;
  const symbol = String(tr.dataset.symbol || '').trim().toUpperCase();
  if (!symbol) return null;
  return {
    symbol,
    sector1: tr.querySelector('input[data-field="sector1"]')?.value || '',
    sector2: tr.querySelector('input[data-field="sector2"]')?.value || '',
    sector3: tr.querySelector('input[data-field="sector3"]')?.value || '',
  };
}

async function saveSectorEditorRow(tr) {
  const payload = collectSectorEditorPayloadFromRow(tr);
  if (!payload) return;
  setSectorEditorStatus(`Saving ${payload.symbol}...`, '');
  try {
    const res = await fetch('/api/sector-admin/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    setSectorEditorStatus(data.message || `Saved ${payload.symbol}`, 'ok');
    await Promise.all([loadSectors(false), loadTopSectors()]);
    if (sectorActivePane === 'charts' && sectorSelect.value) {
      await loadSectorCharts();
    }
    const refreshQuery = (sectorEditorQueryEl && sectorEditorQueryEl.value.trim()) || payload.symbol;
    await loadSectorEditorRows(refreshQuery);
  } catch (e) {
    setSectorEditorStatus(`Error: ${e.message}`, 'err');
  }
}

async function saveSectorEditorForm() {
  const payload = {
    symbol: sectorEditorSymbol ? sectorEditorSymbol.value : '',
    sector1: sectorEditorSector1 ? sectorEditorSector1.value : '',
    sector2: sectorEditorSector2 ? sectorEditorSector2.value : '',
    sector3: sectorEditorSector3 ? sectorEditorSector3.value : '',
  };
  if (!String(payload.symbol || '').trim()) {
    setSectorEditorStatus('Symbol is required.', 'err');
    if (sectorEditorSymbol) sectorEditorSymbol.focus();
    return;
  }
  setSectorEditorStatus(`Saving ${String(payload.symbol).trim().toUpperCase()}...`, '');
  try {
    const res = await fetch('/api/sector-admin/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    setSectorEditorStatus(data.message || `Saved ${String(payload.symbol).trim().toUpperCase()}`, 'ok');
    await Promise.all([loadSectors(false), loadTopSectors()]);
    if (sectorActivePane === 'charts' && sectorSelect.value) {
      await loadSectorCharts();
    }
    clearSectorEditorForm();
    const refreshQuery = (sectorEditorQueryEl && sectorEditorQueryEl.value.trim()) || String(payload.symbol).trim().toUpperCase();
    if (sectorEditorQueryEl) sectorEditorQueryEl.value = refreshQuery;
    await loadSectorEditorRows(refreshQuery);
  } catch (e) {
    setSectorEditorStatus(`Error: ${e.message}`, 'err');
  }
}

function setScreenerStatus(msg, kind) {
  if (!screenerStatus) return;
  screenerStatus.textContent = msg;
  screenerStatus.className = kind ? `screener-status ${kind}` : 'screener-status';
}

function renderScreenerRows(rows, tbody, valueKey, minKey, fmtFn, attachPreviewListeners = true) {
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmtFn(r[valueKey])}</td>
      <td>${fmtFn(r[minKey])}</td>
      <td>${fmt(r.avg_turnover_21d == null ? null : (r.avg_turnover_21d / 1e7), 2)}</td>
    </tr>
  `).join('');
  if (attachPreviewListeners) attachScreenerPreviewListeners(tbody);
  return rows.length;
}

function renderScreenerMeta(d) {
  if (!screenerMetaStrip) return;
  screenerMetaStrip.innerHTML = `
    <span class="screener-meta-chip">As of<b>${escapeHtml(d.as_of)}</b></span>
    <span class="screener-meta-chip">21d window<b>${escapeHtml(d.window_21d.start)} → ${escapeHtml(d.window_21d.end)}</b></span>
    <span class="screener-meta-chip">52w / 15d<b>Near highs only</b></span>
    <span class="screener-meta-chip">Min turnover<b>${fmt(d.min_turnover / 1e7, 2)} cr</b></span>
    <span class="screener-meta-chip">Universe<b>${d.universe}</b></span>
  `;
}

async function initScreenerDefaults() {
  try {
    const res = await fetch('/api/latest-date');
    const data = await res.json();
    if (data.latest_date) {
      screenerAsOfEl.value = data.latest_date;
    } else {
      const now = new Date();
      screenerAsOfEl.value = now.toISOString().slice(0, 10);
    }
  } catch {
    const now = new Date();
    screenerAsOfEl.value = now.toISOString().slice(0, 10);
  }
}

function setGmListStatus(msg, kind) {
  if (!gmlistStatus) return;
  gmlistStatus.textContent = msg;
  gmlistStatus.className = kind ? `screener-status ${kind}` : 'screener-status';
}

function renderGmListMeta(d) {
  if (!gmlistMetaStrip) return;
  gmlistMetaStrip.innerHTML = `
    <span class="screener-meta-chip">As of<b>${escapeHtml(d.as_of)}</b></span>
    <span class="screener-meta-chip">21d window<b>${escapeHtml(d.window_21d.start)} → ${escapeHtml(d.window_21d.end)}</b></span>
    <span class="screener-meta-chip">GMList<b>${escapeHtml(d.universe)}</b></span>
    <span class="screener-meta-chip">Source<b>${escapeHtml(d.source_file || 'gmlist.txt')}</b></span>
  `;
}

function renderGmRows(rows, tbody, valueKey, minKey, fmtFn, attachPreviewListeners = true) {
  return renderScreenerRows(rows, tbody, valueKey, minKey, fmtFn, attachPreviewListeners);
}

function renderGmInsideRows(rows, tbody, attachPreviewListeners = true) {
  if (!tbody) return 0;
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmt(r.high, 2)}</td>
      <td>${fmt(r.low, 2)}</td>
      <td>${fmt(r.prev_high, 2)}</td>
      <td>${fmt(r.prev_low, 2)}</td>
      <td>${fmt(r.avg_turnover_21d == null ? null : (r.avg_turnover_21d / 1e7), 2)}</td>
    </tr>
  `).join('');
  if (attachPreviewListeners) attachScreenerPreviewListeners(tbody);
  return rows.length;
}

function renderGmHdRows(rows, tbody) {
  if (!tbody) return 0;
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmtPct(r.delivery_latest)}</td>
      <td>${fmt(r.delivery_days_3d, 0)}</td>
      <td>${fmtPct(r.delivery_max_3d)}</td>
      <td>${escapeHtml((r.delivery_hits && r.delivery_hits.length) ? r.delivery_hits.map(x => x.date).join(', ') : '—')}</td>
    </tr>
  `).join('');
  attachScreenerPreviewListeners(tbody);
  return rows.length;
}

function renderGmStrongStartRows(rows, tbody) {
  if (!tbody) return 0;
  if (!rows.length) {
    tbody.innerHTML = '';
    return 0;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><button class="screener-sym" type="button" data-symbol="${escapeHtml(r.symbol || '')}">${escapeHtml(r.symbol || '')}</button></td>
      <td>${fmt(r.open, 2)}</td>
      <td>${fmt(r.high, 2)}</td>
      <td>${fmt(r.low, 2)}</td>
      <td>${fmt(r.close, 2)}</td>
      <td>${fmt(r.prev_high, 2)}</td>
      <td>${fmtPct(r.gap_pct)}</td>
    </tr>
  `).join('');
  attachGmStrongStartListeners(tbody);
  return rows.length;
}

function attachGmStrongStartListeners(tbody) {
  if (!tbody) return;
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
    btn.addEventListener('pointerenter', () => queueGmStrongStartHover(btn.dataset.symbol));
    btn.addEventListener('pointerleave', () => cancelGmStrongStartHover(btn.dataset.symbol));
  });
}

function attachGmLivePreviewListeners(tbody) {
  if (!tbody) return;
  const asOf = gmlistLiveAsOfEl ? gmlistLiveAsOfEl.value : '';
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
    btn.addEventListener('pointerenter', () => queueGmLivePreview(btn.dataset.symbol, asOf));
    btn.addEventListener('pointerleave', () => cancelGmLivePreview(btn.dataset.symbol));
  });
}

function queueGmLivePreview(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (gmlistLivePreviewCloseTimer) {
    clearTimeout(gmlistLivePreviewCloseTimer);
    gmlistLivePreviewCloseTimer = null;
  }
  gmlistLivePreviewSymbol = sym;
  if (gmlistLivePreviewTimer) clearTimeout(gmlistLivePreviewTimer);
  gmlistLivePreviewTimer = setTimeout(() => {
    if (gmlistLivePreviewSymbol !== sym) return;
    openGmListLivePreview(sym, asOfOverride);
  }, 120);
}

function cancelGmLivePreview(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (gmlistLivePreviewSymbol === sym) {
    gmlistLivePreviewSymbol = '';
  }
  if (gmlistLivePreviewTimer) {
    clearTimeout(gmlistLivePreviewTimer);
    gmlistLivePreviewTimer = null;
  }
  scheduleGmLivePreviewClose();
}

function scheduleGmLivePreviewClose() {
  if (gmlistLivePreviewCloseTimer) clearTimeout(gmlistLivePreviewCloseTimer);
  gmlistLivePreviewCloseTimer = setTimeout(() => {
    if (!gmlistLivePreviewSymbol) {
      closeGmListLivePreview();
    }
  }, 220);
}

function closeGmListLivePreview() {
  if (gmlistLivePreviewLoading && gmlistLivePreviewLoading.controller) {
    try { gmlistLivePreviewLoading.controller.abort(); } catch {}
  }
  if (gmlistLivePreviewObserver) {
    gmlistLivePreviewObserver.disconnect();
    gmlistLivePreviewObserver = null;
  }
  if (gmlistLivePreviewChart) {
    gmlistLivePreviewChart.remove();
    gmlistLivePreviewChart = null;
  }
  if (screenerPreviewOverlay) screenerPreviewOverlay.classList.remove('open');
  gmlistLivePreviewSymbol = '';
  if (gmlistLivePreviewTimer) {
    clearTimeout(gmlistLivePreviewTimer);
    gmlistLivePreviewTimer = null;
  }
  if (gmlistLivePreviewCloseTimer) {
    clearTimeout(gmlistLivePreviewCloseTimer);
    gmlistLivePreviewCloseTimer = null;
  }
  gmlistLivePreviewLoading = null;
}

if (screenerPreviewCard) {
  screenerPreviewCard.addEventListener('mouseenter', () => {
    if (gmlistLivePreviewCloseTimer) {
      clearTimeout(gmlistLivePreviewCloseTimer);
      gmlistLivePreviewCloseTimer = null;
    }
  });
  screenerPreviewCard.addEventListener('mouseleave', scheduleGmLivePreviewClose);
}

async function fetchGmListLivePreviewCard(symbol, signal, asOfOverride = '') {
  const cacheKey = `${String(symbol || '').trim().toUpperCase()}|${String(asOfOverride || '').trim()}`;
  if (!cacheKey.startsWith('|') && gmlistLivePreviewCache.has(cacheKey)) {
    return gmlistLivePreviewCache.get(cacheKey);
  }
  const qs = new URLSearchParams();
  qs.set('symbol', String(symbol || '').trim().toUpperCase());
  if (asOfOverride) qs.set('date', asOfOverride);
  const res = await fetch('/api/gmlist-live-preview?' + qs.toString(), { signal });
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error || res.statusText);
  }
  const rows = data.rows || [];
  const card = buildPreviewCardFromRows(String(symbol || '').trim().toUpperCase(), rows);
  gmlistLivePreviewCache.set(cacheKey, card);
  return card;
}

async function openGmListLivePreview(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  closeScreenerPreview();
  gmlistLivePreviewSymbol = sym;
  if (screenerPreviewTitle) screenerPreviewTitle.textContent = sym;
  if (screenerPreviewSubtitle) screenerPreviewSubtitle.textContent = 'Live daily chart preview';
  if (screenerPreviewMeta) screenerPreviewMeta.textContent = 'Loading chart...';
  if (screenerPreviewOverlay) screenerPreviewOverlay.classList.add('open');
  if (screenerPreviewChartEl) screenerPreviewChartEl.innerHTML = '';

  if (gmlistLivePreviewObserver) {
    gmlistLivePreviewObserver.disconnect();
    gmlistLivePreviewObserver = null;
  }
  if (gmlistLivePreviewChart) {
    gmlistLivePreviewChart.remove();
    gmlistLivePreviewChart = null;
  }
  if (gmlistLivePreviewLoading && gmlistLivePreviewLoading.controller) {
    try { gmlistLivePreviewLoading.controller.abort(); } catch {}
  }

  const controller = new AbortController();
  gmlistLivePreviewLoading = { symbol: sym, controller };

  try {
    const card = await fetchGmListLivePreviewCard(sym, controller.signal, asOfOverride);
    if (!card || gmlistLivePreviewSymbol !== sym) return;
    if (screenerPreviewMeta) {
      screenerPreviewMeta.textContent = card.end_close == null ? '' : `Close ${fmt(card.end_close, 2)}`;
    }
    requestAnimationFrame(() => {
      if (gmlistLivePreviewSymbol !== sym) return;
      gmlistLivePreviewChart = drawSectorChart(screenerPreviewChartEl, card, true);
      gmlistLivePreviewObserver = new ResizeObserver(() => {
        if (!gmlistLivePreviewChart) return;
        gmlistLivePreviewChart.applyOptions({
          width: screenerPreviewChartEl.clientWidth,
          height: screenerPreviewChartEl.clientHeight,
        });
        gmlistLivePreviewChart.timeScale().fitContent();
      });
      gmlistLivePreviewObserver.observe(screenerPreviewChartEl);
    });
  } catch (e) {
    if (gmlistLivePreviewSymbol !== sym) return;
    if (e.name === 'AbortError') return;
    if (screenerPreviewMeta) screenerPreviewMeta.textContent = `Error: ${e.message}`;
    if (screenerPreviewChartEl) {
      screenerPreviewChartEl.innerHTML = '<div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">Unable to load preview</div>';
    }
  }
}

function getChartDayKey(time) {
  const raw = normalizeChartTime(time);
  if (!raw) return '';
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && String(numeric) === raw) {
    const dt = new Date(numeric < 1e12 ? numeric * 1000 : numeric);
    if (!Number.isNaN(dt.getTime())) {
      return dt.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
    }
  }
  const dt = new Date(raw);
  if (!Number.isNaN(dt.getTime())) {
    return dt.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
  }
  return raw.slice(0, 10);
}

function queueGmStrongStartHover(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (gmlistStrongStartCloseTimer) {
    clearTimeout(gmlistStrongStartCloseTimer);
    gmlistStrongStartCloseTimer = null;
  }
  gmlistStrongStartHoverSymbol = sym;
  gmlistStrongStartHoverDate = String(asOfOverride || '').trim();
  gmlistStrongStartHoverTimer = setTimeout(() => {
    if (gmlistStrongStartHoverSymbol !== sym) return;
    loadGmListStrongStartChart(sym, gmlistStrongStartHoverDate);
  }, 90);
}

function cancelGmStrongStartHover(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (gmlistStrongStartHoverSymbol === sym) {
    gmlistStrongStartHoverSymbol = '';
    gmlistStrongStartHoverDate = '';
  }
  if (gmlistStrongStartHoverTimer) {
    clearTimeout(gmlistStrongStartHoverTimer);
    gmlistStrongStartHoverTimer = null;
  }
  scheduleGmStrongStartClose();
}

function scheduleGmStrongStartClose() {
  if (gmlistStrongStartCloseTimer) clearTimeout(gmlistStrongStartCloseTimer);
  gmlistStrongStartCloseTimer = setTimeout(() => {
    if (!gmlistStrongStartHoverOpen && !gmlistStrongStartHoverSymbol) {
      closeGmListStrongStartOverlay();
    }
  }, 220);
}

function setGmStrongStartHoverOpen(open) {
  gmlistStrongStartHoverOpen = !!open;
  if (gmlistStrongStartCloseTimer) {
    clearTimeout(gmlistStrongStartCloseTimer);
    gmlistStrongStartCloseTimer = null;
  }
}

function attachGmStrongStartPreviewListeners(tbody) {
  if (!tbody) return;
  const asOf = gmlistLiveAsOfEl ? gmlistLiveAsOfEl.value : '';
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
    btn.addEventListener('pointerenter', () => queueGmStrongStartHover(btn.dataset.symbol, asOf));
    btn.addEventListener('pointerleave', () => cancelGmStrongStartHover(btn.dataset.symbol));
  });
}

function handleGmStrongStartOverlayEnter() {
  setGmStrongStartHoverOpen(true);
}

function handleGmStrongStartOverlayLeave() {
  setGmStrongStartHoverOpen(false);
  scheduleGmStrongStartClose();
}

function switchGmListTab(tab) {
  const target = String(tab || 'lv21').trim();
  gmlistActiveTab = target || 'lv21';
  if (gmlistTabs) {
    gmlistTabs.querySelectorAll('.gmlist-tab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tab === gmlistActiveTab);
    });
  }
  document.querySelectorAll('#gmlistPage .gmlist-panel').forEach(panel => {
    panel.classList.toggle('active', panel.dataset.panel === gmlistActiveTab);
  });
  if (gmlistActiveTab === 'live') {
    if (gmlistLiveAsOfEl && !gmlistLiveAsOfEl.value) gmlistLiveAsOfEl.value = localIsoDate();
    loadGmListLive(true);
  }
  if (gmlistActiveTab === 'strong_start') {
    if (gmlistStrongStartAsOfEl) gmlistStrongStartAsOfEl.value = localIsoDate();
    loadGmListStrongStart(true);
  }
}

function switchGmListLiveTab(tab) {
  const target = String(tab || 'lv21').trim();
  gmlistLiveActiveTab = target || 'lv21';
  if (gmlistLiveSubtabs) {
    gmlistLiveSubtabs.querySelectorAll('.gmlist-subtab').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.liveTab === gmlistLiveActiveTab);
    });
  }
  document.querySelectorAll('#gmlistPage .gmlist-live-panel').forEach(panel => {
    panel.classList.toggle('active', panel.dataset.livePanel === gmlistLiveActiveTab);
  });
}

async function initGmListDefaults() {
  try {
    const res = await fetch('/api/latest-date');
    const data = await res.json();
    if (data.latest_date) {
      gmlistAsOfEl.value = data.latest_date;
    } else {
      gmlistAsOfEl.value = localIsoDate();
    }
  } catch {
    gmlistAsOfEl.value = localIsoDate();
  }
  if (gmlistStrongStartAsOfEl) {
    gmlistStrongStartAsOfEl.value = localIsoDate();
  }
  if (gmlistLiveAsOfEl) {
    gmlistLiveAsOfEl.value = localIsoDate();
  }
}

function localIsoDate() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function getIndiaNowParts() {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    hour12: false,
    weekday: 'short',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).formatToParts(new Date());
  const map = {};
  for (const part of parts) {
    if (part.type !== 'literal') map[part.type] = part.value;
  }
  return map;
}

function isNseMarketOpenNow() {
  const p = getIndiaNowParts();
  const weekday = (p.weekday || '').slice(0, 3).toLowerCase();
  if (['sat', 'sun'].includes(weekday)) return false;
  const hour = parseInt(p.hour || '0', 10);
  const minute = parseInt(p.minute || '0', 10);
  const total = hour * 60 + minute;
  return total >= (9 * 60 + 15) && total <= (15 * 60 + 30);
}

function buildPreviewCardFromRows(symbol, rows) {
  const ordered = [...(rows || [])].slice().sort((a, b) => {
    const ta = new Date(a.mktdate || a.time || 0).getTime();
    const tb = new Date(b.mktdate || b.time || 0).getTime();
    return ta - tb;
  });
  const candles = [];
  const volumes = [];
  const ema5 = [];
  const ema10 = [];
  const ema20 = [];
  const ema50 = [];
  let latestClose = null;
  let latestTime = null;
  for (const r of ordered) {
    const time = r.mktdate || r.time;
    if (!time) continue;
    const candle = {
      time,
      open: r.open,
      high: r.high,
      low: r.low,
      close: r.close,
      volume: r.volume,
      change_pct: r.diff ?? r.change_pct ?? null,
    };
    candles.push(candle);
    volumes.push({
      time,
      value: r.volume,
      color: (r.diff != null && Number(r.diff) >= 0) ? '#58b65b' : '#ef6a6a',
    });
    if (r['5dma'] != null) ema5.push({ time, value: r['5dma'] });
    if (r['10dma'] != null) ema10.push({ time, value: r['10dma'] });
    if (r['20DMA'] != null) ema20.push({ time, value: r['20DMA'] });
    if (r['50dma'] != null) ema50.push({ time, value: r['50dma'] });
    latestClose = r.close;
    latestTime = time;
  }
  return {
    symbol,
    sector: 'Screener Preview',
    has_data: candles.length > 0,
    latest_date: latestTime,
    end_close: latestClose,
    move_pct: ordered.length ? (ordered[ordered.length - 1].diff ?? null) : null,
    avg_turnover_21d: null,
    candles,
    volume: volumes,
    ema5,
    ema10,
    ema20,
    ema50,
  };
}

async function fetchScreenerPreviewCard(symbol, signal) {
  const cacheKey = String(symbol || '').trim().toUpperCase();
  if (!cacheKey) return null;
  if (screenerPreviewCache.has(cacheKey)) {
    return screenerPreviewCache.get(cacheKey);
  }
  const res = await fetch(`/api/stock?symbol=${encodeURIComponent(cacheKey)}`, { signal });
  const data = await res.json();
  if (!res.ok || data.error) {
    throw new Error(data.error || res.statusText);
  }
  const card = buildPreviewCardFromRows(cacheKey, data.rows || []);
  screenerPreviewCache.set(cacheKey, card);
  return card;
}

function closeScreenerPreview(event) {
  if (event && event.target && event.target.id !== 'screenerPreviewOverlay' && event.target.id !== 'screenerPreviewCard') return;
  screenerPreviewOverlay.classList.remove('open');
  screenerPreviewSymbol = '';
  if (screenerPreviewObserver) {
    screenerPreviewObserver.disconnect();
    screenerPreviewObserver = null;
  }
  if (screenerPreviewChart) {
    screenerPreviewChart.remove();
    screenerPreviewChart = null;
  }
  screenerPreviewChartEl.innerHTML = '';
  if (screenerPreviewTimer) {
    clearTimeout(screenerPreviewTimer);
    screenerPreviewTimer = null;
  }
  if (screenerPreviewCloseTimer) {
    clearTimeout(screenerPreviewCloseTimer);
    screenerPreviewCloseTimer = null;
  }
}

function scheduleScreenerPreview(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (screenerPreviewCloseTimer) {
    clearTimeout(screenerPreviewCloseTimer);
    screenerPreviewCloseTimer = null;
  }
  if (screenerPreviewSymbol === sym && screenerPreviewOverlay.classList.contains('open')) {
    return;
  }
  if (screenerPreviewTimer) {
    clearTimeout(screenerPreviewTimer);
  }
  screenerPreviewTimer = setTimeout(() => openScreenerPreview(sym), 180);
}

function queueCloseScreenerPreview() {
  if (gmlistLivePreviewSymbol) return;
  if (screenerPreviewTimer) {
    clearTimeout(screenerPreviewTimer);
    screenerPreviewTimer = null;
  }
  if (screenerPreviewCloseTimer) {
    clearTimeout(screenerPreviewCloseTimer);
  }
  screenerPreviewCloseTimer = setTimeout(() => {
    if (!screenerPreviewCard || !screenerPreviewCard.matches(':hover')) {
      closeScreenerPreview();
    }
  }, 220);
}

function attachScreenerPreviewListeners(tbody) {
  if (!tbody) return;
  tbody.querySelectorAll('.screener-sym').forEach(btn => {
  btn.addEventListener('mouseenter', () => scheduleScreenerPreview(btn.dataset.symbol));
  btn.addEventListener('focus', () => scheduleScreenerPreview(btn.dataset.symbol));
  btn.addEventListener('mouseleave', queueCloseScreenerPreview);
  btn.addEventListener('blur', queueCloseScreenerPreview);
  });
}

if (screenerPreviewCard) {
  screenerPreviewCard.addEventListener('mouseenter', () => {
    if (screenerPreviewCloseTimer) {
      clearTimeout(screenerPreviewCloseTimer);
      screenerPreviewCloseTimer = null;
    }
  });
  screenerPreviewCard.addEventListener('mouseleave', queueCloseScreenerPreview);
}

async function openScreenerPreview(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  screenerPreviewSymbol = sym;
  screenerPreviewTitle.textContent = sym;
  screenerPreviewSubtitle.textContent = 'Bhav chart preview';
  screenerPreviewMeta.textContent = 'Loading chart...';
  screenerPreviewOverlay.classList.add('open');
  screenerPreviewChartEl.innerHTML = '';

  if (screenerPreviewObserver) {
    screenerPreviewObserver.disconnect();
    screenerPreviewObserver = null;
  }
  if (screenerPreviewChart) {
    screenerPreviewChart.remove();
    screenerPreviewChart = null;
  }
  if (screenerPreviewLoading && screenerPreviewLoading.controller) {
    try { screenerPreviewLoading.controller.abort(); } catch {}
  }

  const controller = new AbortController();
  screenerPreviewLoading = { symbol: sym, controller };

  try {
    const card = await fetchScreenerPreviewCard(sym, controller.signal);
    if (!card || screenerPreviewSymbol !== sym) return;
    screenerPreviewMeta.textContent = card.end_close == null ? '' : `Close ${fmt(card.end_close, 2)}`;
    requestAnimationFrame(() => {
      if (screenerPreviewSymbol !== sym) return;
      screenerPreviewChart = drawSectorChart(screenerPreviewChartEl, card, true);
      screenerPreviewObserver = new ResizeObserver(() => {
        if (!screenerPreviewChart) return;
        screenerPreviewChart.applyOptions({
          width: screenerPreviewChartEl.clientWidth,
          height: screenerPreviewChartEl.clientHeight,
        });
        screenerPreviewChart.timeScale().fitContent();
      });
      screenerPreviewObserver.observe(screenerPreviewChartEl);
    });
  } catch (e) {
    if (screenerPreviewSymbol !== sym) return;
    if (e.name === 'AbortError') return;
    screenerPreviewMeta.textContent = `Error: ${e.message}`;
    screenerPreviewChartEl.innerHTML = '<div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;">Unable to load preview</div>';
  }
}

async function loadScreener() {
  if (!screenerPage) return;
  setScreenerStatus('Running…');
  screenerRunBtn.disabled = true;
  screenerPanelVol.classList.add('loading');
  screenerPanelVlt.classList.add('loading');
  screenerEmptyVol.style.display = 'none';
  screenerEmptyVlt.style.display = 'none';
  try {
    const d = screenerAsOfEl.value;
    const mt = (parseFloat(screenerTurnoverEl.value) || 10) * 1e7;
    const lm = Math.max(10, parseInt(screenerLimitEl.value, 10) || 200);
    const qs = new URLSearchParams({ min_turnover: mt, limit: lm });
    if (d) qs.set('date', d);
    const res = await fetch('/api/screener?' + qs.toString());
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || res.statusText);
    }

    renderScreenerMeta(data);
    const nV = renderScreenerRows(data.low_volume || [], screenerTbodyVol, 'volume', 'min_vol_21d', fmtVol);
    const nL = renderScreenerRows(data.low_volatility || [], screenerTbodyVlt, 'volatility', 'min_vlt_21d', (v) => fmt(v, 4));
    screenerCountVol.textContent = nV;
    screenerCountVlt.textContent = nL;
    screenerEmptyVol.style.display = nV ? 'none' : 'block';
    screenerEmptyVlt.style.display = nL ? 'none' : 'block';
    screenerAsOfEl.value = data.as_of;
    setScreenerStatus(`OK — ${nV} low-volume · ${nL} low-volatility stocks on ${data.as_of}.`, 'ok');
  } catch (e) {
    setScreenerStatus(`Error: ${e.message}`, 'err');
  } finally {
    screenerPanelVol.classList.remove('loading');
    screenerPanelVlt.classList.remove('loading');
    screenerRunBtn.disabled = false;
  }
}

async function loadGmList() {
  if (!gmlistPage) return;
  setGmListStatus('Running…');
  if (gmlistRunBtn) gmlistRunBtn.disabled = true;
  if (gmlistTabs) gmlistTabs.classList.add('loading');
  try {
    const d = gmlistAsOfEl.value;
    const qs = new URLSearchParams();
    if (d) qs.set('date', d);
    const res = await fetch('/api/gmlist?' + qs.toString());
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || res.statusText);
    }

    gmlistData = data;
    renderGmListMeta(data);

    const lv21 = data.lv21 || [];
    const lowvol21 = data.lowvol21 || [];
    const insideDays = data.inside_days || [];
    const hd = data.hd || [];

    const nLv21 = renderGmRows(lv21, gmlistTbodyLv21, 'volatility', 'min_vlt_21d', (v) => fmt(v, 4));
    const nLowvol21 = renderGmRows(lowvol21, gmlistTbodyLowvol21, 'volume', 'min_vol_21d', fmtVol);
    const nInsideDays = renderGmInsideRows(insideDays, gmlistTbodyInsideDays);
    const nHd = renderGmHdRows(hd, gmlistTbodyHd);

    if (gmlistCountTabLv21) gmlistCountTabLv21.textContent = nLv21;
    if (gmlistCountTabLowvol21) gmlistCountTabLowvol21.textContent = nLowvol21;
    if (gmlistCountTabInsideDays) gmlistCountTabInsideDays.textContent = nInsideDays;
    if (gmlistCountTabHd) gmlistCountTabHd.textContent = nHd;

    if (gmlistCountPanelLv21) gmlistCountPanelLv21.textContent = nLv21;
    if (gmlistCountPanelLowvol21) gmlistCountPanelLowvol21.textContent = nLowvol21;
    if (gmlistCountPanelInsideDays) gmlistCountPanelInsideDays.textContent = nInsideDays;
    if (gmlistCountPanelHd) gmlistCountPanelHd.textContent = nHd;

    if (gmlistEmptyLv21) gmlistEmptyLv21.style.display = nLv21 ? 'none' : 'block';
    if (gmlistEmptyLowvol21) gmlistEmptyLowvol21.style.display = nLowvol21 ? 'none' : 'block';
    if (gmlistEmptyInsideDays) gmlistEmptyInsideDays.style.display = nInsideDays ? 'none' : 'block';
    if (gmlistEmptyHd) gmlistEmptyHd.style.display = nHd ? 'none' : 'block';

    gmlistAsOfEl.value = data.as_of;
    const source = data.source_file ? data.source_file.replace(/^.*[\\/]/, '') : 'gmlist.txt';
    setGmListStatus(`OK — ${nLv21} lv21 · ${nLowvol21} lowvol21 · ${nInsideDays} inside-day · ${nHd} hd stocks from ${source}.`, 'ok');
    gmlistStrongStartData = [];
    gmlistStrongStartLoadedDate = '';
    if (gmlistCountTabStrongStart) gmlistCountTabStrongStart.textContent = 0;
    if (gmlistCountPanelStrongStart) gmlistCountPanelStrongStart.textContent = 0;
    if (gmlistEmptyStrongStart) gmlistEmptyStrongStart.style.display = 'block';
    if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = 'Open the Strong Start tab to scan with Kite.';
    gmlistLiveData = [];
    gmlistLiveLoadedDate = '';
    if (gmlistCountTabLive) gmlistCountTabLive.textContent = 0;
    if (gmlistCountPanelLive) gmlistCountPanelLive.textContent = 0;
    if (gmlistCountPanelLiveLv21) gmlistCountPanelLiveLv21.textContent = 0;
    if (gmlistCountPanelLiveLowvol21) gmlistCountPanelLiveLowvol21.textContent = 0;
    if (gmlistCountPanelLiveInsideDays) gmlistCountPanelLiveInsideDays.textContent = 0;
    if (gmlistLiveEmptyLv21) gmlistLiveEmptyLv21.style.display = 'block';
    if (gmlistLiveEmptyLowvol21) gmlistLiveEmptyLowvol21.style.display = 'block';
    if (gmlistLiveEmptyInsideDays) gmlistLiveEmptyInsideDays.style.display = 'block';
    if (gmlistLiveStatus) gmlistLiveStatus.textContent = 'Open the Live tab to scan Kite daily bars.';
    switchGmListTab(gmlistActiveTab || 'lv21');
  } catch (e) {
    setGmListStatus(`Error: ${e.message}`, 'err');
  } finally {
    if (gmlistRunBtn) gmlistRunBtn.disabled = false;
    if (gmlistTabs) gmlistTabs.classList.remove('loading');
  }
}

async function loadGmListStrongStart(force = false) {
  if (!gmlistPage) return;
  const d = gmlistStrongStartAsOfEl ? gmlistStrongStartAsOfEl.value : '';
  if (!force && gmlistStrongStartData.length) return;
  if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = 'Scanning Kite 5m bars...';
  try {
    const qs = new URLSearchParams();
    if (d) qs.set('date', d);
    const res = await fetch('/api/gmlist-strong-start?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    gmlistStrongStartData = data.strong_start || [];
    gmlistStrongStartLoadedDate = d || '';
    if (gmlistCountTabStrongStart) gmlistCountTabStrongStart.textContent = gmlistStrongStartData.length;
    if (gmlistCountPanelStrongStart) gmlistCountPanelStrongStart.textContent = gmlistStrongStartData.length;
    if (gmlistEmptyStrongStart) gmlistEmptyStrongStart.style.display = gmlistStrongStartData.length ? 'none' : 'block';
    if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = data.strong_start_status || '';
    renderGmStrongStartRows(gmlistStrongStartData, gmlistTbodyStrongStart);
    attachGmStrongStartListeners(gmlistTbodyStrongStart);
  } catch (e) {
    if (gmlistStrongStartNote) gmlistStrongStartNote.textContent = `Error: ${e.message}`;
  }
}

function renderGmLiveMeta(d) {
  if (!gmlistLiveMetaStrip) return;
  gmlistLiveMetaStrip.innerHTML = `
    <span class="screener-meta-chip">As of<b>${escapeHtml(d.as_of)}</b></span>
    <span class="screener-meta-chip">Live source<b>${escapeHtml(d.source_file || 'kite_daily')}</b></span>
    <span class="screener-meta-chip">GMList<b>${escapeHtml(d.universe)}</b></span>
  `;
}

async function loadGmListLive(force = false) {
  if (!gmlistPage) return;
  const d = gmlistLiveAsOfEl ? gmlistLiveAsOfEl.value : '';
  if (!force && gmlistLiveData.length && d === gmlistLiveLoadedDate) return;
  if (gmlistLiveStatus) gmlistLiveStatus.textContent = 'Scanning Kite daily bars...';
  try {
    const qs = new URLSearchParams();
    if (d) qs.set('date', d);
    const res = await fetch('/api/gmlist-live?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    gmlistLiveData = data;
    gmlistLiveLoadedDate = data.as_of || d || '';
    renderGmLiveMeta(data);

    const lv21 = data.lv21 || [];
    const lowvol21 = data.lowvol21 || [];
    const insideDays = data.inside_days || [];

    const nLv21 = renderGmRows(lv21, gmlistLiveTbodyLv21, 'volatility', 'min_vlt_21d', (v) => fmt(v, 4), false);
    const nLowvol21 = renderGmRows(lowvol21, gmlistLiveTbodyLowvol21, 'volume', 'min_vol_21d', fmtVol, false);
    const nInsideDays = renderGmInsideRows(insideDays, gmlistLiveTbodyInsideDays, false);
    attachGmLivePreviewListeners(gmlistLiveTbodyLv21);
    attachGmLivePreviewListeners(gmlistLiveTbodyLowvol21);
    attachGmLivePreviewListeners(gmlistLiveTbodyInsideDays);

    if (gmlistCountTabLive) gmlistCountTabLive.textContent = data.eligible != null ? data.eligible : data.universe;
    if (gmlistCountPanelLive) gmlistCountPanelLive.textContent = data.eligible != null ? data.eligible : data.universe;
    if (gmlistCountPanelLiveLv21) gmlistCountPanelLiveLv21.textContent = nLv21;
    if (gmlistCountPanelLiveLowvol21) gmlistCountPanelLiveLowvol21.textContent = nLowvol21;
    if (gmlistCountPanelLiveInsideDays) gmlistCountPanelLiveInsideDays.textContent = nInsideDays;

    if (gmlistLiveEmptyLv21) gmlistLiveEmptyLv21.style.display = nLv21 ? 'none' : 'block';
    if (gmlistLiveEmptyLowvol21) gmlistLiveEmptyLowvol21.style.display = nLowvol21 ? 'none' : 'block';
    if (gmlistLiveEmptyInsideDays) gmlistLiveEmptyInsideDays.style.display = nInsideDays ? 'none' : 'block';

    if (gmlistLiveAsOfEl && data.as_of) gmlistLiveAsOfEl.value = data.as_of;
    if (gmlistLiveStatus) gmlistLiveStatus.textContent = `OK — ${nLv21} live lv21 · ${nLowvol21} live lowvol21 · ${nInsideDays} live inside-day stocks.`;
    switchGmListLiveTab(gmlistLiveActiveTab || 'lv21');
  } catch (e) {
    if (gmlistLiveStatus) gmlistLiveStatus.textContent = `Error: ${e.message}`;
  }
}

function closeGmListStrongStartOverlay(event) {
  if (event && event.target && event.target.id !== 'gmlistStrongStartOverlay') return;
  if (gmlistStrongStartOverlay) {
    gmlistStrongStartOverlay.classList.remove('open');
  }
  setGmStrongStartHoverOpen(false);
  if (gmlistStrongStartChartObserver) {
    gmlistStrongStartChartObserver.disconnect();
    gmlistStrongStartChartObserver = null;
  }
  if (gmlistStrongStartChart) {
    gmlistStrongStartChart.remove();
    gmlistStrongStartChart = null;
  }
}

async function loadGmListStrongStartChart(symbol, asOfOverride = '') {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return;
  if (gmlistStrongStartTitle) gmlistStrongStartTitle.textContent = sym;
  if (gmlistStrongStartSubtitle) gmlistStrongStartSubtitle.textContent = 'Kite 5-minute intraday chart';
  if (gmlistStrongStartMeta) gmlistStrongStartMeta.textContent = 'Loading...';
  if (gmlistStrongStartOverlay) gmlistStrongStartOverlay.classList.add('open');
  try {
    const qs = new URLSearchParams();
    qs.set('symbol', sym);
    const d = String(asOfOverride || (gmlistStrongStartAsOfEl ? gmlistStrongStartAsOfEl.value : '') || '').trim();
    if (d) qs.set('date', d);
    qs.set('days', '5');
    const res = await fetch('/api/gmlist-strong-start-chart?' + qs.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);

    const card = {
      symbol: data.symbol,
      sector: 'Kite Intraday',
      is_intraday: true,
      has_data: (data.candles || []).length > 0,
      latest_date: data.candles && data.candles.length ? data.candles[data.candles.length - 1].time : null,
      end_close: data.candles && data.candles.length ? data.candles[data.candles.length - 1].close : null,
      move_pct: null,
      avg_turnover_21d: null,
      candles: data.candles || [],
      volume: data.volume || [],
      ema5: data.ema5 || [],
      ema10: data.ema10 || [],
      ema20: data.ema20 || [],
      ema50: data.ema50 || [],
    };
    if (gmlistStrongStartMeta) {
      const rangeStart = data.start_date ? ` from ${data.start_date}` : '';
      gmlistStrongStartMeta.textContent = `${sym} 5m chart${rangeStart}${data.prev_high != null ? ` | prev high ${fmt(data.prev_high, 2)}` : ''}`;
    }
    if (gmlistStrongStartChartObserver) {
      gmlistStrongStartChartObserver.disconnect();
      gmlistStrongStartChartObserver = null;
    }
    if (gmlistStrongStartChart) {
      gmlistStrongStartChart.remove();
      gmlistStrongStartChart = null;
    }
    if (!gmlistStrongStartChartEl) return;
    gmlistStrongStartChartEl.innerHTML = '';
    gmlistStrongStartChart = drawSectorChart(gmlistStrongStartChartEl, card, true);
    const totalCandles = (card.candles || []).length;
    const visibleCandles = Math.min(90, totalCandles || 90);
    const applyStrongStartWindow = () => {
      if (!gmlistStrongStartChart) return;
      if (totalCandles > 0) {
        const from = Math.max(0, totalCandles - visibleCandles);
        const to = totalCandles - 1;
        gmlistStrongStartChart.timeScale().setVisibleLogicalRange({ from, to });
      }
    };
    requestAnimationFrame(applyStrongStartWindow);
    gmlistStrongStartChartObserver = new ResizeObserver(() => {
      if (!gmlistStrongStartChart) return;
      gmlistStrongStartChart.applyOptions({
        width: gmlistStrongStartChartEl.clientWidth,
        height: gmlistStrongStartChartEl.clientHeight,
      });
      applyStrongStartWindow();
    });
    gmlistStrongStartChartObserver.observe(gmlistStrongStartChartEl);
    setGmStrongStartHoverOpen(true);
  } catch (e) {
    if (gmlistStrongStartMeta) gmlistStrongStartMeta.textContent = `Error: ${e.message}`;
    if (gmlistStrongStartChartEl) {
      gmlistStrongStartChartEl.innerHTML = `
        <div class="sector-board-empty" style="height:100%;display:flex;align-items:center;justify-content:center;text-align:center;padding:16px;">
          <div>
            <div style="font-weight:700;margin-bottom:6px;">Strong Start chart unavailable</div>
            <div style="opacity:.85;max-width:420px;">${escapeHtml(e.message || 'Unknown error')}</div>
          </div>
        </div>
      `;
    }
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
      barSpacing: isZoom ? 6.5 : 3.5,
      minBarSpacing: isZoom ? 2.5 : 1.5,
      rightOffset: isZoom ? 12 : 6,
      fixLeftEdge: true,
      fixRightEdge: true,
      lockVisibleTimeRangeOnResize: true,
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
  const numeric = Number(raw);
  const dt = Number.isFinite(numeric) && String(numeric) === raw
    ? new Date(numeric < 1e12 ? numeric * 1000 : numeric)
    : new Date(raw);
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
  const candles = card.candles || [];
  let daySeparator = null;
  const showIntradaySeparator = !!(isZoom && card && card.is_intraday && candles.length);
  const updateIntradaySeparator = () => {
    if (!showIntradaySeparator || !daySeparator || !candles.length) return;
    const latestDay = getChartDayKey(candles[candles.length - 1].time);
    const firstToday = candles.find(c => getChartDayKey(c.time) === latestDay);
    if (!firstToday) {
      daySeparator.style.display = 'none';
      return;
    }
    const coord = chart.timeScale().timeToCoordinate(firstToday.time);
    if (coord == null || Number.isNaN(coord)) {
      daySeparator.style.display = 'none';
      return;
    }
    daySeparator.style.display = 'block';
    daySeparator.style.left = `${Math.round(coord)}px`;
  };
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
  candleSeries.setData(candles);

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
  if (isZoom) {
    chart.timeScale().applyOptions({
      barSpacing: 12,
      minBarSpacing: 4,
      rightOffset: 18,
    });
    if (candles.length > 90) {
      const start = candles.length - 90;
      const end = candles.length - 1;
      chart.timeScale().setVisibleLogicalRange({ from: start, to: end });
    }
    if (showIntradaySeparator) {
      daySeparator = document.createElement('div');
      daySeparator.className = 'intraday-day-separator';
      chartEl.appendChild(daySeparator);
      updateIntradaySeparator();
      chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
        updateIntradaySeparator();
      });
    }
  }
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
  const sectorOpen = sectorZoomOverlay.classList.contains('open');
  const previewOpen = screenerPreviewOverlay.classList.contains('open');
  if (!sectorOpen && !previewOpen) return;
  if (event.key === 'Escape') {
    event.preventDefault();
    if (previewOpen) closeScreenerPreview();
    if (sectorOpen) closeSectorZoom();
  } else if (sectorOpen && event.key === 'ArrowLeft') {
    event.preventDefault();
    stepSectorZoom(-1);
  } else if (sectorOpen && event.key === 'ArrowRight') {
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

