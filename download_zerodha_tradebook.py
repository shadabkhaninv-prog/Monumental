#!/usr/bin/env python3
"""
Download Zerodha Console tradebook CSV into Monumental's tradebook folder.

This uses a persistent browser profile so you can log in once and reuse the
session. Zerodha still requires a fresh login at least once per day.

Examples
--------
python download_zerodha_tradebook.py
python download_zerodha_tradebook.py --start 2026-04-01 --end 2026-04-17
python download_zerodha_tradebook.py --browser edge --client-id DS9072
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from selenium import webdriver
from selenium.common.exceptions import JavascriptException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "input" / "tradebook"
DEFAULT_PROFILE_DIR = BASE_DIR / "input" / "zerodha_console_profile"
TRADEBOOK_URL = "https://console.zerodha.com/reports/tradebook"
IST = ZoneInfo("Asia/Kolkata")

BROWSER_PATHS = {
    "chrome": [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ],
    "edge": [
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    ],
}

JS_SELECT_SEGMENT = r"""
const desired = String(arguments[0] || '').trim().toLowerCase();
if (!desired) return {ok: true, mode: 'skipped'};

function visible(el) {
  if (!el) return false;
  const style = window.getComputedStyle(el);
  const rect = el.getBoundingClientRect();
  return style && style.visibility !== 'hidden' && style.display !== 'none' &&
         rect.width > 0 && rect.height > 0;
}
function norm(text) {
  return String(text || '').replace(/\s+/g, ' ').trim().toLowerCase();
}

const options = [...document.querySelectorAll('select option,[role="option"],li,button,div,a,span')]
  .filter(visible)
  .filter(el => norm(el.innerText || el.textContent) === desired);
if (options.length) {
  options[0].click();
  return {ok: true, mode: 'option-direct'};
}

const selects = [...document.querySelectorAll('select')].filter(visible);
for (const sel of selects) {
  const found = [...sel.options].find(opt => norm(opt.textContent) === desired);
  if (found) {
    sel.value = found.value;
    sel.dispatchEvent(new Event('change', {bubbles: true}));
    return {ok: true, mode: 'select'};
  }
}

const toggles = [...document.querySelectorAll('[role="combobox"],button,div')]
  .filter(visible)
  .filter(el => {
    const txt = norm(el.innerText || el.textContent);
    return txt.includes('segment') || txt === 'equity' || txt === 'equity (external)' || txt === 'eq';
  });

if (toggles.length) {
  toggles[0].click();
  return {ok: false, reason: 'opened-segment-dropdown'};
}

return {ok: false, reason: 'segment-control-not-found'};
"""

JS_SET_DATE_RANGE = r"""
const startIso = String(arguments[0] || '');
const endIso = String(arguments[1] || '');
if (!startIso || !endIso) return {ok: false, reason: 'missing-date-args'};

function visible(el) {
  if (!el) return false;
  const style = window.getComputedStyle(el);
  const rect = el.getBoundingClientRect();
  return style && style.visibility !== 'hidden' && style.display !== 'none' &&
         rect.width > 0 && rect.height > 0;
}
function norm(text) {
  return String(text || '').replace(/\s+/g, ' ').trim().toLowerCase();
}
function setNativeValue(el, value) {
  const proto = Object.getPrototypeOf(el);
  const desc = Object.getOwnPropertyDescriptor(proto, 'value') ||
               Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
  if (desc && desc.set) {
    desc.set.call(el, value);
  } else {
    el.value = value;
  }
  el.dispatchEvent(new Event('input', {bubbles: true}));
  el.dispatchEvent(new Event('change', {bubbles: true}));
  el.dispatchEvent(new Event('blur', {bubbles: true}));
}
function formatFor(el, iso) {
  return el.type === 'date' ? iso : iso.split('-').reverse().join('/');
}

const inputs = [...document.querySelectorAll('input')].filter(visible);
const candidates = inputs.filter(el => {
  const meta = norm([el.placeholder, el.getAttribute('aria-label'), el.name, el.id, el.value].join(' '));
  if (el.type === 'date') return true;
  if (!['text', 'search', 'tel'].includes(el.type || 'text')) return false;
  return meta.includes('date') || meta.includes('from') || meta.includes('to') || /\d{2}\/\d{2}\/\d{4}/.test(meta);
}).sort((a, b) => {
  const ar = a.getBoundingClientRect();
  const br = b.getBoundingClientRect();
  return (ar.top - br.top) || (ar.left - br.left);
});

if (candidates.length >= 2) {
  setNativeValue(candidates[0], formatFor(candidates[0], startIso));
  setNativeValue(candidates[1], formatFor(candidates[1], endIso));
  return {
    ok: true,
    mode: 'two-inputs',
    labels: candidates.slice(0, 2).map(el => ({
      placeholder: el.placeholder || '',
      aria: el.getAttribute('aria-label') || '',
      type: el.type || '',
    })),
  };
}

return {ok: false, reason: 'date-inputs-not-found'};
"""

JS_CLICK_APPLY = r"""
function visible(el) {
  if (!el) return false;
  const style = window.getComputedStyle(el);
  const rect = el.getBoundingClientRect();
  return style && style.visibility !== 'hidden' && style.display !== 'none' &&
         rect.width > 0 && rect.height > 0;
}
function norm(text) {
  return String(text || '').replace(/\s+/g, ' ').trim().toLowerCase();
}

const clickables = [...document.querySelectorAll('button,[role="button"],a')].filter(visible);
for (const el of clickables) {
  const txt = norm(el.innerText || el.textContent);
  if (txt === 'view' || txt === 'apply' || txt === 'go' || txt === 'submit') {
    el.click();
    return {ok: true, mode: txt};
  }
}

const inputs = [...document.querySelectorAll('input')].filter(visible).sort((a, b) => {
  const ar = a.getBoundingClientRect();
  const br = b.getBoundingClientRect();
  return (ar.top - br.top) || (ar.left - br.left);
});
if (inputs.length >= 2) {
  const anchor = inputs[1].getBoundingClientRect();
  const nearby = clickables.find(el => {
    const r = el.getBoundingClientRect();
    return Math.abs(r.top - anchor.top) < 80 && r.left > anchor.right - 10;
  });
  if (nearby) {
    nearby.click();
    return {ok: true, mode: 'nearby-button'};
  }
}

return {ok: false, reason: 'apply-button-not-found'};
"""

JS_CLICK_DOWNLOAD_CSV = r"""
function visible(el) {
  if (!el) return false;
  const style = window.getComputedStyle(el);
  const rect = el.getBoundingClientRect();
  return style && style.visibility !== 'hidden' && style.display !== 'none' &&
         rect.width > 0 && rect.height > 0;
}
function norm(text) {
  return String(text || '').replace(/\s+/g, ' ').trim().toLowerCase();
}

const clickables = [...document.querySelectorAll('button,[role="button"],a,li,div,span')].filter(visible);
for (const el of clickables) {
  const txt = norm(el.innerText || el.textContent);
  if (txt === 'csv' || txt.includes('download csv')) {
    el.click();
    return {ok: true, mode: 'csv-direct'};
  }
}
for (const el of clickables) {
  const txt = norm(el.innerText || el.textContent);
  if (txt === 'download' || txt.includes('download')) {
    el.click();
    return {ok: false, reason: 'opened-download-menu'};
  }
}
return {ok: false, reason: 'download-control-not-found'};
"""


def latest_completed_market_day(now: Optional[datetime] = None) -> date:
    now_ist = now.astimezone(IST) if now is not None else datetime.now(IST)
    today_ist = now_ist.date()
    if today_ist.weekday() >= 5:
        while today_ist.weekday() >= 5:
            today_ist -= timedelta(days=1)
        return today_ist
    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if now_ist >= market_close:
        return today_ist
    out = today_ist - timedelta(days=1)
    while out.weekday() >= 5:
        out -= timedelta(days=1)
    return out


def infer_client_id(output_dir: Path) -> str:
    for path in sorted(output_dir.glob("tradebook-*-*.csv")):
        match = re.match(r"tradebook-([^-]+)-", path.name, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return "DS9072"


def parse_args() -> argparse.Namespace:
    latest_day = latest_completed_market_day()
    default_start = latest_day - timedelta(days=364)
    parser = argparse.ArgumentParser(description="Download Zerodha Console tradebook CSV.")
    parser.add_argument("--start", type=str, default=default_start.isoformat(), help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=latest_day.isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--segment", type=str, default="Equity", help="Console segment label, eg Equity")
    parser.add_argument("--segment-tag", type=str, default="EQ", help="Filename tag for the segment")
    parser.add_argument("--client-id", type=str, default=None, help="Client id used in output filename")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Folder to save CSV")
    parser.add_argument("--profile-dir", type=Path, default=DEFAULT_PROFILE_DIR, help="Persistent browser profile folder")
    parser.add_argument("--browser", choices=["auto", "chrome", "edge"], default="auto")
    parser.add_argument("--page-timeout", type=int, default=180, help="Seconds to wait for console login/page ready")
    parser.add_argument("--download-timeout", type=int, default=120, help="Seconds to wait for CSV download")
    parser.add_argument("--keep-open", action="store_true", help="Leave the browser open after download")
    return parser.parse_args()


def resolve_browser(browser_choice: str) -> tuple[str, Path]:
    if browser_choice != "auto":
        for candidate in BROWSER_PATHS[browser_choice]:
            if candidate.exists():
                return browser_choice, candidate
        raise FileNotFoundError(f"{browser_choice} browser executable not found")

    for name in ("chrome", "edge"):
        for candidate in BROWSER_PATHS[name]:
            if candidate.exists():
                return name, candidate
    raise FileNotFoundError("No supported browser found. Install Chrome or Edge.")


def build_driver(browser_name: str, binary_path: Path, download_dir: Path, profile_dir: Path) -> WebDriver:
    download_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "safebrowsing.enabled": True,
    }

    if browser_name == "chrome":
        options = ChromeOptions()
        options.binary_location = str(binary_path)
        options.add_argument(f"--user-data-dir={profile_dir.resolve()}")
        options.add_argument("--window-size=1500,1000")
        options.add_argument("--disable-notifications")
        options.add_experimental_option("prefs", prefs)
        return webdriver.Chrome(options=options)

    edge_options = webdriver.EdgeOptions()
    edge_options.binary_location = str(binary_path)
    edge_options.use_chromium = True
    edge_options.add_argument(f"--user-data-dir={profile_dir.resolve()}")
    edge_options.add_argument("--window-size=1500,1000")
    edge_options.add_argument("--disable-notifications")
    edge_options.add_experimental_option("prefs", prefs)
    return webdriver.Edge(options=edge_options)


def wait_for_tradebook_page(driver: WebDriver, timeout_sec: int) -> None:
    driver.get(TRADEBOOK_URL)
    print("Browser opened. If Zerodha asks for login / 2FA, finish it in this window.")
    print("Waiting for Console Tradebook page...")

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        current = driver.current_url.lower()
        title = driver.title.lower()
        if "console.zerodha.com/reports/tradebook" in current:
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                pass
            return
        if "tradebook" in title and "console.zerodha.com" in current:
            return
        time.sleep(2)

    raise TimeoutException(
        "Timed out waiting for Zerodha Console Tradebook page. "
        "Please log in manually and rerun if needed."
    )


def run_js(driver: WebDriver, script: str, *args) -> dict:
    try:
        result = driver.execute_script(script, *args)
        return result or {}
    except JavascriptException as exc:
        return {"ok": False, "reason": f"javascript-error: {exc.msg}"}


def wait_for_download(output_dir: Path, started_at: float, timeout_sec: int) -> Path:
    deadline = time.time() + timeout_sec
    last_candidate: Optional[Path] = None
    stable_count = 0

    while time.time() < deadline:
        partials = list(output_dir.glob("*.crdownload")) + list(output_dir.glob("*.tmp"))
        candidates = [
            p for p in output_dir.glob("*.csv")
            if p.stat().st_mtime >= started_at - 1
        ]
        if candidates and not partials:
            latest = max(candidates, key=lambda p: p.stat().st_mtime)
            if last_candidate == latest:
                stable_count += 1
            else:
                last_candidate = latest
                stable_count = 0
            if stable_count >= 2:
                return latest
        time.sleep(1)

    raise TimeoutException(
        f"No completed CSV download found in {output_dir} within {timeout_sec} seconds."
    )


def validate_tradebook_csv(path: Path) -> None:
    try:
        header = path.read_text(encoding="utf-8", errors="ignore").splitlines()[0].lower()
    except Exception as exc:
        raise RuntimeError(f"Downloaded file could not be read: {exc}") from exc
    required = ["symbol", "trade_date", "trade_type", "quantity", "price"]
    missing = [col for col in required if col not in header]
    if missing:
        raise RuntimeError(f"Downloaded CSV does not look like a tradebook. Missing columns: {missing}")


def rename_download(downloaded: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if downloaded.resolve() == target.resolve():
        return target
    if target.exists():
        target.unlink()
    shutil.move(str(downloaded), str(target))
    return target


def auto_prepare_page(driver: WebDriver, start_dt: date, end_dt: date, segment_label: str) -> None:
    if segment_label:
        seg_result = run_js(driver, JS_SELECT_SEGMENT, segment_label)
        if not seg_result.get("ok"):
            print(f"Segment auto-select incomplete: {seg_result}. Continuing.")
            time.sleep(1.5)
            seg_result = run_js(driver, JS_SELECT_SEGMENT, segment_label)
            if not seg_result.get("ok"):
                print("Please verify the Tradebook segment is set correctly in the browser.")

    date_result = run_js(driver, JS_SET_DATE_RANGE, start_dt.isoformat(), end_dt.isoformat())
    if not date_result.get("ok"):
        print(f"Date auto-fill incomplete: {date_result}")
        print("Please verify the date range manually in the browser if needed.")
    else:
        print(f"Date range applied: {start_dt.isoformat()} to {end_dt.isoformat()}")

    apply_result = run_js(driver, JS_CLICK_APPLY)
    if not apply_result.get("ok"):
        print(f"Could not auto-click the page refresh button: {apply_result}")
        print("If the report does not refresh, click the arrow / view button manually.")
    else:
        print(f"Tradebook refresh triggered via: {apply_result.get('mode')}")

    time.sleep(4)


def auto_trigger_download(driver: WebDriver) -> None:
    click1 = run_js(driver, JS_CLICK_DOWNLOAD_CSV)
    if click1.get("ok"):
        print("CSV download clicked directly.")
        return

    print(f"Initial CSV click incomplete: {click1}")
    if click1.get("reason") == "opened-download-menu":
        time.sleep(1)
        click2 = run_js(driver, JS_CLICK_DOWNLOAD_CSV)
        if click2.get("ok"):
            print("CSV option clicked from download menu.")
            return
        print(f"Second CSV click incomplete: {click2}")

    print("If Zerodha's UI changed, click the CSV download manually in the open browser.")


def main() -> int:
    args = parse_args()
    try:
        start_dt = date.fromisoformat(args.start)
        end_dt = date.fromisoformat(args.end)
    except ValueError:
        print("Dates must be in YYYY-MM-DD format.", file=sys.stderr)
        return 2

    if end_dt < start_dt:
        print("End date cannot be before start date.", file=sys.stderr)
        return 2
    if (end_dt - start_dt).days >= 365:
        print("Tradebook in Console can only be downloaded for a maximum 365-day range.", file=sys.stderr)
        return 2

    output_dir = args.output_dir.resolve()
    profile_dir = args.profile_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    client_id = (args.client_id or infer_client_id(output_dir)).strip().upper()
    target_name = f"tradebook-{client_id}-{args.segment_tag}_{start_dt:%Y%m%d}_{end_dt:%Y%m%d}.csv"
    target_path = output_dir / target_name

    try:
        browser_name, binary_path = resolve_browser(args.browser)
        print(f"Using browser: {browser_name} ({binary_path})")
        driver = build_driver(browser_name, binary_path, output_dir, profile_dir)
    except Exception as exc:
        print(f"Could not start the browser automation: {exc}", file=sys.stderr)
        print("Install selenium with: python -m pip install selenium", file=sys.stderr)
        return 1

    downloaded_file: Optional[Path] = None
    try:
        wait_for_tradebook_page(driver, args.page_timeout)
        auto_prepare_page(driver, start_dt, end_dt, args.segment)
        started_at = time.time()
        auto_trigger_download(driver)
        print("Waiting for CSV download to finish...")
        downloaded_file = wait_for_download(output_dir, started_at, args.download_timeout)
        validate_tradebook_csv(downloaded_file)
        final_path = rename_download(downloaded_file, target_path)
        print(f"Tradebook saved to: {final_path}")
        return 0
    except TimeoutException as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Tradebook download failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if args.keep_open:
            print("Keeping the browser open as requested.")
        else:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
