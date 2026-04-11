# -*- coding: utf-8 -*-
"""
kite_get_access_token.py
─────────────────────────────────────────────────────────────────────
Generates a fresh Zerodha Kite access token automatically.
Opens the Kite login page in your browser, captures the
request_token from the redirect, and prints the access token.

REQUIREMENTS:
    pip install kiteconnect

USAGE:
    python kite_get_access_token.py

NOTES:
    - Access token is valid only for the current trading day
    - Run this script once each morning before using Kite API
    - Token is saved to kite_token.txt for use by other scripts
─────────────────────────────────────────────────────────────────────
"""

import os
import sys
import webbrowser
from datetime import datetime

# ── CONFIG — fill these in once ───────────────────────────────────
API_KEY    = "enxrvpfmkswonhxh"
API_SECRET = "fay2zarcps3e16zoljgiau0wvky10vke"
TOKEN_FILE = "kite_token.txt"     # access token saved here
# ─────────────────────────────────────────────────────────────────

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("ERROR: kiteconnect not installed.")
    print("       Run: pip install kiteconnect")
    sys.exit(1)


def save_token(access_token: str, api_key: str):
    """Save access token to file with timestamp."""
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(TOKEN_FILE, "w") as f:
        f.write(f"# Kite Access Token — generated {today}\n")
        f.write(f"API_KEY={api_key}\n")
        f.write(f"ACCESS_TOKEN={access_token}\n")
        f.write(f"GENERATED={today}\n")
    print(f"  Token saved to: {os.path.abspath(TOKEN_FILE)}")


def load_token() -> dict | None:
    """Load token from file if it exists and was generated today."""
    if not os.path.exists(TOKEN_FILE):
        return None
    data = {}
    with open(TOKEN_FILE) as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            data[key.strip()] = val.strip()
    if "GENERATED" not in data:
        return None
    gen_date = data["GENERATED"][:10]
    today    = datetime.now().strftime("%Y-%m-%d")
    if gen_date != today:
        print(f"  Existing token is from {gen_date} — generating a new one.")
        return None
    return data


def get_access_token() -> str:
    """Full flow: open browser → capture request_token → generate access token."""

    if API_KEY == "your_api_key_here":
        print("\nERROR: Please set your API_KEY and API_SECRET in this script.")
        print("       Get them from: https://developers.kite.trade/apps")
        sys.exit(1)

    kite = KiteConnect(api_key=API_KEY)

    # ── Step 1: Open login URL in browser ────────────────────────
    login_url = kite.login_url()
    print("\n" + "=" * 62)
    print("  ZERODHA KITE — ACCESS TOKEN GENERATOR")
    print("=" * 62)
    print(f"\n  Step 1: Opening Kite login page in your browser...")
    print(f"  URL: {login_url}\n")
    webbrowser.open(login_url)

    # ── Step 2: User logs in and gets redirected ──────────────────
    print("  Step 2: After logging in, you will be redirected to a URL like:")
    print("          https://127.0.0.1/?request_token=XXXXXXXX&action=login&status=success")
    print()
    print("  Copy the FULL redirect URL from your browser's address bar.")
    print("  (If redirect fails, just copy the request_token value)\n")

    # ── Step 3: Extract request_token ────────────────────────────
    user_input = input("  Paste the full redirect URL (or just the request_token): ").strip()

    request_token = None

    if "request_token=" in user_input:
        # Parse from full URL
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(user_input)
            params = parse_qs(parsed.query)
            if "request_token" in params:
                request_token = params["request_token"][0]
        except Exception:
            pass
        if not request_token:
            # Manual split fallback
            part = user_input.split("request_token=")[1]
            request_token = part.split("&")[0].strip()
    else:
        # User pasted just the token directly
        request_token = user_input.strip()

    if not request_token:
        print("\nERROR: Could not extract request_token from input.")
        sys.exit(1)

    print(f"\n  request_token : {request_token}")

    # ── Step 4: Generate access token ────────────────────────────
    print("\n  Step 3: Generating access token...")
    try:
        session_data = kite.generate_session(
            request_token=request_token,
            api_secret=API_SECRET
        )
    except Exception as e:
        print(f"\nERROR: Failed to generate session — {e}")
        print("  Possible reasons:")
        print("  1. request_token already used (each token is one-time use)")
        print("  2. API_SECRET is incorrect")
        print("  3. Token expired (valid only for a few minutes after login)")
        sys.exit(1)

    access_token = session_data["access_token"]
    user_name    = session_data.get("user_name", "Unknown")
    user_id      = session_data.get("user_id", "Unknown")
    login_time   = session_data.get("login_time", datetime.now())

    # ── Step 5: Display & save ────────────────────────────────────
    print("\n" + "=" * 62)
    print("  SUCCESS!")
    print(f"  User       : {user_name} ({user_id})")
    print(f"  Login Time : {login_time}")
    print(f"  Token      : {access_token}")
    print("=" * 62)

    save_token(access_token, API_KEY)

    # ── Step 6: Verify token works ────────────────────────────────
    print("\n  Verifying token...")
    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
        print(f"  Token verified ✓  —  {profile.get('user_name', '')} ({profile.get('user_id', '')})")
    except Exception as e:
        print(f"  WARNING: Token verification failed — {e}")

    return access_token


def main():
    # ── Check if today's token already exists ─────────────────────
    existing = load_token()
    if existing and existing.get("ACCESS_TOKEN"):
        print("\n" + "=" * 62)
        print("  EXISTING TOKEN FOUND (generated today)")
        print(f"  API_KEY      : {existing.get('API_KEY', '-')}")
        print(f"  ACCESS_TOKEN : {existing.get('ACCESS_TOKEN', '-')}")
        print(f"  Generated    : {existing.get('GENERATED', '-')}")
        print("=" * 62)

        choice = input("\n  Generate a new token anyway? (y/N): ").strip().lower()
        if choice != "y":
            print("\n  Using existing token. Done!")
            return existing["ACCESS_TOKEN"]

    # ── Generate fresh token ──────────────────────────────────────
    token = get_access_token()

    print("\n  ── How to use this token in your scripts ──────────────")
    print(f"  from kiteconnect import KiteConnect")
    print(f"  kite = KiteConnect(api_key='{API_KEY}')")
    print(f"  kite.set_access_token('{token}')")
    print()
    print("  Or load from file automatically:")
    print("  " + "-" * 48)
    print("  def load_kite_token():")
    print(f"      with open('{TOKEN_FILE}') as f:")
    print("          for line in f:")
    print("              if line.startswith('ACCESS_TOKEN='):")
    print("                  return line.split('=', 1)[1].strip()")
    print()

    return token


if __name__ == "__main__":
    main()