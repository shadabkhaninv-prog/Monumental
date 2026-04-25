# Kite Trade Helper Overlay

This Chrome extension adds a small overlay banner on `kite.zerodha.com` pages.

What it shows
- Whether your local helper server is reachable
- The public IP reported by the local server
- A reminder to whitelist that IP in Kite if order placement is blocked

How to load
1. Open `chrome://extensions`
2. Turn on `Developer mode`
3. Click `Load unpacked`
4. Select `C:\Users\shada\Monumental\kite_overlay_extension`

Notes
- The overlay only runs on `https://kite.zerodha.com/*`
- It reads the helper status from `http://127.0.0.1:8765/api/storage-info`
- If you change the server port, update `API_BASE` in `content.js`
