# Kite Opening Bar Guard

This Chrome extension shows a one-time glossy overlay on Kite chart pages only when the opening bar range is above your threshold.

What it does
- Watches `kite.zerodha.com` chart pages
- Reads the current chart token from the URL
- Asks the local helper server for the opening 5-minute bar
- Shows a glossy one-shot overlay only when the bar is above `2%`
- Stays completely quiet when the bar is below the threshold
- Scans only charts that are present in today's planned/overnight trade-plan targets
- Flashes a few times and auto-closes after 10 seconds

How to load
1. Open `chrome://extensions`
2. Turn on `Developer mode`
3. Click `Load unpacked`
4. Select `C:\Users\shada\Monumental\kite_opening_bar_overlay`

Notes
- The overlay is shown only once per chart key, unless you navigate to a new chart
- If the local helper server is unavailable, the overlay stays quiet
