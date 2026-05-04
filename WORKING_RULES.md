# Working Rules

- After any backend code change, recycle the local server before handoff.
- Verify the affected route or screen after every change.
- Do not hand back a change if the live module still errors or loads stale data.
- Preserve existing functionality by default; only replace or remove UI elements when the user explicitly asks for a redesign or removal.
- Make the smallest possible edit for layout-only requests; avoid broad rewrites of render blocks or related helpers.
- For Streaks work, confirm `/api/streaks` returns `200` and the UI section is populated.
- Keep answers concise and number-focused unless the user asks for detail.
- For number-heavy answers, prefer compact tables with `Input`, `Formula`, and `Result` columns where possible.
