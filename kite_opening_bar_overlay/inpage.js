(function () {
  const source = document.currentScript && document.currentScript.dataset ? document.currentScript.dataset.source : "kite-opening-bar-guard";
  const thresholdPct = Number(document.currentScript && document.currentScript.dataset ? document.currentScript.dataset.thresholdPct : 2.0) || 2.0;

  function post(payload) {
    window.postMessage({ source, payload }, "*");
  }

  function getDateLabel(dt) {
    try {
      return new Intl.DateTimeFormat("en-CA", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
      }).format(new Date(dt));
    } catch (_err) {
      return "";
    }
  }

  function getTimeLabel(dt) {
    try {
      return new Intl.DateTimeFormat("en-GB", {
        timeZone: "Asia/Kolkata",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      }).format(new Date(dt));
    } catch (_err) {
      return "";
    }
  }

  function num(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function candleValue(candle, keys) {
    for (const key of keys) {
      if (candle && candle[key] != null) {
        const n = num(candle[key]);
        if (n != null) return n;
      }
    }
    return null;
  }

  function candleDt(candle) {
    const raw = candle && (candle.DT || candle.dt || candle.date || candle.Date || candle.datetime || candle.timestamp || candle.time);
    if (!raw) return null;
    const dt = new Date(raw);
    return Number.isFinite(dt.getTime()) ? dt : null;
  }

  function findEngine() {
    const candidates = [window.stxx, window.chartEngine, window.ciqChart, window.chart].filter(Boolean);
    for (const candidate of candidates) {
      if (candidate && typeof candidate === "object" && Array.isArray(candidate.masterData)) return candidate;
    }

    const names = Object.getOwnPropertyNames(window);
    for (const name of names) {
      let value;
      try {
        value = window[name];
      } catch (_err) {
        continue;
      }
      if (!value || typeof value !== "object") continue;
      if (Array.isArray(value.masterData)) return value;
      if (value.chart && Array.isArray(value.chart.masterData)) return value;
    }
    return null;
  }

  function detectIntervalMinutes(engine, candles) {
    const layout = engine && engine.layout ? engine.layout : {};
    const candidates = [layout.interval, layout.periodicity, layout.candleInterval, layout.timeUnit].filter(v => v != null && v !== "");
    for (const v of candidates) {
      const n = Number(v);
      if (Number.isFinite(n) && n > 0) return n;
    }
    if (candles && candles.length >= 2) {
      const a = candleDt(candles[0]);
      const b = candleDt(candles[1]);
      if (a && b) {
        const diff = Math.abs((b.getTime() - a.getTime()) / 60000);
        if (Number.isFinite(diff) && diff > 0) return Math.round(diff);
      }
    }
    return null;
  }

  function probe() {
    try {
      const engine = findEngine();
      if (!engine) {
        post({ ok: false, reason: "chart_engine_not_found" });
        return;
      }

      const md = Array.isArray(engine.masterData) ? engine.masterData.slice() : [];
      if (!md.length) {
        post({ ok: false, reason: "no_chart_data" });
        return;
      }

      const candles = md
        .map((candle) => {
          const dt = candleDt(candle);
          const open = candleValue(candle, ["Open", "open", "O", "o"]);
          const high = candleValue(candle, ["High", "high", "H", "h"]);
          const low = candleValue(candle, ["Low", "low", "L", "l"]);
          const close = candleValue(candle, ["Close", "close", "C", "c"]);
          if (!dt || open == null || high == null || low == null || close == null) return null;
          return { dt, open, high, low, close };
        })
        .filter(Boolean)
        .sort((a, b) => a.dt.getTime() - b.dt.getTime());

      if (!candles.length) {
        post({ ok: false, reason: "no_normalized_candles" });
        return;
      }

      const today = getDateLabel(new Date());
      const sessionCandles = candles.filter(c => getDateLabel(c.dt) === today);
      if (!sessionCandles.length) {
        post({ ok: false, reason: "no_today_session" });
        return;
      }

      const opening = sessionCandles[0];
      const intervalMinutes = detectIntervalMinutes(engine, sessionCandles);
      const rangePct = ((opening.high - opening.low) / opening.open) * 100;
      const bodyPct = (Math.abs(opening.close - opening.open) / opening.open) * 100;
      const symbol = (engine.chart && engine.chart.symbol) || engine.chartSymbol || (engine.layout && engine.layout.symbol) || location.pathname.split("/").filter(Boolean).slice(-1)[0] || "Kite";

      post({
        ok: true,
        symbol,
        date: today,
        intervalMinutes,
        opening: {
          time: getTimeLabel(opening.dt),
          open: opening.open,
          high: opening.high,
          low: opening.low,
          close: opening.close,
          rangePct,
          bodyPct
        },
        thresholdPct
      });
    } catch (err) {
      post({ ok: false, reason: "probe_failed", message: String(err && err.message ? err.message : err) });
    }
  }

  probe();
})();
