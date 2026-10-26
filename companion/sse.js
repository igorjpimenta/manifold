// companion/sse.js — minimal SSE subscriber with reconnect backoff.
// One subscription per caller; onEvent receives each parsed JSON frame.
// stop() closes the stream and cancels pending reconnects.
import http from "node:http";
import https from "node:https";

export function subscribeSse({ url, token, onEvent, onConnect = null, onDrop = null, logger = null, name = "sse" }) {
  const base = new URL(url);
  const transport = base.protocol === "https:" ? https : http;
  let stopped = false;
  let req = null;
  let backoffMs = 1000;
  const BACKOFF_MAX_MS = 30_000;

  function connect() {
    if (stopped) return;
    req = transport.get(
      base,
      { headers: { authorization: `Bearer ${token}`, accept: "text/event-stream" } },
      (res) => {
        if (res.statusCode !== 200) {
          logger?.warn?.(`companion ${name}: subscribe got ${res.statusCode}; retrying`);
          res.resume();
          scheduleReconnect();
          return;
        }
        backoffMs = 1000;
        onConnect?.();
        let buf = "";
        res.on("data", (chunk) => {
          buf += chunk.toString("utf8");
          let idx;
          while ((idx = buf.indexOf("\n\n")) >= 0) {
            const frame = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            for (const line of frame.split("\n")) {
              if (!line.startsWith("data:")) continue;
              try {
                onEvent(JSON.parse(line.slice(5).trim()));
              } catch {}
            }
          }
        });
        res.on("close", () => {
          if (!stopped) {
            onDrop?.();
            scheduleReconnect();
          }
        });
        res.on("error", () => {
          try {
            res.destroy();
          } catch {}
        });
      },
    );
    req.on("error", (err) => {
      if (!stopped) {
        logger?.warn?.(`companion ${name}: stream error (${err?.message ?? err}); retrying`);
        scheduleReconnect();
      }
    });
    req.end();
  }

  let reconnectTimer = null;
  function scheduleReconnect() {
    if (stopped) return;
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connect, backoffMs);
    backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);
    reconnectTimer.unref?.();
  }

  connect();
  return {
    stop() {
      stopped = true;
      clearTimeout(reconnectTimer);
      try {
        req?.destroy();
      } catch {}
    },
  };
}
