// serve-ws.js — dumb event pipe over WebSocket. Tails the in-process push
// stream (events/stream.js) and broadcasts every appended entry to all
// connected clients, verbatim. No interpretation, no filtering, no notify
// logic: a gate opening is just another entry, and what it means is the
// subscriber's decision. Framing and client hygiene live in
// events/ws-frame.js (shared with the dashboard's live endpoint).
//
// Wire protocol (JSON text frames, server → client only):
//   {"type":"welcome","lastEventId":N}    on connect (history length)
//   {"type":"history","events":[...]}     backlog, oldest-first, capped
//   {"type":"event","event":{...entry...}} one per appended entry, in order
// Clients track their own seen state: reconnecting replays history, so a
// missed frame costs a re-read, never a lost event. No auth in-process —
// bind localhost, or put a bearer-checked reverse proxy in front.

import http from "node:http";
import { attachWsEndpoint, encodeWsFrame } from "./ws-frame.js";

export const DEFAULT_WS_PORT = 38294;
export const HISTORY_LIMIT = 200;

export function startWsServer({ stream, port = null, host = "127.0.0.1", historyLimit = null, logger } = {}) {
  if (!stream || typeof stream.subscribe !== "function" || typeof stream.history !== "function") {
    throw new Error("manifold: serve-ws requires an events/stream.js stream (subscribe + history)");
  }
  const listenPort = port ?? DEFAULT_WS_PORT;
  const histLimit = historyLimit ?? HISTORY_LIMIT;

  const server = http.createServer((req, res) => {
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("manifold events: websocket only — connect with an Upgrade request\n");
  });

  // Every upgrade target is the event door — path routing is the reverse
  // proxy's job for this transport.
  const door = attachWsEndpoint(server, {
    match: () => true,
    onConnect: (socket) => {
      logger?.info?.(`manifold: ws subscriber connected (${door.clientCount()} total)`);
      const history = stream.history({ limit: histLimit });
      socket.write(encodeWsFrame(JSON.stringify({ type: "welcome", lastEventId: history.length })));
      socket.write(encodeWsFrame(JSON.stringify({ type: "history", events: history })));
    },
    logger,
  });

  const unsubscribe = stream.subscribe((entry) => {
    try {
      door.broadcast({ type: "event", event: entry });
    } catch (err) {
      logger?.warn?.(`manifold: ws broadcast failed: ${err?.message ?? err}`);
    }
  });

  return new Promise((resolve, reject) => {
    server.once("error", (err) => {
      try { unsubscribe(); } catch {}
      reject(err);
    });
    server.listen(listenPort, host, () => {
      resolve({
        port: server.address().port,
        host,
        clients: () => door.clientCount(),
        close: () =>
          new Promise((res) => {
            try { unsubscribe(); } catch {}
            door.close();
            server.close(() => res());
          }),
      });
    });
  });
}
