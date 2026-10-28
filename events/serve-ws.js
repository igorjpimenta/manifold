// serve-ws.js — open-flows sync over WebSocket. Tails the in-process
// event queue (events/stream.js) and, on every connect, first replays the
// backlog and then sends the currently-open flows for that harness —
// sync, not revival. No interpretation of what entries mean beyond
// the harness tag: untagged entries are instance-wide and go to every
// subscriber; tagged entries go only to their harness (or to unscoped
// subscribers). The dashboard SSE path is untouched — this is the door
// for harness subscribers.
//
// Wire protocol (JSON text frames, server → client only):
//   {"type":"welcome","lastEventId":N}    on connect (history length)
//   {"type":"history","events":[...]}     backlog, oldest-first, capped, harness-filtered
//   {"type":"open-flows","harness":H,"flows":[...]}  connect-time snapshot (when getOpenFlows is wired)
//   {"type":"event","event":{...entry...}} one per appended entry, in order, harness-filtered
// The subscriber declares its harness with ?harness=H on the upgrade
// request; no parameter keeps the legacy verbatim feed. Clients track
// their own seen state: reconnecting replays history, so a missed frame
// costs a re-read, never a lost event. No auth in-process — bind
// localhost, or put a bearer-checked reverse proxy in front.

import http from "node:http";
import crypto from "node:crypto";
import { filterEntries, visibleTo } from "./open-flows.js";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
export const DEFAULT_WS_PORT = 38294;
export const HISTORY_LIMIT = 200;

function acceptKey(key) {
  return crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
}

// Server → client frames are never masked (RFC 6455 §5.3).
function encodeFrame(text) {
  const payload = Buffer.from(text, "utf8");
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.from([0x81, len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

export function startWsServer({ stream, port = null, host = "127.0.0.1", historyLimit = null, logger, getOpenFlows } = {}) {
  if (!stream || typeof stream.subscribe !== "function" || typeof stream.history !== "function") {
    throw new Error("manifold: serve-ws requires an events/stream.js stream (subscribe + history)");
  }
  if (getOpenFlows !== undefined && getOpenFlows !== null && typeof getOpenFlows !== "function") {
    throw new Error("manifold: serve-ws getOpenFlows must be a function(harness) when provided");
  }
  const listenPort = port ?? DEFAULT_WS_PORT;
  const histLimit = historyLimit ?? HISTORY_LIMIT;
  // socket -> harness (string) or null for the legacy unscoped feed.
  const clients = new Map();

  // The subscriber declares its harness on the upgrade URL
  // (ws://host:port/?harness=openclaw); anything else stays unscoped.
  function harnessOf(req) {
    try {
      const h = new URL(req.url ?? "/", "http://localhost").searchParams.get("harness");
      const trimmed = typeof h === "string" ? h.trim() : "";
      return trimmed !== "" ? trimmed : null;
    } catch {
      return null;
    }
  }

  function snapshot(harness) {
    return filterEntries(stream.history({ limit: histLimit }), harness);
  }

  function broadcast(obj, entry = null) {
    const frame = encodeFrame(JSON.stringify(obj));
    for (const [sock, harness] of [...clients]) {
      if (sock.destroyed || !sock.writable) {
        clients.delete(sock);
        continue;
      }
      if (entry && !visibleTo(entry, harness)) continue;
      sock.write(frame);
    }
  }

  const server = http.createServer((req, res) => {
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("manifold events: websocket only — connect with an Upgrade request\n");
  });

  server.on("upgrade", async (req, socket) => {
    const key = req.headers["sec-websocket-key"];
    if ((req.headers.upgrade || "").toLowerCase() !== "websocket" || !key) {
      socket.destroy();
      return;
    }
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${acceptKey(key)}\r\n\r\n`
    );
    clients.set(socket, harnessOf(req));
    const clientHarness = clients.get(socket);
    logger?.info?.(`manifold: ws subscriber connected (${clients.size} total${clientHarness ? `, harness=${clientHarness}` : ""})`);
    try {
      const history = snapshot(clientHarness);
      socket.write(encodeFrame(JSON.stringify({ type: "welcome", lastEventId: history.length })));
      socket.write(encodeFrame(JSON.stringify({ type: "history", events: history })));
      // The open-flows snapshot lands right after history: the subscriber
      // learns the open set before the first live frame moves it.
      if (typeof getOpenFlows === "function") {
        const flows = await getOpenFlows(clientHarness).catch((err) => {
          logger?.warn?.(`manifold: ws open-flows snapshot failed: ${err?.message ?? err}`);
          return [];
        });
        socket.write(encodeFrame(JSON.stringify({ type: "open-flows", harness: clientHarness, flows: Array.isArray(flows) ? flows : [] })));
      }
    } catch {
      clients.delete(socket);
    }

    // Client frames are only ever interesting as close/ping; everything
    // else is consumed and ignored. Mask bit respected when skipping
    // payloads.
    let buf = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (buf.length < 2) return;
        const opcode = buf[0] & 0x0f;
        let len = buf[1] & 0x7f;
        let off = 2;
        if (len === 126) {
          if (buf.length < 4) return;
          len = buf.readUInt16BE(2);
          off = 4;
        } else if (len === 127) {
          if (buf.length < 10) return;
          if (buf.readUInt32BE(2) !== 0) {
            socket.destroy();
            return;
          }
          len = buf.readUInt32BE(6);
          off = 10;
        }
        if (buf[1] & 0x80) off += 4;
        if (buf.length < off + len) return;
        buf = buf.slice(off + len);
        if (opcode === 0x8) {
          socket.destroy();
          return;
        }
        if (opcode === 0x9) socket.write(Buffer.from([0x8a, 0x00]));
      }
    });
    const drop = () => clients.delete(socket);
    socket.on("close", drop);
    socket.on("error", drop);
  });

  const unsubscribe = stream.subscribe((entry) => {
    try {
      broadcast({ type: "event", event: entry }, entry);
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
        clients: () => clients.size,
        close: () =>
          new Promise((res) => {
            try { unsubscribe(); } catch {}
            for (const sock of clients.keys()) sock.destroy();
            server.close(() => res());
          }),
      });
    });
  });
}
