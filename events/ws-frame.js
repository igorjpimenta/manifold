// events/ws-frame.js — shared server-side WebSocket framing for manifold's
// loopback event servers (events/serve-ws.js, dashboard/server.js).
//
// One door, two rooms: the handshake accept, server→client text frames, and
// client-frame hygiene (ping→pong, close→destroy, everything else consumed
// and ignored) live here so both servers behave identically. No stream,
// store, or routing coupling — attachWsEndpoint only moves bytes for
// sockets whose upgrade URL matches, and hands each socket to onConnect
// for the server's own hello frames.
import crypto from "node:crypto";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

export function wsAcceptKey(key) {
  return crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
}

// Server → client frames are never masked (RFC 6455 §5.3).
export function encodeWsFrame(text) {
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

// Attach a server→client event door to an existing http server. `match` is
// the server's own path predicate (dashboard strips its /graph prefix
// before matching; serve-ws accepts every upgrade target).
//
// onConnect(socket, req) runs after the 101 response: send hello frames
// there and optionally return a tag stored for broadcast filtering.
// broadcast(obj, visibleTo) encodes once and writes to every open socket
// whose tag passes — untagged sockets always receive.
export function attachWsEndpoint(server, { match, onConnect = null, onMessage = null, logger = null } = {}) {
  if (typeof match !== "function") throw new Error("ws-frame: attachWsEndpoint requires a match(pathname) predicate");
  const clients = new Set();
  const tags = new Map();

  function drop(socket) {
    clients.delete(socket);
    tags.delete(socket);
  }

  function broadcast(obj, visibleTo = null) {
    const frame = encodeWsFrame(JSON.stringify(obj));
    for (const sock of [...clients]) {
      if (sock.destroyed || !sock.writable) {
        drop(sock);
        continue;
      }
      try {
        if (visibleTo && !visibleTo(tags.get(sock) ?? null)) continue;
        sock.write(frame);
      } catch {
        drop(sock);
      }
    }
  }

  // Client frames are only ever interesting as close/ping — unless the
  // endpoint takes messages, in which case complete text messages are
  // delivered to onMessage and everything else is consumed and ignored.
  // Mask bit respected when skipping payloads.
  function wireSocket(socket) {
    let buf = Buffer.alloc(0);
    let textParts = null;
    socket.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (buf.length < 2) return;
        const fin = (buf[0] & 0x80) !== 0;
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
        let mask = null;
        if (buf[1] & 0x80) {
          if (buf.length < off + 4) return;
          mask = buf.slice(off, off + 4);
          off += 4;
        }
        if (buf.length < off + len) return;
        let payload = buf.slice(off, off + len);
        buf = buf.slice(off + len);
        if (mask) {
          const unmasked = Buffer.allocUnsafe(payload.length);
          for (let i = 0; i < payload.length; i++) unmasked[i] = payload[i] ^ mask[i % 4];
          payload = unmasked;
        }
        if (opcode === 0x8) {
          socket.destroy();
          return;
        }
        if (opcode === 0x9) {
          socket.write(Buffer.from([0x8a, 0x00]));
          continue;
        }
        // Text start (0x1) opens a message, continuations (0x0) extend it;
        // anything else (binary, reserved) is consumed and ignored. Without
        // an onMessage taker there is nothing to deliver to.
        if (onMessage && (opcode === 0x1 || (opcode === 0x0 && textParts !== null))) {
          if (opcode === 0x1) textParts = [];
          textParts.push(payload.toString("utf8"));
          if (fin) {
            const text = textParts.join("");
            textParts = null;
            try {
              onMessage(socket, text);
            } catch (err) {
              logger?.warn?.(`ws-frame: onMessage failed: ${err?.message ?? err}`);
            }
          }
          continue;
        }
        textParts = null;
      }
    });
    socket.on("close", () => drop(socket));
    socket.on("error", () => drop(socket));
  }

  function onUpgrade(req, socket) {
    let pathname = "/";
    try {
      pathname = new URL(req.url ?? "/", "http://localhost").pathname;
    } catch { /* keep "/" */ }
    const key = req.headers["sec-websocket-key"];
    if (!match(pathname) || (req.headers.upgrade || "").toLowerCase() !== "websocket" || !key) {
      socket.destroy();
      return;
    }
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\n" +
        "Upgrade: websocket\r\n" +
        "Connection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${wsAcceptKey(key)}\r\n\r\n`
    );
    clients.add(socket);
    wireSocket(socket);
    try {
      const tag = onConnect?.(socket, req);
      if (tag && typeof tag.then === "function") {
        tag.then(
          (t) => { if (t !== undefined) tags.set(socket, t); },
          (err) => logger?.warn?.(`ws-frame: onConnect failed: ${err?.message ?? err}`),
        );
      } else if (tag !== undefined) {
        tags.set(socket, tag);
      }
    } catch (err) {
      logger?.warn?.(`ws-frame: onConnect failed: ${err?.message ?? err}`);
    }
  }

  server.on("upgrade", onUpgrade);

  return {
    broadcast,
    clientCount: () => clients.size,
    close() {
      server.off("upgrade", onUpgrade);
      for (const sock of [...clients]) {
        try { sock.destroy(); } catch {}
      }
      clients.clear();
      tags.clear();
    },
  };
}
