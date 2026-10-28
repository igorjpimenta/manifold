// ws-frame.test.js — shared WS framing: the RFC 6455 accept vector,
// frame length tiers, path matching, per-socket filtering, and close.
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import { wsAcceptKey, encodeWsFrame, attachWsEndpoint } from "../../../events/ws-frame.js";

test("wsAcceptKey matches the RFC 6455 example vector", () => {
  assert.equal(wsAcceptKey("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
});

test("encodeWsFrame tiers by payload length", () => {
  assert.equal(encodeWsFrame("hi")[0], 0x81);
  assert.equal(encodeWsFrame("hi")[1], 2);
  const mid = encodeWsFrame("x".repeat(300));
  assert.equal(mid[1], 126);
  assert.equal(mid.readUInt16BE(2), 300);
  const big = encodeWsFrame("x".repeat(70000));
  assert.equal(big[1], 127);
});

function freeServer() {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => { res.end("http"); });
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

function rawUpgrade(port, path) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const s = net.connect(port, "127.0.0.1", () => {
      s.write(
        `GET ${path} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
      );
    });
    s.on("data", (d) => { chunks.push(d); s.destroy(); resolve(Buffer.concat(chunks).toString()); });
    s.on("error", reject);
    setTimeout(() => reject(new Error("upgrade timed out")), 5000);
  });
}

test("attachWsEndpoint accepts the matched path and destroys the rest", async () => {
  const server = await freeServer();
  const port = server.address().port;
  const door = attachWsEndpoint(server, { match: (p) => p === "/ws" });
  try {
    const ok = await rawUpgrade(port, "/ws");
    assert.match(ok, /^HTTP\/1\.1 101/);
    assert.equal(door.clientCount(), 1);
    await new Promise((resolve) => {
      const s = net.connect(port, "127.0.0.1", () => {
        s.write("GET /nope HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n");
      });
      s.on("close", resolve);
      s.on("error", () => {});
      setTimeout(resolve, 1000);
    });
    assert.equal(door.clientCount(), 1);
  } finally {
    door.close();
    await new Promise((r) => server.close(r));
  }
});

test("broadcast fans out to visible sockets only, close drains", async () => {
  const server = await freeServer();
  const port = server.address().port;
  const seen = [];
  let n = 0;
  const door = attachWsEndpoint(server, {
    match: () => true,
    onConnect: (socket) => {
      socket.write(encodeWsFrame(JSON.stringify({ type: "hello" })));
      n += 1;
      return n === 1 ? "a" : "b";
    },
  });
  const connect = () => new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    ws.onmessage = (e) => seen.push(JSON.parse(String(e.data)));
    ws.onopen = () => resolve(ws);
    ws.onerror = (e) => reject(new Error(`ws error: ${e?.message ?? "unknown"}`));
  });
  const a = await connect();
  const b = await connect();
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(door.clientCount(), 2);
  door.broadcast({ type: "ping", n: 1 }, (tag) => tag === "a");
  await new Promise((r) => setTimeout(r, 200));
  const pings = seen.filter((m) => m.type === "ping");
  assert.equal(pings.length, 1);
  a.close();
  b.close();
  door.close();
  assert.equal(door.clientCount(), 0);
  await new Promise((r) => server.close(r));
});
