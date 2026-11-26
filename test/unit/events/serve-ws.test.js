import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { createEventStream } from "../../../events/stream.js";
import { startWsServer } from "../../../events/serve-ws.js";

let srv, sockets, stream;

beforeEach(() => {
  stream = createEventStream();
  sockets = [];
});
afterEach(async () => {
  for (const s of sockets) {
    try { s.close(); } catch {}
  }
  if (srv) await srv.close();
  srv = undefined;
});

function seed() {
  return stream.append({ event: "agent.dispatched", flow: { id: "F1", nodeId: "n1" }, session: { harness: "h", agent: "a", session: "k" } });
}

function connect(port) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    sockets.push(ws);
    ws.inbox = [];
    ws.onmessage = (e) => {
      ws.inbox.push(JSON.parse(String(e.data)));
    };
    const timer = setTimeout(() => reject(new Error("ws open timed out")), 5000);
    ws.onopen = () => {
      clearTimeout(timer);
      resolve(ws);
    };
    ws.onerror = (e) => {
      clearTimeout(timer);
      reject(new Error(`ws error: ${e?.message ?? "unknown"}`));
    };
  });
}

function collect(ws, count, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (ws.inbox.length >= count) {
        clearInterval(timer);
        resolve(ws.inbox.splice(0, count));
      } else if (Date.now() - started > timeoutMs) {
        clearInterval(timer);
        reject(new Error(`timed out waiting for ${count} frames, got ${ws.inbox.length}`));
      }
    }, 10);
  });
}

test("subscriber gets welcome + history, then live entries verbatim", async () => {
  seed();
  srv = await startWsServer({ stream, port: 0 });
  const ws = await connect(srv.port);
  const [welcome, history] = await collect(ws, 2);
  assert.equal(welcome.type, "welcome");
  assert.equal(typeof welcome.lastEventId, "number");
  assert.equal(history.type, "history");
  assert.equal(history.events.length, 1);
  assert.equal(history.events[0].event, "agent.dispatched");

  seed();
  const [live] = await collect(ws, 1);
  assert.equal(live.type, "event");
  assert.equal(live.event.event, "agent.dispatched");
  assert.equal(live.event.flow.id, "F1");
});

test("every connected client receives the same broadcast", async () => {
  seed();
  srv = await startWsServer({ stream, port: 0 });
  const a = await connect(srv.port);
  const b = await connect(srv.port);
  await collect(a, 2);
  await collect(b, 2);
  seed();
  const [liveA] = await collect(a, 1);
  const [liveB] = await collect(b, 1);
  assert.deepEqual(liveA, liveB);
  assert.equal(srv.clients(), 2);
});

test("plain HTTP hits get 404, not a hijacked upgrade", async () => {
  srv = await startWsServer({ stream, port: 0 });
  const status = await new Promise((resolve, reject) => {
    http.get(`http://127.0.0.1:${srv.port}/`, (res) => {
      res.resume();
      resolve(res.statusCode);
    }).on("error", reject);
  });
  assert.equal(status, 404);
});

test("empty stream: welcome id 0, empty history, still streams later entries", async () => {
  srv = await startWsServer({ stream, port: 0 });
  const ws = await connect(srv.port);
  const [welcome, history] = await collect(ws, 2);
  assert.equal(welcome.lastEventId, 0);
  assert.deepEqual(history.events, []);
  seed();
  const [live] = await collect(ws, 1);
  assert.equal(live.event.flow.id, "F1");
});

test("requires a stream (subscribe + history)", () => {
  assert.throws(() => startWsServer({}), /requires an events\/stream/);
});
