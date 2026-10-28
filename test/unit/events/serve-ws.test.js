// serve-ws.test.js — harness presence: welcome, history replay, the
// connect-time open-flows snapshot, then live frames filtered per
// harness. Real server + real sockets on an ephemeral port. Uses the
// global WebSocket client (Node 24), the same transport winch's
// ws.test.js uses.
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
  return stream.append({ event: "agent.dispatched", flow: { id: "F1", nodeId: "n1" }, session: { harness: "h", agent: "a", sessionKey: "k" } });
}

// The inbox is attached at connect time and never replaced: with several
// clients racing the same broadcast, attaching a fresh onmessage per
// collect() call drops frames that land between collects.
function connect(port, qs = "/") {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}${qs}`);
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
  assert.throws(() => startWsServer({ stream, getOpenFlows: 42 }), /getOpenFlows must be a function/);
});

test("harness-scoped connect gets filtered history plus the open-flows snapshot", async () => {
  stream.append({ event: "agent.dispatched", session: { harness: "h", agent: "a", sessionKey: "k" } });
  stream.append({ event: "agent.dispatched", session: { harness: "other", agent: "a", sessionKey: "k" } });
  const openFlows = [{ id: "F1", pipelineId: "p", nodeId: "n1", status: "running", harness: "h" }];
  srv = await startWsServer({
    stream,
    port: 0,
    getOpenFlows: async (harness) => {
      assert.equal(harness, "h");
      return openFlows;
    },
  });
  const ws = await connect(srv.port, "/?harness=h");
  const [welcome, history, snapshot] = await collect(ws, 3);
  assert.equal(welcome.type, "welcome");
  assert.equal(history.type, "history");
  assert.deepEqual(history.events.map((e) => e.session.harness), ["h"]);
  assert.equal(snapshot.type, "open-flows");
  assert.equal(snapshot.harness, "h");
  assert.deepEqual(snapshot.flows, openFlows);
});

test("live frames filter per harness; untagged entries go everywhere", async () => {
  srv = await startWsServer({ stream, port: 0, getOpenFlows: async () => [] });
  const a = await connect(srv.port, "/?harness=h");
  const b = await connect(srv.port, "/?harness=other");
  await collect(a, 3);
  await collect(b, 3);
  stream.append({ event: "agent.dispatched", session: { harness: "h" } });
  const [liveA] = await collect(a, 1);
  assert.equal(liveA.event.session.harness, "h");
  await new Promise((r) => setTimeout(r, 100));
  assert.deepEqual(b.inbox, []);
  stream.append({ event: "flow.changed", session: { harness: null } });
  const [liveA2] = await collect(a, 1);
  const [liveB2] = await collect(b, 1);
  assert.equal(liveA2.event.event, "flow.changed");
  assert.equal(liveB2.event.event, "flow.changed");
});

test("open-flows snapshot failure degrades to an empty list", async () => {
  srv = await startWsServer({
    stream,
    port: 0,
    getOpenFlows: async () => { throw new Error("db down"); },
  });
  const ws = await connect(srv.port);
  const [welcome, history, snapshot] = await collect(ws, 3);
  assert.equal(welcome.type, "welcome");
  assert.equal(history.type, "history");
  assert.equal(snapshot.type, "open-flows");
  assert.equal(snapshot.harness, null);
  assert.deepEqual(snapshot.flows, []);
});
