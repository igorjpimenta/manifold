// ws-dispatch.test.js — the WebSocket dispatch door: the request travels
// down one socket and the outcome comes back up the same one.
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { createDispatchHub } from "../../../sidecar/dispatch.js";
import { createWsDispatchDoor } from "../../../sidecar/ws-dispatch.js";

function waitFor(predicate, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const tick = () => {
      if (predicate()) return resolve();
      if (Date.now() > deadline) return reject(new Error("waitFor timed out"));
      setTimeout(tick, 10);
    };
    tick();
  });
}

function connect(port, { token = "svc" } = {}) {
  const frames = [];
  const closed = { value: false };
  const ws = new WebSocket(`ws://127.0.0.1:${port}/manifold/internal/dispatch`, {
    headers: token ? { authorization: `Bearer ${token}` } : undefined,
  });
  ws.addEventListener("message", (e) => frames.push(JSON.parse(e.data)));
  ws.addEventListener("close", () => { closed.value = true; });
  ws.addEventListener("error", () => {});
  return { ws, frames, closed };
}

describe("ws dispatch door", () => {
  let server;
  let port;
  let door;
  let hub;

  before(async () => {
    server = http.createServer((req, res) => {
      res.statusCode = 404;
      res.end();
    });
    door = createWsDispatchDoor({ serviceToken: "svc" });
    hub = createDispatchHub({ publish: (frame) => door.publish(frame), acceptTimeoutMs: 300 });
    door.attach(server, hub);
    await new Promise((r) => server.listen(0, "127.0.0.1", r));
    port = server.address().port;
  });

  after(async () => {
    await new Promise((r) => server.close(r));
  });

  it("carries the request down and the outcome back up", async () => {
    const client = connect(port);
    await waitFor(() => client.ws.readyState === 1);
    const pending = hub.request("run", { sessionKey: "s1" });
    await waitFor(() => client.frames.length === 1);
    const req = client.frames[0];
    assert.equal(req.type, "dispatch.request");
    assert.equal(req.op, "run");
    assert.deepEqual(req.args, { sessionKey: "s1" });
    client.ws.send(JSON.stringify({ type: "dispatch.ack", id: req.id }));
    client.ws.send(JSON.stringify({ type: "dispatch.result", id: req.id, ok: true, result: { runId: "r1" } }));
    assert.deepEqual(await pending, { runId: "r1" });
    client.ws.close();
  });

  it("rejects a failed op with the harness error", async () => {
    const client = connect(port);
    await waitFor(() => client.ws.readyState === 1);
    const pending = hub.request("waitForRun", { runId: "r" });
    await waitFor(() => client.frames.length === 1);
    const req = client.frames[0];
    client.ws.send(JSON.stringify({ type: "dispatch.result", id: req.id, ok: false, error: "harness down" }));
    await assert.rejects(pending, /harness down/);
    client.ws.close();
  });

  it("refuses a socket without the service token", async () => {
    const client = connect(port, { token: null });
    await waitFor(() => client.closed.value);
    assert.equal(client.frames.length, 0);
    assert.equal(door.clientCount(), 0);
  });

  it("replays pending requests to a reconnected client", async () => {
    const first = connect(port);
    await waitFor(() => first.ws.readyState === 1);
    const pending = hub.request("getSessionMessages", { sessionKey: "s2" });
    await waitFor(() => first.frames.length === 1);
    const req = first.frames[0];
    first.ws.close();
    await waitFor(() => door.clientCount() === 0);

    const second = connect(port);
    await waitFor(() => second.frames.length === 1);
    assert.equal(second.frames[0].id, req.id);
    assert.equal(second.frames[0].op, "getSessionMessages");
    second.ws.send(JSON.stringify({ type: "dispatch.result", id: req.id, ok: true, result: { messages: [] } }));
    assert.deepEqual(await pending, { messages: [] });
    second.ws.close();
  });
});
