// runner.test.js — dispatch op mapping + the ws round-trip: request down,
// ack and result back up on the same socket.
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { attachWsEndpoint, encodeWsFrame } from "../../../events/ws-frame.js";
import { executeDispatchOp, startDispatchRunner } from "../../../companion/runner.js";

describe("executeDispatchOp", () => {
  it("calls the matching subagent method with args", async () => {
    const calls = [];
    const subagent = {
      run: async (a) => (calls.push(["run", a]), { runId: "r" }),
      waitForRun: async (a) => (calls.push(["wait", a]), { status: "done" }),
      getSessionMessages: async (a) => (calls.push(["msgs", a]), { messages: [] }),
    };
    assert.deepEqual(await executeDispatchOp({ subagent, op: "run", args: { a: 1 } }), { runId: "r" });
    assert.deepEqual(await executeDispatchOp({ subagent, op: "waitForRun", args: {} }), { status: "done" });
    assert.deepEqual(await executeDispatchOp({ subagent, op: "getSessionMessages", args: {} }), { messages: [] });
    assert.equal(calls.length, 3);
  });
  it("rejects unknown ops and missing runtimes", async () => {
    await assert.rejects(executeDispatchOp({ subagent: {}, op: "nope", args: {} }), /unknown op/);
    await assert.rejects(executeDispatchOp({ subagent: {}, op: "run", args: {} }), /unavailable/);
  });
});

describe("startDispatchRunner", () => {
  let server;
  let port;
  let acked = [];
  let results = [];
  let request = null;

  before(async () => {
    server = http.createServer((req, res) => {
      res.statusCode = 404;
      res.end();
    });
    // The sidecar's door, reduced to what the runner sees: one socket, the
    // request frame down, the ack and the result back up.
    attachWsEndpoint(server, {
      match: (pathname) => pathname === "/manifold/internal/dispatch",
      onConnect: (socket) => {
        socket.write(encodeWsFrame(JSON.stringify({ type: "dispatch.request", id: "d1", op: "run", args: { sessionKey: "s" } })));
      },
      onMessage: (socket, text) => {
        const frame = JSON.parse(text);
        if (frame.type === "dispatch.ack") acked.push(frame);
        if (frame.type === "dispatch.result") results.push(frame);
      },
    });
    await new Promise((r) => server.listen(0, "127.0.0.1", r));
    port = server.address().port;
  });

  after(async () => {
    await new Promise((r) => server.close(r));
  });

  it("acks, executes via harness runtime, fulfills", async () => {
    const api = { runtime: { subagent: { run: async (args) => ({ runId: "r9", echo: args.sessionKey }) } } };
    const sub = startDispatchRunner({ api, sidecarUrl: `http://127.0.0.1:${port}`, serviceToken: "svc", logger: null });
    const deadline = Date.now() + 5000;
    while (results.length === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 25));
    sub.stop();
    assert.deepEqual(acked, [{ type: "dispatch.ack", id: "d1" }]);
    assert.equal(results.length, 1);
    assert.equal(results[0].id, "d1");
    assert.equal(results[0].ok, true);
    assert.deepEqual(results[0].result, { runId: "r9", echo: "s" });
  });

  it("fulfills errors when the harness runtime throws", async () => {
    acked = [];
    results = [];
    const api = { runtime: { subagent: { run: async () => { throw new Error("harness down"); } } } };
    const sub = startDispatchRunner({ api, sidecarUrl: `http://127.0.0.1:${port}`, serviceToken: "svc", logger: null });
    const deadline = Date.now() + 5000;
    while (results.length === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 25));
    sub.stop();
    assert.equal(results[0].ok, false);
    assert.match(results[0].error, /harness down/);
  });
});
