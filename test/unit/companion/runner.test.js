// runner.test.js — dispatch op mapping + ack/execute/fulfill round-trip.
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
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
  before(async () => {
    server = http.createServer((req, res) => {
      if (req.url === "/manifold/internal/dispatch" && req.method === "GET") {
        res.writeHead(200, { "Content-Type": "text/event-stream" });
        res.write(`data: ${JSON.stringify({ type: "dispatch.request", id: "d1", op: "run", args: { sessionKey: "s" } })}\n\n`);
        return; // hold open
      }
      let raw = "";
      req.on("data", (c) => { raw += c; });
      req.on("end", () => {
        if (req.url === "/manifold/internal/dispatch/ack") acked.push(JSON.parse(raw));
        if (req.url === "/manifold/internal/dispatch/result") results.push(JSON.parse(raw));
        res.end("{}");
      });
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
    assert.deepEqual(acked, [{ id: "d1" }]);
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
