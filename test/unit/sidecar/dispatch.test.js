// dispatch.test.js — sidecar dispatch hub: request/ack/fulfill lifecycle.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createDispatchHub, createRemoteSubagent, DISPATCH_OPS } from "../../../sidecar/dispatch.js";

function hub({ acceptTimeoutMs = 50, opTimeoutMs = 500 } = {}) {
  const published = [];
  const h = createDispatchHub({ publish: (ch, ev) => published.push({ ch, ev }), acceptTimeoutMs, opTimeoutMs });
  return { h, published };
}

describe("createDispatchHub", () => {
  it("publishes dispatch.request and resolves on fulfill", async () => {
    const { h, published } = hub();
    const p = h.request("run", { sessionKey: "s" });
    assert.equal(published.length, 1);
    assert.equal(published[0].ch, "dispatch");
    assert.equal(published[0].ev.type, "dispatch.request");
    assert.equal(published[0].ev.op, "run");
    const id = published[0].ev.id;
    assert.equal(h.ack(id), true);
    assert.equal(h.fulfill(id, { ok: true, result: { runId: "r1" } }), true);
    assert.deepEqual(await p, { runId: "r1" });
    assert.equal(h.pendingCount(), 0);
  });
  it("rejects fulfillment errors", async () => {
    const { h, published } = hub();
    const p = h.request("run", {});
    const id = published[0].ev.id;
    h.ack(id);
    h.fulfill(id, { ok: false, error: "boom" });
    await assert.rejects(p, /boom/);
  });
  it("fails fast without ack (no harness)", async () => {
    const { h } = hub({ acceptTimeoutMs: 20 });
    await assert.rejects(h.request("run", {}), /no harness subscribed/);
  });
  it("rejects unknown ops and unknown fulfills", async () => {
    const { h } = hub();
    await assert.rejects(h.request("nope", {}), /unknown op/);
    assert.equal(h.fulfill("missing", { ok: true }), false);
    assert.equal(h.ack("missing"), false);
  });
  it("remote subagent exposes the three gated methods", () => {
    assert.deepEqual([...DISPATCH_OPS].sort(), ["getSessionMessages", "run", "waitForRun"]);
    const { h } = hub();
    const sub = createRemoteSubagent({ dispatchHub: h });
    assert.equal(typeof sub.run, "function");
    assert.equal(typeof sub.waitForRun, "function");
    assert.equal(typeof sub.getSessionMessages, "function");
  });
});
