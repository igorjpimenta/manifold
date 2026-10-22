/**
 * Unit tests for events/emit.js
 *
 * Verifies (mirroring test/unit/telemetry/broadcast.test.js conventions):
 *   - WS delivery uses the plugin. prefix + kind as the wire name.
 *   - Envelope stamped (kind, emittedAtMs); stray `type` dropped.
 *   - Negative guards: getter undefined/null/throws → never rejects.
 *   - Validation faults reject (unknown kind, missing fields).
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createEmitter } from "../../../events/emit.js";

function makeBroadcast() {
  const calls = [];
  const fn = (name, payload) => calls.push({ name, payload });
  fn.calls = calls;
  return fn;
}

// ── WS delivery ──────────────────────────────────────────────────────────────

describe("emit — gateway WS", () => {
  test("broadcasts plugin. + kind with stamped envelope", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    const res = await emit("gate.resolved", { flowId: "f", itemId: "i", decision: "approve" });

    assert.equal(res.ok, true);
    assert.deepEqual(res.deliveries, { ws: "sent" });
    assert.equal(broadcast.calls.length, 1);
    assert.equal(broadcast.calls[0].name, "plugin.gate.resolved");
    const payload = broadcast.calls[0].payload;
    assert.equal(payload.kind, "gate.resolved");
    assert.equal(typeof payload.emittedAtMs, "number");
    assert.equal(payload.flowId, "f");
  });

  test("wire name is the kind itself — no legacy mapping", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    await emit("session.transcript.completed", { scope: "node", nodeId: "n", text: "t" });
    assert.equal(broadcast.calls[0].name, "plugin.session.transcript.completed");
    await emit("budget.queue.updated", { decision: "approve", count: 1 });
    assert.equal(broadcast.calls[1].name, "plugin.budget.queue.updated");
  });

  test("dashboard-only kind skips WS without error", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    const res = await emit("run.created", { flowId: "f" });
    assert.equal(res.ok, true);
    assert.deepEqual(res.deliveries, { ws: "skipped" });
    assert.equal(broadcast.calls.length, 0);
  });

  test("stray incoming type is dropped in favor of kind", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    await emit("gate.opened", { flowId: "f", type: "gate.opened" });
    assert.equal(broadcast.calls[0].payload.type, undefined);
    assert.equal(broadcast.calls[0].payload.kind, "gate.opened");
  });
});

// ── negative guards ──────────────────────────────────────────────────────────

// ── negative guards ──────────────────────────────────────────────────────────

describe("emit — negative guards", () => {
  test("missing getter never rejects", async () => {
    const { emit } = createEmitter({});
    await assert.doesNotReject(emit("gate.opened", { flowId: "f" }));
  });

  test("null broadcast never rejects", async () => {
    const { emit } = createEmitter({ getBroadcast: () => null });
    const res = await emit("gate.opened", { flowId: "f" });
    assert.equal(res.ok, true);
    assert.equal(res.deliveries.ws, "skipped");
  });

  test("throwing getter and broadcast report error, never reject", async () => {
    const { emit: e1 } = createEmitter({ getBroadcast: () => { throw new Error("gone"); } });
    const r1 = await e1("gate.opened", { flowId: "f" });
    assert.equal(r1.ok, false);
    assert.equal(r1.deliveries.ws, "error");

    const { emit: e2 } = createEmitter({
      getBroadcast: () => () => { throw new Error("dropped"); },
    });
    const r2 = await e2("gate.opened", { flowId: "f" });
    assert.equal(r2.ok, false);
  });
});

// ── validation faults ────────────────────────────────────────────────────────

describe("emit — validation", () => {
  test("unknown and reserved kinds reject", async () => {
    const { emit } = createEmitter({});
    await assert.rejects(emit("nope", {}), /unknown event kind/);
    await assert.rejects(emit("gate.dissolved", {}), /reserved/);
  });

  test("missing fields reject", async () => {
    const { emit } = createEmitter({});
    await assert.rejects(emit("gate.opened", {}), /flowId/);
  });
});
