/**
 * Unit tests for events/emit.js
 *
 * Verifies (mirroring test/unit/telemetry/broadcast.test.js conventions):
 *   - WS delivery uses the plugin. prefix + legacy wire name.
 *   - SSE delivery only for session/discuss kinds, with legacy type.
 *   - Envelope stamped (kind, emittedAtMs); legacy `type` dropped.
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

function makeSse(result = { ok: true }) {
  const calls = [];
  const fn = async (event) => {
    calls.push(event);
    return result;
  };
  fn.calls = calls;
  return fn;
}

// ── WS delivery ──────────────────────────────────────────────────────────────

describe("emit — gateway WS", () => {
  test("broadcasts plugin. + legacy name with stamped envelope", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    const res = await emit("gate.resolved", { flowId: "f", itemId: "i", decision: "approve" });

    assert.equal(res.ok, true);
    assert.deepEqual(res.deliveries, { ws: "sent", sse: "skipped" });
    assert.equal(broadcast.calls.length, 1);
    assert.equal(broadcast.calls[0].name, "plugin.gate.resolved");
    const payload = broadcast.calls[0].payload;
    assert.equal(payload.kind, "gate.resolved");
    assert.equal(typeof payload.emittedAtMs, "number");
    assert.equal(payload.flowId, "f");
  });

  test("session kind uses legacy discuss.turn wire name", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    await emit("session.transcript.completed", { scope: "node", nodeId: "n", text: "t" });
    assert.equal(broadcast.calls[0].name, "plugin.discuss.turn");
  });

  test("dashboard-only kind skips WS without error", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    const res = await emit("run.created", { flowId: "f" });
    assert.equal(res.ok, true);
    assert.deepEqual(res.deliveries, { ws: "skipped", sse: "skipped" });
    assert.equal(broadcast.calls.length, 0);
  });
});

// ── SSE delivery ─────────────────────────────────────────────────────────────

describe("emit — SSE", () => {
  test("session/discuss kinds reach emitSseEvent with legacy type", async () => {
    const sse = makeSse();
    const { emit } = createEmitter({ getBroadcast: () => null, emitSseEvent: sse });
    const res = await emit("discuss.pending.updated", { flowId: "f", itemId: "i" });

    assert.equal(res.ok, true);
    assert.deepEqual(res.deliveries, { ws: "skipped", sse: "sent" });
    assert.equal(sse.calls.length, 1);
    assert.equal(sse.calls[0].type, "discuss.pending");
    assert.equal(sse.calls[0].kind, "discuss.pending.updated");
  });

  test("non-session/discuss kinds skip SSE", async () => {
    const sse = makeSse();
    const { emit } = createEmitter({ getBroadcast: () => null, emitSseEvent: sse });
    await emit("gate.opened", { flowId: "f" });
    assert.equal(sse.calls.length, 0);
  });

  test("legacy incoming type is dropped in favor of kind", async () => {
    const broadcast = makeBroadcast();
    const { emit } = createEmitter({ getBroadcast: () => broadcast });
    await emit("gate.opened", { flowId: "f", type: "gate.opened" });
    assert.equal(broadcast.calls[0].payload.type, undefined);
    assert.equal(broadcast.calls[0].payload.kind, "gate.opened");
  });
});

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

  test("failing SSE reports error, never rejects", async () => {
    const { emit } = createEmitter({
      getBroadcast: () => null,
      emitSseEvent: async () => ({ ok: false, error: "no_gateway_token" }),
    });
    const res = await emit("discuss.pending.updated", { flowId: "f", itemId: "i" });
    assert.equal(res.ok, false);
    assert.equal(res.deliveries.sse, "error");
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
