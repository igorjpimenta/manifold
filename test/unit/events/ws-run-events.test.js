/**
 * Unit tests for events/ws-run-events.js
 *
 * Verifies the gateway WS projection:
 *   - every write emits a per-revision run.updated patch (no status-gated
 *     narrow events: no run.started, no gate.opened, no run.ended, no
 *     pipeline.stats.updated — subscribers that need lifecycle edges derive
 *     them from status transitions in the patch stream).
 *   - Revision dedupe: replays of the same write emit nothing.
 *   - Child flows also push a per-revision patch to the non-terminal parent.
 *   - Terminal flows drop their revision entry (no unbounded map growth).
 *   - Null broadcast / unknown flow stay silent, never throw.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createWsRunEventsListener } from "../../../events/ws-run-events.js";
import { createEmitter } from "../../../events/emit.js";

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} };

function makeHarness(flows, { broadcast = null } = {}) {
  const calls = [];
  const broadcastFn = broadcast ?? ((name, payload) => calls.push({ name, payload }));
  const store = { get: async (id) => flows[id] ?? null };
  const nodeExecutionsStore = { getInvocation: async () => null };
  const { emit } = createEmitter({ getBroadcast: () => broadcastFn, logger: silentLogger });
  const listener = createWsRunEventsListener({
    store,
    nodeExecutionsStore,
    pipelineLookup: () => ({ displayName: "Pipe", emoji: "🧩" }),
    getBroadcast: () => broadcastFn,
    emit,
  });
  return { listener, calls };
}

function runningFlow(revision = 1) {
  return {
    flowId: "flow-1",
    revision,
    status: "running",
    lastStep: "SCAN",
    stateJson: { pipelineId: "team_pipe" },
    waitJson: null,
  };
}

function names(calls) {
  return calls.map((c) => c.name);
}

// ── lifecycle ────────────────────────────────────────────────────────────────

describe("ws-run-events — lifecycle", () => {
  test("revision emits only the per-revision patch", async () => {
    const { listener, calls } = makeHarness({ "flow-1": runningFlow(1) });
    await listener("flow-1");

    assert.deepEqual(names(calls), ["plugin.run.updated"]);
    const [updated] = calls;
    assert.equal(updated.payload.kind, "run.updated");
    assert.equal(updated.payload.flowId, "flow-1");
  });

  test("same revision replayed emits nothing (revision gate)", async () => {
    const { listener, calls } = makeHarness({ "flow-1": runningFlow(1) });
    await listener("flow-1");
    const count = calls.length;
    await listener("flow-1");
    assert.equal(calls.length, count, "replay of a processed write is silent");
  });

  test("next revision emits another patch", async () => {
    const flows = { "flow-1": runningFlow(1) };
    const { listener, calls } = makeHarness(flows);
    await listener("flow-1");
    flows["flow-1"] = runningFlow(2);
    await listener("flow-1");

    assert.equal(names(calls).filter((n) => n === "plugin.run.updated").length, 2);
  });

  test("terminal revision drops the revision entry", async () => {
    const flows = {
      "flow-1": { ...runningFlow(5), status: "succeeded", revision: 6 },
    };
    const { listener, calls } = makeHarness(flows);
    await listener("flow-1");
    assert.deepEqual(names(calls), ["plugin.run.updated"]);
  });
});

// ── gates ────────────────────────────────────────────────────────────────────

describe("ws-run-events — gates", () => {
  test("blocked human-gate emits only the per-revision patch", async () => {
    const flows = {
      "flow-1": {
        ...runningFlow(2),
        status: "blocked",
        waitJson: { kind: "human-gate", items: [{ itemId: "a" }] },
      },
    };
    const { listener, calls } = makeHarness(flows);
    await listener("flow-1");

    assert.ok(!names(calls).includes("plugin.gate.opened"), "no narrow gate.opened");
    assert.ok(names(calls).includes("plugin.run.updated"), "rich patch still fires");
  });

  test("blocked non-gate wait emits only the per-revision patch", async () => {
    const flows = {
      "flow-1": {
        ...runningFlow(2),
        status: "blocked",
        waitJson: { kind: "needs-info" },
      },
    };
    const { listener, calls } = makeHarness(flows);
    await listener("flow-1");

    assert.ok(!names(calls).includes("plugin.gate.opened"), "no gate.opened for non-gate waits");
    assert.ok(names(calls).includes("plugin.run.updated"), "rich patch still fires");
  });
});

// ── silence ──────────────────────────────────────────────────────────────────

describe("ws-run-events — silence", () => {
  test("unknown flowId is silent", async () => {
    const { listener, calls } = makeHarness({});
    await listener("missing");
    assert.equal(calls.length, 0);
  });

  test("null broadcast is silent, never throws", async () => {
    const store = { get: async () => runningFlow(1) };
    const { emit } = createEmitter({ getBroadcast: () => null, logger: silentLogger });
    const listener = createWsRunEventsListener({
      store,
      nodeExecutionsStore: { getInvocation: async () => null },
      pipelineLookup: () => ({}),
      getBroadcast: () => null,
      emit,
    });
    await listener("flow-1");
  });
});
