/**
 * Unit tests for events/ws-run-events.js
 *
 * Verifies (previously zero coverage — the gateway WS projection):
 *   - running → run.started (once per status) + run.updated (per revision).
 *   - Revision dedupe: replays of the same write emit nothing.
 *   - blocked human-gate → thin gate.opened; other waits emit none.
 *   - terminal → run.ended; stats projection follows via timer.
 *   - All egress through canonical kinds with legacy wire names.
 *   - Null broadcast / unknown flow stay silent, never throw.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createWsRunEventsListener } from "../../../events/ws-run-events.js";
import { createEmitter } from "../../../events/emit.js";

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} };

function makeHarness(flows, { broadcast = null, stats = {} } = {}) {
  const calls = [];
  const broadcastFn = broadcast ?? ((name, payload) => calls.push({ name, payload }));
  const store = { get: async (id) => flows[id] ?? null };
  const readStore = { statsFor: async () => ({ ...stats }) };
  const nodeExecutionsStore = { getInvocation: async () => null };
  const { emit } = createEmitter({ getBroadcast: () => broadcastFn, logger: silentLogger });
  const listener = createWsRunEventsListener({
    store,
    readStore,
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
  test("running revision emits started once + updated with canonical kinds", async () => {
    const { listener, calls } = makeHarness({ "flow-1": runningFlow(1) });
    await listener("flow-1");

    assert.ok(names(calls).includes("plugin.pipeclaw.run.started"), "started fires");
    assert.ok(names(calls).includes("plugin.pipeclaw.run.updated"), "updated fires");
    for (const c of calls) {
      assert.ok(c.payload.kind, "envelope stamps kind");
    }
    const started = calls.find((c) => c.name === "plugin.pipeclaw.run.started");
    assert.equal(started.payload.kind, "run.started");
    assert.equal(started.payload.flowId, "flow-1");
  });

  test("same revision replayed emits nothing (revision gate)", async () => {
    const { listener, calls } = makeHarness({ "flow-1": runningFlow(1) });
    await listener("flow-1");
    const count = calls.length;
    await listener("flow-1");
    assert.equal(calls.length, count, "replay of a processed write is silent");
  });

  test("next revision emits updated only, not a second started", async () => {
    const flows = { "flow-1": runningFlow(1) };
    const { listener, calls } = makeHarness(flows);
    await listener("flow-1");
    flows["flow-1"] = runningFlow(2);
    await listener("flow-1");

    assert.equal(names(calls).filter((n) => n === "plugin.pipeclaw.run.started").length, 1);
    assert.equal(names(calls).filter((n) => n === "plugin.pipeclaw.run.updated").length, 2);
  });

  test("terminal emits ended + stats projection", async () => {
    const flows = {
      "flow-1": { ...runningFlow(5), status: "succeeded", revision: 6 },
    };
    const { listener, calls } = makeHarness(flows, { stats: { todayActive: 3 } });
    await listener("flow-1");
    await new Promise((r) => setTimeout(r, 250));

    const ended = calls.find((c) => c.name === "plugin.pipeclaw.run.ended");
    assert.ok(ended, "run.ended fires");
    assert.equal(ended.payload.kind, "run.ended");
    assert.equal(ended.payload.status, "succeeded");
    const stats = calls.find((c) => c.name === "plugin.pipeclaw.pipeline.stats.updated");
    assert.ok(stats, "stats projection follows the terminal transition");
    assert.equal(stats.payload.kind, "pipeline.stats.updated");
    assert.equal(stats.payload.id, "team_pipe");
  });
});

// ── gates ────────────────────────────────────────────────────────────────────

describe("ws-run-events — gates", () => {
  test("blocked human-gate emits thin gate.opened", async () => {
    const flows = {
      "flow-1": {
        ...runningFlow(2),
        status: "blocked",
        waitJson: { kind: "human-gate", items: [{ itemId: "a" }] },
      },
    };
    const { listener, calls } = makeHarness(flows);
    await listener("flow-1");

    const opened = calls.find((c) => c.name === "plugin.gate.opened");
    assert.ok(opened, "gate.opened fires");
    assert.equal(opened.payload.kind, "gate.opened");
    assert.equal(opened.payload.flowId, "flow-1");
  });

  test("blocked non-gate wait emits no gate.opened", async () => {
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
    assert.ok(names(calls).includes("plugin.pipeclaw.run.updated"), "rich patch still fires");
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
      readStore: { statsFor: async () => ({}) },
      nodeExecutionsStore: { getInvocation: async () => null },
      pipelineLookup: () => ({}),
      getBroadcast: () => null,
      emit,
    });
    await listener("flow-1");
  });
});
