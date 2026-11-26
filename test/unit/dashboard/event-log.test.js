import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { createEventBus } from "../../../dashboard/event-log.js";
import { flowStateEmitter } from "../../../events.js";

function makeStore(rows) {
  const byId = new Map(rows.map((r) => [r.flowId, r]));
  return {
    async list() { return [...byId.values()]; },
    async get(flowId) { return byId.get(flowId) ?? null; },
    _set(row) { byId.set(row.flowId, row); },
  };
}

function makeNodeExecutionsStore(initial) {
  const visited = new Map(Object.entries(initial).map(([k, v]) => [k, new Set(v)]));
  return {
    async listVisitedNodeIdsByFlow(flowIds) {
      const result = new Map();
      for (const id of flowIds) result.set(id, new Set(visited.get(id) ?? []));
      return result;
    },
    _addVisited(flowId, nodeId) {
      if (!visited.has(flowId)) visited.set(flowId, new Set());
      visited.get(flowId).add(nodeId);
    },
  };
}

async function waitForEvent(bus, predicate, timeoutMs = 500) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { unsub(); reject(new Error("timed out waiting for event")); }, timeoutMs);
    const unsub = bus.subscribe((evt) => {
      if (predicate(evt)) { clearTimeout(timer); unsub(); resolve(evt); }
    });
  });
}

describe("dashboard/event-log.js — invocation-based stateKeysAdded/Removed", () => {
  let bus;

  afterEach(() => {
    bus?.stop();
    bus = null;
  });

  test("run.updated diffs added/removed node ids via listVisitedNodeIdsByFlow, not Object.keys(stateJson)", async () => {
    const flowId = "diff-test-flow";
    const row = { flowId, revision: 1, status: "running", lastStep: "SCAN", stateJson: { pipelineId: "p", args: {} } };
    const store = makeStore([row]);
    const nodeExecutionsStore = makeNodeExecutionsStore({ [flowId]: ["SCAN"] });
    bus = createEventBus({ store, nodeExecutionsStore, logger: null, dbPath: null });
    await bus.start();

    nodeExecutionsStore._addVisited(flowId, "DRAFT");
    store._set({ ...row, revision: 2, lastStep: "DRAFT" });
    const evtPromise = waitForEvent(bus, (e) => e.kind === "run.updated" && e.flowId === flowId);
    flowStateEmitter.emit("change", flowId);
    const evt = await evtPromise;

    assert.deepEqual(evt.stateKeysAdded, ["DRAFT"]);
    assert.deepEqual(evt.stateKeysRemoved, []);
  });

  test("start() priming batches listVisitedNodeIdsByFlow across all rows in one call", async () => {
    const rows = [
      { flowId: "f1", revision: 1, status: "running", lastStep: "A", stateJson: {} },
      { flowId: "f2", revision: 1, status: "running", lastStep: "A", stateJson: {} },
    ];
    const store = makeStore(rows);
    let callCount = 0;
    const nodeExecutionsStore = makeNodeExecutionsStore({ f1: ["A"], f2: ["A"] });
    const realList = nodeExecutionsStore.listVisitedNodeIdsByFlow.bind(nodeExecutionsStore);
    nodeExecutionsStore.listVisitedNodeIdsByFlow = async (flowIds) => { callCount++; return realList(flowIds); };

    bus = createEventBus({ store, nodeExecutionsStore, logger: null, dbPath: null });
    await bus.start();

    assert.equal(callCount, 1, "priming must batch all flowIds into a single call, not one per row");
  });

  test("missing nodeExecutionsStore degrades to empty diff sets, never throws", async () => {
    const flowId = "no-node-execs-flow";
    const row = { flowId, revision: 1, status: "running", lastStep: "A", stateJson: {} };
    const store = makeStore([row]);
    bus = createEventBus({ store, logger: null, dbPath: null });
    await bus.start();

    store._set({ ...row, revision: 2, lastStep: "B" });
    const evtPromise = waitForEvent(bus, (e) => e.kind === "run.updated" && e.flowId === flowId);
    flowStateEmitter.emit("change", flowId);
    const evt = await evtPromise;

    assert.deepEqual(evt.stateKeysAdded, []);
    assert.deepEqual(evt.stateKeysRemoved, []);
  });
});
