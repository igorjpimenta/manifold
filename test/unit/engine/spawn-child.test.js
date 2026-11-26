import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { resolveOrCreateChild } from "../../../engine/spawn-child.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const dbPath = tmpPath("spawn-child");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const store = createStateStore({}, "test-owner", { runStore });
  return { runStore, nodeExecutionsStore, store };
}

async function spawn(args) {
  await args.runStore.insert({ flowId: args.flowId, status: "running" });
  return resolveOrCreateChild(args);
}

describe("resolveOrCreateChild", () => {
  let harness;
  afterEach(() => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
  });

  test("creates a new child on first call", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();

    const result = await spawn({
      flowId, pipelineId: "parent_pipe", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "parent_pipe::LOOP", initialState: { item: "item-1" },
      store, runStore, nodeExecutionsStore,
    });

    assert.equal(result.created, true);
    assert.ok(result.childFlowId);
    assert.ok(result.invocationId);

    const child = await runStore.get(result.childFlowId);
    assert.equal(child.parentInvocationId, result.invocationId);
    assert.equal(child.pipelineId, "parent_pipe::LOOP");
  });

  test("second call for the same identity returns the existing child, not a new one", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();
    const args = {
      flowId, pipelineId: "parent_pipe", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "parent_pipe::LOOP", initialState: { item: "item-1" },
      store, runStore, nodeExecutionsStore,
    };

    const first = await spawn(args);
    const second = await spawn(args);

    assert.equal(first.created, true);
    assert.equal(second.created, false);
    assert.equal(second.childFlowId, first.childFlowId);
    assert.equal(second.invocationId, first.invocationId);
  });

  test("different subflowKeys under the same loop node get different invocations and children", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();
    const base = {
      flowId, pipelineId: "parent_pipe", nodeId: "LOOP",
      childPipelineId: "parent_pipe::LOOP", store, runStore, nodeExecutionsStore,
    };

    const a = await spawn({ ...base, subflowKey: "item-1", initialState: { item: "item-1" } });
    const b = await spawn({ ...base, subflowKey: "item-2", initialState: { item: "item-2" } });

    assert.notEqual(a.invocationId, b.invocationId);
    assert.notEqual(a.childFlowId, b.childFlowId);
  });

  test("root-level identity (no subflowKey) is stable across repeated calls", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();
    const args = {
      flowId, pipelineId: "parent_pipe", nodeId: "ROOT_NODE", subflowKey: null,
      childPipelineId: "parent_pipe::ROOT_NODE", initialState: {},
      store, runStore, nodeExecutionsStore,
    };

    const first = await spawn(args);
    const second = await spawn(args);

    assert.equal(second.created, false);
    assert.equal(second.childFlowId, first.childFlowId);
  });

  test("race safety: two concurrent calls for the same identity converge on one child", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();
    const args = {
      flowId, pipelineId: "parent_pipe", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "parent_pipe::LOOP", initialState: { item: "item-1" },
      store, runStore, nodeExecutionsStore,
    };

    const results = await Promise.all(Array.from({ length: 5 }, () => spawn(args)));
    const uniqueChildIds = new Set(results.map((r) => r.childFlowId));
    assert.equal(uniqueChildIds.size, 1, "all calls should resolve to the same single child");
    assert.equal(results.filter((r) => r.created).length, 1, "exactly one call should have created the child");
  });

  test("a rejecting guard skips child creation entirely and reports guarded:true", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();

    const result = await spawn({
      flowId, pipelineId: "parent_pipe", nodeId: "SCORE", subflowKey: "listing-1",
      childPipelineId: "parent_pipe::SCORE", initialState: { item: "listing-1" },
      store, runStore, nodeExecutionsStore,
      guard: async () => false,
    });

    assert.equal(result.guarded, true);
    assert.equal(result.childFlowId, null);
    assert.equal(result.invocationId, null);
    assert.equal(result.created, false);
  });

  test("an approving guard behaves exactly like no guard at all", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const flowId = randomUUID();

    const result = await spawn({
      flowId, pipelineId: "parent_pipe", nodeId: "SCORE", subflowKey: "listing-1",
      childPipelineId: "parent_pipe::SCORE", initialState: { item: "listing-1" },
      store, runStore, nodeExecutionsStore,
      guard: async () => true,
    });

    assert.equal(result.created, true);
    assert.ok(result.childFlowId);
    assert.ok(result.invocationId);
  });
});
