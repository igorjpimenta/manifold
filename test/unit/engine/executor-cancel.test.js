/**
 * Unit tests for engine/executor-cancel.js
 *
 * Covers: completeCancellation — returns store.cancel's result. Also
 * covers Phase 2's cascade-cancel step — real sqlite-backed stores, since
 * that logic is a real join across run-store.js/node-executions.js.
 *
 * killActiveSessions (the old branchService-era in-flight session abort
 * step) was deleted outright 2026-08-12 — permanently a no-op since
 * branchService's removal, never actually aborted anything. See
 * executor-cancel.js's own header comment and CANCEL_SESSION_ABORT
 * .design.md for the real (still-open) gap this leaves: cancelling a flow
 * doesn't interrupt an in-flight `kind:"agent"` dispatch, root or subflow.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { completeCancellation } from "../../../engine/executor-cancel.js";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { resolveOrCreateChild } from "../../../engine/spawn-child.js";

function makeDeps(overrides = {}) {
  const calls = [];
  const store = {
    cancel: ({ flowId }) => { calls.push({ op: "cancel", flowId }); return { cancelled: true }; },
    ...overrides.store,
  };
  return { store, calls };
}

describe("completeCancellation — call order", () => {
  test("completeCancellation no longer calls any branch store — store.cancel fires and that's it", async () => {
    const { store, calls } = makeDeps();
    await completeCancellation({ store, flowId: "f1", stateJson: {}, lastStep: "END", revision: 3 });
    assert.equal(calls.length, 1, "only store.cancel should fire");
    assert.equal(calls[0].op, "cancel", "store.cancel still fires");
  });

  test("passes flowId and cfg to store.cancel", async () => {
    let cancelArgs = null;
    const store = { cancel: (args) => { cancelArgs = args; return { cancelled: true }; } };
    const cfg = { maxRetries: 3 };
    await completeCancellation({ store, flowId: "flow-abc", stateJson: {}, lastStep: null, revision: 0, cfg });
    assert.equal(cancelArgs.flowId, "flow-abc");
    assert.equal(cancelArgs.cfg, cfg);
  });

  test("passes an explicit reason through to store.cancel", async () => {
    let cancelArgs = null;
    const store = { cancel: (args) => { cancelArgs = args; return { cancelled: true }; } };
    await completeCancellation({
      store, flowId: "flow-abc", stateJson: {}, lastStep: null, revision: 0,
      reason: "cancelled by operator",
    });
    assert.equal(cancelArgs.reason, "cancelled by operator");
  });

  test("defaults reason to null when the caller doesn't supply one", async () => {
    let cancelArgs = null;
    const store = { cancel: (args) => { cancelArgs = args; return { cancelled: true }; } };
    await completeCancellation({ store, flowId: "flow-abc", stateJson: {}, lastStep: null, revision: 0 });
    assert.equal(cancelArgs.reason, null);
  });
});

describe("completeCancellation — return value", () => {
  test("returns the result of store.cancel()", async () => {
    const store = { cancel: () => ({ cancelled: true, flowId: "f1" }) };
    const result = await completeCancellation({ store, flowId: "f1", stateJson: {}, lastStep: null, revision: 0 });
    assert.deepEqual(result, { cancelled: true, flowId: "f1" });
  });
});

describe("completeCancellation — accepts any stateJson shape without throwing", () => {
  test("plain new-style stateJson", async () => {
    const store = { cancel: () => ({ cancelled: true }) };
    await assert.doesNotReject(
      completeCancellation({ store, flowId: "f", stateJson: { pipelineId: "x" }, lastStep: null, revision: 0 })
    );
  });

  test("a stale old-model stateJson.loop shape is simply ignored, not read", async () => {
    const store = { cancel: () => ({ cancelled: true }) };
    const stateJson = { loop: { LOOP: { "branch-1": { step: "__end__", doneAt: 123 } } } };
    await assert.doesNotReject(
      completeCancellation({ store, flowId: "f", stateJson, lastStep: null, revision: 0 })
    );
  });
});

// ── cascade-cancel (Phase 2) ─────────────────────────────────────────────────

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("cancel-cascade-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("cancel-cascade-execs"));
  const store = createStateStore({}, "test-owner", { runStore });
  return { runStore, nodeExecutionsStore, store };
}

async function spawnChild(harness, parentFlowId, subflowKey, { nodeId = "LOOP" } = {}) {
  const { store, runStore, nodeExecutionsStore } = harness;
  const { childFlowId } = await resolveOrCreateChild({
    flowId: parentFlowId, pipelineId: "p", nodeId, subflowKey,
    childPipelineId: "p::LOOP", initialState: {}, store, runStore, nodeExecutionsStore,
  });
  return childFlowId;
}

describe("completeCancellation — cascade-cancel childFlows children", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("cancels every non-terminal child when the parent is cancelled", async () => {
    harness = await makeHarness();
    const { store, runStore, nodeExecutionsStore } = harness;
    const parentFlowId = randomUUID();
    const child1 = await spawnChild(harness, parentFlowId, "item-1");
    const child2 = await spawnChild(harness, parentFlowId, "item-2");

    await completeCancellation({
      store, flowId: parentFlowId, stateJson: {}, lastStep: "LOOP", revision: 0,
      runStore, nodeExecutionsStore,
    });

    assert.equal((await runStore.get(child1)).status, "cancelled");
    assert.equal((await runStore.get(child2)).status, "cancelled");
  });

  test("cascaded children get an explicit 'parent flow ... was cancelled' reason, not a stale or NULL one", async () => {
    harness = await makeHarness();
    const { store, runStore, nodeExecutionsStore } = harness;
    const parentFlowId = randomUUID();
    const child1 = await spawnChild(harness, parentFlowId, "item-1");

    await completeCancellation({
      store, flowId: parentFlowId, stateJson: {}, lastStep: "LOOP", revision: 0,
      runStore, nodeExecutionsStore,
    });

    assert.equal((await runStore.get(child1)).statusReason, `parent flow ${parentFlowId.slice(0, 8)} was cancelled`);
  });

  test("leaves already-terminal children untouched", async () => {
    harness = await makeHarness();
    const { store, runStore, nodeExecutionsStore } = harness;
    const parentFlowId = randomUUID();
    const child1 = await spawnChild(harness, parentFlowId, "item-1");
    const child2 = await spawnChild(harness, parentFlowId, "item-2");
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });

    await completeCancellation({
      store, flowId: parentFlowId, stateJson: {}, lastStep: "LOOP", revision: 0,
      runStore, nodeExecutionsStore,
    });

    assert.equal((await runStore.get(child1)).status, "succeeded", "already-terminal child must not be touched");
    assert.equal((await runStore.get(child2)).status, "cancelled");
  });

  test("cascades recursively — a grandchild (child-of-child) also gets cancelled", async () => {
    harness = await makeHarness();
    const { store, runStore, nodeExecutionsStore } = harness;
    const parentFlowId = randomUUID();
    const child = await spawnChild(harness, parentFlowId, "item-1");
    // The child itself has its own childFlows loop node, spawning a grandchild.
    const grandchild = await spawnChild(harness, child, "sub-item-1", { nodeId: "INNER" });

    await completeCancellation({
      store, flowId: parentFlowId, stateJson: {}, lastStep: "LOOP", revision: 0,
      runStore, nodeExecutionsStore,
    });

    assert.equal((await runStore.get(child)).status, "cancelled");
    assert.equal((await runStore.get(grandchild)).status, "cancelled", "grandchild must be cancelled by recursion, not just the direct child");
  });

  test("a flow with no childFlows children is unaffected (no-op cascade)", async () => {
    harness = await makeHarness();
    const { store, runStore, nodeExecutionsStore } = harness;
    const flowId = randomUUID();
    await store.create({ pipelineId: "p", initialState: {} });

    const result = await completeCancellation({
      store, flowId, stateJson: {}, lastStep: null, revision: 0,
      runStore, nodeExecutionsStore,
    });
    assert.equal(result.cancelled, false, "cancelling a nonexistent flowId is still handled by the ordinary store.cancel path");
  });

  test("does not throw when runStore/nodeExecutionsStore are omitted (backward compat)", async () => {
    const store = { cancel: () => ({ cancelled: true }) };
    await assert.doesNotReject(
      completeCancellation({ store, flowId: "f", stateJson: {}, lastStep: null, revision: 0 })
    );
  });
});
