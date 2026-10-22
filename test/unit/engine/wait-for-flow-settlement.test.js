/**
 * Unit tests for engine/wait-for-flow-settlement.js — the fix for the
 * one-shot CLI's childFlows-orphaning bug (agent/cli.js's start/rerun/
 * retry/approve commands each await exactly one runFlow()/resume() call
 * for the flow they were given, which resolves before any childFlows
 * children that flow spawns along the way ever get a chance to run).
 *
 * The core scenario (test 3 below) uses a real executor/store/runStore/
 * nodeExecutionsStore — same harness shape as
 * executor-subflow-children-score-e2e.test.js — specifically to prove
 * this closes the exact gap a bare `await executor.runFlow(flowId,
 * "fresh")` leaves open for a childFlows:true subflow node.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { waitForFlowSettlement } from "../../../engine/wait-for-flow-settlement.js";

// Always-admits-immediately fake — admissionQueueStore is required by createExecutor's
// subflow-children paths. These tests don't exercise queue behavior.
function makeAdmissionQueueStore(overrides = {}) {
  return {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
    ...overrides,
  };
}

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("wfs-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("wfs-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });
  return { runStore, nodeExecutionsStore, store, resumeParentRef, logger };
}

describe("waitForFlowSettlement", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("already terminal, no children: resolves immediately", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const created = await store.create({ pipelineId: "p", initialState: {} });
    await store.finish({ flowId: created.flowId, expectedRevision: 0, stateJson: {} });

    const result = await waitForFlowSettlement(created.flowId, { store, runStore, nodeExecutionsStore, logger: harness.logger }, { pollMs: 20 });
    assert.deepEqual(result, { settled: true });
  });

  test("blocked on a real human-gate, no children: resolves immediately without waiting for anything else", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const created = await store.create({ pipelineId: "p", initialState: {} });
    await store.setWaiting({
      flowId: created.flowId, expectedRevision: 0, lastStep: "GATE",
      blockingNodeId: "GATE", stateJson: {}, waitJson: { kind: "human-gate", items: [] },
    });

    const result = await waitForFlowSettlement(created.flowId, { store, runStore, nodeExecutionsStore, logger: harness.logger }, { pollMs: 20 });
    assert.deepEqual(result, { settled: true });
  });

  // Regression test — found in review, not live. A kind:"agent" node
  // dispatched outside a gateway request scope (which the one-shot CLI
  // always is) throws GatewayHandoffNeeded, blocking with
  // waitJson.kind:"needs-gateway-handoff" — a wait kind with NO automatic
  // resume anywhere in the codebase (executor-errors.js's own docstring).
  // Without excluding it as a rest point, this module's unbounded poll
  // loop would hang forever on any pipeline hitting this, instead of
  // surfacing the (already pre-existing, already-expected) blocked status
  // promptly.
  test("blocked on needs-gateway-handoff (a kind:\"agent\" node dispatched outside gateway context): resolves immediately, does not hang", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const created = await store.create({ pipelineId: "p", initialState: {} });
    await store.setWaiting({
      flowId: created.flowId, expectedRevision: 0, lastStep: "SOME_AGENT_NODE",
      blockingNodeId: "SOME_AGENT_NODE", stateJson: {}, waitJson: { kind: "needs-gateway-handoff" },
    });

    const result = await waitForFlowSettlement(created.flowId, { store, runStore, nodeExecutionsStore, logger: harness.logger }, { pollMs: 20 });
    assert.deepEqual(result, { settled: true });
  });

  test("a plain running flow: waits until it actually goes terminal, not before", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const created = await store.create({ pipelineId: "p", initialState: {} });

    let resolvedAt = null;
    const waitPromise = waitForFlowSettlement(created.flowId, { store, runStore, nodeExecutionsStore, logger: harness.logger }, { pollMs: 20 })
      .then((r) => { resolvedAt = Date.now(); return r; });

    // Flip to terminal only after a short delay — if waitForFlowSettlement
    // resolved before this, the polling loop would be broken (returning
    // "settled" on a still-running flow).
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(resolvedAt, null, "must not resolve while the flow is still running");
    await store.finish({ flowId: created.flowId, expectedRevision: 0, stateJson: {} });

    const result = await waitPromise;
    assert.deepEqual(result, { settled: true });
    assert.ok(resolvedAt !== null);
  });

  // The core bug this module fixes, proven against a real executor: a
  // childFlows:true subflow fires its children via a bare setImmediate
  // that a single `await executor.runFlow(flowId, "fresh")` never waits
  // on. This test dispatches the SAME way agent/cli.js's start command
  // does (one initial runFlow call, nothing more), then confirms
  // waitForFlowSettlement is what actually catches every child up to a
  // real terminal state — not the initial runFlow call itself.
  test("real childFlows:true fan-out: catches every child a single runFlow() call leaves in flight", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, resumeParentRef, logger } = harness;

    const FANOUT_NODE = {
      kind: "subflow",
      childFlows: true,
      mode: "all",
      itemKey: "id",
      selector: (state) => state.items ?? [],
      flow: {
        nodes: {
          WORK: {
            kind: "code",
            run: async (state) => {
              await new Promise((r) => setTimeout(r, 30));
              return { WORK: { done: true, id: state.item.id } };
            },
          },
        },
        edges: { __start__: "WORK", WORK: () => "__end__" },
      },
    };
    const parentPipeline = {
      id: "test_wfs_parent", state: {},
      nodes: { FANOUT: FANOUT_NODE },
      edges: { __start__: "FANOUT", FANOUT: () => "__end__" },
    };
    const childPipeline = {
      id: "test_wfs_parent::FANOUT", state: {},
      nodes: FANOUT_NODE.flow.nodes, edges: FANOUT_NODE.flow.edges,
    };
    const pipelinesRef = { value: new Map([["test_wfs_parent", parentPipeline], ["test_wfs_parent::FANOUT", childPipeline]]) };

    const executor = createExecutor({
      api: { runtime: {}, config: {} }, store, pipelinesRef, ensurePipelinesLoaded: null, logger,
      getWorkingSubagent: null, failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore, runStore,
    admissionQueueStore: makeAdmissionQueueStore(),
    });
    resumeParentRef.resume = executor.resume;

    const record = await executor.start("test_wfs_parent", {}, { kind: "test" });
    const flow = await store.get(record.flowId);
    await store.setRunning({
      flowId: record.flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
      stateJson: { ...flow.stateJson, items: [{ id: "a" }, { id: "b" }, { id: "c" }] },
    });

    // Exactly what agent/cli.js's start command does today — the bug.
    await executor.runFlow(record.flowId, "fresh");
    const rightAfterInitialDispatch = await store.get(record.flowId);
    assert.equal(rightAfterInitialDispatch.status, "blocked", "the single runFlow() call must NOT have waited for children — this is the bug being characterized");

    // The fix: this must actually drive/observe the tree to real completion.
    const result = await waitForFlowSettlement(record.flowId, { store, runStore, nodeExecutionsStore, logger }, { pollMs: 20 });
    assert.deepEqual(result, { settled: true });

    const final = await store.get(record.flowId);
    assert.equal(final.status, "succeeded", "the parent must have actually resumed and finished, not just gone quiet");

    const invocationIds = await nodeExecutionsStore.listInvocationIdsForNode(record.flowId, "FANOUT");
    const children = await runStore.listForParentInvocations(invocationIds);
    assert.equal(children.length, 3);
    for (const child of children) {
      assert.equal(child.status, "succeeded", `child ${child.flowId} must have actually run WORK to completion, not been left orphaned at __start__`);
      assert.equal(child.lastStep, "WORK");
    }
  });
});
