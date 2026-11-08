/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors freelance_workana-poll-listings'
 * EVICT_STALE (single kind:"code" sub-node, CANCEL → __end__), the
 * simplest topology of any migration so far: no gate, no fallback, one
 * sub-node. CANCEL is a trivial no-op stand-in — zero real HTTP calls to
 * /manifold/gate/resolve anywhere in this fixture.
 *
 * Isolated-sandbox discipline (per the incident writeup):
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForTerminal } from "../../helpers/wait-for-event.js";
import { waitForChildFlowIds, waitForTerminalStatus } from "../../helpers/wait-for-children.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { getAllSubflowGroups } from "../../../state/subflow-state.js";
import { assembleState } from "../../../engine/executor-helpers.js";

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

function makeEvictStaleLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "id",
    selector: (state) => state.staleGates ?? [],
    flow: {
      nodes: {
        CANCEL: {
          kind: "code",
          run: (state) => {
            const item = state.item;
            return { CANCEL: { targetFlowId: item.targetFlowId, targetBranchKey: item.targetBranchKey, evicted: true } };
          },
        },
      },
      edges: {
        __start__: "CANCEL",
        CANCEL: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("evict-stale-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("evict-stale-e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const EVICT_STALE_NODE = makeEvictStaleLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { EVICT_STALE: EVICT_STALE_NODE },
    edges: { __start__: "EVICT_STALE", EVICT_STALE: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::EVICT_STALE",
    state: {},
    nodes: EVICT_STALE_NODE.flow.nodes,
    edges: EVICT_STALE_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::EVICT_STALE", childPipeline]]) };

  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    runTurn: null,
    failureStore: null,
    nodeHealthStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex: null,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore: makeAdmissionQueueStore(),
  });
  resumeParentRef.resume = executor.resume;

  return { runStore, nodeExecutionsStore, store, executor };
}

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "EVICT_STALE");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

async function store_seedStaleGates(harness, flowId, staleGates) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, staleGates },
  });
}

describe("childFlows:true EVICT_STALE-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("single stale gate — child spawns, CANCEL runs, reaches __end__, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, [
      { id: "abcd1234:job-1", targetFlowId: "abcd1234-full-uuid", targetBranchKey: "job-1", reason: "listing gone" },
    ]);
    const childDone1 = waitForTerminal((q) => q.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childDone1;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.CANCEL?.evicted, true);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("multiple stale gates — every selected item spawns a real child (this fixture's 3 items stay under the shared 8-wide default cap)", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, [
      { id: "aaaa1111:job-a", targetFlowId: "aaaa1111-full", targetBranchKey: "job-a", reason: "bid cap reached" },
      { id: "bbbb2222:job-b", targetFlowId: "bbbb2222-full", targetBranchKey: "job-b", reason: "listing gone" },
      { id: "cccc3333:job-c", targetFlowId: "cccc3333-full", targetBranchKey: "job-c", reason: "bid cap reached" },
    ]);
    await executor.runFlow(record.flowId);

    const ids = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "EVICT_STALE", 3);
    assert.equal(ids.length, 3, "all 3 selected stale gates spawn a child, no fan-out limiter");
    for (const id of ids) assert.equal((await waitForTerminalStatus(runStore, id)).status, "succeeded");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("no stale gates — zero children spawned, parent still succeeds", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, []);
    await executor.runFlow(record.flowId);
    await waitForTerminalStatus(runStore, record.flowId);

    assert.equal((await childFlowIds(harness, record.flowId)).length, 0);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("getAllSubflowGroups resolves under groups.EVICT_STALE", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, [
      { id: "dddd4444:job-d", targetFlowId: "dddd4444-full", targetBranchKey: "job-d", reason: "listing gone" },
    ]);
    await executor.runFlow(record.flowId);
    const [childFlowId] = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "EVICT_STALE", 1);
    await waitForTerminalStatus(runStore, childFlowId);

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.EVICT_STALE);
    const row = groups.EVICT_STALE["dddd4444:job-d"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.CANCEL?.evicted, true);
  });
});
