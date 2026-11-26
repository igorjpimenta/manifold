import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForTerminal } from "../../helpers/wait-for-event.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { getAllSubflowGroups } from "../../../state/subflow-state.js";
import { assembleState } from "../../../engine/executor-helpers.js";

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

function makeEvictGatesLikeNode() {
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
  const dbPath = tmpPath("evict-gates-e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const EVICT_GATES_NODE = makeEvictGatesLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { EVICT_GATES: EVICT_GATES_NODE },
    edges: { __start__: "EVICT_GATES", EVICT_GATES: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::EVICT_GATES",
    state: {},
    nodes: EVICT_GATES_NODE.flow.nodes,
    edges: EVICT_GATES_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::EVICT_GATES", childPipeline]]) };

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
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "EVICT_GATES");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedStaleGates(harness, flowId, staleGates) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, staleGates },
  });
}

describe("childFlows:true EVICT_GATES-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("single stale gate — child spawns, CANCEL runs, reaches __end__, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, [
      { id: "abcd1234:job-1", targetFlowId: "abcd1234-full-uuid", targetBranchKey: "job-1", reason: "no inbound in 48h" },
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
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, [
      { id: "aaaa1111:job-a", targetFlowId: "aaaa1111-full", targetBranchKey: "job-a", reason: "listing won" },
      { id: "bbbb2222:job-b", targetFlowId: "bbbb2222-full", targetBranchKey: "job-b", reason: "no inbound in 48h" },
      { id: "cccc3333:job-c", targetFlowId: "cccc3333-full", targetBranchKey: "job-c", reason: "listing won" },
    ]);
    await executor.runFlow(record.flowId);
    await flush();

    const ids = await childFlowIds(harness, record.flowId);
    assert.equal(ids.length, 3, "all 3 selected stale gates spawn a child, no fan-out limiter");
    for (const id of ids) assert.equal((await runStore.get(id)).status, "succeeded");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("no stale gates — zero children spawned, parent still succeeds", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, []);
    await executor.runFlow(record.flowId);
    await flush();

    assert.equal((await childFlowIds(harness, record.flowId)).length, 0);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("getAllSubflowGroups resolves under groups.EVICT_GATES", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedStaleGates(harness, record.flowId, [
      { id: "dddd4444:job-d", targetFlowId: "dddd4444-full", targetBranchKey: "job-d", reason: "no inbound in 48h" },
    ]);
    await executor.runFlow(record.flowId);
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.EVICT_GATES);
    const row = groups.EVICT_GATES["dddd4444:job-d"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.CANCEL?.evicted, true);
  });
});
