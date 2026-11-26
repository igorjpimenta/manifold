import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForTerminal, waitForAll } from "../../helpers/wait-for-event.js";
import { waitForChildFlowIds, waitForTerminalStatus } from "../../helpers/wait-for-children.js";
import { flowEmitter } from "../../../events.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { getAllSubflowGroups } from "../../../state/subflow-state.js";
import { assembleState } from "../../../engine/executor-helpers.js";
import { getGlobalConcurrencyLimit, _resetForTests as resetChildFanoutCap } from "../../../engine/global-concurrency.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

function makeScoreLikeNode(concurrency) {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "id",
    ...(concurrency != null ? { concurrency } : {}),
    selector: (state) => state.filtered ?? [],
    flow: {
      nodes: {
        CACHE_CHECK: {
          kind: "code",
          run: (state) => {
            const listing = state.item;
            const hit = !!listing.precachedHit;
            return { CACHE_CHECK: hit ? { hit: true, score: 42 } : { hit: false } };
          },
        },
        SCORE_LLM: {
          kind: "code",
          run: (state) => ({ SCORE_LLM: { score: 77, rawScore: 77 } }),
        },
        CACHE_SAVE: {
          kind: "code",
          run: (state) => ({ CACHE_SAVE: { saved: true } }),
        },
      },
      edges: {
        __start__: "CACHE_CHECK",
        CACHE_CHECK: (state) => state.CACHE_CHECK?.hit ? "__end__" : "SCORE_LLM",
        SCORE_LLM: () => "CACHE_SAVE",
        CACHE_SAVE: () => "__end__",
      },
    },
  };
}

async function makeHarness(concurrency) {
  const dbPath = tmpPath("score-e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const admissionQueueStore = createAdmissionQueueStore({ dbPath, getLimit: getGlobalConcurrencyLimit });
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, admissionQueueStore, logger });

  const SCORE_NODE = makeScoreLikeNode(concurrency);
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { SCORE: SCORE_NODE },
    edges: { __start__: "SCORE", SCORE: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::SCORE",
    state: {},
    nodes: SCORE_NODE.flow.nodes,
    edges: SCORE_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::SCORE", childPipeline]]) };

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
    admissionQueueStore,
  });
  resumeParentRef.resume = executor.resume;

  return { runStore, nodeExecutionsStore, store, executor, pipelinesRef };
}

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "SCORE");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

async function store_seedFiltered(harness, flowId, filtered) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, filtered },
  });
}

describe("childFlows:true SCORE-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
    resetChildFanoutCap();
  });

  test("cache hit — child reaches __end__ immediately, SCORE_LLM/CACHE_SAVE never dispatched", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, record.flowId, [{ id: "job-1", precachedHit: true }]);
    const childDone1 = waitForTerminal((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childDone1;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.CACHE_CHECK?.hit, true);
    assert.equal(childState.SCORE_LLM, undefined, "cache hit — SCORE_LLM must never dispatch");
    assert.equal(childState.CACHE_SAVE, undefined, "cache hit — CACHE_SAVE must never dispatch");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("cache miss — full CACHE_CHECK → SCORE_LLM → CACHE_SAVE path runs, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, record.flowId, [{ id: "job-2", precachedHit: false }]);
    const childDone2 = waitForTerminal((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childDone2;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.CACHE_CHECK?.hit, false);
    assert.equal(childState.SCORE_LLM?.score, 77);
    assert.equal(childState.CACHE_SAVE?.saved, true);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("getAllSubflowGroups resolves under the renamed groupId — groups.SCORE, not groups.scored", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, record.flowId, [
      { id: "job-3", precachedHit: true },
      { id: "job-4", precachedHit: false },
    ]);
    const bothDone = waitForAll(flowEmitter, "flow.terminal", (p) => p.flowId !== record.flowId, 2);
    await executor.runFlow(record.flowId);
    await bothDone;

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.SCORE, "must resolve under the node's own id, SCORE — not the old custom groupId \"scored\"");
    assert.equal(groups.scored, undefined, "the old groupId key must not exist at all");

    const cachedRow = groups.SCORE["job-3"];
    assert.ok(cachedRow);
    assert.equal(cachedRow.status, "succeeded");
    assert.equal(cachedRow.data.CACHE_CHECK?.hit, true);

    const freshRow = groups.SCORE["job-4"];
    assert.ok(freshRow);
    assert.equal(freshRow.status, "succeeded");
    assert.equal(freshRow.data.SCORE_LLM?.score, 77);
    assert.equal(freshRow.data.CACHE_SAVE?.saved, true);
  });

  test("multiple items, no concurrency declared: every selected item spawns a real child, no fan-out limiter", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, record.flowId, [
      { id: "job-5", precachedHit: true },
      { id: "job-6", precachedHit: false },
      { id: "job-7", precachedHit: true },
      { id: "job-8", precachedHit: false },
    ]);
    const allDone1 = waitForAll(flowEmitter, "flow.terminal", (p) => p.flowId !== record.flowId, 4);
    await executor.runFlow(record.flowId);
    await allDone1;

    const ids = await childFlowIds(harness, record.flowId);
    assert.equal(ids.length, 4, "all 4 selected listings spawn a child, no fan-out limiter");
    for (const id of ids) {
      assert.equal((await runStore.get(id)).status, "succeeded");
    }
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("multiple items, global cap set to 2: dispatch is capped, but every item still eventually succeeds via auto-wake on sibling completion", async () => {
    resetChildFanoutCap(2);
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, record.flowId, [
      { id: "job-5", precachedHit: true },
      { id: "job-6", precachedHit: false },
      { id: "job-7", precachedHit: true },
      { id: "job-8", precachedHit: false },
    ]);
    const allDone2 = waitForAll(flowEmitter, "flow.terminal", (p) => p.flowId !== record.flowId, 4);
    await executor.runFlow(record.flowId);
    await allDone2;

    const ids = await childFlowIds(harness, record.flowId);
    assert.equal(ids.length, 4, "the cap governs dispatch timing, not how many children get created");
    for (const id of ids) {
      assert.equal((await runStore.get(id)).status, "succeeded", "every child eventually completes, whether it ran immediately or was queued behind the cap and later auto-woken");
    }
    assert.equal((await runStore.get(record.flowId)).status, "succeeded", "the parent flow itself completes once every child (immediate or queued) has resolved");
  });

  test("concurrency:false opts SCORE out entirely — every child dispatches immediately even with a global cap of 1", async () => {
    resetChildFanoutCap(1);
    harness = await makeHarness(false);
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, record.flowId, [
      { id: "job-9", precachedHit: true },
      { id: "job-10", precachedHit: false },
      { id: "job-11", precachedHit: true },
    ]);
    await executor.runFlow(record.flowId);

    const ids = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "SCORE", 3);
    assert.equal(ids.length, 3);
    for (const id of ids) {
      assert.equal((await waitForTerminalStatus(runStore, id)).status, "succeeded");
    }
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("global wake works across unrelated pipelines — a different pipeline's queued child is resumed once THIS pipeline's child finishes", async () => {
    resetChildFanoutCap(1);
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, pipelinesRef } = harness;

    const OTHER_NODE = makeScoreLikeNode();
    pipelinesRef.value.set("other_parent", {
      id: "other_parent", state: {},
      nodes: { SCORE: OTHER_NODE },
      edges: { __start__: "SCORE", SCORE: "__end__" },
    });
    pipelinesRef.value.set("other_parent::SCORE", {
      id: "other_parent::SCORE", state: {},
      nodes: OTHER_NODE.flow.nodes, edges: OTHER_NODE.flow.edges,
    });

    const recordA = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, recordA.flowId, [{ id: "job-a", precachedHit: false }]);
    await executor.runFlow(recordA.flowId);

    const recordB = await executor.start("other_parent", {}, { kind: "test" });
    await store_seedFiltered(harness, recordB.flowId, [{ id: "job-b", precachedHit: true }]);
    await executor.runFlow(recordB.flowId);

    const [childA] = await waitForChildFlowIds(nodeExecutionsStore, runStore, recordA.flowId, "SCORE", 1);
    const [childB] = await waitForChildFlowIds(nodeExecutionsStore, runStore, recordB.flowId, "SCORE", 1);

    assert.equal((await waitForTerminalStatus(runStore, childA)).status, "succeeded");
    assert.equal((await waitForTerminalStatus(runStore, childB)).status, "succeeded", "a different pipeline's queued child must still get woken by the shared global hook, not just siblings of the same pipeline");
    assert.equal((await waitForTerminalStatus(runStore, recordA.flowId)).status, "succeeded");
    assert.equal((await waitForTerminalStatus(runStore, recordB.flowId)).status, "succeeded");
  });
});
