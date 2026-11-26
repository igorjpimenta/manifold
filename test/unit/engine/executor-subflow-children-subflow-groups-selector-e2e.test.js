// Guards a real bug: executeSubflowNode never enriched state before node.selector(state), so selectors reading state._subflowGroups saw undefined and silently no-op'd.
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
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

function makeScoreLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "id",
    selector: (state) => state.candidates ?? [],
    flow: {
      nodes: {
        RATE: {
          kind: "code",
          run: (state) => {
            const item = state.item;
            return { RATE: { score: item.value * 2 } };
          },
        },
      },
      edges: {
        __start__: "RATE",
        RATE: () => "__end__",
      },
    },
  };
}

function makeSelectNextLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "id",
    selector: (state) =>
      Object.entries(state._subflowGroups?.SCORE ?? {})
        .filter(([, branch]) => branch?.data?.RATE?.score > 5)
        .map(([id, branch]) => ({ id, score: branch.data.RATE.score })),
    flow: {
      nodes: {
        PICK: {
          kind: "code",
          run: (state) => ({ PICK: { picked: true, score: state.item.score } }),
        },
      },
      edges: {
        __start__: "PICK",
        PICK: () => "__end__",
      },
    },
  };
}

async function makeHarness({ scoreNode = makeScoreLikeNode() } = {}) {
  const dbPath = tmpPath("branch-groups-selector-e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const SCORE_NODE = scoreNode;
  const SELECT_NEXT_NODE = makeSelectNextLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { SCORE: SCORE_NODE, SELECT_NEXT: SELECT_NEXT_NODE },
    edges: { __start__: "SCORE", SCORE: "SELECT_NEXT", SELECT_NEXT: "__end__" },
  };
  const scoreChildPipeline = {
    id: "test_parent::SCORE",
    state: {},
    nodes: SCORE_NODE.flow.nodes,
    edges: SCORE_NODE.flow.edges,
  };
  const selectNextChildPipeline = {
    id: "test_parent::SELECT_NEXT",
    state: {},
    nodes: SELECT_NEXT_NODE.flow.nodes,
    edges: SELECT_NEXT_NODE.flow.edges,
  };
  const pipelinesRef = {
    value: new Map([
      ["test_parent", parentPipeline],
      ["test_parent::SCORE", scoreChildPipeline],
      ["test_parent::SELECT_NEXT", selectNextChildPipeline],
    ]),
  };

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

async function childFlowIds(harness, parentFlowId, nodeId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, nodeId);
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedCandidates(harness, flowId, candidates) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, candidates },
  });
}

describe("childFlows:true loop selector reading a prior loop node's state._subflowGroups, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("SELECT_NEXT's selector sees SCORE's real per-branch results and spawns children for qualifying ids", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [
      { id: "job-1", value: 1 },  // RATE.score = 2, below threshold (5)
      { id: "job-2", value: 4 },  // RATE.score = 8
      { id: "job-3", value: 6 },  // RATE.score = 12
    ]);
    await executor.runFlow(record.flowId);
    // Two loop stages, each needs its own flush for children to run + parent to auto-resume.
    await flush();
    await flush();
    await flush();

    const scoreChildIds = await childFlowIds(harness, record.flowId, "SCORE");
    assert.equal(scoreChildIds.length, 3, "all 3 candidates spawn a SCORE child");
    for (const id of scoreChildIds) assert.equal((await runStore.get(id)).status, "succeeded");

    const selectChildIds = await childFlowIds(harness, record.flowId, "SELECT_NEXT");
    assert.equal(selectChildIds.length, 2, "SELECT_NEXT's selector correctly read state._subflowGroups.SCORE and picked the 2 qualifying ids");
    const pickedScores = (await Promise.all(selectChildIds.map(async (id) => {
      const r = await runStore.get(id);
      const rState = await assembleState(r.flowId, { runStore, nodeExecutionsStore, baseState: r.stateJson });
      return rState.PICK?.score;
    }))).sort((a, b) => a - b);
    assert.deepEqual(pickedScores, [8, 12]);

    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("one SCORE branch failing does not suppress the others from reaching SELECT_NEXT", async () => {
    const throwingScoreNode = {
      kind: "subflow",
      childFlows: true,
      itemKey: "id",
      selector: (state) => state.candidates ?? [],
      flow: {
        nodes: {
          RATE: {
            kind: "code",
            run: (state) => {
              const item = state.item;
              if (item.shouldFail) throw new Error("simulated subagent timeout");
              return { RATE: { score: item.value * 2 } };
            },
          },
        },
        edges: { __start__: "RATE", RATE: () => "__end__" },
      },
    };
    harness = await makeHarness({ scoreNode: throwingScoreNode });
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [
      { id: "job-a", value: 10, shouldFail: false }, // score 20
      { id: "job-b", value: 10, shouldFail: true },  // score 20, throws
      { id: "job-c", value: 10, shouldFail: false }, // score 20
    ]);
    await executor.runFlow(record.flowId);
    await flush();
    await flush();
    await flush();

    const scoreChildIds = await childFlowIds(harness, record.flowId, "SCORE");
    assert.equal(scoreChildIds.length, 3);
    const statuses = (await Promise.all(scoreChildIds.map((id) => runStore.get(id)))).map((r) => r.status).sort();
    assert.deepEqual(statuses, ["failed", "succeeded", "succeeded"], "1 branch fails, 2 succeed — matches the live incident's 9-succeed/1-timeout shape");

    const selectChildIds = await childFlowIds(harness, record.flowId, "SELECT_NEXT");
    assert.equal(selectChildIds.length, 2, "the 1 failed SCORE branch does not suppress the 2 successful ones from reaching SELECT_NEXT");
  });

  test("getAllSubflowGroups resolves under groups.SCORE with every branch's data intact, independent of the enrichment fix", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ id: "job-1", value: 3 }]);
    await executor.runFlow(record.flowId);
    await flush();
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.SCORE);
    assert.equal(groups.SCORE["job-1"]?.data?.RATE?.score, 6);
  });
});
