import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForTerminal, waitForStatus } from "../../helpers/wait-for-event.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";

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

// The invocation row is written after the parent's step resolves — reading it right after runFlow() can race that write.
async function waitForInvocationIds(nodeExecutionsStore, flowId, nodeId, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    const ids = await nodeExecutionsStore.listInvocationIdsForNode(flowId, nodeId);
    if (ids.length > 0) return ids;
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${nodeId} to have an invocation row`);
    }
    await new Promise((r) => setTimeout(r, 10));
  }
}

async function makeHarness(pipelineDef) {
  const dbPath = tmpPath("e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });
  const pipelinesRef = { value: new Map(Object.entries(pipelineDef)) };

  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store, pipelinesRef, ensurePipelinesLoaded: null, logger,
    getWorkingSubagent: null, failureStore: null, nodeHealthStore: null,
    notifyFlowEnteredWaiting: null, sessionIndex: null,
    nodeExecutionsStore, runStore,
    admissionQueueStore: makeAdmissionQueueStore(),
  });
  resumeParentRef.resume = executor.resume;

  const gateResolver = createGateResolver({
    api: { logger }, store, executor, runStore, nodeExecutionsStore,
    pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
  });

  return { runStore, nodeExecutionsStore, store, executor, gateResolver, pipelinesRef };
}

let draftCalls;
function buildPipelines() {
  draftCalls = [];
  const PROPOSE_NODE = {
    kind: "subflow",
    childFlows: true,
    mode: "any",
    selector: (state) => state.shortlist ?? [],
    itemKey: "id",
    flow: {
      nodes: {
        DRAFT: {
          kind: "code",
          needs: { scan: { hops: 1, nodeId: "SCAN" } },
          run: async (state, ctx) => {
            const scan = await ctx.needs("scan");
            draftCalls.push(scan);
            return { DRAFT: { text: `drafted from ${JSON.stringify(scan)}` } };
          },
        },
      },
      edges: { __start__: "DRAFT", DRAFT: "__end__" },
    },
  };
  return {
    test_needs_parent: {
      id: "test_needs_parent",
      state: {},
      nodes: {
        SCAN: { kind: "code", run: () => ({ SCAN: { listings: ["a", "b"] }, shortlist: [{ id: "x" }] }) },
        PROPOSE: PROPOSE_NODE,
      },
      edges: { __start__: "SCAN", SCAN: "PROPOSE", PROPOSE: "__end__" },
    },
    "test_needs_parent::PROPOSE": {
      id: "test_needs_parent::PROPOSE",
      state: {},
      nodes: PROPOSE_NODE.flow.nodes,
      edges: PROPOSE_NODE.flow.edges,
    },
  };
}

describe('"needs:" — real dispatch loop, cross-boundary hops:1 read', () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("happy path: SCAN completes before PROPOSE spawns its child — DRAFT resolves needs:{hops:1,SCAN} immediately and succeeds", async () => {
    harness = await makeHarness(buildPipelines());
    const { executor, runStore } = harness;

    const record = await executor.start("test_needs_parent", {}, { kind: "test" });
    await executor.runFlow(record.flowId, "fresh");

    const parent = await waitForStatus(runStore, record.flowId, (r) => r.status === "succeeded" || r.status === "failed");
    assert.equal(parent.status, "succeeded", "SCAN already complete by the time PROPOSE dispatches — nothing should ever block");

    assert.equal(draftCalls.length, 1, "DRAFT's run() must have been called exactly once — never dispatched while pending");
    assert.deepEqual(draftCalls[0], { listings: ["a", "b"] }, "DRAFT read the real parent SCAN value via _needs, not a cloned copy");
  });

  test("not-ready: DRAFT blocks with needs-not-ready (never calls run()) while its declared dependency is still in flight, then resolves once it completes", async () => {
    harness = await makeHarness(buildPipelines());
    const { executor, runStore, store, nodeExecutionsStore } = harness;

    const record = await executor.start("test_needs_parent", {}, { kind: "test" });
    const enterResult = await nodeExecutionsStore.enter({ flowId: record.flowId, pipelineId: "test_needs_parent", nodeId: "SCAN", subflowKey: null, kind: "fresh" });
    const seed = await store.get(record.flowId);
    await store.setRunning({ flowId: record.flowId, expectedRevision: seed.revision, lastStep: "SCAN", stateJson: { ...seed.stateJson, shortlist: [{ id: "x" }] } });
    await executor.runFlow(record.flowId, "retry");
    const invocationIds = await waitForInvocationIds(nodeExecutionsStore, record.flowId, "PROPOSE");
    assert.ok(invocationIds.length >= 1, "PROPOSE must have spawned its one child despite SCAN not being complete — needs: blocking is DRAFT's problem, not PROPOSE's");
    const children = await runStore.listForParentInvocations(invocationIds);
    assert.equal(children.length, 1);
    const childFlowId = children[0].flowId;

    const child = await waitForStatus(runStore, childFlowId, (r) => r.status === "blocked");
    assert.equal(child.status, "blocked", "DRAFT must block rather than dispatch against a still-pending SCAN");
    assert.equal(child.waitJson.kind, "needs-not-ready");
    assert.equal(child.waitJson.needed[0], "SCAN");
    assert.equal(draftCalls.length, 0, "DRAFT's run() must NEVER have been called while its declared need was pending");

    await nodeExecutionsStore.writeResult(enterResult.invocationId, { SCAN: { listings: ["late"] } });
    const blockedChild = await store.get(childFlowId);
    await store.setRunning({ flowId: childFlowId, expectedRevision: blockedChild.revision, lastStep: blockedChild.lastStep, stateJson: blockedChild.stateJson });
    await executor.resume(childFlowId);
    const childAfter = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(childAfter.status, "succeeded");
    assert.equal(draftCalls.length, 1, "DRAFT's run() must have been called exactly once now that its need resolved");
    assert.deepEqual(draftCalls[0], { listings: ["late"] });
  });
});

let submitCalls;
function buildNestedPipelines() {
  submitCalls = [];
  const SUBMIT_NODE = {
    kind: "subflow",
    childFlows: true,
    mode: "any",
    selector: (state) => [{ id: "only" }],
    itemKey: "id",
    flow: {
      nodes: {
        SUBMIT_CODE: {
          kind: "code",
          needs: { runId: { hops: 2, entry: "runId" } },
          run: async (state, ctx) => {
            const runId = await ctx.needs("runId");
            submitCalls.push(runId);
            return { SUBMIT_CODE: { token: `token-for-${runId}` } };
          },
        },
      },
      edges: { __start__: "SUBMIT_CODE", SUBMIT_CODE: "__end__" },
    },
  };
  const PROPOSE_NODE = {
    kind: "subflow",
    childFlows: true,
    mode: "any",
    selector: (state) => state.shortlist ?? [],
    itemKey: "id",
    flow: {
      nodes: { SUBMIT: SUBMIT_NODE },
      edges: { __start__: "SUBMIT", SUBMIT: "__end__" },
    },
  };
  return {
    test_needs_nested: {
      id: "test_needs_nested",
      state: {},
      nodes: { PROPOSE: PROPOSE_NODE },
      edges: { __start__: "PROPOSE", PROPOSE: "__end__" },
    },
    "test_needs_nested::PROPOSE": {
      id: "test_needs_nested::PROPOSE",
      state: {},
      nodes: PROPOSE_NODE.flow.nodes,
      edges: PROPOSE_NODE.flow.edges,
    },
    "test_needs_nested::PROPOSE::SUBMIT": {
      id: "test_needs_nested::PROPOSE::SUBMIT",
      state: {},
      nodes: SUBMIT_NODE.flow.nodes,
      edges: SUBMIT_NODE.flow.edges,
    },
  };
}

describe('"needs:" — entry-based hops:2 read (root\'s own runId, real PROPOSE>SUBMIT nesting shape)', () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("SUBMIT_CODE, nested two levels deep, resolves the TRUE ROOT's own runId — not PROPOSE's inherited copy, not undefined", async () => {
    harness = await makeHarness(buildNestedPipelines());
    const { executor, runStore, store } = harness;

    const record = await executor.start("test_needs_nested", {}, { kind: "test" });
    const rootFlow = await runStore.get(record.flowId);
    const realRunId = rootFlow.stateJson.runId;
    assert.ok(realRunId, "sanity: the root flow really does have its own runId, set once at start()");

    const seed = await store.get(record.flowId);
    await store.setRunning({ flowId: record.flowId, expectedRevision: seed.revision, lastStep: seed.lastStep, stateJson: { ...seed.stateJson, shortlist: [{ id: "x" }] } });
    await executor.runFlow(record.flowId, "fresh");
    const parent = await waitForStatus(runStore, record.flowId, (r) => r.status === "succeeded" || r.status === "failed");
    assert.equal(parent.status, "succeeded");
    assert.equal(submitCalls.length, 1, "SUBMIT_CODE's run() must have been called exactly once");
    assert.equal(submitCalls[0], realRunId, "resolved value must be the TRUE ROOT's runId, reached by walking 2 hops from the doubly-nested SUBMIT_CODE");
  });
});

let sameFlowCalls;
function buildSameFlowPipeline() {
  sameFlowCalls = [];
  return {
    test_needs_hop0: {
      id: "test_needs_hop0",
      state: { sourceFilter: { includeAgentSelfRediscovery: true } },
      nodes: {
        WALK: { kind: "code", run: () => ({ WALK: { sessions: ["s1", "s2"] } }) },
        EXTRACT: {
          kind: "code",
          needs: {
            walkResult: { hops: 0, nodeId: "WALK" },
            sourceFilter: { hops: 0, config: "sourceFilter" },
          },
          run: async (state, ctx) => {
            const [walkResult, sourceFilter] = await Promise.all([ctx.needs("walkResult"), ctx.needs("sourceFilter")]);
            sameFlowCalls.push({ sessions: walkResult.sessions, sourceFilter });
            return { extracted: true };
          },
        },
      },
      edges: { __start__: "WALK", WALK: "EXTRACT", EXTRACT: "__end__" },
    },
  };
}

describe('"needs:" — hops:0 (same-flow), both nodeId and config shapes', () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("EXTRACT resolves WALK's real sessions AND the pipeline's declared sourceFilter, even though neither survives in the persisted stateJson after WALK", async () => {
    harness = await makeHarness(buildSameFlowPipeline());
    const { executor, runStore } = harness;

    const record = await executor.start("test_needs_hop0", {}, { kind: "test" });
    await executor.runFlow(record.flowId, "fresh");

    const flow = await waitForStatus(runStore, record.flowId, (r) => r.status === "succeeded" || r.status === "failed");
    assert.equal(flow.status, "succeeded");
    assert.equal(sameFlowCalls.length, 1, "EXTRACT's run() must have been called exactly once");
    assert.deepEqual(sameFlowCalls[0].sessions, ["s1", "s2"], "resolved WALK's own resultJson via nodeId-based needs at hops:0, no walk");
    assert.deepEqual(sameFlowCalls[0].sourceFilter, { includeAgentSelfRediscovery: true }, "resolved the pipeline-declared config live off the definition, not off stateJson");

    assert.equal(flow.stateJson?.sourceFilter, undefined, "sourceFilter must be absent from persisted stateJson — the whole point of routing this through config-type needs() instead");
  });
});
