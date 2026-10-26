/**
 * Unit tests for the run-detail node tree endpoints:
 *   GET /manifold/runs/:flowId/tree
 *   GET /manifold/runs/:flowId/tree/expand?type=node&invocationId=X
 *   GET /manifold/runs/:flowId/tree/expand?type=flow&childFlowId=Y
 *   GET /manifold/runs/:flowId/tree/invocation/:invocationId
 */
import { test, describe, mock } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../plugins/openclaw/manifold/auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
  },
});

let listInvocationsForFlowImpl = () => [];
let getInvocationImpl = () => null;
let listExecutionsForInvocationImpl = () => [];
let hasInvocationsImpl = () => new Set();
// Phase 1.5:
// flow_node_executions carries no error content at all — buildTreeNodeRows/
// sendInvocationDetail cross-reference failureStore.queryErrorsByFlow
// instead. ErrorEntry-shaped rows (executionId/invocationId/message) tests
// seed here to control which rows read as "failed".
let queryErrorsByFlowImpl = () => [];
// A subflow-kind row's own invocation (subflow_key=null) is never a child's
// parent_invocation_id — each branch item gets its own separate invocation.
// Tests simulate this by returning the REAL per-item invocation ids here,
// distinct from whatever id the row itself carries.
let listInvocationIdsForNodeImpl = () => [];
const nodeExecutionsUrl = new URL("../../../state/node-executions.js", import.meta.url).href;
await mock.module(nodeExecutionsUrl, {
  exports: {
    getNodeExecutionsStore: () => ({
      listInvocationsForFlow: (flowId) => listInvocationsForFlowImpl(flowId),
      getInvocation: (id) => getInvocationImpl(id),
      listExecutionsForInvocation: (id) => listExecutionsForInvocationImpl(id),
      hasInvocations: (flowIds) => hasInvocationsImpl(flowIds),
      listInvocationIdsForNode: (flowId, nodeId) => listInvocationIdsForNodeImpl(flowId, nodeId),
      listInvocationIdsForNodes: async (flowId, nodeIds) => {
        const out = new Map();
        for (const nodeId of (nodeIds ?? [])) {
          const ids = await listInvocationIdsForNodeImpl(flowId, nodeId);
          if (ids?.length) out.set(nodeId, ids);
        }
        return out;
      },
      queryErrorsByFlow: () => [],
    }),
  },
});

const { registerRunRoutes } = await import("../../../routes/runs.js");

function authReq(method, path) {
  return createMockReq(method, path, { authorization: "Bearer x" });
}

function prefixHandler(routes) {
  return routes.find((r) => r.path === "/manifold/runs/" && r.match === "prefix").handler;
}

function resetMocks() {
  listInvocationsForFlowImpl = () => [];
  getInvocationImpl = () => null;
  listExecutionsForInvocationImpl = () => [];
  hasInvocationsImpl = () => new Set();
  listInvocationIdsForNodeImpl = () => [];
  queryErrorsByFlowImpl = () => [];
}

// Most tests just want kind/agent resolved from whatever's currently in
// pipelinesRef, no real SCD2 time-travel — this mock reads `pipelinesRef`
// live (late-bound via the ref object itself, not a snapshot of .value at
// construction time) so it stays in sync with whatever a test sets
// pipelinesRef.value to, before or after makeDeps() is called. `atMs` is
// ignored here; the one test that needs genuine point-in-time behavior
// (a node whose live definition has since changed/vanished) overrides
// pipelineRegistryStore directly instead of using this shared mock.
function makeMockPipelineRegistryStore(pipelinesRef) {
  return {
    getNodeVersionsBatch: async (requests) => requests.map(({ pipelineId, nodeName }) => {
      const node = pipelinesRef.value.get(pipelineId)?.nodes?.[nodeName];
      return node ? { kind: node.kind, agent: node.agent ?? null } : null;
    }),
    getNodeVersionAt: async ({ pipelineId, nodeName }) => {
      const node = pipelinesRef.value.get(pipelineId)?.nodes?.[nodeName];
      return node ? { kind: node.kind, agent: node.agent ?? null } : null;
    },
    getPipelineVersionAt: async ({ pipelineId }) => {
      const pipeline = pipelinesRef.value.get(pipelineId);
      return pipeline ? { kind: pipeline.kind ?? null, allowConcurrent: pipeline.allowConcurrent ?? null } : null;
    },
  };
}

function makeDeps(api, overrides = {}) {
  const pipelinesRef = overrides.pipelinesRef ?? { value: new Map() };
  return {
    api,
    readStore: { list: () => [], get: () => null, listQueued: () => [] },
    costForAllPipelines: async () => ({ pipelines: [], sinceMs: null }),
    runStore: { listForParentInvocations: () => [] },
    pipelinesRef,
    pipelineRegistryStore: makeMockPipelineRegistryStore(pipelinesRef),
    invokeElevated: async () => ({}),
    auth: {
      authorizeBridgeRequest: async () => ({ ok: true }),
      sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
    },
    failureStore: { queryErrorsByFlow: async (flowId) => queryErrorsByFlowImpl(flowId) },
    discussStore: { getComponent: () => null, listComponents: () => [], getBulkSummaries: () => ({}) },
    handleDiscuss:           async () => {},
    handleDiscussHistory:    async () => {},
    handleDiscussApplyDraft: async () => {},
    handleDiscussAudio:      async () => {},
    handleDiscussAudioGet:   async () => {},
    ...overrides,
  };
}

const FIXTURE_PIPELINE = {
  id: "team_pipe",
  nodes: {
    CACHE_CHECK: { kind: "code" },
    SCORE_LLM: { kind: "agent", agent: "scorer" },
    PROPOSE: { kind: "subflow" },
  },
};

// A flow-level kind:"cascade" pipeline (README.md's "retry-on-failure as a
// flow characteristic") — its own root nodes are tried in sequence until
// one succeeds or all are exhausted, no dedicated node kind involved.
const CASCADE_PIPELINE = {
  id: "team_cascade",
  kind: "cascade",
  nodes: {
    CODE_LOGIN: { kind: "code" },
    AGENT_LOGIN: { kind: "agent", agent: "login-agent" },
  },
};

// ── /tree root ──────────────────────────────────────────────────────────────

describe("GET /manifold/runs/:flowId/tree", () => {
  test("maps invocations onto node rows with kind/agent resolved from the pipeline definition", async () => {
    resetMocks();
    const flow = {
      flowId: "flow-a", status: "succeeded", currentStep: null,
      stateJson: { pipelineId: "team_pipe" },
    };
    listInvocationsForFlowImpl = (flowId) => flowId === "flow-a" ? [
      { invocationId: "inv-1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: "exec-1", kind: "fresh" },
      { invocationId: "inv-2", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 1, enteredAt: 200, exitedAt: 250, latestExecutionId: "exec-2", kind: "fresh" },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-a" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-a/tree"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.items.length, 2);
    assert.equal(body.items[0].nodeId, "CACHE_CHECK");
    assert.equal(body.items[0].kind, "code");
    assert.equal(body.items[0].agent, null);
    assert.equal(body.items[0].status, "succeeded");
    assert.equal(body.items[0].hasChildren, false);
    assert.equal(body.items[1].nodeId, "SCORE_LLM");
    assert.equal(body.items[1].kind, "agent");
    assert.equal(body.items[1].agent, "scorer");
    assert.equal(body.items[1].execKind, "fresh");
  });

  test("an execution with error != null is status:failed regardless of position", async () => {
    resetMocks();
    const flow = { flowId: "flow-b", status: "failed", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: "exec-1", kind: "fresh" },
      { invocationId: "inv-2", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 2, enteredAt: 200, exitedAt: 210, latestExecutionId: "exec-2", kind: "retry" },
    ];
    queryErrorsByFlowImpl = () => [{ nodeId: "SCORE_LLM", subflowKey: null, message: "boom", at: 210, executionId: "exec-2", invocationId: "inv-2" }];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-b" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-b/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[1].status, "failed");
    assert.equal(body.items[1].executionCount, 2);
  });

  test("the LAST invocation with no error mirrors the flow's own live status (running/blocked/queued) — independent of any currentStep field", async () => {
    resetMocks();
    // Real flow_runs rows never carry a currentStep field (column dropped
    // in migration v7, state/run-store.js "Phase 2.5") — deliberately
    // omitted here so this test can't silently pass for the wrong reason
    // the way the old version did (it hand-built a currentStep field the
    // real code path never receives, so it kept passing even after the
    // comparison it was meant to exercise became permanently dead code).
    const flow = { flowId: "flow-c", status: "blocked", stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
      { invocationId: "inv-2", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 1, enteredAt: 200, exitedAt: null, latestExecutionId: null, kind: "fresh" },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-c" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-c/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].status, "succeeded", "earlier, already-completed node stays succeeded");
    assert.equal(body.items[1].status, "blocked", "most-recently-invoked node mirrors the flow's own current status");
  });

  test("the LAST invocation on a TERMINAL flow (failed) mirrors that terminal status too, not just running/blocked/queued/skipped", async () => {
    // A terminal flow whose last node's own execution recorded no error must
    // still mirror the flow's terminal status — defaulting to "succeeded"
    // would show a green row on a flow the operator can plainly see failed.
    resetMocks();
    const flow = { flowId: "flow-terminal-fail", status: "failed", stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
      { invocationId: "inv-2", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 1, enteredAt: 200, exitedAt: 210, latestExecutionId: null, kind: "fresh" },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-terminal-fail" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-terminal-fail/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].status, "succeeded", "earlier, already-completed node stays succeeded");
    assert.equal(body.items[1].status, "failed", "last node mirrors the flow's own terminal status even though its own execution recorded no error");
  });

  test("the LAST invocation on a flow.status==='skipped' flow is labeled skipped", async () => {
    resetMocks();
    const flow = { flowId: "flow-d", status: "skipped", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-d" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-d/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].status, "skipped");
  });

  test("a subflow-kind node with spawned children gets hasChildren:true and a shallow status rollup", async () => {
    resetMocks();
    const flow = { flowId: "flow-e", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    // inv-propose is the node's OWN dispatch (subflow_key=null) — its id is
    // NEVER a child's parent_invocation_id. Children point at the separate
    // per-item invocations (inv-item-1/2/3), resolved via
    // listInvocationIdsForNode(flowId, nodeId), not via inv-propose's id.
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-propose", nodeId: "PROPOSE", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 500, latestExecutionId: null, kind: "fresh" },
    ];
    listInvocationIdsForNodeImpl = (flowId, nodeId) =>
      flowId === "flow-e" && nodeId === "PROPOSE" ? ["inv-item-1", "inv-item-2", "inv-item-3"] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-e" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: (ids) => ids.includes("inv-item-1") ? [
          { flowId: "child-1", parentInvocationId: "inv-item-1", status: "succeeded" },
          { flowId: "child-2", parentInvocationId: "inv-item-2", status: "failed" },
          { flowId: "child-3", parentInvocationId: "inv-item-3", status: "running" },
        ] : [],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-e/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].hasChildren, true);
    assert.deepEqual(body.items[0].rollup, { succeeded: 1, failed: 1, pending: 1, skipped: 0, cancelled: 0 });
  });

  test("a flow with a subflow node scored via multiple item invocations shows ONE row, not one per item", async () => {
    resetMocks();
    const flow = { flowId: "flow-multi", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-score-toplevel", nodeId: "PROPOSE", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 500, latestExecutionId: null, kind: "fresh" },
    ];
    listInvocationIdsForNodeImpl = () => ["inv-item-1", "inv-item-2", "inv-item-3"];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-multi" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: () => [
          { flowId: "child-1", parentInvocationId: "inv-item-1", status: "succeeded" },
          { flowId: "child-2", parentInvocationId: "inv-item-2", status: "succeeded" },
          { flowId: "child-3", parentInvocationId: "inv-item-3", status: "succeeded" },
        ],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-multi/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items.length, 1, "one row for the node dispatch, not one per item");
    assert.equal(body.items[0].nodeId, "PROPOSE");
    assert.equal(body.items[0].rollup.succeeded, 3);
  });

  test("a subflow-kind node with zero spawned children gets hasChildren:false and no rollup key", async () => {
    resetMocks();
    const flow = { flowId: "flow-f", status: "running", currentStep: "PROPOSE", stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-propose", nodeId: "PROPOSE", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: null, latestExecutionId: null, kind: "fresh" },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-f" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
      runStore: { listForParentInvocations: () => [] },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-f/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].hasChildren, false);
    assert.deepEqual(body.items[0].rollup, { succeeded: 0, failed: 0, pending: 0, skipped: 0, cancelled: 0 });
  });

  // everRetriedOrRevived. listInvocationsForFlow rows already carry this field
  // (see state/node-executions.test.js); buildTreeNodeRows just threads it
  // through onto the row unchanged.
  test("everRetriedOrRevived passes through from the invocation row onto the node row", async () => {
    resetMocks();
    const flow = { flowId: "flow-h", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "PROPOSE", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 3, enteredAt: 300, exitedAt: 310, latestExecutionId: "exec-3", kind: "recheck", everRetriedOrRevived: true },
      { invocationId: "inv-2", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 400, executionCount: 1, enteredAt: 400, exitedAt: 410, latestExecutionId: "exec-4", kind: "fresh", everRetriedOrRevived: false },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-h" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-h/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].execKind, "recheck", "sanity: latest kind masks the real retry, as documented");
    assert.equal(body.items[0].everRetriedOrRevived, true, "the independent field still surfaces it");
    assert.equal(body.items[1].everRetriedOrRevived, false);
  });

  test("kind/agent are null when the pipeline definition is unavailable (not yet loaded)", async () => {
    resetMocks();
    const flow = { flowId: "flow-g", status: "succeeded", currentStep: null, stateJson: { pipelineId: "unknown_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "SOME_NODE", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-g" ? flow : null },
      pipelinesRef: { value: new Map() },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-g/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].kind, null);
    assert.equal(body.items[0].agent, null);
    assert.equal(body.items[0].hasChildren, false, "unresolvable kind can't be subflow, so never expandable");
  });

  // Real evidence this once actually happened in production: a flow ran
  // and rendered correctly, its pipeline's directory got deleted
  // afterward (or a node renamed/its kind changed), and the SAME flow's
  // tree silently degraded to kind:null with no error. pipelineRegistryStore
  // is what's supposed to prevent that — this row's kind/agent must come
  // from the historical snapshot, not from pipelinesRef (which here has
  // nothing at all for this pipeline, simulating the deleted-directory case).
  test("a node whose pipeline definition has since changed/vanished still renders its ORIGINAL kind/agent, not null", async () => {
    resetMocks();
    const flow = { flowId: "flow-h", status: "succeeded", currentStep: null, stateJson: { pipelineId: "deleted_pipe" } };
    listInvocationsForFlowImpl = () => [
      { invocationId: "inv-1", nodeId: "SUBMIT", subflowKey: null, visitSeq: 1, invokedAt: 5000, executionCount: 1, enteredAt: 5000, exitedAt: 5010, latestExecutionId: null, kind: "fresh" },
      { invocationId: "inv-2", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 6000, executionCount: 1, enteredAt: 6000, exitedAt: 6010, latestExecutionId: null, kind: "fresh" },
    ];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-h" ? flow : null },
      // pipelinesRef has NOTHING for this pipeline at all — the directory
      // is gone, exactly like the design doc's own repro.
      pipelinesRef: { value: new Map() },
      // pipelineRegistryStore still remembers what these nodes WERE at
      // their own dispatch time (invokedAt 5000/6000) — the whole point
      // of keeping SCD2 history independent of the live, mutable Map.
      pipelineRegistryStore: {
        getNodeVersionsBatch: async (requests) => requests.map((r) => {
          if (r.pipelineId !== "deleted_pipe") return null;
          if (r.nodeName === "SUBMIT") return { kind: "subflow", agent: null };
          if (r.nodeName === "SCORE_LLM") return { kind: "agent", agent: "scorer" };
          return null;
        }),
        getNodeVersionAt: async () => null,
        getPipelineVersionAt: async () => null,
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-h/tree"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].kind, "subflow", "must render the historical kind, not fall back to null just because the live definition is gone");
    assert.equal(body.items[1].kind, "agent");
    assert.equal(body.items[1].agent, "scorer", "agent field must also survive from the historical snapshot");
  });

  test("returns { items: [] } for a flow with no invocations yet", async () => {
    resetMocks();
    const flow = { flowId: "flow-h", status: "running", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-h" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-h/tree"), res);
    assert.equal(res.statusCode, 200);
    assert.deepEqual(JSON.parse(res.body).items, []);
  });
});

// ── /tree/expand ────────────────────────────────────────────────────────────

describe("GET /manifold/runs/:flowId/tree/expand", () => {
  test("type=node returns the spawned child flows as type:flow rows", async () => {
    resetMocks();
    const flow = { flowId: "flow-i", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    // inv-propose is the node's OWN dispatch (subflow_key=null) — the
    // endpoint must resolve the real per-item invocation ids via
    // listInvocationIdsForNode before calling listForParentInvocations,
    // not pass inv-propose's id straight through.
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-i", nodeId: "PROPOSE", subflowKey: null, visitSeq: 1, invokedAt: 100 } : null;
    listInvocationIdsForNodeImpl = (flowId, nodeId) =>
      flowId === "flow-i" && nodeId === "PROPOSE" ? ["inv-item-1"] : [];
    hasInvocationsImpl = (flowIds) => new Set(flowIds.includes("child-1") ? ["child-1"] : []);
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-i" ? flow : null },
      runStore: {
        listForParentInvocations: (ids) => ids.includes("inv-item-1") ? [
          { flowId: "child-1", status: "succeeded", createdAt: 10, updatedAt: 20, endedAt: 20, stateJson: { _display: { title: "Listing A" } } },
        ] : [],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-i/tree/expand?type=node&invocationId=inv-propose"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.items.length, 1);
    assert.equal(body.items[0].type, "flow");
    assert.equal(body.items[0].flowId, "child-1");
    assert.equal(body.items[0].status, "succeeded");
    assert.equal(body.items[0].display.title, "Listing A");
    assert.equal(body.items[0].hasChildren, true);
    assert.equal(body.items[0].reason, null, "succeeded rows never surface a reason, even if statusReason is stale-populated");
  });

  test("cascadeAttempt: a cascade-kind child that fell through to its 2nd root node reports cascadeAttempt:2", async () => {
    resetMocks();
    const flow = { flowId: "flow-cascade", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-cascade", nodeId: "PROPOSE" } : null;
    listInvocationsForFlowImpl = (flowId) => flowId === "child-cascade" ? [
      { invocationId: "inv-c1", nodeId: "CODE_LOGIN", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, error: "unavailable", kind: "fresh" },
      { invocationId: "inv-c2", nodeId: "AGENT_LOGIN", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 1, enteredAt: 200, exitedAt: 250, latestExecutionId: null, kind: "fresh" },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-cascade" ? flow : null },
      pipelinesRef: { value: new Map([["team_cascade", CASCADE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-cascade", status: "succeeded", createdAt: 10, updatedAt: 20, endedAt: 20,
          stateJson: { pipelineId: "team_cascade" },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-cascade/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].cascadeAttempt, 2);
  });

  test("cascadeAttempt: null when a cascade-kind child succeeds on its first root node", async () => {
    resetMocks();
    const flow = { flowId: "flow-cascade-ok", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-cascade-ok", nodeId: "PROPOSE" } : null;
    listInvocationsForFlowImpl = (flowId) => flowId === "child-cascade-ok" ? [
      { invocationId: "inv-c1", nodeId: "CODE_LOGIN", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-cascade-ok" ? flow : null },
      pipelinesRef: { value: new Map([["team_cascade", CASCADE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-cascade-ok", status: "succeeded", createdAt: 10, updatedAt: 20, endedAt: 20,
          stateJson: { pipelineId: "team_cascade" },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-cascade-ok/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].cascadeAttempt, null);
  });

  // everRetriedOrRevived on a type:"flow" row — a SPAWNED CHILD's own
  // retry, independent of both cascadeAttempt (root-node fallback count)
  // and the subflow NODE's own row above — the parent node dispatching
  // cleanly doesn't mean none of its children ever retried.
  test("everRetriedOrRevived: true when the child's own root invocation history includes a retry", async () => {
    resetMocks();
    const flow = { flowId: "flow-child-retry", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-child-retry", nodeId: "PROPOSE" } : null;
    listInvocationsForFlowImpl = (flowId) => flowId === "child-retried" ? [
      { invocationId: "inv-c1", nodeId: "SUBMIT_CODE", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 2, enteredAt: 200, exitedAt: 210, latestExecutionId: null, kind: "retry", everRetriedOrRevived: true },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-child-retry" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-retried", status: "succeeded", createdAt: 10, updatedAt: 20, endedAt: 20,
          stateJson: { pipelineId: "team_pipe" },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-child-retry/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].everRetriedOrRevived, true);
  });

  test("everRetriedOrRevived: false when the child dispatched clean, even alongside an unrelated cascadeAttempt", async () => {
    resetMocks();
    const flow = { flowId: "flow-child-clean", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-child-clean", nodeId: "PROPOSE" } : null;
    listInvocationsForFlowImpl = (flowId) => flowId === "child-cascade-2" ? [
      { invocationId: "inv-c1", nodeId: "CODE_LOGIN", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh", everRetriedOrRevived: false },
      { invocationId: "inv-c2", nodeId: "AGENT_LOGIN", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 1, enteredAt: 200, exitedAt: 250, latestExecutionId: null, kind: "fresh", everRetriedOrRevived: false },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-child-clean" ? flow : null },
      pipelinesRef: { value: new Map([["team_cascade", CASCADE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-cascade-2", status: "succeeded", createdAt: 10, updatedAt: 20, endedAt: 20,
          stateJson: { pipelineId: "team_cascade" },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-child-clean/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].cascadeAttempt, 2, "sanity: this child DID need a 2nd root node");
    assert.equal(body.items[0].everRetriedOrRevived, false, "but no individual node within it ever retried — a different signal");
  });

  test("cascadeAttempt: null for a non-cascade child even with multiple root invocations (ordinary sequential nodes, not fallback alternatives)", async () => {
    resetMocks();
    const flow = { flowId: "flow-plain", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-plain", nodeId: "PROPOSE" } : null;
    listInvocationsForFlowImpl = (flowId) => flowId === "child-plain" ? [
      { invocationId: "inv-p1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
      { invocationId: "inv-p2", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 200, executionCount: 1, enteredAt: 200, exitedAt: 250, latestExecutionId: null, kind: "fresh" },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-plain" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-plain", status: "succeeded", createdAt: 10, updatedAt: 20, endedAt: 20,
          stateJson: { pipelineId: "team_pipe" },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-plain/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].cascadeAttempt, null);
  });

  test("reason: a human-gate-blocked child surfaces waitJson's rich item title, not the generic statusReason", async () => {
    resetMocks();
    const flow = { flowId: "flow-gate", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-gate", nodeId: "PROPOSE" } : null;
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-gate" ? flow : null },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-blocked", status: "blocked", createdAt: 10, updatedAt: 20, endedAt: null,
          stateJson: { _display: { title: "blockchain listing" } },
          statusReason: "awaiting 1 approval at GAP_GATE",
          waitJson: {
            kind: "human-gate", nodeId: "GAP_GATE",
            items: [{ itemId: "x:gap:1", requestPayload: { title: "Cyrano needs 2 things to draft blockchain listing" } }],
          },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-gate/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].reason, "Cyrano needs 2 things to draft blockchain listing");
  });

  test("reason: falls back to statusReason for non-human-gate waits (e.g. queued)", async () => {
    resetMocks();
    const flow = { flowId: "flow-queued", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-queued", nodeId: "PROPOSE" } : null;
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-queued" ? flow : null },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-queued", status: "queued", createdAt: 10, updatedAt: 20, endedAt: null,
          stateJson: {},
          statusReason: "queued: waiting for an agent-dispatch slot (node SCORE_LLM)",
          waitJson: { kind: "agent-semaphore", nodeId: "SCORE_LLM" },
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-queued/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].reason, "queued: waiting for an agent-dispatch slot (node SCORE_LLM)");
  });

  test("reason: null when neither a human-gate title nor statusReason exists", async () => {
    resetMocks();
    const flow = { flowId: "flow-bare", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-propose" ? { id: "inv-propose", flowId: "flow-bare", nodeId: "PROPOSE" } : null;
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-bare" ? flow : null },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-bare", status: "running", createdAt: 10, updatedAt: 20, endedAt: null, stateJson: {},
        }],
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-bare/tree/expand?type=node&invocationId=inv-propose"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].reason, null);
  });

  test("type=node with missing invocationId → 400", async () => {
    resetMocks();
    const flow = { flowId: "flow-j", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-j" ? flow : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-j/tree/expand?type=node"), res);
    assert.equal(res.statusCode, 400);
  });

  test("type=node with an invocationId belonging to a DIFFERENT flow → 404", async () => {
    resetMocks();
    const flow = { flowId: "flow-k", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-other" ? { id: "inv-other", flowId: "SOME-OTHER-FLOW", nodeId: "PROPOSE" } : null;
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-k" ? flow : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-k/tree/expand?type=node&invocationId=inv-other"), res);
    assert.equal(res.statusCode, 404);
  });

  test("type=flow returns the descendant flow's own node list (same shape as /tree)", async () => {
    resetMocks();
    const flow = { flowId: "flow-l", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    const childFlow = { flowId: "child-9", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    listInvocationsForFlowImpl = (flowId) => flowId === "child-9" ? [
      { invocationId: "inv-c1", nodeId: "CACHE_CHECK", subflowKey: null, visitSeq: 1, invokedAt: 100, executionCount: 1, enteredAt: 100, exitedAt: 110, latestExecutionId: null, kind: "fresh" },
    ] : [];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-l" ? flow : (id === "child-9" ? childFlow : null) },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-l/tree/expand?type=flow&childFlowId=child-9"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.items.length, 1);
    assert.equal(body.items[0].type, "node");
    assert.equal(body.items[0].nodeId, "CACHE_CHECK");
  });

  test("type=flow with unknown childFlowId → 404", async () => {
    resetMocks();
    const flow = { flowId: "flow-m", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-m" ? flow : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-m/tree/expand?type=flow&childFlowId=nope"), res);
    assert.equal(res.statusCode, 404);
  });

  test("missing/invalid type → 400", async () => {
    resetMocks();
    const flow = { flowId: "flow-n", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-n" ? flow : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-n/tree/expand?type=bogus"), res);
    assert.equal(res.statusCode, 400);
  });
});

// ── /tree/invocation/:id ────────────────────────────────────────────────────

describe("GET /manifold/runs/:flowId/tree/invocation/:invocationId", () => {
  test("returns the full execution history for the invocation", async () => {
    resetMocks();
    const flow = { flowId: "flow-o", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-retried" ? { id: "inv-retried", flowId: "flow-o", nodeId: "SCORE_LLM", subflowKey: null, visitSeq: 1, invokedAt: 100 } : null;
    listExecutionsForInvocationImpl = (id) => id === "inv-retried" ? [
      { executionId: "e1", enteredAt: 100, exitedAt: 110, kind: "fresh" },
      { executionId: "e2", enteredAt: 200, exitedAt: 230, kind: "retry" },
    ] : [];
    // Error content is cross-referenced from failureStore now (Phase 1.5) —
    // sendInvocationDetail merges message onto the matching executionId.
    queryErrorsByFlowImpl = () => [{ nodeId: "SCORE_LLM", subflowKey: null, message: "timeout", at: 110, executionId: "e1", invocationId: "inv-retried" }];
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-o" ? flow : null },
      pipelinesRef: { value: new Map([["team_pipe", FIXTURE_PIPELINE]]) },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-o/tree/invocation/inv-retried"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.nodeId, "SCORE_LLM");
    assert.equal(body.kind, "agent");
    assert.equal(body.agent, "scorer");
    assert.equal(body.executions.length, 2);
    assert.equal(body.executions[0].error, "timeout");
    assert.equal(body.executions[1].kind, "retry");
  });

  test("invocation not found → 404", async () => {
    resetMocks();
    const flow = { flowId: "flow-p", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-p" ? flow : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-p/tree/invocation/nope"), res);
    assert.equal(res.statusCode, 404);
  });

  test("invocation belonging to a different flow → 404 (not leaked cross-flow)", async () => {
    resetMocks();
    const flow = { flowId: "flow-q", status: "succeeded", currentStep: null, stateJson: { pipelineId: "team_pipe" } };
    getInvocationImpl = (id) => id === "inv-elsewhere" ? { id: "inv-elsewhere", flowId: "OTHER-FLOW", nodeId: "X" } : null;
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-q" ? flow : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-q/tree/invocation/inv-elsewhere"), res);
    assert.equal(res.statusCode, 404);
  });
});
