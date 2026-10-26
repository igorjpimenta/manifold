/**
 * Unit tests for the /manifold/runs/:flowId/subflows endpoint.
 *
 * sendSubflowsList serves GET /manifold/runs/:flowId/subflows and resolves
 * a subflow node's real flow_runs children via getAllSubflowGroups
 * (nodeExecutionsStore/runStore). The old branchService path is gone.
 */
import { test, describe, mock } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

// auth.js checks Authorization: Bearer <token> against OPENCLAW_GATEWAY_TOKEN.
// Set the env var before importing so the real auth function passes our test token.
const TEST_TOKEN = "test-token-for-unit-tests-runs-branches";
process.env.OPENCLAW_GATEWAY_TOKEN = TEST_TOKEN;

// sendSubflowsList now calls getNodeExecutionsStore() (via getAllSubflowGroups)
// — mocked so tests are deterministic regardless of the real (or
// singleton-cached) DB. Each test controls the childFlows surface directly via
// these mutable impls rather than touching a real sqlite file.
let listSubflowNodeIdsForFlowImpl = () => [];
let listInvocationIdsForNodeImpl = () => [];
let getInvocationImpl = () => null;
const nodeExecutionsUrl = new URL("../../../state/node-executions.js", import.meta.url).href;
await mock.module(nodeExecutionsUrl, {
  exports: {
    getNodeExecutionsStore: () => ({
      queryErrorsByFlow: () => [],
      listSubflowNodeIdsForFlow: (flowId) => listSubflowNodeIdsForFlowImpl(flowId),
      listInvocationIdsForNode: (flowId, nodeId) => listInvocationIdsForNodeImpl(flowId, nodeId),
      getInvocation: (id) => getInvocationImpl(id),
      summarizeChildrenByFlowIds: () => new Map(),
    }),
  },
});

const authUrl = new URL("../../../plugins/openclaw/manifold/auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
  },
});

const { registerRunRoutes } = await import("../../../routes/runs.js");

// Wraps createMockReq with the auth header so the prefix handler doesn't 401.
function authReq(method, path) {
  return createMockReq(method, path, { authorization: `Bearer ${TEST_TOKEN}` });
}

const FULL_DEPS_BASE = {
  costForAllPipelines: async () => ({ pipelines: [], sinceMs: null }),
  pipelinesRef: { value: new Map() },
  invokeElevated: async () => ({}),
    auth: {
      authorizeBridgeRequest: async () => ({ ok: true }),
      sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
    },
  discussStore: { getComponent: () => null, listComponents: () => [], getBulkSummaries: () => ({}) },
  handleDiscuss:           async () => {},
  handleDiscussHistory:    async () => {},
  handleDiscussApplyDraft: async () => {},
  handleDiscussAudio:      async () => {},
  handleDiscussAudioGet:   async () => {},
};

function makeDeps(api, overrides = {}) {
  return {
    api,
    readStore: { listStatusCounts: () => ({}), listRunsPage: () => [], listCostSnapshot: () => [], get: () => null },
    runStore: { listForParentInvocations: () => [] },
    ...FULL_DEPS_BASE,
    ...overrides,
  };
}

// Resets the getNodeExecutionsStore mock's childFlows surface to "no subflow
// node is migrated" — the default every test not explicitly exercising the
// dual-mode path should start from.
function resetNodeExecutionsMock() {
  listSubflowNodeIdsForFlowImpl = () => [];
  listInvocationIdsForNodeImpl = () => [];
  getInvocationImpl = () => null;
}

function prefixHandler(routes) {
  return routes.find((r) => r.path === "/manifold/runs/" && r.match === "prefix").handler;
}


function subflowReq(flowId) {
  return authReq("GET", `/manifold/runs/${flowId}/subflows`);
}

// ── GET /manifold/runs/:flowId/subflows ───────────────────────────────────────

describe("sendSubflowsList — 200 + subflow data", () => {
  test("returns { subflows: [] } when no subflows for the flow", async () => {
    const { api, routes } = createMockApi();
    const flowId = "flow-no-branches";
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === flowId ? { flowId } : null },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(subflowReq(flowId), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(Array.isArray(body.subflows), "subflows is array");
    assert.equal(body.subflows.length, 0);
  });

});


describe("sendSubflowsList — dual-mode (Phase 2): real childFlows children", () => {
  test("a childFlows-migrated subflow node's real flow_runs children are returned, even when branchService.listByFlow has nothing", async () => {
    resetNodeExecutionsMock();
    const { api, routes } = createMockApi();
    const flowId = "flow-with-real-children";
    const invocationId = "inv-1";
    const childFlowId = "child-1";

    listSubflowNodeIdsForFlowImpl = (id) => id === flowId ? ["SCORE"] : [];
    listInvocationIdsForNodeImpl = (id, nodeId) => (id === flowId && nodeId === "SCORE") ? [invocationId] : [];
    getInvocationImpl = (id) => id === invocationId
      ? { id: invocationId, flowId, nodeId: "SCORE", subflowKey: "job-99" }
      : null;

    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === flowId ? { flowId } : null },
      runStore: {
        listForParentInvocations: (ids) => ids.includes(invocationId)
          ? [{
              flowId: childFlowId, parentInvocationId: invocationId,
              status: "succeeded", lastStep: "__end__",
              createdAt: 1000, updatedAt: 2000, endedAt: 2000,
              stateJson: { SCORE_LLM: { score: 77 } }, waitJson: null, currentStep: null,
            }]
          : [],
      },
    }));

    const res = createMockRes();
    await prefixHandler(routes)(subflowReq(flowId), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.subflows.length, 1, "the real childFlows child must surface as a subflow row");
    const b = body.subflows[0];
    assert.equal(b.groupId, "SCORE");
    assert.equal(b.subflowKey, "job-99");
    assert.equal(b.status, "succeeded");
    assert.equal(b.flowId, childFlowId);
    assert.deepEqual(b.data, { SCORE_LLM: { score: 77 } });
  });

  test("a childFlows-blocked-on-gate child folds into status 'running' with an awaiting payload, matching the old model's shape", async () => {
    resetNodeExecutionsMock();
    const { api, routes } = createMockApi();
    const flowId = "flow-with-blocked-child";
    const invocationId = "inv-2";

    listSubflowNodeIdsForFlowImpl = () => ["GATE_LOOP"];
    listInvocationIdsForNodeImpl = () => [invocationId];
    getInvocationImpl = () => ({ id: invocationId, flowId, nodeId: "GATE_LOOP", subflowKey: "item-x" });

    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === flowId ? { flowId } : null },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-2", parentInvocationId: invocationId,
          status: "blocked", lastStep: "GATE", currentStep: "GATE",
          createdAt: 1000, updatedAt: 1500, endedAt: null,
          stateJson: {}, waitJson: { kind: "human-gate", items: [{ itemId: "item-x" }] },
        }],
      },
    }));

    const res = createMockRes();
    await prefixHandler(routes)(subflowReq(flowId), res);
    const b = JSON.parse(res.body).subflows[0];
    assert.equal(b.status, "running", "blocked-on-gate child folds into 'running' to match the old model");
    assert.equal(b.awaiting?.requestPayload?.kind, "human-gate");
    assert.equal(b.awaiting?.subNodeId, "GATE");
  });

  test("returns real children groups from flow_runs", async () => {
    resetNodeExecutionsMock();
    const { api, routes } = createMockApi();
    const flowId = "flow-migrated-with-real-children";
    const invocationId = "inv-3";

    listSubflowNodeIdsForFlowImpl = () => ["SCORE"];
    listInvocationIdsForNodeImpl = () => [invocationId];
    getInvocationImpl = () => ({ id: invocationId, flowId, nodeId: "SCORE", subflowKey: "job-real" });

    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === flowId ? { flowId } : null },
      runStore: {
        listForParentInvocations: () => [{
          flowId: "child-3", parentInvocationId: invocationId,
          status: "succeeded", lastStep: "__end__",
          createdAt: 1000, updatedAt: 2000, endedAt: 2000,
          stateJson: {}, waitJson: null, currentStep: null,
        }],
      },
    }));

    const res = createMockRes();
    await prefixHandler(routes)(subflowReq(flowId), res);
    const subflows = JSON.parse(res.body).subflows;
    assert.equal(subflows.length, 1, "only real children are returned");
    assert.equal(subflows[0].subflowKey, "job-real");
  });
});

describe("sendSubflowsList — error cases", () => {
  test("returns 404 when flow is not in readStore", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: () => null },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(subflowReq("unknown-flow"), res);
    assert.equal(res.statusCode, 404);
    const body = JSON.parse(res.body);
    assert.equal(body.ok, false);
  });

  test("returns 400 when no flowId segment in URL", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/"), res);
    assert.equal(res.statusCode, 400);
  });

  test("returns 405 for non-GET/POST on prefix route", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    // 405 is checked before auth, so no token needed
    await prefixHandler(routes)(createMockReq("DELETE", "/manifold/runs/some-flow/subflows"), res);
    assert.equal(res.statusCode, 405);
  });
});

