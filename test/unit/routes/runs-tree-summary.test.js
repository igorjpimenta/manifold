/**
 * Unit tests for the batched tree-summary endpoint:
 *   GET /manifold/tree-summary?flowIds=id1,id2,...
 */
import { test, describe, mock } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
  },
});

let listVisitedNodeIdsByFlowImpl = () => new Map();
const nodeExecutionsUrl = new URL("../../../state/node-executions.js", import.meta.url).href;
await mock.module(nodeExecutionsUrl, {
  exports: {
    getNodeExecutionsStore: () => ({
      listVisitedNodeIdsByFlow: (flowIds) => listVisitedNodeIdsByFlowImpl(flowIds),
    }),
  },
});

const { registerRunRoutes } = await import("../../../routes/runs.js");

function authReq(method, path) {
  return createMockReq(method, path, { authorization: "Bearer x" });
}

function handler(routes) {
  return routes.find((r) => r.path === "/manifold/tree-summary" && r.match === "exact").handler;
}

function resetMocks() {
  listVisitedNodeIdsByFlowImpl = () => new Map();
}

function makeDeps(api, overrides = {}) {
  return {
    api,
    readStore: { list: () => [], get: () => null, listQueued: () => [] },
    costForAllPipelines: async () => ({ pipelines: [], sinceMs: null }),
    runStore: { listForParentInvocations: () => [] },
    pipelinesRef: { value: new Map() },
    invokeElevated: async () => ({}),
    failureStore: { queryErrorsByFlow: async () => [], queryErrorsByFlowIds: async () => new Map() },
    discussStore: { getComponent: () => null, listComponents: () => [], getBulkSummaries: () => ({}) },
    handleDiscuss:           async () => {},
    handleDiscussHistory:    async () => {},
    handleDiscussApplyDraft: async () => {},
    handleDiscussAudio:      async () => {},
    handleDiscussAudioGet:   async () => {},
    ...overrides,
  };
}

describe("GET /manifold/tree-summary", () => {
  test("returns 405 for non-GET", async () => {
    resetMocks();
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await handler(routes)(authReq("POST", "/manifold/tree-summary?flowIds=a"), res);
    assert.equal(res.statusCode, 405);
  });

  test("returns 400 when flowIds is missing or empty", async () => {
    resetMocks();
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await handler(routes)(authReq("GET", "/manifold/tree-summary"), res);
    assert.equal(res.statusCode, 400);
  });

  test("parses comma-separated flowIds, trims whitespace, drops empties, caps at 500", async () => {
    resetMocks();
    let received = null;
    listVisitedNodeIdsByFlowImpl = (flowIds) => { received = flowIds; return new Map(); };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await handler(routes)(authReq("GET", "/manifold/tree-summary?flowIds=a,%20b%20,,c"), res);
    assert.deepEqual(received, ["a", "b", "c"]);
  });

  test("serializes the Map<flowId, Set<nodeId>> into {summaries: {flowId: {nodeIds: [...]}}} JSON", async () => {
    resetMocks();
    listVisitedNodeIdsByFlowImpl = () => new Map([
      ["flow-a", new Set(["PRIME", "DRAFT"])],
      ["flow-b", new Set(["SCAN"])],
    ]);
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await handler(routes)(authReq("GET", "/manifold/tree-summary?flowIds=flow-a,flow-b"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.deepEqual(new Set(body.summaries["flow-a"].nodeIds), new Set(["PRIME", "DRAFT"]));
    assert.deepEqual(body.summaries["flow-b"].nodeIds, ["SCAN"]);
  });

  test("a flowId absent from the store's Map is absent from summaries too", async () => {
    resetMocks();
    listVisitedNodeIdsByFlowImpl = () => new Map([["flow-a", new Set(["PRIME"])]]);
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await handler(routes)(authReq("GET", "/manifold/tree-summary?flowIds=flow-a,flow-no-visits"), res);
    const body = JSON.parse(res.body);
    assert.equal("flow-a" in body.summaries, true);
    assert.equal("flow-no-visits" in body.summaries, false);
  });

  test("returns 500 with the error message when the store throws", async () => {
    resetMocks();
    listVisitedNodeIdsByFlowImpl = () => { throw new Error("db unavailable"); };
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const res = createMockRes();
    await handler(routes)(authReq("GET", "/manifold/tree-summary?flowIds=a"), res);
    assert.equal(res.statusCode, 500);
    const body = JSON.parse(res.body);
    assert.equal(body.ok, false);
    assert.match(body.error, /db unavailable/);
  });
});
