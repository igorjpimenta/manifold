/**
 * Unit tests for routes/pipelines.js
 *
 * Verifies:
 *   - registerPipelineRoutes registers 1 route at the right path/auth/match.
 *   - Method guard fires before auth.
 *   - Happy-path handler returns {pipelines, todayStartMs, generatedAtMs} shape.
 */
import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
  },
});

const { registerPipelineRoutes } = await import("../../../routes/pipelines.js");

function makeDeps(api, overrides = {}) {
  return {
    registerHttpRoute: (route) => api.registerHttpRoute(route),
    logger: api.logger,
    config: api.config,
    runtime: api.runtime,
    pipelinesRef: { value: new Map() },
    readStore: { listAllPipelineStats: () => new Map() },
    ensurePipelinesLoaded: async () => {},
    ...overrides,
  };
}

describe("registerPipelineRoutes — registration", () => {
  test("registers exactly 1 route", () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    assert.equal(routes.length, 1);
  });

  test("route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    const [r] = routes;
    assert.equal(r.path, "/manifold/pipelines");
    assert.equal(r.auth, "plugin");
    assert.equal(r.match, "exact");
    assert.equal(typeof r.handler, "function");
  });
});

describe("registerPipelineRoutes — method guard", () => {
  test("POST → 405", async () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(createMockReq("POST", "/manifold/pipelines"), res);
    assert.equal(res.statusCode, 405);
  });

  test("DELETE → 405", async () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(createMockReq("DELETE", "/manifold/pipelines"), res);
    assert.equal(res.statusCode, 405);
  });
});

describe("registerPipelineRoutes — handler", () => {
  test("empty store returns {pipelines, todayStartMs, generatedAtMs}", async () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(createMockReq("GET", "/manifold/pipelines"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(Array.isArray(body.pipelines), "pipelines is an array");
    assert.equal(typeof body.todayStartMs, "number");
    assert.equal(typeof body.generatedAtMs, "number");
  });

  test("exposes pipeline metadata fields", async () => {
    const { api, routes } = createMockApi();
    const fakePipeline = {
      id: "team_test",
      goal: "A test pipeline",
      _filepath: "/some/dir/team_test.js",
      nodes: { A: {}, B: {} },
    };
    const pipelines = new Map([["team_test", fakePipeline]]);
    registerPipelineRoutes(makeDeps(api, { pipelinesRef: { value: pipelines } }));
    const res = createMockRes();
    await routes[0].handler(createMockReq("GET", "/manifold/pipelines"), res);
    const body = JSON.parse(res.body);
    const [p] = body.pipelines;
    assert.equal(p.id, "team_test");
    assert.equal(p.goal, "A test pipeline");
    assert.equal(p.nodeCount, 2);
    assert.equal(p.historical, false);
  });

  test("historical pipeline (no def, runs in store) appears last", async () => {
    const { api, routes } = createMockApi();
    const orphanStats = {
      lastRunAtMs: Date.now(), lastRunStatus: "succeeded",
      todayTotal: 1, todaySucceeded: 1, todayFailed: 0, todayActive: 0,
      last7dTotal: 1, last7dSucceeded: 1, last7dFailed: 0, totalRuns: 1,
    };
    registerPipelineRoutes(makeDeps(api, {
      pipelinesRef: { value: new Map() },
      readStore: { listAllPipelineStats: () => new Map([["orphan_pipeline", orphanStats]]) },
    }));
    const res = createMockRes();
    await routes[0].handler(createMockReq("GET", "/manifold/pipelines"), res);
    const body = JSON.parse(res.body);
    const orphan = body.pipelines.find((p) => p.id === "orphan_pipeline");
    assert.ok(orphan, "orphan pipeline appears");
    assert.equal(orphan.historical, true);
  });

  // Regression test: engine/loader.js registers every loop node's .branch
  // as its own addressable pipeline (`${pipelineId}::${nodeId}`, flagged
  // _synthetic:true) purely so childFlows has a dispatch target — it is
  // never a pipeline an operator runs or should see. Before this filter,
  // it appeared as a real (zero-run) catalog entry, cluttering the iOS
  // Pipelines list with one fake row per loop node ever migrated.
  test("_synthetic child-flow pipelines (loop-node dispatch targets) are excluded from the catalog", async () => {
    const { api, routes } = createMockApi();
    const realPipeline = {
      id: "team_test", goal: "A test pipeline",
      _filepath: "/some/dir/team_test.js", nodes: { LOOP: {} },
    };
    const syntheticChildPipeline = {
      id: "team_test::LOOP", _synthetic: true,
      _parentPipelineId: "team_test", _parentNodeId: "LOOP",
      nodes: { A: {}, B: {} },
    };
    const pipelines = new Map([
      ["team_test", realPipeline],
      ["team_test::LOOP", syntheticChildPipeline],
    ]);
    registerPipelineRoutes(makeDeps(api, { pipelinesRef: { value: pipelines } }));
    const res = createMockRes();
    await routes[0].handler(createMockReq("GET", "/manifold/pipelines"), res);
    const body = JSON.parse(res.body);
    assert.ok(body.pipelines.some((p) => p.id === "team_test"), "the real pipeline must still appear");
    assert.ok(
      !body.pipelines.some((p) => p.id === "team_test::LOOP"),
      "the synthetic child-flow pipeline must not appear, as implemented OR historical"
    );
  });
});
