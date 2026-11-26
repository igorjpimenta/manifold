import { test, describe, after } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const TEST_BEARER = "test-bearer-pipelines";
const SAVED_BEARER = process.env.MANIFOLD_BEARER;
process.env.MANIFOLD_BEARER = TEST_BEARER;
after(() => {
  if (SAVED_BEARER === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = SAVED_BEARER;
});

const { registerPipelineRoutes } = await import("../../../routes/pipelines.js");

function authedReq(method, path) {
  return createMockReq(method, path, { authorization: `Bearer ${TEST_BEARER}` });
}

function makeDeps(api, overrides = {}) {
  return {
    api,
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
    await routes[0].handler(authedReq("POST", "/manifold/pipelines"), res);
    assert.equal(res.statusCode, 405);
  });

  test("DELETE → 405", async () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(authedReq("DELETE", "/manifold/pipelines"), res);
    assert.equal(res.statusCode, 405);
  });
});

describe("registerPipelineRoutes — handler", () => {
  test("empty store returns {pipelines, todayStartMs, generatedAtMs}", async () => {
    const { api, routes } = createMockApi();
    registerPipelineRoutes(makeDeps(api));
    const res = createMockRes();
    await routes[0].handler(authedReq("GET", "/manifold/pipelines"), res);
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
    await routes[0].handler(authedReq("GET", "/manifold/pipelines"), res);
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
    await routes[0].handler(authedReq("GET", "/manifold/pipelines"), res);
    const body = JSON.parse(res.body);
    const orphan = body.pipelines.find((p) => p.id === "orphan_pipeline");
    assert.ok(orphan, "orphan pipeline appears");
    assert.equal(orphan.historical, true);
  });

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
    await routes[0].handler(authedReq("GET", "/manifold/pipelines"), res);
    const body = JSON.parse(res.body);
    assert.ok(body.pipelines.some((p) => p.id === "team_test"), "the real pipeline must still appear");
    assert.ok(
      !body.pipelines.some((p) => p.id === "team_test::LOOP"),
      "the synthetic child-flow pipeline must not appear, as implemented OR historical"
    );
  });
});
