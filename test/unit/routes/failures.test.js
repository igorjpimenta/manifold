import { test, mock, describe, after } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const TEST_BEARER = "test-bearer-failures";
const SAVED_BEARER = process.env.MANIFOLD_BEARER;
process.env.MANIFOLD_BEARER = TEST_BEARER;
after(() => {
  if (SAVED_BEARER === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = SAVED_BEARER;
});

const { registerFailureRoutes } = await import("../../../routes/failures.js");

function authedReq(method, path) {
  return createMockReq(method, path, { authorization: `Bearer ${TEST_BEARER}` });
}

function makeDeps(api, overrides = {}) {
  return {
    api,
    failureStore: { list: () => [], get: () => null },
    invokeElevated: async () => ({}),
    ...overrides,
  };
}

describe("registerFailureRoutes — registration", () => {
  test("registers exactly 2 routes", () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    assert.equal(routes.length, 2);
  });

  test("list route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const r = routes.find((r) => r.match === "exact");
    assert.ok(r, "exact-match route exists");
    assert.equal(r.path, "/manifold/failures");
    assert.equal(r.auth, "plugin");
  });

  test("action route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const r = routes.find((r) => r.match === "prefix");
    assert.ok(r, "prefix-match route exists");
    assert.equal(r.path, "/manifold/failures/");
    assert.equal(r.auth, "plugin");
  });

  test("both handlers are functions", () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    for (const r of routes) {
      assert.equal(typeof r.handler, "function");
    }
  });
});

describe("registerFailureRoutes — method guards", () => {
  test("list route: POST → 405", async () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(authedReq("POST", "/manifold/failures"), res);
    assert.equal(res.statusCode, 405);
  });

  test("list route: DELETE → 405", async () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(authedReq("DELETE", "/manifold/failures"), res);
    assert.equal(res.statusCode, 405);
  });
});

describe("registerFailureRoutes — list handler", () => {
  test("returns {rows, count} with empty rows", async () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(authedReq("GET", "/manifold/failures"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(Array.isArray(body.rows), "body.rows is an array");
    assert.equal(body.count, 0);
  });

describe("registerFailureRoutes — retry handler", () => {
  test("forwards stamped caller identity to _elev/retry-failure", async () => {
    const { api, routes } = createMockApi();
    const invokeElevated = mock.fn(async () => ({
      status: 200, body: JSON.stringify({ ok: true }),
    }));
    registerFailureRoutes(makeDeps(api, { invokeElevated }));
    const { handler } = routes.find((r) => r.match === "prefix");
    const body = { via: "app", session: "agent:main:k1", agentId: "main" };
    const req = {
      ...authedReq("POST", "/manifold/failures/f1/retry"),
      [Symbol.asyncIterator]: async function* () { yield Buffer.from(JSON.stringify(body)); },
    };
    const res = createMockRes();
    await handler(req, res);
    assert.equal(invokeElevated.mock.calls.length, 1);
    const [path, payload] = invokeElevated.mock.calls[0].arguments;
    assert.equal(path, "/manifold/_elev/retry-failure");
    assert.equal(payload.id, "f1");
    assert.equal(payload.session, "agent:main:k1");
    assert.equal(payload.agentId, "main");
    assert.equal(res.statusCode, 200);
  });
});

  test("returns failure records from store in body.rows", async () => {
    const { api, routes } = createMockApi();
    const fakeFailure = {
      id: "f1", flow_id: "fl1", pipeline_id: "test_pipe",
      failure_node: "NODE_A", branch_item_id: null,
      failed_at: Date.now(), error_type: null, failure_summary: "boom",
      recovery_hint: "investigate", resolved_at: null,
      invocation_id: null, execution_id: null,
    };
    registerFailureRoutes(makeDeps(api, {
      failureStore: { list: () => [fakeFailure], get: () => fakeFailure },
    }));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(authedReq("GET", "/manifold/failures"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.rows.length, 1);
    assert.equal(body.rows[0].id, "f1");
    assert.equal(body.count, 1);
  });
});
