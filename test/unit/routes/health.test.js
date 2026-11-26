import { test, describe, after } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const TEST_BEARER = "test-bearer-health";
const SAVED_BEARER = process.env.MANIFOLD_BEARER;
process.env.MANIFOLD_BEARER = TEST_BEARER;
after(() => {
  if (SAVED_BEARER === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = SAVED_BEARER;
});

const { registerHealthRoutes } = await import("../../../routes/health.js");

function authedReq(method, path) {
  return createMockReq(method, path, { authorization: `Bearer ${TEST_BEARER}` });
}

describe("registerHealthRoutes", () => {
  test("registers GET /manifold/health and answers with version + per-file status", async () => {
    const { api, routes } = createMockApi();
    registerHealthRoutes({ api });
    assert.equal(routes.length, 1);
    assert.equal(routes[0].path, "/manifold/health");
    const res = createMockRes();
    await routes[0].handler(authedReq("GET", "/manifold/health"), res);
    assert.equal(res.statusCode, 200);
    const out = JSON.parse(res.body);
    assert.equal(typeof out.ok, "boolean");
    assert.ok(typeof out.manifold === "string");
    assert.ok(out.uptimeMs >= 0);
    assert.ok(out.db && typeof out.db.runtime === "object" && typeof out.db.ops === "object");
    assert.equal(typeof out.db.runtime.ok, "boolean");
  });

  test("POST rejected with 405", async () => {
    const { api, routes } = createMockApi();
    registerHealthRoutes({ api });
    const res = createMockRes();
    await routes[0].handler(authedReq("POST", "/manifold/health"), res);
    assert.equal(res.statusCode, 405);
  });
});
