/**
 * Unit tests for routes/failures.js
 *
 * Verifies:
 *   - registerFailureRoutes registers exactly 2 routes at the right paths/auth/match.
 *   - Method guards fire before the auth check (no live-activity-bridge needed).
 *   - Happy-path handler returns failure list JSON (auth mocked).
 */
import { test, mock, describe, before } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

// ── auth mock ────────────────────────────────────────────────────────────────
// Mock before any import of routes/failures.js so the dynamic import inside
// the handler gets the stub, not the real live-activity-bridge module.

const authUrl = new URL("../../../plugins/openclaw/manifold/auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res, reason) => { res.statusCode = 401; res.end(reason ?? "unauthorized"); },
  },
});

const { registerFailureRoutes } = await import("../../../routes/failures.js");

// ── fixtures ─────────────────────────────────────────────────────────────────

function makeDeps(api, overrides = {}) {
  return {
    api,
    failureStore: { list: () => [], get: () => null },
    invokeElevated: async () => ({}),
    auth: {
      authorizeBridgeRequest: async () => ({ ok: true }),
      sendUnauthorized: (res, reason) => { res.statusCode = 401; res.end(reason ?? "unauthorized"); },
    },
    ...overrides,
  };
}

// ── registration tests ───────────────────────────────────────────────────────

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

// ── method-guard tests (fire before auth, no mock needed) ────────────────────

describe("registerFailureRoutes — method guards", () => {
  test("list route: POST → 405", async () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("POST", "/manifold/failures"), res);
    assert.equal(res.statusCode, 405);
  });

  test("list route: DELETE → 405", async () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("DELETE", "/manifold/failures"), res);
    assert.equal(res.statusCode, 405);
  });
});

// ── handler behaviour tests (auth mocked) ────────────────────────────────────

describe("registerFailureRoutes — list handler", () => {
  test("returns {rows, count} with empty rows", async () => {
    const { api, routes } = createMockApi();
    registerFailureRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/failures"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(Array.isArray(body.rows), "body.rows is an array");
    assert.equal(body.count, 0);
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
    await handler(createMockReq("GET", "/manifold/failures"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.rows.length, 1);
    assert.equal(body.rows[0].id, "f1");
    assert.equal(body.count, 1);
  });
});
