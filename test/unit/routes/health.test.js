/**
 * Unit tests for routes/health.js
 *
 * Manifold's own liveness: version + uptime, no harness contact.
 */
import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../auth/bridge.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res, reason) => { res.statusCode = 401; res.end(reason ?? "unauthorized"); },
  },
});

const { registerHealthRoutes } = await import("../../../routes/health.js");

describe("registerHealthRoutes", () => {
  test("registers GET /manifold/health and answers with version + per-file status", async () => {
    const { api, routes } = createMockApi();
    registerHealthRoutes({ registerHttpRoute: (route) => api.registerHttpRoute(route), logger: api.logger });
    assert.equal(routes.length, 1);
    assert.equal(routes[0].path, "/manifold/health");
    const res = createMockRes();
    await routes[0].handler(createMockReq("GET", "/manifold/health"), res);
    assert.equal(res.statusCode, 200);
    const out = JSON.parse(res.body);
    assert.equal(typeof out.ok, "boolean");
    assert.ok(typeof out.manifold === "string");
    assert.ok(out.uptimeMs >= 0);
    // Per-file honesty: shape asserted, value depends on the machine.
    // Missing files report ok:false (never created by the probe).
    assert.ok(out.db && typeof out.db.runtime === "object" && typeof out.db.ops === "object");
    assert.equal(typeof out.db.runtime.ok, "boolean");
  });

  test("POST rejected with 405", async () => {
    const { api, routes } = createMockApi();
    registerHealthRoutes({ registerHttpRoute: (route) => api.registerHttpRoute(route), logger: api.logger });
    const res = createMockRes();
    await routes[0].handler(createMockReq("POST", "/manifold/health"), res);
    assert.equal(res.statusCode, 405);
  });
});
