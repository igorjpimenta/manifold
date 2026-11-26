
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { registerHttpRoutes } from "../../../telemetry/http.js";

function collectRoutes({ store = null, logger = null, getBroadcast = () => null } = {}) {
  const routes = [];
  const api = { registerHttpRoute: (def) => routes.push(def) };
  registerHttpRoutes({ api, getStore: () => store, logger, getBroadcast });
  return routes;
}

function fakeRes() {
  return {
    statusCode: 200,
    headers: {},
    body: "",
    setHeader(k, v) { this.headers[k] = v; },
    end(chunk) { this.body = chunk ?? ""; },
  };
}

describe("registerHttpRoutes — wiring", () => {
  test("mounts every documented route under /manifold/telemetry/ with plugin auth", () => {
    const routes = collectRoutes();
    const paths = routes.map((r) => r.path);
    for (const expected of [
      "/manifold/telemetry/cost/today",
      "/manifold/telemetry/cost/by-root",
      "/manifold/telemetry/cost/by-trigger",
      "/manifold/telemetry/cost/by-cron-job",
      "/manifold/telemetry/cost/by-pipeline",
      "/manifold/telemetry/cost/by-flow",
      "/manifold/telemetry/cost/by-node",
      "/manifold/telemetry/cost/by-model",
      "/manifold/telemetry/cost/by-day",
      "/manifold/telemetry/cost/top-calls",
      "/manifold/telemetry/cost/home",
    ]) {
      assert.ok(paths.includes(expected), `${expected} must be registered`);
    }
    for (const r of routes) {
      assert.equal(r.auth, "plugin", `${r.path} must be plugin-auth`);
      assert.equal(typeof r.handler, "function");
    }
  });

  test("registering twice on one api does not touch the HTTP server itself", () => {
    const api = { registerHttpRoute() {} };
    assert.doesNotThrow(() => registerHttpRoutes({
      api, getStore: () => null, logger: null, getBroadcast: () => null,
    }));
  });
});

describe("registerHttpRoutes — shared response helpers", () => {
  test("a handler with no store answers 503 JSON 'lineage store not ready'", async () => {
    const routes = collectRoutes({ store: null });
    const today = routes.find((r) => r.path === "/manifold/telemetry/cost/today");
    const res = fakeRes();
    await today.handler({ url: "/manifold/telemetry/cost/today" }, res);
    assert.equal(res.statusCode, 503);
    assert.equal(res.headers["content-type"], "application/json");
    assert.deepEqual(JSON.parse(res.body), { error: "lineage store not ready" });
  });

  test("the weekly/report routes share the same store gate", async () => {
    const routes = collectRoutes({ store: null });
    for (const path of ["/manifold/telemetry/cost/by-root", "/manifold/telemetry/cost/by-model"]) {
      const route = routes.find((r) => r.path === path);
      const res = fakeRes();
      await route.handler({ url: path }, res);
      assert.equal(res.statusCode, 503, `${path} must gate on the store`);
      assert.deepEqual(JSON.parse(res.body), { error: "lineage store not ready" });
    }
  });
});
