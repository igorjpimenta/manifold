// The adapter's whole job is the seam: spawn the core, serve the turn
// bridge, stamp metadata. These tests pin the seam without a gateway.

import { registerHooks } from "node:module";
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as sdkHooks from "../../../../helpers/openclaw-plugin-sdk-hooks.js";
import { createMockRes } from "../../../../helpers/mock-api.js";

registerHooks(sdkHooks);
const { default: entry, buildCoreEnv } = await import("../../../../../plugins/openclaw/manifold/index.js");

function makeApi({ pluginConfig = {} } = {}) {
  const routes = [];
  const services = [];
  const api = {
    pluginConfig,
    config: {},
    logger: { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} },
    registerHttpRoute: (spec) => routes.push(spec),
    registerService: (svc) => services.push(svc),
    on: () => {},
    runtime: {
      subagent: {
        run: async () => ({ runId: "run-1" }),
        waitForRun: async () => ({ status: "ok" }),
        getSessionMessages: async () => ({ messages: [{ role: "assistant", content: "reply" }] }),
      },
    },
  };
  return { api, routes, services };
}

function fakeReq({ method = "POST", authorization, body = {} } = {}) {
  const chunks = [Buffer.from(JSON.stringify(body))];
  return {
    method,
    headers: { host: "127.0.0.1:18789", ...(authorization ? { authorization } : {}) },
    async *[Symbol.asyncIterator]() { yield* chunks; },
  };
}

describe("adapter registration", () => {
  it("serves the turn bridge and supervises exactly one core process", () => {
    const { api, routes, services } = makeApi();
    entry.register(api);
    assert.deepEqual(routes.map((r) => r.path), ["/api/manifold/turn"]);
    assert.deepEqual(services.map((s) => s.id), ["manifold-core", "manifold-runtime-capture"]);
  });

  it("rejects non-POST on the bridge", async () => {
    const { api, routes } = makeApi();
    entry.register(api);
    const res = createMockRes();
    await routes[0].handler(fakeReq({ method: "GET" }), res);
    assert.equal(res.statusCode, 405);
    assert.equal(res.headers.allow, "POST");
  });

  it("rejects a request without the per-boot turn token", async () => {
    const { api, routes } = makeApi();
    entry.register(api);
    const res = createMockRes();
    await routes[0].handler(fakeReq({ authorization: "Bearer not-the-token" }), res);
    assert.equal(res.statusCode, 401);
  });
});

describe("buildCoreEnv", () => {
  const base = { baseEnv: { PATH: "/bin", HOME: "/home/u" } };
  const common = { coreApiPort: 44301, turnUrl: "http://127.0.0.1:18789/api/manifold/turn", turnToken: "tok", dashboardPort: 44300, dashboardHost: "127.0.0.1" };

  it("carries the harness identity and the bridge credentials", () => {
    const env = buildCoreEnv({ userCfg: {}, ...common, ...base });
    assert.equal(env.MANIFOLD_HARNESS, "openclaw");
    assert.equal(env.MANIFOLD_API_PORT, "44301");
    assert.equal(env.MANIFOLD_TURN_URL, common.turnUrl);
    assert.equal(env.MANIFOLD_TURN_TOKEN, "tok");
    assert.equal(env.PATH, "/bin");
  });

  it("forwards optional config only when set", () => {
    const env = buildCoreEnv({
      userCfg: { pipelinesDir: "/pipes", ownerSessionKey: "owner", globalConcurrency: 4, discuss: { defaultAgent: "main" } },
      ...common, ...base,
    });
    assert.equal(env.MANIFOLD_PIPELINES_DIR, "/pipes");
    assert.equal(env.MANIFOLD_OWNER_SESSION_KEY, "owner");
    assert.equal(env.MANIFOLD_GLOBAL_CONCURRENCY, "4");
    assert.equal(env.MANIFOLD_DISCUSS_DEFAULT_AGENT, "main");

    const bare = buildCoreEnv({ userCfg: {}, ...common, ...base });
    assert.equal(bare.MANIFOLD_PIPELINES_DIR, undefined);
    assert.equal(bare.MANIFOLD_OWNER_SESSION_KEY, undefined);
  });

  it("disables the dashboard only on an explicit false", () => {
    assert.equal(buildCoreEnv({ userCfg: { dashboardEnabled: false }, ...common, ...base }).MANIFOLD_DASHBOARD_ENABLED, "false");
    assert.equal(buildCoreEnv({ userCfg: {}, ...common, ...base }).MANIFOLD_DASHBOARD_ENABLED, "true");
  });
});
