
import { registerHooks } from "node:module";
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as sdkHooks from "../../../../helpers/openclaw-plugin-sdk-hooks.js";
import { createMockRes } from "../../../../helpers/mock-api.js";

registerHooks(sdkHooks);
const { default: entry, buildCoreEnv, sessionIdFor } = await import("../../../../../plugins/openclaw/manifold/index.js");

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
  it("serves the turn, elevated-turn and session bridges and supervises exactly one core process", () => {
    const { api, routes, services } = makeApi();
    entry.register(api);
    assert.deepEqual(routes.map((r) => r.path), ["/api/manifold/_elev/turn", "/api/manifold/turn", "/api/manifold/session"]);
    assert.deepEqual(services.map((s) => s.id), ["manifold-core", "manifold-runtime-capture"]);
  });

  function routeAt(routes, path) {
    const found = routes.find((r) => r.path === path);
    assert.ok(found, `route ${path} registered`);
    return found;
  }

  it("rejects non-POST on either turn route", async () => {
    const { api, routes } = makeApi();
    entry.register(api);
    for (const path of ["/api/manifold/turn", "/api/manifold/_elev/turn"]) {
      const res = createMockRes();
      await routeAt(routes, path).handler(fakeReq({ method: "GET" }), res);
      assert.equal(res.statusCode, 405, path);
      assert.equal(res.headers.allow, "POST", path);
    }
  });

  it("rejects a request without the per-boot turn token on the public bridge", async () => {
    const { api, routes } = makeApi();
    entry.register(api);
    const res = createMockRes();
    await routeAt(routes, "/api/manifold/turn").handler(fakeReq({ authorization: "Bearer not-the-token" }), res);
    assert.equal(res.statusCode, 401);
  });

  it("runs the turn on the gateway-scoped route, which carries the operator scope its runtime call needs", async () => {
    const { api, routes } = makeApi();
    entry.register(api);
    const route = routeAt(routes, "/api/manifold/_elev/turn");
    assert.equal(route.auth, "gateway", "the turn runs under ambient operator scope, not the plugin route's none");
    const res = createMockRes();
    await route.handler(fakeReq({ body: { session: "agent:a:manifold:abcdef01-N", message: "hi", agentId: "a" } }), res);
    assert.equal(res.statusCode, 200);
    assert.equal(JSON.parse(res.body).ok, true);
    assert.match(JSON.parse(res.body).runId, /^mf-run-/, "the adapter maps the gateway runId to a core handle");
  });
});

describe("session-id bridge", () => {
  function sessionRoute() {
    const { api, routes } = makeApi();
    entry.register(api);
    return routes.find((r) => r.path === "/api/manifold/session");
  }

  it("mints this harness's id shape for a dispatch", () => {
    const minted = sessionIdFor({ agentId: "scorer", flowId: "abcdef01-0000-0000-0000-000000000000", nodeId: "SCORE_LLM" });
    assert.equal(minted.session, "agent:scorer:manifold:abcdef01-SCORE_LLM");
  });

  it("refuses missing coordinates instead of defaulting them", () => {
    assert.match(sessionIdFor({ agentId: "scorer" }).error, /flowId/);
    assert.match(sessionIdFor().error, /agentId/);
  });

  it("is POST-only and needs the per-boot token, like the turn bridge", async () => {
    const route = sessionRoute();
    const notPost = createMockRes();
    await route.handler(fakeReq({ method: "GET" }), notPost);
    assert.equal(notPost.statusCode, 405);
    const untokenized = createMockRes();
    await route.handler(fakeReq({ authorization: "Bearer not-the-token", body: { agentId: "a", flowId: "f", nodeId: "N" } }), untokenized);
    assert.equal(untokenized.statusCode, 401);
  });
});

describe("buildCoreEnv", () => {
  const base = { baseEnv: { PATH: "/bin", HOME: "/home/u" } };
  const common = { coreApiPort: 44301, turnUrl: "http://127.0.0.1:18789/api/manifold/turn", turnToken: "tok", sessionUrl: "http://127.0.0.1:18789/api/manifold/session", dashboardPort: 44300, dashboardHost: "127.0.0.1" };

  it("carries the harness identity and the bridge credentials", () => {
    const env = buildCoreEnv({ userCfg: {}, ...common, ...base });
    assert.equal(env.MANIFOLD_HARNESS, "openclaw");
    assert.equal(env.MANIFOLD_API_PORT, "44301");
    assert.equal(env.MANIFOLD_TURN_URL, common.turnUrl);
    assert.equal(env.MANIFOLD_SESSION_URL, common.sessionUrl);
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
