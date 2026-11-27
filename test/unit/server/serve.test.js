import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  serve,
  resolveServeConfig,
  createHttpRunTurn,
  createHttpSessionId,
  createHostSessions,
  localSessions,
  DEFAULT_API_PORT,
} from "../../../server/serve.js";

describe("resolveServeConfig", () => {
  it("defaults the API to 44301 and the dashboard to 44300", () => {
    const cfg = resolveServeConfig({ env: { MANIFOLD_DATA_DIR: "/tmp/manifold-cfg-test" } });
    assert.equal(cfg.apiPort, DEFAULT_API_PORT);
    assert.equal(cfg.config.dashboardPort, 44300);
    assert.equal(cfg.apiHost, "127.0.0.1");
    assert.equal(cfg.config.dashboardEnabled, true);
  });

  it("MANIFOLD_PIPELINES_DIR wins over the store default", () => {
    const cfg = resolveServeConfig({
      env: { MANIFOLD_DATA_DIR: "/tmp/manifold-cfg-test", MANIFOLD_PIPELINES_DIR: "/tmp/pipes" },
    });
    assert.equal(cfg.config.pipelinesDir, "/tmp/pipes");
  });

  it("keeps explicit ports, hosts and the turn URL", () => {
    const cfg = resolveServeConfig({
      env: {
        MANIFOLD_DATA_DIR: "/tmp/manifold-cfg-test",
        MANIFOLD_API_PORT: "7500",
        MANIFOLD_API_HOST: "0.0.0.0",
        MANIFOLD_DASHBOARD_PORT: "7501",
        MANIFOLD_DASHBOARD_HOST: "0.0.0.0",
        MANIFOLD_DASHBOARD_ENABLED: "false",
        MANIFOLD_TURN_URL: "http://127.0.0.1:18789/api/manifold/turn",
        MANIFOLD_TURN_TOKEN: "tok",
      },
    });
    assert.equal(cfg.apiPort, 7500);
    assert.equal(cfg.apiHost, "0.0.0.0");
    assert.equal(cfg.config.dashboardPort, 7501);
    assert.equal(cfg.config.dashboardHost, "0.0.0.0");
    assert.equal(cfg.config.dashboardEnabled, false);
    assert.equal(cfg.turnUrl, "http://127.0.0.1:18789/api/manifold/turn");
    assert.equal(cfg.turnToken, "tok");
  });

  it("turn token comes only from MANIFOLD_TURN_TOKEN, not from the bearer", () => {
    const cfg = resolveServeConfig({
      env: { MANIFOLD_DATA_DIR: "/tmp/manifold-cfg-test", MANIFOLD_BEARER: "shared" },
    });
    assert.equal(cfg.turnToken, null);
  });
});

describe("createHttpRunTurn", () => {
  it("refuses to run without a turn URL, naming the missing env", async () => {
    const runTurn = createHttpRunTurn({ url: null, token: null });
    await assert.rejects(() => runTurn({ text: "hi" }), /MANIFOLD_TURN_URL is unset/);
  });

  it("posts the turn args with the bearer and returns text + runId", async () => {
    let seen;
    const runTurn = createHttpRunTurn({
      url: "http://127.0.0.1:18789/api/manifold/turn",
      token: "secret",
      fetchImpl: async (url, init) => {
        seen = { url, init };
        return { ok: true, status: 200, json: async () => ({ ok: true, text: "done", runId: "run-1" }) };
      },
    });
    const result = await runTurn({ flowId: "f1", text: "work" });
    assert.equal(result.text, "done");
    assert.equal(result.runId, "run-1");
    assert.equal(seen.url, "http://127.0.0.1:18789/api/manifold/turn");
    assert.equal(seen.init.headers.authorization, "Bearer secret");
    assert.equal(JSON.parse(seen.init.body).flowId, "f1");
  });

  it("throws with the bridge's error text on a failed turn", async () => {
    const runTurn = createHttpRunTurn({
      url: "http://127.0.0.1:18789/api/manifold/turn",
      token: "secret",
      fetchImpl: async () => ({ ok: false, status: 502, json: async () => ({ ok: false, error: "agent exploded" }) }),
    });
    await assert.rejects(() => runTurn({ text: "x" }), /agent exploded/);
  });
});

describe("localSessions", () => {
  it("mints the same id for the same flow/gate/item", () => {
    const sessions = localSessions();
    const a = sessions.newDiscussSessionId({ agentId: "a", flowId: "f", gateNodeId: "g", itemId: "0" });
    const b = sessions.newDiscussSessionId({ agentId: "a", flowId: "f", gateNodeId: "g", itemId: "0" });
    assert.equal(a, b);
    assert.match(a, /^manifold:discuss:f:g:0:a$/);
  });

  it("has no harness model table and no cooldown source", async () => {
    const sessions = localSessions();
    assert.equal(await sessions.resolveAgentEffectiveModelPrimary(null, "a"), null);
    assert.equal(sessions.isProviderCooldown("a", "anthropic"), false);
  });
});

describe("createHttpSessionId", () => {
  it("posts the coordinates to the harness bridge and passes its answer through", async () => {
    let seen = null;
    const mint = createHttpSessionId({
      url: "http://127.0.0.1:18789/api/manifold/session",
      token: "tok",
      fetchImpl: async (url, init) => {
        seen = { url, init };
        return { ok: true, status: 200, json: async () => ({ ok: true, session: "agent:scorer:manifold:abcdef01-SCORE_LLM" }) };
      },
    });
    const session = await mint({ agentId: "scorer", flowId: "abcdef01-0000", nodeId: "SCORE_LLM" });
    assert.equal(session, "agent:scorer:manifold:abcdef01-SCORE_LLM");
    assert.equal(seen.url, "http://127.0.0.1:18789/api/manifold/session");
    assert.equal(seen.init.headers.authorization, "Bearer tok");
    assert.deepEqual(JSON.parse(seen.init.body), { agentId: "scorer", flowId: "abcdef01-0000", nodeId: "SCORE_LLM" });
  });

  it("fails the dispatch instead of falling back on an invented id", async () => {
    const mint = createHttpSessionId({
      url: "http://127.0.0.1:18789/api/manifold/session",
      fetchImpl: async () => ({ ok: false, status: 502, json: async () => ({ ok: false, error: "boom" }) }),
    });
    await assert.rejects(() => mint({ agentId: "a", flowId: "f", nodeId: "N" }), /gave no session for node "N": boom/);
  });

  it("treats a 200 with no session as a failure", async () => {
    const mint = createHttpSessionId({
      url: "http://127.0.0.1:18789/api/manifold/session",
      fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ ok: true }) }),
    });
    await assert.rejects(() => mint({ agentId: "a", flowId: "f", nodeId: "N" }), /gave no session/);
  });
});

describe("createHostSessions", () => {
  it("mints dispatch ids through the bridge when the harness advertises one", async () => {
    const sessions = createHostSessions({
      sessionUrl: "http://127.0.0.1:18789/api/manifold/session",
      fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ session: "agent:a:manifold:abcdef01-N" }) }),
    });
    assert.equal(typeof sessions.newNodeSessionId, "function");
    assert.equal(await sessions.newNodeSessionId({ agentId: "a", flowId: "abcdef01-x", nodeId: "N" }), "agent:a:manifold:abcdef01-N");
  });

  it("has no dispatch mint when no bridge is advertised — the node must declare its own", () => {
    assert.equal(createHostSessions({}).newNodeSessionId, undefined);
  });
});

describe("serve — boot guard", () => {
  it("refuses to start without MANIFOLD_BEARER", async () => {
    await assert.rejects(
      () => serve({
        env: { MANIFOLD_DATA_DIR: "/tmp/manifold-serve-guard-test" },
        logger: { info() {}, warn() {}, error() {} },
      }),
      /MANIFOLD_BEARER is required/,
    );
  });
});
