import { describe, it } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { createOpenClawHarness } from "../../../harness/openclaw.js";
import { defaultAgentSessionKey } from "../../../engine/executor-helpers.js";

function makeApi(subagent) {
  const tools = [];
  const hooks = [];
  return {
    api: {
      runtime: subagent ? { subagent } : {},
      registerTool: (def) => { tools.push(def); return def; },
      on: (kind, fn) => { hooks.push([kind, fn]); },
    },
    tools,
    hooks,
  };
}

describe("openclaw harness adapter", () => {
  it("constructs with default id openclaw", () => {
    const { api } = makeApi(null);
    assert.equal(createOpenClawHarness({ api }).id, "openclaw");
    assert.equal(createOpenClawHarness({ api, id: "x" }).id, "x");
  });

  it("spawn delegates to the resolved subagent", async () => {
    const calls = [];
    const subagent = {
      run: async (args) => { calls.push(args); return { runId: "r1" }; },
      waitForRun: async ({ runId }) => ({ status: "ok", runId }),
      getSessionMessages: async () => ({ messages: [] }),
    };
    const { api } = makeApi(subagent);
    const harness = createOpenClawHarness({ api, getWorkingSubagent: () => null });
    const out = await harness.spawn({ sessionKey: "k", message: "hi" });
    assert.equal(out.runId, "r1");
    assert.equal(calls.length, 1);
    const waited = await harness.waitForRun({ runId: "r1" });
    assert.equal(waited.status, "ok");
  });

  it("prefers getWorkingSubagent over api.runtime.subagent", async () => {
    const calls = [];
    const working = { run: async () => ({ runId: "w" }) };
    const { api } = makeApi({ run: async () => ({ runId: "api" }) });
    const harness = createOpenClawHarness({ api, getWorkingSubagent: () => working });
    void calls;
    const out = await harness.spawn({});
    assert.equal(out.runId, "w");
  });

  it("throws GatewayHandoffNeeded-style error when subagent is missing", async () => {
    const { api } = makeApi(null);
    const harness = createOpenClawHarness({ api, getWorkingSubagent: () => null });
    await assert.rejects(() => harness.spawn({}), /subagent runtime unavailable/);
    await assert.rejects(() => harness.waitForRun({ runId: "x" }), /subagent runtime unavailable/);
    await assert.rejects(() => harness.getSessionMessages({}), /subagent runtime unavailable/);
  });

  it("getSubagent returns the gated runtime", () => {
    const subagent = { run: async () => ({}), waitForRun: async () => ({}), getSessionMessages: async () => ({}) };
    const { api } = makeApi(subagent);
    const harness = createOpenClawHarness({ api, whenGatewayReady: async () => {} });
    assert.ok(harness.getSubagent());
  });

  it("emit calls the injected emitter and defaults to no-op", () => {
    const seen = [];
    const { api } = makeApi(null);
    createOpenClawHarness({ api, emit: (e) => seen.push(e) }).emit({ t: 1 });
    assert.deepEqual(seen, [{ t: 1 }]);
    createOpenClawHarness({ api }).emit({ t: 2 });
  });

  it("registerTool/registerHook delegate to api, record locally otherwise", () => {
    const { api, tools, hooks } = makeApi(null);
    const harness = createOpenClawHarness({ api });
    harness.registerTool("t", { name: "t" });
    harness.registerHook("ev", () => {});
    assert.equal(tools.length, 1);
    assert.equal(hooks.length, 1);
    const bare = createOpenClawHarness({});
    bare.registerTool("t", { name: "t" });
    bare.registerHook("ev", () => {});
  });

  it("storePaths honors MANIFOLD_RUNTIME_DB/MANIFOLD_OPS_DB overrides", () => {
    const { api } = makeApi(null);
    const harness = createOpenClawHarness({ api });
    const dataDir = path.join(os.homedir(), ".openclaw");
    assert.deepEqual(harness.storePaths(), {
      dataDir,
      runtimeDb: path.join(dataDir, "runtime.sqlite"),
      opsDb: path.join(dataDir, "ops.sqlite"),
    });
    process.env.MANIFOLD_RUNTIME_DB = "/tmp/r.sqlite";
    process.env.MANIFOLD_OPS_DB = "/tmp/o.sqlite";
    try {
      assert.deepEqual(harness.storePaths(), { dataDir, runtimeDb: "/tmp/r.sqlite", opsDb: "/tmp/o.sqlite" });
    } finally {
      delete process.env.MANIFOLD_RUNTIME_DB;
      delete process.env.MANIFOLD_OPS_DB;
    }
  });

  it("sessionKey keeps the gateway-native key shape and fails loudly when agent missing", () => {
    const { api } = makeApi(null);
    const harness = createOpenClawHarness({ api });
    const flowId = "12345678-aaaa-bbbb-cccc-000000000000";
    const key = harness.sessionKey({ agent: "scout", flowId, nodeId: "PROPOSE" });
    const seed = defaultAgentSessionKey("scout", flowId, "PROPOSE");
    assert.equal(key, seed);
    assert.match(key, /^agent:scout:subagent:gf-/);
    assert.throws(() => harness.sessionKey({ flowId, nodeId: "PROPOSE" }), /unstamped/);
  });
});
