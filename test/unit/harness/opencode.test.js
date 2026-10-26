import { describe, it } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { createOpencodeHarness, OPENCODE_SPAWN_UNSUPPORTED } from "../../../harness/opencode.js";
import { createHarnessPort } from "../../../harness/Harness.js";
import { defaultAgentSessionKey } from "../../../engine/executor-helpers.js";
import { createExecutor } from "../../../engine/executor.js";

describe("opencode harness adapter", () => {
  it("constructs with default id opencode and satisfies the port shape", () => {
    const harness = createOpencodeHarness();
    assert.equal(harness.id, "opencode");
    assert.equal(createOpencodeHarness({ id: "x" }).id, "x");
    assert.doesNotThrow(() => createHarnessPort({ ...harness }));
  });

  it("spawn throws the declared unsupported error", async () => {
    const harness = createOpencodeHarness();
    await assert.rejects(() => harness.spawn({ sessionKey: "k", message: "hi" }), new RegExp("spawn unsupported"));
    await assert.rejects(() => harness.spawn({}), new RegExp("no run slot"));
    assert.match(OPENCODE_SPAWN_UNSUPPORTED, /read-only\/dry-run/);
  });

  it("waitForRun reports unsupported and getSessionMessages is empty", async () => {
    const harness = createOpencodeHarness();
    assert.deepEqual(await harness.waitForRun({ runId: "anything" }), { status: "unsupported" });
    assert.deepEqual(await harness.getSessionMessages({ sessionKey: "k" }), { messages: [] });
  });

  it("getSubagent returns null so the executor falls back cleanly", () => {
    assert.equal(createOpencodeHarness().getSubagent(), null);
  });

  it("emit records locally and forwards to the injected emitter", () => {
    const seen = [];
    const fwd = createOpencodeHarness({ emit: (e) => seen.push(e) });
    fwd.emit({ t: 1 });
    assert.deepEqual(seen, [{ t: 1 }]);
    assert.doesNotThrow(() => createOpencodeHarness().emit({ t: 2 }));
  });

  it("registerTool/registerHook record locally", () => {
    const harness = createOpencodeHarness();
    assert.doesNotThrow(() => {
      harness.registerTool("t", { name: "t" });
      harness.registerHook("ev", () => {});
    });
  });

  it("storePaths stays package-owned and never under .openclaw", () => {
    const harness = createOpencodeHarness();
    const fallback = path.join(os.homedir(), ".manifold");
    assert.deepEqual(harness.storePaths(), {
      dataDir: fallback,
      runtimeDb: path.join(fallback, "runtime.sqlite"),
      opsDb: path.join(fallback, "ops.sqlite"),
    });
    for (const v of Object.values(harness.storePaths())) {
      assert.doesNotMatch(String(v), /\.openclaw/);
    }
    const custom = createOpencodeHarness({ dataDir: "/tmp/manifold-opencode" });
    assert.deepEqual(custom.storePaths().dataDir, "/tmp/manifold-opencode");
    for (const v of Object.values(custom.storePaths())) {
      assert.doesNotMatch(String(v), /\.openclaw/);
    }
  });

  it("sessionKey keeps the gateway-native key shape and fails loudly when agent missing", () => {
    const harness = createOpencodeHarness();
    const flowId = "12345678-aaaa-bbbb-cccc-000000000000";
    const key = harness.sessionKey({ agent: "scout", flowId, nodeId: "PROPOSE" });
    assert.equal(key, defaultAgentSessionKey("scout", flowId, "PROPOSE"));
    assert.match(key, /^agent:scout:subagent:gf-/);
    assert.throws(() => harness.sessionKey({ flowId, nodeId: "PROPOSE" }), /unstamped/);
  });

  it("executor with opencode harness and no api runtime resolves getSubagent() to null without throwing", () => {
    const harness = createOpencodeHarness();
    const ex = createExecutor({
      api: {},
      store: { get: async () => null },
      pipelinesRef: { value: new Map() },
      ensurePipelinesLoaded: async () => new Map(),
      logger: { info() {}, warn() {}, error() {} },
      harness,
      sessionIndex: null,
      nodeExecutionsStore: { write: async () => ({}), findCurrentStep: async () => null },
      runStore: null,
      admissionQueueStore: { admit: async () => ({ admitted: true }) },
    });
    assert.ok(!ex.getSubagent());
  });
});
