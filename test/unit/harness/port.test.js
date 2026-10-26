import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHarnessPort, assertHarnessShape } from "../../../harness/Harness.js";
import { formatIdentity, parseIdentity, stampCallMeta } from "../../../harness/identity.js";
import { createExecutor } from "../../../engine/executor.js";

function makeStubHarness({ id = "stub" } = {}) {
  const runs = new Map();
  let seq = 0;
  const methods = {
    async spawn({ sessionKey, message }) {
      seq += 1;
      const runId = `stub-run-${seq}`;
      runs.set(runId, { sessionKey, message });
      return { runId };
    },
    async waitForRun({ runId }) {
      return runs.has(runId) ? { status: "ok", runId } : { status: "not-found" };
    },
    async getSessionMessages({ sessionKey, limit = 5 } = {}) {
      const msgs = [...runs.values()]
        .filter((r) => r.sessionKey === sessionKey)
        .slice(-limit)
        .map((r) => ({ role: "assistant", content: r.message ?? "" }));
      return { messages: msgs };
    },
    emit() {},
    registerTool() {},
    registerHook() {},
    storePaths() {
      return { dataDir: "/tmp/manifold-stub", runtimeDb: "/tmp/manifold-stub/runtime.sqlite", opsDb: "/tmp/manifold-stub/ops.sqlite" };
    },
    sessionKey({ agent, session }) {
      if (!agent) throw new Error("manifold identity: unstamped call refused (agent must be stamped)");
      return session ?? `agent:${agent}:stub`;
    },
  };
  return createHarnessPort({
    ...methods,
    id,
    getSubagent() {
      return { run: methods.spawn, waitForRun: methods.waitForRun, getSessionMessages: methods.getSessionMessages };
    },
  });
}

describe("harness port", () => {
  it("rejects unnamed harness", () => {
    assert.throws(() => assertHarnessShape({}), /harness\.id/);
  });

  it("rejects incomplete harness", () => {
    assert.throws(() => assertHarnessShape({ id: "x" }), /harness\.\w+\(\) missing/);
  });

  it("identity triple round-trips, unstamped calls fail", () => {
    const key = formatIdentity({ harness: "openclaw", agent: "a1", session: "s1" });
    assert.equal(key, "openclaw:a1:s1");
    assert.deepEqual(parseIdentity(key), { harness: "openclaw", agent: "a1", session: "s1" });
    assert.throws(() => stampCallMeta({}, { harness: "openclaw", agent: "a1" }), /unstamped/);
  });

  it("inline stub harness spawns and reads back", async () => {
    const harness = makeStubHarness({ id: "stub" });
    const { runId } = await harness.spawn({ sessionKey: "fake:a:s", message: "hi" });
    const waited = await harness.waitForRun({ runId });
    assert.equal(waited.status, "ok");
    const msgs = await harness.getSessionMessages({ sessionKey: "fake:a:s" });
    assert.equal(msgs.messages.length, 1);
  });

  it("executor accepts injected harness without api runtime", async () => {
    const harness = makeStubHarness({ id: "stub" });
    const noopStore = { get: async () => null };
    const ex = createExecutor({
      api: {},
      store: noopStore,
      pipelinesRef: { value: new Map() },
      ensurePipelinesLoaded: async () => new Map(),
      logger: { info() {}, warn() {}, error() {} },
      harness,
      sessionIndex: null,
      nodeExecutionsStore: { write: async () => ({}), findCurrentStep: async () => null },
      runStore: null,
      admissionQueueStore: { admit: async () => ({ admitted: true }) },
    });
    assert.equal(typeof ex.start, "function");
    assert.ok(ex.getSubagent());
  });
});
