import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHarnessPort, assertHarnessShape } from "../../../harness/Harness.js";
import { formatIdentity, parseIdentity, stampCallMeta } from "../../../harness/identity.js";
import { createFakeHarness } from "../../../harness/fake.js";
import { createExecutor } from "../../../engine/executor.js";

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

  it("fake harness spawns and reads back", async () => {
    const { harness } = createFakeHarness({ id: "fake" });
    const { runId } = await harness.spawn({ sessionKey: "fake:a:s", message: "hi" });
    const waited = await harness.waitForRun({ runId });
    assert.equal(waited.status, "ok");
    const msgs = await harness.getSessionMessages({ sessionKey: "fake:a:s" });
    assert.equal(msgs.messages.length, 1);
  });

  it("executor accepts injected harness without api runtime", async () => {
    const { harness } = createFakeHarness({ id: "fake" });
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
