/**
 * Unit tests for routes/operator-resume.js
 *
 * Verifies resumeFlowFromGate's outcomes:
 *   - Applied path resumes via setImmediate and reports queued:false.
 *   - Full pool leaves status:queued to the admission cycle, no resume.
 *   - Unknown flow reports flow_not_found without touching admission.
 *   - Failed flip reports the code and releases the claimed slot.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mock } from "node:test";
import { resumeFlowFromGate } from "../../../routes/operator-resume.js";

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} };
const apiStub = { logger: silentLogger };

function makeFlow(overrides = {}) {
  return {
    flowId: "flow-1",
    revision: 3,
    lastStep: "GATE",
    stateJson: { pipelineId: "p" },
    parentInvocationId: null,
    ...overrides,
  };
}

function drainImmediate() {
  return new Promise((resolve) => setImmediate(resolve));
}

// ── applied path ─────────────────────────────────────────────────────────────

describe("resumeFlowFromGate — applied", () => {
  test("flips running, resumes on setImmediate, reports queued:false", async () => {
    const flow = makeFlow();
    const store = {
      get: async () => ({ ...flow }),
      setRunning: mock.fn(async () => ({ applied: true })),
    };
    const admissionQueueStore = {
      admit: async () => ({ id: "adm-1", status: "admitted" }),
      release: mock.fn(async () => ({ released: true, promoted: null })),
    };
    const executor = { resume: mock.fn(async () => {}) };

    const result = await resumeFlowFromGate({
      store, readStore: null, admissionQueueStore, runStore: {},
      executor, logger: silentLogger, api: apiStub,
      flowId: "flow-1", nodeId: "GATE", decision: "approve",
    });

    assert.deepEqual(result, { ok: true, flowId: "flow-1", queued: false });
    assert.equal(store.setRunning.mock.calls.length, 1);
    await drainImmediate();
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 1);
    assert.deepEqual(executor.resume.mock.calls[0].arguments, ["flow-1"]);
  });

  test("full pool parks at queued without resuming", async () => {
    const flow = makeFlow();
    const store = {
      get: async () => ({ ...flow }),
      setQueued: mock.fn(async () => ({ applied: true })),
    };
    const admissionQueueStore = {
      admit: async () => ({ id: "adm-2", status: "queued" }),
      release: mock.fn(async () => ({ released: true, promoted: null })),
    };
    const executor = { resume: mock.fn(async () => {}) };

    const result = await resumeFlowFromGate({
      store, readStore: null, admissionQueueStore, runStore: {},
      executor, logger: silentLogger, api: apiStub,
      flowId: "flow-1", nodeId: "GATE", decision: "approve",
    });

    assert.deepEqual(result, { ok: true, flowId: "flow-1", queued: true });
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 0, "queued flows resume via the admission cycle, not here");
  });
});

// ── failure paths ────────────────────────────────────────────────────────────

describe("resumeFlowFromGate — failures", () => {
  test("unknown flow reports flow_not_found", async () => {
    const admit = mock.fn(async () => ({ id: "x", status: "admitted" }));
    const result = await resumeFlowFromGate({
      store: { get: async () => null }, readStore: null,
      admissionQueueStore: { admit }, runStore: {},
      executor: { resume: async () => {} },
      logger: silentLogger, api: apiStub,
      flowId: "missing", nodeId: "GATE", decision: "approve",
    });
    assert.deepEqual(result, { ok: false, code: "flow_not_found" });
    assert.equal(admit.mock.calls.length, 0, "no admission claimed for a missing flow");
  });

  test("failed flip reports the code and releases the claimed slot", async () => {
    const flow = makeFlow();
    const store = {
      get: async () => ({ ...flow }),
      setRunning: async () => ({ applied: false, code: "revision_conflict" }),
    };
    const release = mock.fn(async () => ({ released: true, promoted: null }));
    const admissionQueueStore = {
      admit: async () => ({ id: "adm-3", status: "admitted" }),
      release,
    };

    const result = await resumeFlowFromGate({
      store, readStore: null, admissionQueueStore, runStore: {},
      executor: { resume: async () => {} },
      logger: silentLogger, api: apiStub,
      flowId: "flow-1", nodeId: "GATE", decision: "approve",
    });

    assert.equal(result.ok, false);
    assert.match(result.code, /revision_conflict/);
    assert.equal(release.mock.calls.length, 1, "claimed slot given back on CAS loss");
  });
});
