import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createPersist, REVISION_RETRY_LIMIT } from "../../../engine/executor-persist.js";

const logger = { warn: () => {}, info: () => {} };

function makeStore(flow, opts = {}) {
  let current = { ...flow };
  let rev = flow.revision ?? 1;
  return {
    get: (id) => (id === current.flowId ? { ...current, revision: rev } : null),
    setRunning: ({ expectedRevision, lastStep, stateJson }) => {
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, lastStep, stateJson, revision: ++rev };
      return { applied: true };
    },
    setWaiting: ({ expectedRevision, waitJson, stateJson }) => {
      if (opts.alwaysConflict) return { applied: false, code: "revision_conflict" };
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, waitJson, stateJson, status: "waiting", revision: ++rev };
      return { applied: true };
    },
    setQueued: ({ expectedRevision, waitJson, stateJson }) => {
      if (opts.alwaysConflict) return { applied: false, code: "revision_conflict" };
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, waitJson, stateJson, status: "queued", revision: ++rev };
      return { applied: true };
    },
    skip: ({ expectedRevision }) => {
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, status: "skipped", revision: ++rev };
      return { applied: true };
    },
    finish: ({ expectedRevision }) => {
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, status: "succeeded", revision: ++rev };
      return { applied: true };
    },
    fail: ({ expectedRevision }) => {
      if (expectedRevision !== rev) return { applied: false, code: "revision_conflict" };
      current = { ...current, status: "failed", revision: ++rev };
      return { applied: true };
    },
    _current: () => current,
  };
}

function makeFlow(overrides = {}) {
  return {
    flowId: "12345678-0000-0000-0000-000000000001",
    status: "running",
    revision: 1,
    lastStep: "NODE_A",
    stateJson: { pipelineId: "test_pipeline" },
    ...overrides,
  };
}

describe("persistRunningRetrying", () => {
  it("applies setRunning on first try", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const { persistRunningRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistRunningRetrying({ flowId: flow.flowId, revision: 1, lastStep: "NODE_B", state: { pipelineId: "x" } });
    assert.equal(result.applied, true);
    assert.equal(store._current().lastStep, "NODE_B");
  });

  it("retries on revision_conflict and succeeds", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    store.setRunning({ expectedRevision: 1, lastStep: "EXTERNAL", stateJson: { pipelineId: "x" } });
    const { persistRunningRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistRunningRetrying({ flowId: flow.flowId, revision: 1, lastStep: "NODE_B", state: { pipelineId: "x" } });
    assert.equal(result.applied, true);
  });

  it("returns retry_exhausted after REVISION_RETRY_LIMIT conflicts", async () => {
    const flow = makeFlow();
    const store = makeStore(flow, { alwaysConflict: true });
    store.setRunning = () => ({ applied: false, code: "revision_conflict" });
    const { persistRunningRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistRunningRetrying({ flowId: flow.flowId, revision: 1, lastStep: "NODE_B", state: {} });
    assert.equal(result.applied, false);
    assert.equal(result.code, "retry_exhausted");
  });
});

describe("persistWaitingRetrying", () => {
  it("applies setWaiting and calls notifyFlowEnteredWaiting", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    let notified = false;
    const { persistWaitingRetrying } = createPersist({
      store, failureStore: null, logger,
      notifyFlowEnteredWaiting: () => { notified = true; },
    });
    const result = await persistWaitingRetrying({
      flowId: flow.flowId, revision: 1, lastStep: "NODE_A",
      state: {}, waitJson: { kind: "budget-gate-approval" }, statusReason: "blocked",
    });
    assert.equal(result.applied, true);
    assert.equal(notified, true);
  });

  it("handles async notifyFlowEnteredWaiting rejection gracefully", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const { persistWaitingRetrying } = createPersist({
      store, failureStore: null, logger,
      notifyFlowEnteredWaiting: () => Promise.reject(new Error("notify failed")),
    });
    const result = await persistWaitingRetrying({
      flowId: flow.flowId, revision: 1, lastStep: "NODE_A",
      state: {}, waitJson: { kind: "budget-gate-approval" }, statusReason: "x",
    });
    assert.equal(result.applied, true);
  });
});

describe("persistQueuedRetrying", () => {
  it("applies setQueued on first try", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const { persistQueuedRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistQueuedRetrying({
      flowId: flow.flowId, revision: 1, lastStep: "NODE_A",
      state: {}, waitJson: { kind: "agent-semaphore" }, statusReason: "queued",
    });
    assert.equal(result.applied, true);
    assert.equal(store._current().status, "queued");
  });

  it("does not call notifyFlowEnteredWaiting — that hook is for external-channel gate delivery, not internal queueing", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    let notified = false;
    const { persistQueuedRetrying } = createPersist({
      store, failureStore: null, logger,
      notifyFlowEnteredWaiting: () => { notified = true; },
    });
    await persistQueuedRetrying({
      flowId: flow.flowId, revision: 1, lastStep: "NODE_A",
      state: {}, waitJson: { kind: "agent-semaphore" }, statusReason: "queued",
    });
    assert.equal(notified, false);
  });

  it("retries on revision_conflict and succeeds", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    store.setRunning({ expectedRevision: 1, lastStep: "EXTERNAL", stateJson: { pipelineId: "x" } });
    const { persistQueuedRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistQueuedRetrying({
      flowId: flow.flowId, revision: 1, lastStep: "NODE_A", state: {}, waitJson: {}, statusReason: "x",
    });
    assert.equal(result.applied, true);
  });

  it("returns retry_exhausted after REVISION_RETRY_LIMIT conflicts", async () => {
    const flow = makeFlow();
    const store = makeStore(flow, { alwaysConflict: true });
    const { persistQueuedRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistQueuedRetrying({
      flowId: flow.flowId, revision: 1, lastStep: "NODE_A", state: {}, waitJson: {}, statusReason: "x",
    });
    assert.equal(result.applied, false);
    assert.equal(result.code, "retry_exhausted");
  });
});

describe("persistSkipRetrying", () => {
  it("applies skip on first try", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const { persistSkipRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistSkipRetrying({ flowId: flow.flowId, revision: 1, state: {}, reason: "duplicate" });
    assert.equal(result.applied, true);
  });
});

describe("persistFinishRetrying", () => {
  it("applies finish on first try", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const { persistFinishRetrying } = createPersist({ store, failureStore: null, notifyFlowEnteredWaiting: null, logger });
    const result = await persistFinishRetrying({ flowId: flow.flowId, revision: 1, state: {} });
    assert.equal(result.applied, true);
  });
});

describe("persistFailRetrying", () => {
  it("applies fail and calls recordEngineFailure", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    let recorded = null;
    const failureStore = { recordEngineFailure: (args) => { recorded = args; } };
    const { persistFailRetrying } = createPersist({ store, failureStore, notifyFlowEnteredWaiting: null, logger });
    const result = await persistFailRetrying({
      flowId: flow.flowId, revision: 1, state: { pipelineId: "p" },
      message: "DRAFT: agent failed", failureNode: "DRAFT",
    });
    assert.equal(result.applied, true);
    assert.equal(recorded?.failureNode, "DRAFT");
    assert.equal(recorded?.flowId, flow.flowId);
    assert.equal(recorded?.invocationId, null, "defaults to null when omitted");
    assert.equal(recorded?.executionId, null);
  });

  it("threads invocationId/executionId through to recordEngineFailure", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    let recorded = null;
    const failureStore = { recordEngineFailure: (args) => { recorded = args; } };
    const { persistFailRetrying } = createPersist({ store, failureStore, notifyFlowEnteredWaiting: null, logger });
    await persistFailRetrying({
      flowId: flow.flowId, revision: 1, state: { pipelineId: "p" },
      message: "DRAFT: agent failed", failureNode: "DRAFT",
      invocationId: "inv-1", executionId: "exec-1",
    });
    assert.equal(recorded?.invocationId, "inv-1");
    assert.equal(recorded?.executionId, "exec-1");
  });

  it("threads errorType through to recordEngineFailure, null when omitted", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    let recorded = null;
    const failureStore = { recordEngineFailure: (args) => { recorded = args; } };
    const { persistFailRetrying } = createPersist({ store, failureStore, notifyFlowEnteredWaiting: null, logger });
    await persistFailRetrying({
      flowId: flow.flowId, revision: 1, state: { pipelineId: "p" },
      message: "landing page unresolved", failureNode: "SUBMIT_CODE",
      invocationId: "inv-1", executionId: "exec-1", errorType: "provider-5xx",
    });
    assert.equal(recorded?.errorType, "provider-5xx");

    const flow2 = makeFlow({ flowId: "12345678-0000-0000-0000-000000000002" });
    const store2 = makeStore(flow2);
    let recorded2 = null;
    const failureStore2 = { recordEngineFailure: (args) => { recorded2 = args; } };
    const { persistFailRetrying: persistFailRetrying2 } = createPersist({ store: store2, failureStore: failureStore2, notifyFlowEnteredWaiting: null, logger });
    await persistFailRetrying2({
      flowId: flow2.flowId, revision: 1, state: {}, message: "plain crash", failureNode: "NODE",
    });
    assert.equal(recorded2?.errorType, null, "defaults to null when omitted");
  });

  it("does not throw when recordEngineFailure throws", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    const failureStore = { recordEngineFailure: () => { throw new Error("db locked"); } };
    const { persistFailRetrying } = createPersist({ store, failureStore, notifyFlowEnteredWaiting: null, logger });
    const result = await persistFailRetrying({
      flowId: flow.flowId, revision: 1, state: {}, message: "err", failureNode: "NODE",
    });
    assert.equal(result.applied, true);
  });

  // A sync throw is caught by try/catch even unawaited; only an async rejection exposes a missing await.
  it("does not throw (and settles before returning) when recordEngineFailure REJECTS asynchronously", async () => {
    const flow = makeFlow();
    const store = makeStore(flow);
    let settled = false;
    const failureStore = {
      recordEngineFailure: async () => {
        await new Promise((r) => setTimeout(r, 5));
        settled = true;
        throw new Error("async db write failed");
      },
    };
    const { persistFailRetrying } = createPersist({ store, failureStore, notifyFlowEnteredWaiting: null, logger });
    const result = await persistFailRetrying({
      flowId: flow.flowId, revision: 1, state: {}, message: "err", failureNode: "NODE",
    });
    assert.equal(result.applied, true);
    assert.equal(settled, true, "persistFailRetrying must wait for recordEngineFailure to settle, not fire-and-forget it");
  });
});

describe("createPersist — misc", () => {
  it("re-exports REVISION_RETRY_LIMIT", () => {
    assert.equal(REVISION_RETRY_LIMIT, 5);
  });
});
