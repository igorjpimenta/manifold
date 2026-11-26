import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createExecutor } from "../../../engine/executor.js";

const logger = { info: () => {}, warn: () => {}, error: () => {} };
const api = { runtime: {}, config: {} };

function makeAdmissionQueueStore() {
  return {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
  };
}

// Must inject — the real singleton defaults to ~/.openclaw/runtime.sqlite.
function makeNodeExecutionsStore() {
  const resultsByFlow = new Map();
  const invocationMeta = new Map();
  return {
    enter: async (args) => {
      const invocationId = `inv-${args.flowId}-${args.nodeId}`;
      invocationMeta.set(invocationId, { flowId: args.flowId, nodeId: args.nodeId });
      return { invocationId, isNew: args.kind === "fresh" };
    },
    write: () => {},
    writeResult: async (invocationId, result) => {
      const meta = invocationMeta.get(invocationId);
      if (meta?.flowId && meta?.nodeId) {
        if (!resultsByFlow.has(meta.flowId)) resultsByFlow.set(meta.flowId, new Map());
        resultsByFlow.get(meta.flowId).set(meta.nodeId, JSON.stringify(result ?? null));
      }
      return { ok: true, tooLarge: false, bytes: 0 };
    },
    listResultsForFlow: async (flowId) => {
      const m = resultsByFlow.get(flowId);
      if (!m) return [];
      return [...m.entries()].map(([nodeId, resultJson]) => ({
        nodeId, visitSeq: 1, resultWrittenAt: Date.now(), resultTooLargeBytes: null, resultJson,
      }));
    },
    writeGateRecord: async () => ({ ok: true }),
    getGateRecord: async () => null,
    findCurrentStep: async () => null,
    queryByFlow: () => [],
    queryByNode: () => [],
    queryByPipeline: () => [],
    queryErrorsByFlow: () => [],
    queryErrorsByFlowIds: () => new Map(),
    close: () => {},
  };
}

function makeStore() {
  const map = new Map();
  let rev = 10;
  return {
    get: (id) => map.get(id) ?? null,
    create: ({ pipelineId, goal, initialState }) => {
      const flowId = `sc-${Math.random().toString(36).slice(2, 10)}`;
      const stateJson = { ...initialState, pipelineId };
      const record = { flowId, pipelineId, goal, status: "running", revision: ++rev, lastStep: "__start__", stateJson, waitJson: null };
      map.set(flowId, record);
      return record;
    },
    listFor: (pid) => [...map.values()].filter((f) => f.pipelineId === pid),
    setRunning: ({ flowId, expectedRevision, lastStep, stateJson }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      map.set(flowId, { ...f, lastStep, stateJson, revision: ++rev });
      return { applied: true };
    },
    setWaiting: ({ flowId, expectedRevision, waitJson, stateJson, lastStep, statusReason }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      map.set(flowId, { ...f, status: "waiting", waitJson, stateJson, lastStep, statusReason, revision: ++rev });
      return { applied: true };
    },
    fail: ({ flowId, expectedRevision, stateJson, statusReason }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      map.set(flowId, { ...f, status: "failed", stateJson, statusReason, revision: ++rev });
      return { applied: true };
    },
    finish: ({ flowId, expectedRevision, stateJson }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      map.set(flowId, { ...f, status: "succeeded", stateJson, revision: ++rev });
      return { applied: true };
    },
    skip: ({ flowId, expectedRevision }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      map.set(flowId, { ...f, status: "skipped", revision: ++rev });
      return { applied: true };
    },
    cancel: async ({ flowId }) => {
      const f = map.get(flowId);
      if (!f) return { found: false, cancelled: false };
      map.set(flowId, { ...f, status: "cancelled" });
      return { found: true, cancelled: true, flow: map.get(flowId) };
    },
    _map: map,
  };
}

function makeExecutor(pipelineMap) {
  const store = makeStore();
  const executor = createExecutor({
    api,
    store,
    pipelinesRef: { value: new Map(Object.entries(pipelineMap)) },
    ensurePipelinesLoaded: null,
    logger,
    runTurn: null,
    failureStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex: null,
    nodeExecutionsStore: makeNodeExecutionsStore(),
    admissionQueueStore: makeAdmissionQueueStore(),
  });
  return { executor, store };
}

function makePipeline({ runAudit, noopReturn } = {}) {
  return {
    goal: "test",
    state: {},
    allowConcurrent: true,
    nodes: { NOOP: { kind: "code", run: () => noopReturn ?? {} } },
    edges: { __start__: "NOOP", NOOP: "__end__" },
    ...(runAudit !== undefined ? { runAudit } : {}),
  };
}

describe("executor __end__: successCriteria routing", () => {
  it("marks flow succeeded when successCriteria returns 'succeeded'", async () => {
    const { executor, store } = makeExecutor({
      p: makePipeline({ runAudit: { successCriteria: () => "succeeded" } }),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  it("marks flow failed when successCriteria returns 'failed'", async () => {
    const { executor, store } = makeExecutor({
      p: makePipeline({ runAudit: { successCriteria: () => "failed" } }),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "failed");
  });

  it("marks flow skipped when successCriteria returns 'skipped'", async () => {
    const { executor, store } = makeExecutor({
      p: makePipeline({ runAudit: { successCriteria: () => "skipped" } }),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "skipped");
  });

  it("defaults to succeeded when no successCriteria is defined", async () => {
    const { executor, store } = makeExecutor({ p: makePipeline() });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  it("defaults to succeeded when runAudit has no successCriteria", async () => {
    const { executor, store } = makeExecutor({
      p: makePipeline({ runAudit: { summarize: () => ({}) } }),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  it("defaults to succeeded when successCriteria throws", async () => {
    const { executor, store } = makeExecutor({
      p: makePipeline({ runAudit: { successCriteria: () => { throw new Error("boom"); } } }),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  it("defaults to succeeded when successCriteria returns an unrecognised string", async () => {
    const { executor, store } = makeExecutor({
      p: makePipeline({ runAudit: { successCriteria: () => "maybe" } }),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  it("uses state._successCriteriaReason as the fail message", async () => {
    const { executor, store } = makeExecutor({
      p: {
        goal: "test",
        state: {},
        allowConcurrent: true,
        nodes: {
          NOOP: { kind: "code", run: () => ({ _successCriteriaReason: "nothing sent today" }) },
        },
        edges: { __start__: "NOOP", NOOP: "__end__" },
        runAudit: { successCriteria: () => "failed" },
      },
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    const flow = store.get(r.flowId);
    assert.equal(flow.status, "failed");
    assert.ok(
      flow.statusReason?.includes("nothing sent today"),
      `expected reason in statusReason, got: ${flow.statusReason}`,
    );
  });

  it("successCriteria receives the display-enriched finalState", async () => {
    let capturedState = null;
    const { executor, store } = makeExecutor({
      p: {
        goal: "test",
        state: {},
        allowConcurrent: true,
        nodes: { NOOP: { kind: "code", run: () => ({ sentinel: 42 }) } },
        edges: { __start__: "NOOP", NOOP: "__end__" },
        display: (s) => ({ title: "run done", summary: null }),
        runAudit: {
          successCriteria: (s) => {
            capturedState = s;
            return "succeeded";
          },
        },
      },
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(capturedState?.sentinel, 42, "criteria should see node state");
    assert.equal(capturedState?._display?.title, "run done", "criteria should see display enrichment");
  });
});
