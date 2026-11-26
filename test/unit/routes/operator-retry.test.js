import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";

const admissionQueueStore = {
  admit: async () => ({ id: "fake-admission", status: "admitted" }),
  release: async () => ({ released: true, promoted: null }),
};

let queryErrorsByFlowImpl = () => [];
let getLatestExecutionForNodeImpl = ({ nodeId }) => ({ invocationId: `inv-${nodeId}`, executionId: `exec-${nodeId}` });
let listInvocationIdsForNodeImpl = () => [];

await mock.module("../../../state/node-executions.js", {
  exports: {
    getNodeExecutionsStore: () => ({
      getLatestExecutionForNode: async (args) => getLatestExecutionForNodeImpl(args),
      listInvocationIdsForNode: async (flowId, nodeId) => listInvocationIdsForNodeImpl(flowId, nodeId),
    }),
  },
});

const { createRetryFailure } = await import("../../../routes/operator-retry.js");

const logger = { info: () => {}, warn: () => {} };

function errorEntries(nodeIds) {
  return nodeIds.map((nodeId) => ({ nodeId, branchKey: null, message: "boom", at: 1, executionId: `exec-${nodeId}`, invocationId: `inv-${nodeId}` }));
}

function makeFailureStore(failure = null) {
  return {
    get: async (id) => (id === failure?.id ? failure : null),
    queryErrorsByFlow: async (flowId) => queryErrorsByFlowImpl(flowId),
  };
}

const TERMINAL_STATUSES = new Set(["succeeded", "failed", "cancelled", "skipped"]);

function makeStore(flow = null) {
  let current = flow ? { ...flow } : null;
  let setRunningResult = { applied: true };
  return {
    get: async (id) => (current && current.flowId === id ? { ...current } : null),
    setRunning: async ({ flowId, expectedRevision, lastStep, stateJson }) => {
      if (!current || current.flowId !== flowId) return { applied: false, code: "not_found" };
      if (current.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      current = { ...current, lastStep, stateJson, revision: current.revision + 1, status: "running" };
      return setRunningResult;
    },
    forceBlock: async ({ flowId, expectedRevision, currentStep, stateJson, statusReason, ownerKey }) => {
      if (!current || current.flowId !== flowId) return { applied: false, code: "not_found" };
      if (current.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      if (!TERMINAL_STATUSES.has(current.status)) return { applied: false, code: "not_terminal" };
      current = {
        ...current,
        status: "blocked",
        currentStep,
        stateJson,
        statusReason,
        ownerKey,
        revision: current.revision + 1,
      };
      return { applied: true };
    },
    _current: () => current,
    _forceSetRunningResult: (r) => { setRunningResult = r; },
  };
}

function makeReadStore(flow = null) {
  return { get: async (id) => (flow && flow.flowId === id ? { ...flow } : null) };
}

function makePipelinesRef(pipelines = {}) {
  return { value: new Map(Object.entries(pipelines)) };
}

function makeExecutor() {
  const ran = [];
  return {
    runFlow: async (flowId) => { ran.push(flowId); },
    _ran: () => ran,
  };
}

function makeFailure({ id = "fail-1", flowId = "flow-aabbccdd-0000-0000-0000-000000000001", failureNode = "DRAFT", branchItemId = null, executionId = `exec-${failureNode}` } = {}) {
  return { id, flow_id: flowId, failure_node: failureNode, branch_item_id: branchItemId, execution_id: executionId };
}

function makeFlow({ flowId = "flow-aabbccdd-0000-0000-0000-000000000001", status = "failed", revision = 5, state = {}, waitJson = null } = {}) {
  return {
    flowId,
    status,
    revision,
    lastStep: "DRAFT",
    stateJson: { pipelineId: "test_pipe", ...state },
    waitJson,
  };
}

function makePipeline({ nodes = {}, edges = {} } = {}) {
  return { nodes, edges };
}

describe("retryFailureBranch — failure not found", () => {
  it("returns not_found when failure record is missing", async () => {
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(), readStore: makeReadStore(),
      failureStore: makeFailureStore(null), pipelinesRef: makePipelinesRef(),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: "no-such-id", via: "test" });
    assert.equal(result.code, "not_found");
    assert.ok(result.reason.includes("failure event not found"));
  });
});

describe("retryFailureBranch — no failureNode (bootstrap-level failure)", () => {
  it("retries the whole flow from __start__ when the flow/pipeline can be resolved", async () => {
    const failure = { ...makeFailure(), failure_node: null };
    const flow = makeFlow({ status: "failed", state: { _attemptNumber: 1 } });
    const pipeline = makePipeline({ edges: { __start__: "DRAFT", DRAFT: "__end__" } });
    const executor = makeExecutor();
    const store = makeStore(flow);
    const { retryFailureBranch } = createRetryFailure({
      store, readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.equal(result.mode, "flow-restart");
    assert.equal(result.rootNode, "__start__");
    const updated = store._current();
    assert.equal(updated.lastStep, "__start__");
    assert.equal(updated.stateJson?._attemptNumber, 2);
    await flushSetImmediate();
    assert.deepEqual(executor._ran(), [flow.flowId]);
  });

  it("still returns not_found when the flow itself is gone from the registry", async () => {
    const failure = { ...makeFailure(), failure_node: null };
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(null), readStore: makeReadStore(null),
      failureStore: makeFailureStore(failure), pipelinesRef: makePipelinesRef(),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "not_found");
    assert.ok(result.reason.includes("flow gone from registry"));
  });

  it("still returns not_found when the pipeline is not loaded (e.g. the watcher dropped the synthetic entry)", async () => {
    const failure = { ...makeFailure(), failure_node: null };
    const flow = makeFlow({ status: "failed" });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure), pipelinesRef: makePipelinesRef({}),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "not_found");
    assert.ok(result.reason.includes("pipeline not loaded"));
  });
});

describe("retryFailureBranch — flow-level: flow not found", () => {
  it("returns not_found when flow is gone from registry", async () => {
    const failure = makeFailure({ branchItemId: null });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(null), readStore: makeReadStore(null),
      failureStore: makeFailureStore(failure), pipelinesRef: makePipelinesRef(),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "not_found");
    assert.ok(result.reason.includes("flow gone from registry"));
  });
});

describe("retryFailureBranch — flow-level: pipeline not found", () => {
  it("returns not_found when pipeline is not loaded", async () => {
    const failure = makeFailure({ branchItemId: null });
    const flow = makeFlow({ status: "failed" });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure), pipelinesRef: makePipelinesRef({}),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "not_found");
    assert.ok(result.reason.includes("pipeline not loaded"));
  });
});

describe("retryFailureBranch — flow-level: flow not terminal", () => {
  it("returns conflict when flow is running", async () => {
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" });
    const flow = makeFlow({ status: "running" });
    const pipeline = makePipeline({ edges: { __start__: "DRAFT", DRAFT: "__end__" } });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.ok(result.reason.includes("running"));
  });
});

describe("retryFailureBranch — flow-level: no edge resolves to rootNode", () => {
  it("returns conflict when no edge resolves to the failure node", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["ORPHAN"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "ORPHAN" });
    const flow = makeFlow({ status: "failed", state: { errors: [{ node: "ORPHAN", message: "boom" }] } });
    const pipeline = makePipeline({
      nodes: { DRAFT: {}, ORPHAN: {} },
      edges: { __start__: "DRAFT", DRAFT: "__end__" },
    });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.ok(result.reason.includes("no edge"));
  });
});

describe("retryFailureBranch — flow-level: ok", () => {
  it("clears failure node from state, calls setRunning, enqueues runFlow", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" });
    const flow = makeFlow({
      status: "failed",
      state: { DRAFT: { text: "old" }, errors: [{ node: "DRAFT", message: "boom" }] },
    });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const store = makeStore(flow);
    const failureStore = makeFailureStore(failure);
    const { retryFailureBranch } = createRetryFailure({
      store, readStore: makeReadStore(),
      failureStore,
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "cli" });
    assert.equal(result.code, "ok");
    assert.equal(result.mode, "flow-step");
    assert.equal(result.failureNode, "DRAFT");
    const updated = store._current();
    assert.equal(updated.stateJson?.DRAFT, undefined, "DRAFT should be cleared from state");
  });

  it("increments _attemptNumber in state", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["WORK"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "WORK" });
    const flow = makeFlow({
      status: "failed",
      state: { _attemptNumber: 2, WORK: { done: false }, errors: [{ node: "WORK", message: "boom" }] },
    });
    const pipeline = makePipeline({
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const store = makeStore(flow);
    const { retryFailureBranch } = createRetryFailure({
      store, readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(store._current().stateJson?._attemptNumber, 3);
  });
});

function makeSlowExecutor() {
  const ran = [];
  return {
    runFlow: async (flowId) => {
      await new Promise((r) => setImmediate(r));
      ran.push(flowId);
    },
    _ran: () => ran,
  };
}

describe("retryFailureBranch — awaitCompletion", () => {
  function setUp(executor) {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" });
    const flow = makeFlow({
      status: "failed",
      state: { DRAFT: { text: "old" }, errors: [{ node: "DRAFT", message: "boom" }] },
    });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    return { retryFailureBranch, failure, flow };
  }

  it("awaits runFlow to genuine completion when awaitCompletion:true — the CLI's one-shot process can't rely on a merely-scheduled macrotask surviving", async () => {
    const executor = makeSlowExecutor();
    const { retryFailureBranch, failure, flow } = setUp(executor);
    const result = await retryFailureBranch({ id: failure.id, via: "cli", awaitCompletion: true });
    assert.equal(result.code, "ok");
    assert.deepEqual(executor._ran(), [flow.flowId], "runFlow must have fully resolved before retryFailureBranch itself returns");
  });

  it("still fire-and-forgets (runFlow not yet run) when awaitCompletion is omitted — HTTP/Telegram callers need the fast return", async () => {
    const executor = makeSlowExecutor();
    const { retryFailureBranch, failure, flow } = setUp(executor);
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.deepEqual(executor._ran(), [], "runFlow must not have started yet — only scheduled");
    await flushSetImmediate();
    await flushSetImmediate();
    assert.deepEqual(executor._ran(), [flow.flowId], "runFlow does eventually run, just not before retryFailureBranch returned");
  });
});

describe("retryFailureBranch — flow-level: already-resolved failure", () => {
  it("returns conflict when the failure row's execution_id is not the node's latest execution", async () => {
    queryErrorsByFlowImpl = () => [];
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT", executionId: "exec-DRAFT-superseded" });
    const flow = makeFlow({ status: "failed", state: { errors: [] } });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.match(result.reason, /already resolved by an earlier retry/);
    assert.deepEqual(executor._ran(), [], "must not enqueue a re-run");
  });

  it("returns conflict when other nodes show errors but this failure's execution is not the node's latest", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["OTHER_NODE"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT", executionId: "exec-DRAFT-superseded" });
    const flow = makeFlow({ status: "failed", state: { errors: [{ node: "OTHER_NODE", message: "unrelated" }] } });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor: makeExecutor(), ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.match(result.reason, /already resolved by an earlier retry/);
  });
});

describe("retryFailureBranch — flow-level: eligibility keys off the CURRENT execution, not stale error rows", () => {
  it("rejects a stale failure_events row when the node's latest execution has since succeeded (superseded, no error)", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const prevImpl = getLatestExecutionForNodeImpl;
    getLatestExecutionForNodeImpl = ({ nodeId }) =>
      nodeId === "DRAFT" ? { invocationId: "inv-draft", executionId: "exec-draft-RETRY-2" } : prevImpl({ nodeId });
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" });
    const flow = makeFlow({ status: "failed" });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    getLatestExecutionForNodeImpl = prevImpl;
    assert.equal(result.code, "conflict");
    assert.match(result.reason, /already resolved by an earlier retry/);
    assert.deepEqual(executor._ran(), [], "must not enqueue a re-run of a superseded node");
  });

  it("accepts when the failure row's own execution_id still matches the node's latest execution", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT", executionId: "exec-DRAFT" });
    const flow = makeFlow({
      status: "blocked",
      state: { DRAFT: { text: "old" } },
    });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.equal(result.rootNode, "DRAFT");
  });

  it("rejects when the failure row's execution_id doesn't match the node's latest execution, even though the node still shows an error entry", async () => {
    queryErrorsByFlowImpl = () => [{ nodeId: "DRAFT", branchKey: null, message: "boom v2", at: 2, executionId: "exec-DRAFT-v2", invocationId: "inv-draft" }];
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT", executionId: "exec-DRAFT-v1" });
    const flow = makeFlow({ status: "failed" });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.deepEqual(executor._ran(), []);
  });

  it("rejects a failure row with no execution_id — eligibility is anchored to execution_id", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT", executionId: null });
    const flow = makeFlow({ status: "blocked", state: { DRAFT: { text: "old" } } });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(flow), readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.deepEqual(executor._ran(), []);
  });
});

describe("retryFailureBranch — branch_item_id is informational, does not block retry", () => {
  it("retries a branchItemId-present failure the same way as a branchItemId-null one", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const failure = makeFailure({ branchItemId: "job-999", failureNode: "DRAFT" });
    const flow = makeFlow({
      status: "failed",
      state: { DRAFT: { text: "old" }, errors: [{ node: "DRAFT", message: "boom" }] },
    });
    const pipeline = makePipeline({
      nodes: { PRIME: {}, DRAFT: {} },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const executor = makeExecutor();
    const store = makeStore(flow);
    const { retryFailureBranch } = createRetryFailure({
      store, readStore: makeReadStore(),
      failureStore: makeFailureStore(failure),
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, ownerSessionKey: "k", admissionQueueStore, logger,
    });
    const result = await retryFailureBranch({ id: failure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.equal(result.mode, "flow-step");
    assert.equal(result.failureNode, "DRAFT");
    const updated = store._current();
    assert.equal(updated.stateJson?.DRAFT, undefined, "DRAFT should be cleared from state");
  });
});

function flushSetImmediate() {
  return new Promise((resolve) => setImmediate(resolve));
}

function makeCascadeFailureStore(byId, byFlowId) {
  return {
    get: async (id) => byId[id] ?? null,
    list: async ({ flowId }) => byFlowId[flowId] ?? [],
    queryErrorsByFlow: async (flowId) => queryErrorsByFlowImpl(flowId),
  };
}

describe("retryFailureBranch — cascade retry redirect", () => {
  it("redirects to the real failure beneath a single-child propagation, and retries THAT flow", async () => {
    const propFailure = { id: "fail-prop", flow_id: "parent-flow", failure_node: "SUBMIT", error_type: "subflow-mode-not-satisfied" };
    const realFailure = makeFailure({ id: "fail-real", flowId: "child-flow", failureNode: "SUBMIT_CODE" });

    listInvocationIdsForNodeImpl = (flowId, nodeId) =>
      flowId === "parent-flow" && nodeId === "SUBMIT" ? ["inv-item-1"] : [];
    const runStore = {
      listForParentInvocations: async (ids) => ids.includes("inv-item-1")
        ? [{ flowId: "child-flow", parentInvocationId: "inv-item-1", status: "failed" }] : [],
    };
    queryErrorsByFlowImpl = () => errorEntries(["SUBMIT_CODE"]);
    const failureStore = makeCascadeFailureStore(
      { [propFailure.id]: propFailure, [realFailure.id]: realFailure },
      { "child-flow": [realFailure] },
    );
    const childFlow = makeFlow({ flowId: "child-flow", status: "failed" });
    const pipeline = makePipeline({ nodes: { SUBMIT_CODE: {} }, edges: { __start__: "SUBMIT_CODE", SUBMIT_CODE: "__end__" } });
    const executor = makeExecutor();

    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(childFlow), readStore: makeReadStore(childFlow), failureStore,
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, runStore, ownerSessionKey: "k", admissionQueueStore, logger,
    });

    const result = await retryFailureBranch({ id: propFailure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.equal(result.mode, "cascade-redirect");
    assert.deepEqual(result.redirectedTo, [realFailure.id]);
    assert.equal(result.results.length, 1);
    assert.equal(result.results[0].code, "ok");
    assert.equal(result.results[0].flowId, "child-flow", "the flow actually retried is the CHILD, not the propagation's own flow");
    await flushSetImmediate();
    assert.deepEqual(executor._ran(), ["child-flow"]);
  });

  it("recurses through a nested propagation to find the real failure two levels down", async () => {
    const topFailure = { id: "fail-top", flow_id: "grandparent-flow", failure_node: "PROPOSE", error_type: "subflow-mode-not-satisfied" };
    const midFailure = { id: "fail-mid", flow_id: "parent-flow", failure_node: "SUBMIT", error_type: "subflow-mode-not-satisfied" };
    const realFailure = makeFailure({ id: "fail-real", flowId: "child-flow", failureNode: "SUBMIT_CODE" });

    listInvocationIdsForNodeImpl = (flowId, nodeId) => {
      if (flowId === "grandparent-flow" && nodeId === "PROPOSE") return ["inv-item-top"];
      if (flowId === "parent-flow" && nodeId === "SUBMIT") return ["inv-item-mid"];
      return [];
    };
    const runStore = {
      listForParentInvocations: async (ids) => {
        if (ids.includes("inv-item-top")) return [{ flowId: "parent-flow", parentInvocationId: "inv-item-top", status: "failed" }];
        if (ids.includes("inv-item-mid")) return [{ flowId: "child-flow", parentInvocationId: "inv-item-mid", status: "failed" }];
        return [];
      },
    };
    queryErrorsByFlowImpl = () => errorEntries(["SUBMIT_CODE"]);
    const failureStore = makeCascadeFailureStore(
      { [topFailure.id]: topFailure, [midFailure.id]: midFailure, [realFailure.id]: realFailure },
      { "parent-flow": [midFailure], "child-flow": [realFailure] },
    );
    const childFlow = makeFlow({ flowId: "child-flow", status: "failed" });
    const pipeline = makePipeline({ nodes: { SUBMIT_CODE: {} }, edges: { __start__: "SUBMIT_CODE", SUBMIT_CODE: "__end__" } });
    const executor = makeExecutor();

    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(childFlow), readStore: makeReadStore(childFlow), failureStore,
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, runStore, ownerSessionKey: "k", admissionQueueStore, logger,
    });

    const result = await retryFailureBranch({ id: topFailure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.deepEqual(result.redirectedTo, [realFailure.id], "walked past the mid-level propagation straight to the real failure");
    await flushSetImmediate();
    assert.deepEqual(executor._ran(), ["child-flow"]);
  });

  it("returns conflict when no real failure exists yet beneath the propagation (e.g. children still legitimately pending)", async () => {
    const propFailure = { id: "fail-prop", flow_id: "parent-flow", failure_node: "SUBMIT", error_type: "subflow-mode-not-satisfied" };
    listInvocationIdsForNodeImpl = () => ["inv-item-1"];
    const runStore = {
      listForParentInvocations: async () => [{ flowId: "child-flow", parentInvocationId: "inv-item-1", status: "blocked" }],
    };
    const failureStore = makeCascadeFailureStore({ [propFailure.id]: propFailure }, { "child-flow": [] });

    const { retryFailureBranch } = createRetryFailure({
      store: makeStore(), readStore: makeReadStore(), failureStore,
      pipelinesRef: makePipelinesRef(), executor: makeExecutor(), runStore, ownerSessionKey: "k", admissionQueueStore, logger,
    });

    const result = await retryFailureBranch({ id: propFailure.id, via: "test" });
    assert.equal(result.code, "conflict");
    assert.match(result.reason, /no real failure found/);
  });

  it("retries EVERY real failure found, not just the first, when multiple children are simultaneously failed", async () => {
    const propFailure = { id: "fail-prop", flow_id: "parent-flow", failure_node: "PROPOSE", error_type: "subflow-mode-not-satisfied" };
    const failureA = makeFailure({ id: "fail-a", flowId: "child-a", failureNode: "SUBMIT_CODE" });
    const failureB = makeFailure({ id: "fail-b", flowId: "child-b", failureNode: "SUBMIT_CODE" });

    listInvocationIdsForNodeImpl = () => ["inv-item-a", "inv-item-b"];
    const runStore = {
      listForParentInvocations: async () => [
        { flowId: "child-a", parentInvocationId: "inv-item-a", status: "failed" },
        { flowId: "child-b", parentInvocationId: "inv-item-b", status: "failed" },
      ],
    };
    queryErrorsByFlowImpl = () => errorEntries(["SUBMIT_CODE"]);
    const failureStore = makeCascadeFailureStore(
      { [propFailure.id]: propFailure, [failureA.id]: failureA, [failureB.id]: failureB },
      { "child-a": [failureA], "child-b": [failureB] },
    );
    const pipeline = makePipeline({ nodes: { SUBMIT_CODE: {} }, edges: { __start__: "SUBMIT_CODE", SUBMIT_CODE: "__end__" } });
    const flows = new Map([
      ["child-a", makeFlow({ flowId: "child-a", status: "failed" })],
      ["child-b", makeFlow({ flowId: "child-b", status: "failed" })],
    ]);
    const store = {
      get: async (id) => flows.get(id) ? { ...flows.get(id) } : null,
      forceBlock: async ({ flowId, expectedRevision, currentStep, stateJson, statusReason, ownerKey }) => {
        const f = flows.get(flowId);
        if (!f || f.revision !== expectedRevision) return { applied: false, code: "conflict" };
        flows.set(flowId, { ...f, status: "blocked", currentStep, stateJson, statusReason, ownerKey, revision: f.revision + 1 });
        return { applied: true };
      },
      setRunning: async ({ flowId, expectedRevision, lastStep, stateJson }) => {
        const f = flows.get(flowId);
        if (!f || f.revision !== expectedRevision) return { applied: false, code: "conflict" };
        flows.set(flowId, { ...f, status: "running", lastStep, stateJson, revision: f.revision + 1 });
        return { applied: true };
      },
    };
    const executor = makeExecutor();

    const { retryFailureBranch } = createRetryFailure({
      store, readStore: { get: store.get }, failureStore,
      pipelinesRef: makePipelinesRef({ test_pipe: pipeline }),
      executor, runStore, ownerSessionKey: "k", admissionQueueStore, logger,
    });

    const result = await retryFailureBranch({ id: propFailure.id, via: "test" });
    assert.equal(result.code, "ok");
    assert.equal(result.redirectedTo.length, 2);
    await flushSetImmediate();
    assert.deepEqual(new Set(executor._ran()), new Set(["child-a", "child-b"]));
  });
});

