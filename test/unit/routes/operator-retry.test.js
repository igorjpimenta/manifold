import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";

// Always-admits-immediately fake — retryFailureBranch now re-requests
// admission (admitAndSetRunning, engine/child-resume-trigger.js) before
// resuming a flow. None of these tests exercise queue/capacity behavior
// itself.
const admissionQueueStore = {
  admit: async () => ({ id: "fake-admission", status: "admitted" }),
  release: async () => ({ released: true, promoted: null }),
};

// NOTE — forceBlockOutOfMapFlow (in ../../../routes/operator-retry.js) used
// to do a raw DatabaseSync write straight to ~/.openclaw/runtime.sqlite by
// hardcoded path, bypassing the store/readStore test doubles entirely — any
// flow-level retry test whose flow.status was succeeded/failed/cancelled/
// skipped (CAS_EXCLUDED_STATUSES) routed through it and got a spurious
// "revision changed during force-block" conflict here. Fixed:
// forceBlockOutOfMapFlow now goes
// through store.forceBlock (run-store.js's forceBlock CAS via
// state-store.js), so makeStore()'s forceBlock below is real DI, not a
// bypass — status: "failed" tests now genuinely exercise that path instead
// of routing around it.

// errorNodes (the flow-level retry-eligibility check) now reads
// failure_events (part of the "Eradicate
// soft errors" effort; queryErrorsByFlow moved from node-executions.js to
// failure-store.js, called on the injected failureStore, not
// nodeExecutionsStore) instead of state.errors[] — a module-level var so
// each test controls exactly what "errors for this flow" resolves to,
// independent of the pure in-memory store/readStore/failureStore fixtures
// already used throughout this file. Wired into makeFailureStore() below.
let queryErrorsByFlowImpl = () => [];
// Default: every error entry's own executionId IS the latest for its node
// (matches errorEntries()'s deterministic exec-<nodeId> shape below) — so
// existing tests, which don't care about the stale-row distinction, keep
// passing unchanged. Individual tests override this to simulate a node
// whose error row has since been superseded by a newer (possibly
// successful) execution.
let getLatestExecutionForNodeImpl = ({ nodeId }) => ({ invocationId: `inv-${nodeId}`, executionId: `exec-${nodeId}` });
// findRealFailuresBeneath's
// child-lookup, same two-step pattern buildTreeNodeRows already uses
// (routes/runs.js): item invocation ids for a subflow node, then the real
// spawned children via runStore.listForParentInvocations.
let listInvocationIdsForNodeImpl = () => [];

// All async — node-executions.js's real store methods are (Drizzle/
// better-sqlite3 migration); a plain synchronous double here
// would silently tolerate a missing `await` at the call site, exactly the
// class of bug this migration introduced in operator-retry.js itself
// (every store/readStore/failureStore/nodeExecutionsStore call needed an
// added `await` — see git history around this date).
await mock.module("../../../state/node-executions.js", {
  exports: {
    getNodeExecutionsStore: () => ({
      getLatestExecutionForNode: async (args) => getLatestExecutionForNodeImpl(args),
      listInvocationIdsForNode: async (flowId, nodeId) => listInvocationIdsForNodeImpl(flowId, nodeId),
    }),
  },
});

const { createRetryFailure } = await import("../../../routes/operator-retry.js");

// ── helpers ───────────────────────────────────────────────────────────────────

const logger = { info: () => {}, warn: () => {} };

// Converts bare node-id strings into the ErrorEntry shape queryErrorsByFlow
// returns, for seeding queryErrorsByFlowImpl per test. executionId defaults
// to exec-<nodeId>, matching getLatestExecutionForNodeImpl's default so the
// entry reads as "still current" unless a test overrides one or the other.
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
    // Mirrors run-store.js's forceBlock CAS (via state-store.js): only
    // applies when the row is currently in a terminal status.
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

function makeFailure({ id = "fail-1", flowId = "flow-aabbccdd-0000-0000-0000-000000000001", failureNode = "DRAFT", branchItemId = null, executionId = null } = {}) {
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

// ── failure not found ─────────────────────────────────────────────────────────

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

// A failure with no failure_node means it happened before any node ever
// dispatched on that flow (e.g. the flow's own pipeline failed to resolve
// at load time — the exact shape of a "pipeline not loaded" bootstrap
// failure with no flow_node_invocations/flow_node_executions row to derive
// a failure_node from). There's nothing to partially clear, so the only
// sane retry target is the whole flow from __start__.
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

// ── flow-level retry ──────────────────────────────────────────────────────────

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
    // Pipeline has no edge that points to ORPHAN
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
    // State should have DRAFT cleared
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

// makeExecutor()'s runFlow resolves on the same tick it's called, so it
// can't distinguish "awaited to completion" from "merely scheduled and
// happened to already be done" — this executor forces a real async gap
// (one setImmediate) so a test can tell the two apart.
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
  it("returns conflict when the failure node has no matching entry in flow_node_executions", async () => {
    // Flow status is "failed" (terminal), but flow_node_executions has no
    // error rows for this flow — as it would be right after an earlier
    // retry already cleared this exact failure and the node subsequently
    // succeeded (or the flow otherwise moved on). Retrying the same stale
    // failure event a second time must not silently re-run the node.
    queryErrorsByFlowImpl = () => [];
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" });
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

  it("returns conflict when flow_node_executions has errors for OTHER nodes but not this one", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["OTHER_NODE"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" });
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
    // queryErrorsByFlow still returns DRAFT's OLD failed row (query never
    // deletes history), but getLatestExecutionForNode says DRAFT's most
    // recent execution is a DIFFERENT (successful) one — the exact
    // "failed once, retried, now succeeds" case a raw nodeId Set can't see.
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
      status: "blocked", // sidesteps forceBlockOutOfMapFlow's real-DB write (see file header note)
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

  it("rejects via the execution_id anchor even when a same-named node's Set membership alone would have allowed it", async () => {
    // A stricter check than the Set-only fallback: the failure row's own
    // execution_id no longer matches DRAFT's latest execution (someone
    // retried again since), even though DRAFT still shows up in
    // queryErrorsByFlow's results (its current failed attempt has a
    // DIFFERENT execution_id than the one this stale failure row recorded).
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

  it("legacy row with no execution_id falls back to the nodeId Set check (pre-migration behavior preserved)", async () => {
    queryErrorsByFlowImpl = () => errorEntries(["DRAFT"]);
    const failure = makeFailure({ branchItemId: null, failureNode: "DRAFT" }); // execution_id defaults to null
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
    assert.equal(result.code, "ok");
  });
});

// ── branch retry ──────────────────────────────────────────────────────────────
//
// A failure event's branch_item_id is not a routing signal. It comes fresh
// from flow_node_invocations.subflow_key (the current per-item dispatch key,
// see node-executions.js's migration from the legacy branch_key column),
// and a childFlows subflow item runs as a real child flow with its own
// flow_id (executor-subflow-children.js), so failure.flow_id already names
// the exact flow to retry regardless of whether branch_item_id is set.
// Treating a present branch_item_id as unretryable was a live bug (Track A
// finding 5): every subflow-item failure was permanently unretryable.
// Confirmed fixed: a branchItemId-present failure now retries exactly like a
// branchItemId-null one.
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

// ── cascade retry redirect ────────────────────────────────────────────────────
// A "subflow-mode-not-satisfied"
// failure's own flow already dispatched cleanly; retrying it directly would
// just re-fail immediately with the identical reason. These tests exercise
// the redirect-to-the-real-failure walk instead.

// retryFailureBranch fires executor.runFlow via a bare setImmediate (fire-
// and-forget, matching its own documented contract) — a macrotask, not a
// microtask, so `await retryFailureBranch(...)` alone returns before it
// runs. Existing tests only ever assert runFlow was NOT called (no timing
// issue proving an absence); these new ones assert it WAS, which needs the
// macrotask queue flushed first.
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
    // makeStore() models exactly one flow's worth of CAS state (a single
    // `current` closure var) — this test needs two independently-CAS'd
    // flows (child-a, child-b), each starting "failed" (so each must go
    // through its own real forceBlock -> setRunning transition), so it
    // gets its own small multi-flow double rather than reusing makeStore().
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

