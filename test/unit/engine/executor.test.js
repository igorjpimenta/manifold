import { describe, it, afterEach } from "node:test";
import assert from "node:assert/strict";
import { createExecutor } from "../../../engine/executor.js";
import { NodeFailure } from "../../../engine/executor-errors.js";
import { assembleState } from "../../../engine/executor-helpers.js";
import { REVIVAL_MSG } from "../../../engine/revival-messages.js";
import { nodeEmitter } from "../../../events.js";
import { getGlobalConcurrencyLimit, _resetForTests as resetGlobalConcurrency } from "../../../engine/global-concurrency.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { createRunStore } from "../../../state/run-store.js";
import { createStateStore } from "../../../state/state-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";

// ── helpers ───────────────────────────────────────────────────────────────────

const logger = { info: () => {}, warn: () => {}, error: () => {} };
const api = { runtime: {}, config: {} };

// createExecutor's nodeExecutionsStore is injectable specifically so tests
// don't fall through to the real getNodeExecutionsStore() singleton, which
// defaults to ~/.openclaw/runtime.sqlite — without this, every test-flow-*
// fixture created below would write real rows into production data.
function makeNodeExecutionsStore() {
  // resultJson-by-flow tracking: assembleState() reads
  // listResultsForFlow() to reconstruct a node's own output — this mock must
  // provide it, since state_json is entry-only and carries no accumulated
  // blob as a fallback baseState. Without it, any test pipeline whose edge
  // function reads a
  // prior node's own output (e.g. `state.WORK ? "__end__" : "WORK"`) would
  // see it as permanently undefined and loop forever.
  // enter() returns a deterministic
  // per-{flowId,nodeId} invocationId (mirroring makeSpyNodeExecutionsStore's
  // real invocationId tracking below) and writeResult() correlates through
  // that map — order-independent, matching how the real store and
  // the spy both already work.
  const resultsByFlow = new Map();
  const invocationMeta = new Map();
  const latestInvocationByKey = new Map();
  return {
    enter: async (args) => {
      const invocationId = `inv-${args.flowId}-${args.nodeId}`;
      invocationMeta.set(invocationId, { flowId: args.flowId, nodeId: args.nodeId });
      latestInvocationByKey.set(`${args.flowId}::${args.nodeId}`, invocationId);
      return { invocationId, isNew: args.kind === "fresh" };
    },
    // reviveAgentKindOrphan looks up the
    // crashed node's existing invocation by flowId+nodeId to call
    // writeResult() on it — mirrors the real store's getLatestInvocationForNode.
    getLatestInvocationForNode: async (flowId, nodeId) => {
      const id = latestInvocationByKey.get(`${flowId}::${nodeId}`);
      return id ? { id, visitSeq: 1 } : null;
    },
    // async, matching the real store's contract — a plain sync return here
    // would silently tolerate a missing `await` at the call site (see
    // makeSpyNodeExecutionsStore's comment below for the exact bug class
    // this guards against).
    write: async () => {},
    // resultJson write, recorded (see
    // block comment above) so assembleState() can reconstruct real state.
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
    // gate_records (MIGRATIONS v11) — a gate node's own point-identity
    // record, independent of resultJson entirely.
    writeGateRecord: async () => ({ ok: true }),
    getGateRecord: async () => null,
    findCurrentStep: async () => null,
    queryByPipeline: () => [],
    close: () => {},
  };
}

// Records every write() call's args (specifically kind) so tests
// can assert exactly what executor.js computed and threaded through, without
// needing a real sqlite-backed store. enter() tracks calls separately so that
// spy.calls.length reflects only write() calls, preserving existing test
// assertions. Returns deterministic {invocationId, executionId} ids keyed by
// the enter() counter so failure-linking tests can confirm the EXACT ids are
// threaded from enter() → write() → persistFailRetrying/recordEngineFailure.
// findCurrentStep defaults to "no prior invocation" (null) — the common
// case for tests that don't care about fresh-vs-continuation resolution.
// Pass an override (a plain value, or a function of flowId) for tests
// that specifically exercise runFlow's forceNew/kind derivation (see
// "runFlow — kind threading" below), which reads this instead of
// state._stepHistory.
function makeSpyNodeExecutionsStore({ findCurrentStep: findCurrentStepOverride = null } = {}) {
  const calls = [];
  const enterCalls = [];
  const resultCalls = [];
  let enterCount = 0;
  // invocationId -> {flowId, nodeId}, resultJson-by-flow — see
  // makeNodeExecutionsStore's block comment above for why
  // this is load-bearing, not decorative. This mock has real
  // invocationIds (unlike the plain one), so correlation is a direct map
  // lookup rather than "most recent write() call" inference.
  const invocationMeta = new Map();
  const resultsByFlow = new Map();
  const latestInvocationByKey = new Map();
  return {
    enter: async (args) => {
      enterCount++;
      enterCalls.push(args);
      const invocationId = `inv-${enterCount}`;
      invocationMeta.set(invocationId, { flowId: args.flowId, nodeId: args.nodeId });
      latestInvocationByKey.set(`${args.flowId}::${args.nodeId}`, invocationId);
      return { invocationId, isNew: args.kind === "fresh" };
    },
    // See makeNodeExecutionsStore's identical addition above.
    getLatestInvocationForNode: async (flowId, nodeId) => {
      const id = latestInvocationByKey.get(`${flowId}::${nodeId}`);
      return id ? { id, visitSeq: 1 } : null;
    },
    // async, matching the real store's contract — a synchronous
    // safeWriteExecution call site (executor.js's own
    // missing-`await` class of bug: `written` came back as an unresolved
    // Promise, and `written.invocationId` was always undefined) is
    // tolerated by a synchronous mock. Keeping
    // this genuinely async is what makes that class of regression fail a
    // test instead of only failing in production.
    write: async (args) => {
      calls.push(args);
      // If invocationId was provided (threaded from enter()), preserve it so
      // failure-linking assertions see the same id as enter() returned.
      const invocationId = args.invocationId ?? `inv-${calls.length}`;
      if (!invocationMeta.has(invocationId)) invocationMeta.set(invocationId, { flowId: args.flowId, nodeId: args.nodeId });
      return { invocationId, executionId: `exec-${calls.length}` };
    },
    findCurrentStep: async (flowId) =>
      typeof findCurrentStepOverride === "function" ? findCurrentStepOverride(flowId) : findCurrentStepOverride,
    listResultsForFlow: async (flowId) => {
      const m = resultsByFlow.get(flowId);
      if (!m) return [];
      return [...m.entries()].map(([nodeId, resultJson]) => ({
        nodeId, visitSeq: 1, resultWrittenAt: Date.now(), resultTooLargeBytes: null, resultJson,
      }));
    },
    // records every call so tests can assert exactly
    // what got written to resultJson, same spy convention as write() above.
    writeResult: async (invocationId, result) => {
      const meta = invocationMeta.get(invocationId);
      if (meta?.flowId && meta?.nodeId) {
        if (!resultsByFlow.has(meta.flowId)) resultsByFlow.set(meta.flowId, new Map());
        resultsByFlow.get(meta.flowId).set(meta.nodeId, JSON.stringify(result ?? null));
      }
      resultCalls.push({ invocationId, result });
      return { ok: true, tooLarge: false, bytes: JSON.stringify(result ?? null).length };
    },
    writeGateRecord: async () => ({ ok: true }),
    getGateRecord: async () => null,
    queryByPipeline: () => [],
    close: () => {},
    calls,
    enterCalls,
    resultCalls,
  };
}

function makePipeline(overrides = {}) {
  return {
    goal: "test pipeline",
    state: {},
    nodes: { FIRST: { kind: "code", run: () => ({}) } },
    edges: { __start__: "FIRST", FIRST: "__end__" },
    ...overrides,
  };
}

function makeStore(flows = []) {
  const map = new Map(flows.map((f) => [f.flowId, f]));
  let revCounter = 10;
  return {
    get: (id) => map.get(id) ?? null,
    create: ({ pipelineId, goal, initialState }) => {
      const flowId = `test-flow-${Math.random().toString(36).slice(2, 10)}`;
      // Embed pipelineId into stateJson; runFlow reads state.pipelineId to resolve the pipeline.
      const stateJson = { ...initialState, pipelineId };
      const record = { flowId, pipelineId, goal, status: "running", revision: ++revCounter, lastStep: "__start__", stateJson, waitJson: null };
      map.set(flowId, record);
      return record;
    },
    listFor: (pipelineId) => [...map.values()].filter((f) => f.pipelineId === pipelineId),
    setRunning: ({ flowId, expectedRevision, lastStep, stateJson }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      const updated = { ...f, lastStep, stateJson, revision: ++revCounter };
      map.set(flowId, updated);
      return { applied: true };
    },
    setWaiting: ({ flowId, expectedRevision, waitJson, stateJson, lastStep, statusReason }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      const updated = { ...f, status: "waiting", waitJson, stateJson, lastStep, statusReason, revision: ++revCounter };
      map.set(flowId, updated);
      return { applied: true };
    },
    setQueued: ({ flowId, expectedRevision, waitJson, stateJson, lastStep, statusReason }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      const updated = { ...f, status: "queued", waitJson, stateJson, lastStep, statusReason, revision: ++revCounter };
      map.set(flowId, updated);
      return { applied: true };
    },
    fail: ({ flowId, expectedRevision, stateJson, statusReason }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      const updated = { ...f, status: "failed", stateJson, statusReason, revision: ++revCounter };
      map.set(flowId, updated);
      return { applied: true };
    },
    finish: ({ flowId, expectedRevision, stateJson }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      const updated = { ...f, status: "succeeded", stateJson, revision: ++revCounter };
      map.set(flowId, updated);
      return { applied: true };
    },
    skip: ({ flowId, expectedRevision }) => {
      const f = map.get(flowId);
      if (!f || f.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      const updated = { ...f, status: "skipped", revision: ++revCounter };
      map.set(flowId, updated);
      return { applied: true };
    },
    cancel: async ({ flowId }) => {
      const f = map.get(flowId);
      if (!f) return { found: false, cancelled: false };
      const updated = { ...f, status: "cancelled" };
      map.set(flowId, updated);
      return { found: true, cancelled: true, flow: updated };
    },
    _map: map,
  };
}

// Fake run-store for the allowConcurrent:false singleton-gate tests below —
// only listRunsPage is exercised by executor.js's start().
function makeRunStore(overrides = {}) {
  return {
    listRunsPage: async () => [],
    ...overrides,
  };
}

// Always-admits-immediately fake — start() and subflow dispatch need a
// working admissionQueueStore even when a given test has nothing to say
// about admission-queue behavior itself (covered by admission-queue-
// store.test.js).
function makeAdmissionQueueStore(overrides = {}) {
  return {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
    ...overrides,
  };
}

function makeExecutor(pipelineMap = {}, storeFlows = [], opts = {}) {
  const store = makeStore(storeFlows);
  const pipelinesRef = { value: new Map(Object.entries(pipelineMap)) };
  return {
    executor: createExecutor({
      api,
      store,
      pipelinesRef,
      ensurePipelinesLoaded: opts.ensurePipelinesLoaded ?? null,
      logger,
      runTurn: opts.runTurn ?? null,
      failureStore: opts.failureStore ?? null,
      nodeHealthStore: opts.nodeHealthStore ?? null,
      notifyFlowEnteredWaiting: opts.notifyFlowEnteredWaiting ?? null,
      sessionIndex: null,
      nodeExecutionsStore: opts.nodeExecutionsStore ?? makeNodeExecutionsStore(),
      admissionQueueStore: opts.admissionQueueStore ?? makeAdmissionQueueStore(),
      runStore: opts.runStore ?? makeRunStore(),
    }),
    store,
    pipelinesRef,
  };
}

// ── start ─────────────────────────────────────────────────────────────────────

describe("start", () => {
  // The shared global limit (global-concurrency.js) applies to every
  // pipeline regardless of allowConcurrent — tests that need a specific
  // limit set it via resetGlobalConcurrency(n) instead. Restored after every
  // test so no test's limit leaks into the next. allowConcurrent:false is a
  // separate, per-pipelineId gate on top of that shared pool (see tests
  // below).
  afterEach(() => { resetGlobalConcurrency(); });

  it("creates a flow record with status=running", async () => {
    const { executor, store } = makeExecutor({ my_pipeline: makePipeline() });
    const record = await executor.start("my_pipeline");
    assert.ok(record.flowId, "should have flowId");
    assert.equal(store.get(record.flowId)?.status, "running");
  });

  it("throws for unknown pipelineId", async () => {
    const { executor } = makeExecutor({});
    await assert.rejects(() => executor.start("nonexistent"), /unknown pipeline/);
  });

  // admission-queue-store.js: start() queues when the
  // shared pool is full rather than throwing. These 3 tests need a REAL
  // admissionQueueStore (not the file's own always-admits fake) since
  // they're specifically exercising real capacity behavior against
  // resetGlobalConcurrency(n).
  function makeExecutorWithRealAdmission(pipelineMap) {
    const dbPath = path.join(os.tmpdir(), `executor-test-admission-${randomUUID()}.sqlite`);
    const store = createAdmissionQueueStore({ dbPath, getLimit: getGlobalConcurrencyLimit });
    const runStore = createRunStore({ dbPath });
    const admit = store.admit.bind(store);
    const admissionQueueStore = new Proxy(store, {
      get(target, key) {
        if (key === "admit") {
          return async (args) => {
            const runs = await runStore;
            await runs.insert({ flowId: args.flowId, status: "running" });
            return admit(args);
          };
        }
        const value = target[key];
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    return makeExecutor(pipelineMap, [], { admissionQueueStore });
  }

  it("queues instead of running when the shared concurrency limit (1) is already reached", async () => {
    resetGlobalConcurrency(1);
    const { executor, store } = makeExecutorWithRealAdmission({ p: makePipeline() });
    const r1 = await executor.start("p");
    assert.equal(r1.status, "running");
    const r2 = await executor.start("p");
    assert.equal(r2.status, "queued", "the pool is full — the 2nd flow queues rather than being rejected");
    assert.equal(store.get(r2.flowId)?.status, "queued");
  });

  it("allows multiple flows when the shared limit covers them", async () => {
    resetGlobalConcurrency(2);
    const { executor } = makeExecutorWithRealAdmission({ p: makePipeline() });
    const r1 = await executor.start("p");
    const r2 = await executor.start("p");
    assert.notEqual(r1.flowId, r2.flowId);
    assert.equal(r1.status, "running");
    assert.equal(r2.status, "running");
  });

  it("allows up to N flows when the shared limit is N, queues the (N+1)th", async () => {
    resetGlobalConcurrency(2);
    const { executor } = makeExecutorWithRealAdmission({ p: makePipeline() });
    const r1 = await executor.start("p");
    const r2 = await executor.start("p");
    const r3 = await executor.start("p");
    assert.equal(r1.status, "running");
    assert.equal(r2.status, "running");
    assert.equal(r3.status, "queued");
  });

  // The pool is shared across EVERY pipeline now, not scoped to one — a
  // flow from a completely different pipeline holding the only slot must
  // still queue a new "p" start, same as a same-pipeline flow would.
  it("a flow from a DIFFERENT pipeline holding the only slot still forces the next start() to queue", async () => {
    resetGlobalConcurrency(1);
    const { executor } = makeExecutorWithRealAdmission({ p: makePipeline(), other: makePipeline() });
    const r1 = await executor.start("other");
    assert.equal(r1.status, "running");
    const r2 = await executor.start("p");
    assert.equal(r2.status, "queued");
  });

  // allowConcurrent:false — per-pipelineId singleton gate, independent of
  // the shared admission pool above.
  it("allowConcurrent:false skips a second start() while the first is still active", async () => {
    const runStore = makeRunStore({
      listRunsPage: async ({ pipelineId }) => [
        { flowId: "existing-flow-id", pipelineId, status: "running" },
      ],
    });
    const { executor, store } = makeExecutor({ p: makePipeline({ allowConcurrent: false }) }, [], { runStore });
    const r1 = await executor.start("p");
    assert.equal(store.get(r1.flowId)?.status, "skipped");
  });

  it("allowConcurrent:false does not skip when the only other run is terminal", async () => {
    const runStore = makeRunStore({
      listRunsPage: async ({ pipelineId }) => [
        { flowId: "old-flow-id", pipelineId, status: "succeeded" },
      ],
    });
    const { executor, store } = makeExecutor({ p: makePipeline({ allowConcurrent: false }) }, [], { runStore });
    const r1 = await executor.start("p");
    assert.equal(store.get(r1.flowId)?.status, "running");
  });

  it("allowConcurrent:false does not treat the flow's own just-created row as a conflict", async () => {
    const runStore = makeRunStore({
      listRunsPage: async ({ pipelineId }) => [
        // Simulates listRunsPage returning the very row start() just
        // inserted — must be excluded by flowId, not just by status.
      ],
    });
    const { executor, store } = makeExecutor({ p: makePipeline({ allowConcurrent: false }) }, [], { runStore });
    const r1 = await executor.start("p");
    assert.equal(store.get(r1.flowId)?.status, "running");
  });

  it("unset allowConcurrent (default) never consults run-store and allows overlap", async () => {
    let called = false;
    const runStore = makeRunStore({ listRunsPage: async () => { called = true; return []; } });
    const { executor } = makeExecutor({ p: makePipeline() }, [], { runStore });
    const r1 = await executor.start("p");
    const r2 = await executor.start("p");
    assert.equal(r1.status, "running");
    assert.equal(r2.status, "running");
    assert.equal(called, false, "default (unset) allowConcurrent must not query run-store at all");
  });

  it("does not count a gate-blocked flow toward the concurrency limit (approval waits hold no dispatch slot)", async () => {
    resetGlobalConcurrency(1);
    const blockedFlow = { flowId: "existing-blocked", pipelineId: "p", status: "blocked", revision: 1 };
    const { executor } = makeExecutor({ p: makePipeline() }, [blockedFlow]);
    const record = await executor.start("p");
    assert.ok(record.flowId, "start should succeed despite a gate-blocked sibling");
  });

  it("stamps triggeredBy in initial state", async () => {
    const { executor, store } = makeExecutor({ p: makePipeline() });
    const trigger = { kind: "cli", label: "CLI run" };
    const record = await executor.start("p", {}, trigger);
    assert.deepEqual(store.get(record.flowId)?.stateJson?.triggeredBy, trigger);
  });
});

// ── runFlow ───────────────────────────────────────────────────────────────────

describe("runFlow — flow not found", () => {
  it("returns without error for unknown flowId", async () => {
    const { executor } = makeExecutor({ p: makePipeline() });
    await executor.runFlow("nonexistent-flow-id");
  });
});

describe("runFlow — terminal status", () => {
  it("returns immediately for succeeded flow", async () => {
    const { executor, store } = makeExecutor({ p: makePipeline() });
    const r = await executor.start("p");
    // Manually set terminal status
    const f = store._map.get(r.flowId);
    store._map.set(r.flowId, { ...f, status: "succeeded" });
    await executor.runFlow(r.flowId); // should return without acting
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });
});

describe("runFlow — simple code pipeline to __end__", () => {
  it("drives a code node to __end__ and marks flow succeeded", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ done: true }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });
});

describe("runFlow — _blocked without a _kind fails the flow", () => {
  it("fails (does not wait) when the node's _blocked result omits _kind", async () => {
    const pipeline = makePipeline({
      nodes: {
        ASK: {
          kind: "code",
          run: () => ({ _blocked: { reason: "need url", needed: [], hint: "search first" } }),
        },
      },
      edges: { __start__: "ASK", ASK: "__end__" },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    // A block without an explicit
    // _kind is a programming error in the node, so the flow fails outright.
    const final = store.get(r.flowId);
    assert.equal(final?.status, "failed", `expected failed, got ${final?.status}`);
    assert.match(final?.statusReason ?? "", /need url/);
  });
});

// ── kind threading (status_reason work) ──────────────────────────────────────
// executor.test.js's other tests all use a no-op nodeExecutionsStore with no
// call-site assertions — these use makeSpyNodeExecutionsStore to confirm
// runFlow actually computes and threads `kind` correctly (a genuinely fresh
// node entry always resolves to "fresh" regardless of the caller-supplied
// kind; only a non-fresh re-entry "spends" it), on top of executor-
// helpers.test.js's exhaustive coverage of isFreshNodeEntry itself.

describe("runFlow — kind threading", () => {
  it("resolves kind:fresh on the first-ever entry to a node, regardless of the caller-supplied kind", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ done: true }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore();
    const { executor } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    assert.equal(spy.calls.length, 1);
    assert.equal(spy.calls[0].nodeId, "WORK");
    assert.equal(spy.calls[0].kind, "fresh");
  });

  it("resolves kind:retry when findCurrentStep already matches the node being (re)entered — operator-retry-shaped continuation", async () => {
    // Mirrors exactly what routes/operator-retry.js does: rewind lastStep
    // to the step BEFORE the failed node, so DRAFT's own invocation row
    // (from its first attempt) is still the most recent one for this flow.
    // resolveEdge lands back on the same node — this must reuse the
    // original invocation (kind:"retry", the caller-supplied kind), not
    // start a fresh round.
    const pipeline = makePipeline({
      nodes: {
        PRIME: { kind: "code", run: () => ({}) },
        DRAFT: { kind: "code", run: () => ({ text: "hello" }) },
      },
      edges: { __start__: "PRIME", PRIME: "DRAFT", DRAFT: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore({ findCurrentStep: () => "DRAFT" });
    const flow = {
      flowId: "retry-flow-1", pipelineId: "p", status: "running", revision: 1,
      lastStep: "PRIME", waitJson: null,
      stateJson: { pipelineId: "p" },
    };
    const { executor } = makeExecutor({ p: pipeline }, [flow], { nodeExecutionsStore: spy });
    await executor.runFlow(flow.flowId, "retry");
    const draftCall = spy.calls.find((c) => c.nodeId === "DRAFT");
    assert.ok(draftCall, "DRAFT should have been dispatched");
    assert.equal(draftCall.kind, "retry", "must reuse the same invocation as the original DRAFT attempt, not start a new round");
  });

  it("resolves kind:fresh when findCurrentStep differs from the node being entered — a genuine loop-back pass", async () => {
    // DRAFT was visited earlier, the flow moved on to other nodes (X is
    // the most recently entered node per findCurrentStep), and now a
    // different edge (X→DRAFT) loops back into it — a real second,
    // independent pass, not a continuation. The
    // top-level runFlow() call is tagged "recheck" here (arbitrary — any
    // valid kind works) to prove it's genuinely irrelevant once forceNew
    // resolves true for this specific node.
    const pipeline = makePipeline({
      nodes: { DRAFT: { kind: "code", run: () => ({}) } },
      edges: { __start__: "X", X: "DRAFT", DRAFT: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore({ findCurrentStep: () => "X" });
    const flow = {
      flowId: "loopback-flow-1", pipelineId: "p", status: "running", revision: 1,
      lastStep: "X", waitJson: null,
      stateJson: { pipelineId: "p" },
    };
    const { executor } = makeExecutor({ p: pipeline }, [flow], { nodeExecutionsStore: spy });
    await executor.runFlow(flow.flowId, "recheck");
    const draftCall = spy.calls.find((c) => c.nodeId === "DRAFT");
    assert.ok(draftCall, "DRAFT should have been dispatched");
    assert.equal(draftCall.kind, "fresh", "a genuine second pass through DRAFT must get its own invocation");
  });
});

// ── failure-linking (invocationId/executionId threaded through) ────────────
// The 6 sites that call nodeExecutions.write() BEFORE persistFailRetrying
// (write-then-fail, not the reverse) so the failure can be linked to the
// invocation/execution it just created. makeSpyNodeExecutionsStore's write()
// returns a deterministic {invocationId, executionId} per call — these
// tests confirm executor.js threads exactly those ids through to
// recordEngineFailure, and that the 4 structurally-exempt sites (no node
// ever dispatched, nothing to link) correctly pass null/undefined instead.

describe("runFlow — failure-linking", () => {
  function makeFailureStoreSpy() {
    let recorded = null;
    return { failureStore: { recordEngineFailure: (args) => { recorded = args; } }, get recorded() { return recorded; } };
  }

  it("thrown error: threads write()'s returned ids into recordEngineFailure", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => { throw new Error("boom"); } } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore();
    const fsSpy = makeFailureStoreSpy();
    const { executor } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy, failureStore: fsSpy.failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(spy.calls.length, 1, "write() must run before persistFailRetrying, not skipped");
    assert.equal(fsSpy.recorded?.invocationId, "inv-1");
    assert.equal(fsSpy.recorded?.executionId, "exec-1");
  });

  it("blocked without declaring a wait kind: threads write()'s returned ids into recordEngineFailure", async () => {
    const pipeline = makePipeline({
      nodes: { ASK: { kind: "code", run: () => ({ _blocked: { reason: "need url", needed: [], hint: "search first" } }) } },
      edges: { __start__: "ASK", ASK: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore();
    const fsSpy = makeFailureStoreSpy();
    const { executor } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy, failureStore: fsSpy.failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(spy.calls.length, 1);
    assert.equal(fsSpy.recorded?.invocationId, "inv-1");
    assert.equal(fsSpy.recorded?.executionId, "exec-1");
  });

  it("gate-layout validation failure (blockedInfo path): threads write()'s returned ids into recordEngineFailure", async () => {
    const pipeline = makePipeline({
      nodes: {
        GATE: {
          kind: "code",
          run: () => ({ _blocked: { _kind: "human-gate", reason: "review", items: [{ itemId: "x" }] } }),
        },
      },
      edges: { __start__: "GATE", GATE: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore();
    const fsSpy = makeFailureStoreSpy();
    const { executor } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy, failureStore: fsSpy.failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(spy.calls.length, 1, "item missing requestPayload.layout must fail validateGateLayout");
    assert.equal(fsSpy.recorded?.invocationId, "inv-1");
    assert.equal(fsSpy.recorded?.executionId, "exec-1");
  });

  it("gate-layout validation failure (waitJson path): threads write()'s returned ids into recordEngineFailure", async () => {
    const pipeline = makePipeline({
      nodes: {
        GATE: {
          kind: "human-gate",
          pendingSelector: () => ["item-1"],
          // Omits `layout` — validateGateLayout requires requestPayload.layout
          // to be an array, so this throws.
          approvalRequestBuilder: (s, itemId) => ({ title: itemId }),
        },
      },
      edges: { __start__: "GATE", GATE: "__end__" },
    });
    const spy = makeSpyNodeExecutionsStore();
    const fsSpy = makeFailureStoreSpy();
    const { executor } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy, failureStore: fsSpy.failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(spy.calls.length, 1);
    assert.equal(fsSpy.recorded?.invocationId, "inv-1");
    assert.equal(fsSpy.recorded?.executionId, "exec-1");
  });

  it("structurally-exempt site (pipeline not loaded): recordEngineFailure gets null/undefined ids, not fabricated ones", async () => {
    const fsSpy = makeFailureStoreSpy();
    const flow = {
      flowId: "no-pipeline-flow", pipelineId: "missing_pipe", status: "running", revision: 1,
      lastStep: "__start__", waitJson: null, stateJson: { pipelineId: "missing_pipe" },
    };
    const { executor } = makeExecutor({}, [flow], { failureStore: fsSpy.failureStore });
    await executor.runFlow(flow.flowId, "fresh");

    assert.equal(fsSpy.recorded?.invocationId ?? null, null, "no node was ever dispatched — nothing to link");
    assert.equal(fsSpy.recorded?.executionId ?? null, null);
  });
});

// ── state persist failure on the plain success path ─────────────────────────
// A node executes successfully, but persistRunningRetrying's underlying
// store.setRunning() fails for a reason OTHER than revision_conflict (e.g.
// a corrupted runtime.sqlite returning run_store_failed). The failure
// must be checked and returned; control must not fall through to
// nodeExecutions.write() and back to the top of runFlow's while(true),
// which would re-resolve the SAME edge (lastStep never advanced, since the
// persist never landed) and re-dispatch the node. Left unchecked, this is
// an unbounded loop: each iteration re-runs the node (an LLM call)
// forever, never reaching a terminal state. It must instead
// fail the flow immediately, exactly like every other failure branch in
// this file.

describe("runFlow — state persist failure on the plain success path", () => {
  function makeFailStoreOnSetRunning(flows, code = "run_store_failed") {
    const base = makeStore(flows);
    let setRunningCalls = 0;
    return {
      ...base,
      setRunning: (args) => {
        setRunningCalls++;
        return { applied: false, code };
      },
      get setRunningCalls() { return setRunningCalls; },
    };
  }

  it("fails the flow instead of looping when setRunning fails for a non-revision_conflict reason", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ ok: true }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const store = makeFailStoreOnSetRunning([]);
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const spy = makeSpyNodeExecutionsStore();
    const executor = createExecutor({
      api, store, pipelinesRef, logger,
      ensurePipelinesLoaded: null, runTurn: null,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.setRunningCalls, 1, "must not retry/redispatch after a non-revision_conflict failure");
    const final = store.get(r.flowId);
    assert.equal(final?.status, "failed", `expected failed, got ${final?.status}`);
    assert.match(final?.statusReason ?? "", /run_store_failed/);
  });

  it("threads write()'s returned ids into recordEngineFailure, same as every other failure branch", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ ok: true }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const store = makeFailStoreOnSetRunning([]);
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const spy = makeSpyNodeExecutionsStore();
    let recorded = null;
    const executor = createExecutor({
      api, store, pipelinesRef, logger,
      ensurePipelinesLoaded: null, runTurn: null,
      failureStore: { recordEngineFailure: (args) => { recorded = args; } },
      nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(spy.calls.length, 1);
    assert.equal(recorded?.invocationId, "inv-1");
    assert.equal(recorded?.executionId, "exec-1");
    assert.match(recorded?.message ?? "", /run_store_failed/, "error content lives on the recordEngineFailure call, not write()");
  });
});

// ── runFlow — result-before-lastStep write ordering ────────────────────────
// A gateway restart landing between "flow_runs.last_step advances to
// nextNodeId" and "this node's own result_json actually commits" leaves
// last_step durably claiming the node completed while its output is gone.
// Boot-time orphan revival and the admission-queue promotion wake
// (engine/admission-promotion-resume.js) both trust last_step and jump
// straight to resolving the node's outbound edge without re-running
// apply() — so the edge function reads an empty
// state.<nodeId> and returns null, which the engine surfaces as
// "edge <nodeId> returned no next node". writeResult() must
// commit before setRunning
// (persistRunningRetrying) advances lastStep past the node. This test
// pins that order so it can't silently regress.
describe("runFlow — writeResult commits before last_step advances", () => {
  it("calls nodeExecutionsStore.writeResult() before store.setRunning() for a completed node", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ WORK: { done: true } }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const order = [];
    const baseStore = makeStore([]);
    const store = {
      ...baseStore,
      setRunning: (args) => {
        order.push("setRunning");
        return baseStore.setRunning(args);
      },
    };
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const baseSpy = makeSpyNodeExecutionsStore();
    const spy = {
      ...baseSpy,
      writeResult: async (invocationId, result) => {
        order.push("writeResult");
        return baseSpy.writeResult(invocationId, result);
      },
    };
    const executor = createExecutor({
      api, store, pipelinesRef, logger,
      ensurePipelinesLoaded: null, runTurn: null,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.deepEqual(
      order,
      ["writeResult", "setRunning"],
      "a node's result must be durably written before last_step advances past it — " +
      "otherwise a crash in between leaves last_step claiming the node completed " +
      "while its output is unrecoverable, and the node's own outbound edge sees an " +
      "empty state (regression: SCORE_LLM 'edge returned no next node')",
    );
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  // "race" means a CONCURRENT writer already committed real content for
  // this exact invocation — advancing is correct, the data genuinely is
  // there, just not from this call. For every other reason
  // ("no-invocation-id"/"no-such-invocation"/"unserializable"/"db-error",
  // or a genuine throw from writeResult itself), no one wrote: last_step
  // must not point at a node whose result was never durably written —
  // the next runFlow() iteration's edge resolution would dead-end with
  // "no next node", hiding the real cause (the SCORE_LLM
  // "edge returned no next node" shape). Any non-"race" writeResult
  // failure fails the flow outright with the real reason.
  it("fails the flow (not a silent advance) when writeResult fails for a reason other than race", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ WORK: { done: true } }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const warnCalls = [];
    const spyLogger = { info: () => {}, warn: (msg) => warnCalls.push(msg), error: () => {} };
    const store = makeStore([]);
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const baseSpy = makeSpyNodeExecutionsStore();
    const spy = {
      ...baseSpy,
      writeResult: async () => ({ ok: false, reason: "no-such-invocation" }),
    };
    const executor = createExecutor({
      api, store, pipelinesRef, logger: spyLogger,
      ensurePipelinesLoaded: null, runTurn: null,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(warnCalls.length, 1);
    assert.match(warnCalls[0], /resultJson write failed \(no-such-invocation\)/);
    const final = store.get(r.flowId);
    assert.equal(final.status, "failed", "must fail outright, not dead-end downstream with 'no next node'");
    assert.match(final.statusReason, /resultJson write failed \(no-such-invocation\)/);
  });

  it("still advances (does not fail) when writeResult reports a race — a concurrent writer already committed real content", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ WORK: { done: true } }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const store = makeStore([]);
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const baseSpy = makeSpyNodeExecutionsStore();
    // A real race means a CONCURRENT writer's own call actually won and
    // stored the real content — the loser just gets told "race". Simulating
    // that honestly (not just stubbing {ok:false}) is what makes this test
    // prove the flow can still succeed afterward, not merely that it didn't
    // crash.
    const spy = {
      ...baseSpy,
      writeResult: async (invocationId, result) => {
        await baseSpy.writeResult(invocationId, result);
        return { ok: false, reason: "race" };
      },
    };
    const executor = createExecutor({
      api, store, pipelinesRef, logger,
      ensurePipelinesLoaded: null, runTurn: null,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId)?.status, "succeeded", "a race loser must not fail the flow — the winner's write already has real data");
  });
});

// ── cascade-kind flow: cascading dispatch ────────────────────────────────────
// A pipeline-level `kind: "cascade"` is orthogonal to node kind — it's a
// flow characteristic read by runFlow's catch block. `node.flow.kind`
// carries this same value through onto every synthesized subflow-child
// pipeline (loader.js), which is how every migrated cascade sub-node (see
// each pipeline's own submit/index.js, send-nudge.js, ensure-login.js,
// etc.) gets its throw-then-retry-from-start semantics. Inert for every
// other pipeline; these tests exercise the branch in isolation with small
// synthetic pipelines.
//
// On ANY throw — terminal or not, that distinction has no special meaning
// here — lastStep advances to the node that just threw, exactly like a
// normal successful dispatch, just with no stateUpdate to merge. It's THAT
// node's own edge function that decides what happens next, informed only
// by whether its own output landed in state. No central "restart the whole
// flow" step, no dedicated bookkeeping node, no engine-level _terminal
// check: exhaustion is just an edge function returning no next node, which
// already fails the flow via the engine's pre-existing !nextNodeId path.
// The engine also durably records every cascade-continuing
// throw immediately (failureStore.recordEngineFailure, unconditional —
// see below), not just the ones that eventually give up — an edge function
// that needs more than presence/absence can query state/failure-store.js's
// queryErrorsByFlow/queryErrorsByFlowIds directly.

describe("runFlow — cascade-kind flow: cascading dispatch", () => {
  it("a throw: lastStep advances to the failed node, whose own edge retries it — resolves once it stops throwing", async () => {
    let attempts = 0;
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        WORK: {
          kind: "code",
          run: () => {
            attempts++;
            if (attempts === 1) throw new Error("transient");
            return { WORK: { ok: true } };
          },
        },
      },
      edges: {
        __start__: "WORK",
        // Called both after a success (state.WORK present → move on) and
        // after a cascaded non-terminal throw (state.WORK absent → retry
        // WORK itself) — the same edge function handles both cases.
        WORK: (state) => (state.WORK ? "__end__" : "WORK"),
      },
    });
    const spy = makeSpyNodeExecutionsStore();
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(attempts, 2, "WORK must be dispatched again after cascading, not abandoned");
    const flow = store.get(r.flowId);
    assert.equal(flow.status, "succeeded", "the flow resolves normally — persistFailRetrying must never fire for a non-terminal throw");
    assert.equal(spy.calls.length, 2, "both the failing and the succeeding attempt get their own tracked execution");
  });

  it("a throw: escalates to a sibling entry via the failed node's own edge, not a central router", async () => {
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE_LOGIN: { kind: "code", run: () => { throw new Error("script crashed"); } },
        AGENT_LOGIN: { kind: "code", run: () => ({ AGENT_LOGIN: { ok: true } }) },
      },
      edges: {
        __start__: "CODE_LOGIN",
        // CODE_LOGIN's own edge escalates to AGENT_LOGIN when its own
        // output never landed (it threw) — __start__ is never re-consulted.
        CODE_LOGIN: (state) => (state.CODE_LOGIN ? "__end__" : "AGENT_LOGIN"),
        AGENT_LOGIN: "__end__",
      },
    });
    const nodeExecutionsStore = makeNodeExecutionsStore();
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    const flow = store.get(r.flowId);
    assert.equal(flow.status, "succeeded", "CODE_LOGIN's failure cascades straight to AGENT_LOGIN and the flow completes");
    const flowState = await assembleState(flow.flowId, { runStore: store, nodeExecutionsStore, baseState: flow.stateJson });
    assert.equal(flowState.AGENT_LOGIN?.ok, true);
  });

  it("exhaustion: an edge function that returns no next node still fails the flow — no _terminal marker or engine check needed", async () => {
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE_LOGIN: { kind: "code", run: () => { throw new Error("script crashed"); } },
        AGENT_LOGIN: { kind: "code", run: () => { throw new Error("agent also failed"); } },
      },
      edges: {
        __start__: "CODE_LOGIN",
        CODE_LOGIN: (state) => (state.CODE_LOGIN ? "__end__" : "AGENT_LOGIN"),
        // AGENT_LOGIN is the last resort — once its own output never lands
        // either, there's nothing left to try. Returning no next node is
        // the whole "give up" signal; no _terminal tag required.
        AGENT_LOGIN: (state) => (state.AGENT_LOGIN ? "__end__" : null),
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    const flow = store.get(r.flowId);
    assert.equal(flow.status, "failed", "an exhausted edge returning no next node still fails the flow — cascading only prevents an IMMEDIATE fail, not a real dead end");
  });

  it("non-cascade-kind flow: a non-terminal throw fails the flow — unchanged default behavior", async () => {
    const pipeline = makePipeline({
      // No `kind` declared at all — the overwhelming majority of pipelines.
      nodes: { WORK: { kind: "code", run: () => { throw new Error("boom"); } } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    const flow = store.get(r.flowId);
    assert.equal(flow.status, "failed", "the retry-from-start branch must never engage for a pipeline that isn't kind:\"cascade\"");
  });
});

// ── cascade-kind flow: NodeFailure / edge-function error passing ────────────
// A cascade leg throws
// NodeFailure(message, {errorType}) instead of soft-
// returning {ok:false, error_type}; the engine carries errorType across the
// throw via a transient `_cascadeError` state key, consumed by a 2-arg edge
// function on the very next resolveEdge call for that same lastStep.

describe("runFlow — cascade-kind flow: NodeFailure carries errorType to a 2-arg edge", () => {
  it("a 2-arg edge on the thrown node receives {message, errorType} — not undefined/state-based", async () => {
    let attempts = 0;
    let receivedOnRetry = null;
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE: {
          kind: "code",
          run: () => {
            attempts++;
            if (attempts === 1) throw new NodeFailure("landing page unresolved", { errorType: "infra_unavailable" });
            return { CODE: { ok: true } };
          },
        },
      },
      edges: {
        __start__: "CODE",
        CODE: (state, error) => {
          if (state.CODE?.ok) return "__end__";
          receivedOnRetry = error;
          return "CODE";
        },
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(attempts, 2);
    assert.equal(store.get(r.flowId).status, "succeeded");
    assert.deepEqual(receivedOnRetry, { message: "landing page unresolved", errorType: "infra_unavailable" });
  });

  it("a 2-arg edge distinguishes errorType to pick a specific fallback leg, not a generic one", async () => {
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE: { kind: "code", run: () => { throw new NodeFailure("bad creds", { errorType: "blocked" }); } },
        INFRA_RETRY: { kind: "code", run: () => ({ INFRA_RETRY: { ok: true } }) },
        AGENT_FALLBACK: { kind: "code", run: () => ({ AGENT_FALLBACK: { ok: true } }) },
      },
      edges: {
        __start__: "CODE",
        CODE: (state, error) => {
          if (state.CODE?.ok) return "__end__";
          if (error?.errorType === "infra_unavailable") return "INFRA_RETRY";
          if (error?.errorType === "blocked") return null; // known-actionable, give up — never escalates
          return "AGENT_FALLBACK";
        },
        INFRA_RETRY: () => "CODE",
        AGENT_FALLBACK: "__end__",
      },
    });
    const nodeExecutionsStore = makeNodeExecutionsStore();
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    const flow = store.get(r.flowId);
    assert.equal(flow.status, "failed", "errorType:'blocked' must give up directly, never touch INFRA_RETRY or AGENT_FALLBACK");
    const flowState = await assembleState(flow.flowId, { runStore: store, nodeExecutionsStore, baseState: flow.stateJson });
    assert.equal(flowState.INFRA_RETRY, undefined);
    assert.equal(flowState.AGENT_FALLBACK, undefined);
  });

  it("a plain Error (not NodeFailure) thrown in a cascade still cascades — edge receives errorType: null", async () => {
    let attempts = 0;
    let receivedOnRetry = "not called";
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE: {
          kind: "code",
          run: () => {
            attempts++;
            if (attempts === 1) throw new Error("plain crash, no NodeFailure");
            return { CODE: { ok: true } };
          },
        },
      },
      edges: {
        __start__: "CODE",
        CODE: (state, error) => {
          if (state.CODE?.ok) return "__end__";
          receivedOnRetry = error;
          return "CODE";
        },
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId).status, "succeeded");
    assert.equal(receivedOnRetry.message, "plain crash, no NodeFailure");
    assert.equal(receivedOnRetry.errorType, null, "a non-NodeFailure throw must not fabricate an errorType");
  });

  // Pipeline/shared code and the engine can
  // load executor-errors.js through different module registries (the
  // gateway daemon's data:-URL pipeline loader vs. jiti-transpiled CJS for
  // CLI tooling), producing structurally-identical but reference-distinct
  // NodeFailure classes — `instanceof` returns false across that
  // boundary even for a genuine NodeFailure, discarding its errorType.
  // This test can't reproduce the cross-registry case directly (everything
  // in a unit test runs in one registry), but it proves the engine reads
  // `err.errorType` directly (duck-typed) rather than
  // gating on `instanceof NodeFailure` — so an object that merely LOOKS
  // like a NodeFailure (same shape, not the same class reference) still
  // carries its errorType through correctly.
  it("a NodeFailure-shaped error that ISN'T `instanceof` the real class still carries its errorType (duck-typed, not nominal)", async () => {
    let receivedOnRetry = "not called";
    let attempts = 0;
    class LookalikeNodeFailure extends Error {
      constructor(message, { errorType = null } = {}) {
        super(message);
        this.name = "NodeFailure";
        this.errorType = errorType;
      }
    }
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE: {
          kind: "code",
          run: () => {
            attempts++;
            if (attempts === 1) throw new LookalikeNodeFailure("cross-registry infra failure", { errorType: "infra_unavailable" });
            return { CODE: { ok: true } };
          },
        },
      },
      edges: {
        __start__: "CODE",
        CODE: (state, error) => {
          if (state.CODE?.ok) return "__end__";
          receivedOnRetry = error;
          return "CODE";
        },
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId).status, "succeeded");
    assert.equal(receivedOnRetry.errorType, "infra_unavailable", "errorType must survive even though LookalikeNodeFailure is NOT instanceof the real NodeFailure class");
  });

  it("_cascadeError does not leak to a DIFFERENT node's edge call — scoped to the exact node that threw", async () => {
    let bReceived = "not called";
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        A: { kind: "code", run: () => { throw new NodeFailure("a failed", { errorType: "inconclusive" }); } },
        B: { kind: "code", run: () => ({ B: { ok: true } }) },
      },
      edges: {
        __start__: "A",
        // A's own edge escalates to B regardless of outcome (state.A absent
        // after the throw) — B must NOT see A's error on its own edge call.
        A: () => "B",
        B: (state, error) => { bReceived = error; return "__end__"; },
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId).status, "succeeded");
    assert.equal(bReceived, null, "B's edge call is for lastStep='B', not 'A' — _cascadeError's nodeId guard must exclude it");
  });

  it("errorType is threaded into recordEngineFailure, for operator-retry/dashboard/SUBMIT_SUMMARY consumers (write() itself carries no error content)", async () => {
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE: { kind: "code", run: () => { throw new NodeFailure("infra hiccup", { errorType: "infra_unavailable" }); } },
        AGENT: { kind: "code", run: () => ({ AGENT: { ok: true } }) },
      },
      edges: {
        __start__: "CODE",
        CODE: (state) => (state.CODE ? "__end__" : "AGENT"),
        AGENT: "__end__",
      },
    });
    const recordedCalls = [];
    const failureStore = { recordEngineFailure: (args) => { recordedCalls.push(args); } };
    const { executor } = makeExecutor({ p: pipeline }, [], { failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(recordedCalls.length, 1, "CODE's throw must record immediately, even though the flow goes on to succeed via AGENT");
    assert.equal(recordedCalls[0].message, "infra hiccup");
    assert.equal(recordedCalls[0].errorType, "infra_unavailable");
    assert.equal(recordedCalls[0].failureNode, "CODE");
  });

  it("_cascadeError is cleared once its node succeeds — a later edge call for the SAME node never sees a stale error", async () => {
    // A success doesn't
    // remove a stale _cascadeError from an earlier failed attempt on its
    // own, so without an explicit clear, an edge function checking `error`
    // before `state` would wrongly see attempt 1's error even after
    // attempt 2 succeeded. This exercises that exact ordering.
    let attempts = 0;
    const seenErrors = [];
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE: {
          kind: "code",
          run: () => {
            attempts++;
            if (attempts === 1) throw new NodeFailure("first attempt failed", { errorType: "infra_unavailable" });
            return { CODE: { ok: true } };
          },
        },
      },
      edges: {
        __start__: "CODE",
        // Deliberately checks `error` FIRST, unlike the earlier tests'
        // `state.CODE?.ok` guard — this is the ordering that would expose
        // a stale marker if the clear didn't happen.
        CODE: (state, error) => {
          seenErrors.push(error);
          if (error) return "CODE";
          return "__end__";
        },
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId).status, "succeeded");
    assert.equal(seenErrors.length, 2, "CODE's edge is consulted once after the throw (retry) and once after the success (advance)");
    assert.deepEqual(seenErrors[0], { message: "first attempt failed", errorType: "infra_unavailable" });
    assert.equal(seenErrors[1], null, "the 2nd edge call, made after CODE succeeded, must not see attempt 1's stale error");
  });
});

// ── cascade-kind flow: every throw persists to failure_events, no duplicate ──
// row on exhaustion ────────────────────────────────────────────────────────
// Every cascade-leg throw calls
// failureStore.recordEngineFailure immediately (unconditional on what the
// edge decides next), not just the terminal give-up. The exhaustion path's
// own persistFailRetrying->recordEngineFailure call reuses the SAME
// invocationId/executionId as the last leg's own direct call — the real
// DB's execution_id UPSERT collapses those into ONE row; this test can
// only verify the identity-reuse half
// (the actual dedup mechanics are failure-store.test.js's upsertFailureRow
// coverage).

describe("runFlow — cascade-kind flow: failure_events persistence", () => {
  it("a cascade-continuing throw records immediately, even though the flow later succeeds", async () => {
    let attempts = 0;
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        WORK: {
          kind: "code",
          run: () => {
            attempts++;
            if (attempts === 1) throw new NodeFailure("transient", { errorType: "infra_unavailable" });
            return { WORK: { ok: true } };
          },
        },
      },
      edges: { __start__: "WORK", WORK: (state) => (state.WORK ? "__end__" : "WORK") },
    });
    const recordedCalls = [];
    const failureStore = { recordEngineFailure: (args) => { recordedCalls.push(args); } };
    const { executor, store } = makeExecutor({ p: pipeline }, [], { failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId).status, "succeeded");
    assert.equal(recordedCalls.length, 1, "the transient throw must persist immediately, independent of the eventual success");
    assert.equal(recordedCalls[0].errorType, "infra_unavailable");
  });

  it("escalation-then-exhaustion: the give-up call reuses the last leg's own invocationId/executionId, not a fabricated new one", async () => {
    const spy = makeSpyNodeExecutionsStore();
    const recordedCalls = [];
    const failureStore = { recordEngineFailure: (args) => { recordedCalls.push(args); } };
    const pipeline = makePipeline({
      kind: "cascade",
      nodes: {
        CODE_LOGIN: { kind: "code", run: () => { throw new NodeFailure("script crashed", { errorType: "inconclusive" }); } },
        AGENT_LOGIN: { kind: "code", run: () => { throw new NodeFailure("agent also failed", { errorType: "blocked" }); } },
      },
      edges: {
        __start__: "CODE_LOGIN",
        CODE_LOGIN: (state) => (state.CODE_LOGIN ? "__end__" : "AGENT_LOGIN"),
        AGENT_LOGIN: (state) => (state.AGENT_LOGIN ? "__end__" : null),
      },
    });
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeExecutionsStore: spy, failureStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");

    assert.equal(store.get(r.flowId).status, "failed");
    // CODE_LOGIN's own throw (leg 1), AGENT_LOGIN's own throw (leg 2), and
    // the exhaustion give-up (persistFailRetrying's internal call) — 3 calls
    // total, but the last two must share identity for the DB upsert to dedupe.
    assert.equal(recordedCalls.length, 3);
    assert.equal(recordedCalls[0].failureNode, "CODE_LOGIN");
    assert.equal(recordedCalls[1].failureNode, "AGENT_LOGIN");
    assert.equal(recordedCalls[2].executionId, recordedCalls[1].executionId, "the give-up call must reuse AGENT_LOGIN's own throw identity, not fabricate a new one");
    assert.equal(recordedCalls[2].invocationId, recordedCalls[1].invocationId);
  });
});

// ── blocked-wait reason synthesis (describeWaitFallback) ────────────────────
// The generic `blocked at ${nodeId}` fallback that
// fires for every waitJson-returning node kind, discarding real data
// (pending-item count, child-flow count) that was already in scope. Only
// human-gate and children-pending ever reach this fallback — every other
// wait kind (resource-lock, agent-semaphore, needs-gateway-handoff,
// pipeline-load-pending, budget-gate-approval) carries a real reason by
// construction and never hits it.

describe("runFlow — blocked-wait reason synthesis", () => {
  it("human-gate: synthesizes 'awaiting N approval(s) at NODE', not the generic template", async () => {
    const pipeline = makePipeline({
      nodes: {
        GATE: {
          kind: "human-gate",
          pendingSelector: () => ["item-1", "item-2"],
          approvalRequestBuilder: (s, itemId) => ({ title: itemId, layout: [] }),
        },
      },
      edges: { __start__: "GATE", GATE: "__end__" },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    const flow = store.get(r.flowId);
    assert.equal(flow.statusReason, "awaiting 2 approvals at GATE");
  });

  it("human-gate: singular 'approval' for exactly one pending item", async () => {
    const pipeline = makePipeline({
      nodes: {
        GATE: {
          kind: "human-gate",
          pendingSelector: () => ["item-1"],
          approvalRequestBuilder: (s, itemId) => ({ title: itemId, layout: [] }),
        },
      },
      edges: { __start__: "GATE", GATE: "__end__" },
    });
    const { executor, store } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId, "fresh");
    const flow = store.get(r.flowId);
    assert.equal(flow.statusReason, "awaiting 1 approval at GATE");
  });

  // children-pending and the unrecognised-kind fallback are covered as
  // direct unit tests of describeWaitFallback() itself in
  // executor-helpers.test.js — a `code`-kind node's run() return doesn't
  // pass waitJson through (only human-gate/loop dispatch do, see
  // executor-nodes.js's executeNodeBody), so exercising those two cases
  // through a full runFlow() would need a real childFlows loop dispatch,
  // unnecessary complexity for testing a pure string-synthesis function.
});

// ── agent circuit breaker ────────────────────────────────────────────────────
// node.circuitBreaker:true opt-in, checked by dispatchAgentEntry
// (executor-nodes.js) for ANY "agent"-kind node regardless of which kind of
// flow it's running inside — not specific to cascade sub-nodes, though
// those are exactly the nodes that need this opt-in.

// Methods return Promises — the real store is async, and a sync mock here
// would let a missing `await` at the call site pass green.
function makeFakeNodeHealthStore() {
  const blocked = new Set();
  const marked = [];
  return {
    isBlocked: async (pipelineId, nodeId) => blocked.has(`${pipelineId}/${nodeId}`),
    markUnhealthy: async (pipelineId, nodeId, info) => {
      blocked.add(`${pipelineId}/${nodeId}`);
      marked.push({ pipelineId, nodeId, ...info });
      return { marked: true };
    },
    block: (pipelineId, nodeId) => blocked.add(`${pipelineId}/${nodeId}`), // test setup helper
    marked,
  };
}

function makeCircuitBreakerAgentNode(overrides = {}) {
  return { kind: "agent", agent: "scout", session: "agent:scout:test", circuitBreaker: true, promptBuilder: () => "hi", apply: (s, o) => ({ reply: o }), ...overrides };
}

describe("runFlow — agent circuit breaker: check", () => {
  it("fails immediately without dispatching the agent when it's pre-blocked", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: makeCircuitBreakerAgentNode() },
      edges: { __start__: "AGENT", AGENT: "__end__" },
    });
    const nodeHealthStore = makeFakeNodeHealthStore();
    nodeHealthStore.block("p", "AGENT");
    // No runTurn provided — if the short-circuit didn't fire, dispatch
    // would refuse ("no agent runtime bound") and the statusReason below
    // would not match, proving the agent was never actually invoked.
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeHealthStore });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    const final = store.get(r.flowId);
    assert.equal(final?.status, "failed");
    assert.match(final?.statusReason ?? "", /unhealthy agent dispatch/);
  });

  it("dispatches normally when the blocked key does not match this node's id", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: makeCircuitBreakerAgentNode() },
      edges: { __start__: "AGENT", AGENT: "__end__" },
    });
    const nodeHealthStore = makeFakeNodeHealthStore();
    nodeHealthStore.block("p", "OTHER_NODE");
    const fakeRunTurn = async () => ({ text: "done", runId: "run-1" });
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeHealthStore, runTurn: fakeRunTurn });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    assert.equal(store.get(r.flowId)?.status, "succeeded");
  });

  it("does not gate a plain agent node without circuitBreaker:true, even if the store has an entry under its id", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: { kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "hi", apply: (s, o) => ({ o }) } },
      edges: { __start__: "AGENT", AGENT: "__end__" },
    });
    const nodeHealthStore = makeFakeNodeHealthStore();
    nodeHealthStore.block("p", "AGENT");
    const fakeRunTurn = async () => ({ text: "done", runId: "run-1" });
    const { executor, store } = makeExecutor({ p: pipeline }, [], { nodeHealthStore, runTurn: fakeRunTurn });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    assert.equal(store.get(r.flowId)?.status, "succeeded", "the breaker only gates nodes that opt in via circuitBreaker:true");
  });
});

describe("runFlow — agent circuit breaker: mark", () => {
  it("marks the node unhealthy, keyed by its own nodeId, when its dispatch throws", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: makeCircuitBreakerAgentNode() },
      edges: { __start__: "AGENT", AGENT: "__end__" },
    });
    const nodeHealthStore = makeFakeNodeHealthStore();
    const fakeRunTurn = async () => { throw new Error("provider down"); };
    const { executor, store } = makeExecutor({ p: pipeline }, [], {
      nodeHealthStore,
      runTurn: fakeRunTurn,
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    assert.equal(store.get(r.flowId)?.status, "failed");
    assert.equal(nodeHealthStore.marked.length, 1);
    assert.equal(nodeHealthStore.marked[0].pipelineId, "p");
    assert.equal(nodeHealthStore.marked[0].nodeId, "AGENT");
  });

  it("does not mark unhealthy for a plain agent node without circuitBreaker:true", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: { kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "hi", apply: (s, o) => ({ o }) } },
      edges: { __start__: "AGENT", AGENT: "__end__" },
    });
    const nodeHealthStore = makeFakeNodeHealthStore();
    const fakeRunTurn = async () => { throw new Error("provider down"); };
    const { executor, store } = makeExecutor({ p: pipeline }, [], {
      nodeHealthStore,
      runTurn: fakeRunTurn,
    });
    const r = await executor.start("p");
    await executor.runFlow(r.flowId);
    assert.equal(store.get(r.flowId)?.status, "failed");
    assert.equal(nodeHealthStore.marked.length, 0);
  });
});


// ── rerun / rerunFromSource ───────────────────────────────────────────────────

describe("rerun", () => {
  it("throws when source flow is not found", async () => {
    const { executor } = makeExecutor({ p: makePipeline() });
    await assert.rejects(() => executor.rerun("no-such-flow"), /source flow not found/);
  });

  it("creates a new flow from source", async () => {
    const { executor, store } = makeExecutor({ p: makePipeline() });
    const r1 = await executor.start("p");
    // Manually set status to succeeded so singleton doesn't block
    const f = store._map.get(r1.flowId);
    store._map.set(r1.flowId, { ...f, status: "succeeded" });
    const r2 = await executor.rerun(r1.flowId);
    assert.notEqual(r1.flowId, r2.flowId);
    assert.equal(store.get(r2.flowId)?.status, "running");
  });
});

describe("rerunFromSource", () => {
  it("throws when sourceFlow has no pipelineId", async () => {
    const { executor } = makeExecutor({ p: makePipeline() });
    await assert.rejects(
      () => executor.rerunFromSource({ flowId: "src", stateJson: {} }),
      /pipelineId/
    );
  });
});

// ── cancel ────────────────────────────────────────────────────────────────────

describe("cancel", () => {
  it("cancels an active flow", async () => {
    const { executor, store } = makeExecutor({ p: makePipeline() });
    const r = await executor.start("p");
    const result = await executor.cancel(r.flowId);
    assert.equal(result.cancelled, true);
    assert.equal(store.get(r.flowId)?.status, "cancelled");
  });

  it("returns cancelled=false for unknown flow", async () => {
    const { executor } = makeExecutor({ p: makePipeline() });
    const result = await executor.cancel("no-such-flow");
    assert.equal(result.cancelled, false);
  });
});

// ── reviveAgentKindOrphan ──────────────────────────────────────────────────────

describe("reviveAgentKindOrphan", () => {
  function makeOrphanFlow({ flowId = "orphan-1", revision = 5, lastStep = "AGENT", pipelineId = "p" } = {}) {
    return { flowId, revision, lastStep, status: "running", stateJson: { pipelineId }, waitJson: null };
  }

  it("sends REVIVAL_MSG to the same deterministic session, applies the reply, and continues the flow", async () => {
    const pipeline = makePipeline({
      nodes: {
        AGENT: {
          kind: "agent", agent: "scout", session: "agent:scout:orphan-session",
          promptBuilder: () => "original prompt — must NOT be sent on revival",
          apply: (state, output) => ({ agentReply: output.text }),
        },
      },
      edges: { __start__: "AGENT", AGENT: "__end__" },
    });
    let capturedSession, capturedMessage;
    const fakeRunTurn = async ({ session, message }) => {
      capturedSession = session;
      capturedMessage = message;
      return { text: "already submitted, done", runId: "run-1" };
    };
    const { executor, store } = makeExecutor({ p: pipeline }, [], { runTurn: fakeRunTurn });
    const flow = makeOrphanFlow();
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    assert.equal(capturedMessage, REVIVAL_MSG, "must send the continuation prompt, not promptBuilder's output");
    assert.equal(capturedSession, "agent:scout:orphan-session", "must resume the SAME session the original dispatch used");
    const final = store.get(flow.flowId);
    assert.equal(final.status, "succeeded", "flow continues past AGENT to __end__");
    // state_json is entry-only — agentReply (AGENT's own apply() output)
    // does not land there. Reconstruction via assembleState() is
    // covered by executor-assemble-state-e2e.test.js; this test's own job
    // is the revival/session behavior above, not resultJson replay.
    assert.equal(final.stateJson.agentReply, undefined, "state_json must not carry node output");
  });

  it("does nothing for a node that is not kind:agent", async () => {
    const pipeline = makePipeline({ nodes: { CODE_STEP: { kind: "code", run: () => ({}) } } });
    const { executor, store } = makeExecutor({ p: pipeline }, []);
    const flow = makeOrphanFlow({ lastStep: "CODE_STEP" });
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    assert.equal(store.get(flow.flowId).status, "running", "left untouched — no dispatch, no persist");
  });

  it("fails the flow when the revival dispatch throws", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: { kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "unused", apply: (s, o) => ({ o }) } },
    });
    const fakeRunTurn = async () => { throw new Error("provider down"); };
    const { executor, store } = makeExecutor({ p: pipeline }, [], { runTurn: fakeRunTurn });
    const flow = makeOrphanFlow();
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    const final = store.get(flow.flowId);
    assert.equal(final.status, "failed");
    assert.ok(final.statusReason.includes("revival failed"));
  });

  it("passes kind:revival to nodeExecutions.write() on revival failure, even with no matching prior invocation", async () => {
    // makeOrphanFlow's flow has no prior dispatch for this node at all — if
    // kind were derived the same way runFlow's loop derives it
    // (isFreshNodeEntry against findCurrentStep → "fresh"), a null
    // lastNodeId would incorrectly resolve to "fresh".
    // reviveAgentKindOrphan hardcodes kind:"revival" instead, since revival
    // is always a continuation of the exact node that crashed, not a fresh
    // dispatch decision — this proves the hardcode is actually load-bearing.
    const pipeline = makePipeline({
      nodes: { AGENT: { kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "unused", apply: (s, o) => ({ o }) } },
    });
    const fakeRunTurn = async () => { throw new Error("provider down"); };
    const spy = makeSpyNodeExecutionsStore();
    const { executor, store } = makeExecutor({ p: pipeline }, [], { runTurn: fakeRunTurn, nodeExecutionsStore: spy });
    const flow = makeOrphanFlow();
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    assert.equal(spy.calls.length, 1);
    assert.equal(spy.calls[0].nodeId, "AGENT");
    assert.equal(spy.calls[0].kind, "revival");
  });

  it("thrown revival dispatch: threads write()'s returned ids into recordEngineFailure", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: { kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "unused", apply: (s, o) => ({ o }) } },
    });
    const fakeRunTurn = async () => { throw new Error("provider down"); };
    const spy = makeSpyNodeExecutionsStore();
    let recorded = null;
    const failureStore = { recordEngineFailure: (args) => { recorded = args; } };
    const { executor, store } = makeExecutor({ p: pipeline }, [], { runTurn: fakeRunTurn, nodeExecutionsStore: spy, failureStore });
    const flow = makeOrphanFlow();
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    assert.equal(spy.calls.length, 1, "write() must run before persistFailRetrying, not skipped");
    assert.equal(recorded?.invocationId, "inv-1");
    assert.equal(recorded?.executionId, "exec-1");
  });

  it("revival result blocked without declaring a wait kind: fails the flow and threads write()'s returned ids", async () => {
    const pipeline = makePipeline({
      nodes: {
        AGENT: {
          kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "unused",
          apply: () => ({ _blocked: { reason: "needs review" } }),
        },
      },
    });
    const fakeRunTurn = async () => ({ text: "done", runId: "run-1" });
    const spy = makeSpyNodeExecutionsStore();
    let recorded = null;
    const failureStore = { recordEngineFailure: (args) => { recorded = args; } };
    const { executor, store } = makeExecutor({ p: pipeline }, [], { runTurn: fakeRunTurn, nodeExecutionsStore: spy, failureStore });
    const flow = makeOrphanFlow();
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    const final = store.get(flow.flowId);
    assert.equal(final.status, "failed");
    assert.ok(final.statusReason.includes("needs review"));
    assert.equal(spy.calls.length, 1);
    assert.equal(recorded?.invocationId, "inv-1");
    assert.equal(recorded?.executionId, "exec-1");
  });

  it("fails the flow when no agent runtime is bound (the harness must inject runTurn)", async () => {
    const pipeline = makePipeline({
      nodes: { AGENT: { kind: "agent", agent: "scout", session: "agent:scout:test", promptBuilder: () => "unused", apply: (s, o) => ({ o }) } },
    });
    // No runTurn provided — dispatch refuses outright ("no agent runtime
    // bound"). A runTurn that throws GatewayHandoffNeeded mid-turn still
    // blocks instead of failing; that harness-side path is covered by
    // plugins/openclaw/manifold/run-turn.test.js.
    const { executor, store } = makeExecutor({ p: pipeline }, []);
    const flow = makeOrphanFlow();
    store._map.set(flow.flowId, flow);

    await executor.reviveAgentKindOrphan(flow);

    const final = store.get(flow.flowId);
    assert.equal(final.status, "failed");
    assert.match(final.statusReason ?? "", /revival failed/);
    assert.match(final.statusReason ?? "", /no agent runtime bound/);
  });
});

// ── reviveAgentKindOrphan — writeResult commits before last_step advances ──
// reviveAgentKindOrphan is a separate code path from runFlow's main dispatch
// loop (above): it re-runs apply() before touching state (no
// stale-read-on-entry), but must also call writeResult() for the node it
// just revived before persistRunningRetrying
// advances last_step past it — otherwise the immediately-following
// runFlow(flowId, "revival") call's assembleState() sees an empty result
// for that node, and any outbound edge depending on the node's own output
// dead-ends with "no next node".
describe("reviveAgentKindOrphan — writeResult commits before last_step advances", () => {
  it("calls nodeExecutionsStore.writeResult() before store.setRunning(), and the revived node's own output is visible to its outbound edge", async () => {
    const pipeline = makePipeline({
      nodes: {
        AGENT: {
          kind: "agent", agent: "scout", session: "agent:scout:orphan-session",
          promptBuilder: () => "original prompt — must NOT be sent on revival",
          apply: (state, output) => ({ agentReply: output.text }),
        },
      },
      edges: {
        __start__: "AGENT",
        // A real business-logic edge, same shape as SCORE_LLM's: dead-ends
        // (returns null → "no next node") if the node's own output isn't
        // visible yet.
        AGENT: (state) => (state.agentReply ? "__end__" : null),
      },
    });
    const fakeRunTurn = async () => ({ text: "already submitted, done", runId: "run-1" });
    const order = [];
    const baseStore = makeStore([]);
    const store = {
      ...baseStore,
      setRunning: (args) => {
        order.push("setRunning");
        return baseStore.setRunning(args);
      },
    };
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const baseSpy = makeSpyNodeExecutionsStore();
    const spy = {
      ...baseSpy,
      writeResult: async (invocationId, result) => {
        order.push("writeResult");
        return baseSpy.writeResult(invocationId, result);
      },
    };
    const executor = createExecutor({
      api, store, pipelinesRef, logger,
      ensurePipelinesLoaded: null, runTurn: fakeRunTurn,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const flow = { flowId: "orphan-1", revision: 5, lastStep: "AGENT", status: "running", stateJson: { pipelineId: "p" }, waitJson: null };
    store._map.set(flow.flowId, flow);
    // The crashed dispatch's own enter() call, made BEFORE the crash — this
    // invocation row already exists by the time revival runs in production
    // (nodeExecutionsStore.enter() runs before executeNode() in the main
    // loop). Revival must find and reuse this same row, not skip the
    // writeResult step because it created none of its own.
    await spy.enter({ flowId: flow.flowId, pipelineId: "p", nodeId: "AGENT", subflowKey: null, kind: "fresh" });

    await executor.reviveAgentKindOrphan(flow);

    assert.deepEqual(
      order,
      ["writeResult", "setRunning"],
      "the revived node's result must be durably written before last_step advances past it",
    );
    const final = store.get(flow.flowId);
    assert.equal(
      final.status, "succeeded",
      "AGENT's own edge must see agentReply and resolve to __end__, not dead-end with 'no next node'",
    );
  });

  // Same requirement as the main dispatch loop above, but more
  // reachable here: revival's invocationId comes from a pure read
  // (getLatestInvocationForNode) with no creation fallback, unlike the main
  // loop's enter(), so "no-such-invocation" is a real case, not just
  // theoretical — e.g. the crashed dispatch's invocation row was itself
  // never durably created before the crash.
  it("fails the flow (not a silent advance) when writeResult fails for a reason other than race", async () => {
    const pipeline = makePipeline({
      nodes: {
        AGENT: {
          kind: "agent", agent: "scout", session: "agent:scout:orphan-session",
          promptBuilder: () => "original prompt — must NOT be sent on revival",
          apply: (state, output) => ({ agentReply: output.text }),
        },
      },
      edges: { __start__: "AGENT", AGENT: (state) => (state.agentReply ? "__end__" : null) },
    });
    const fakeRunTurn = async () => ({ text: "already submitted, done", runId: "run-1" });
    const warnCalls = [];
    const spyLogger = { info: () => {}, warn: (msg) => warnCalls.push(msg), error: () => {} };
    const store = makeStore([]);
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const baseSpy = makeSpyNodeExecutionsStore();
    const spy = {
      ...baseSpy,
      writeResult: async () => ({ ok: false, reason: "no-such-invocation" }),
    };
    const executor = createExecutor({
      api, store, pipelinesRef, logger: spyLogger,
      ensurePipelinesLoaded: null, runTurn: fakeRunTurn,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const flow = { flowId: "orphan-2", revision: 5, lastStep: "AGENT", status: "running", stateJson: { pipelineId: "p" }, waitJson: null };
    store._map.set(flow.flowId, flow);
    await spy.enter({ flowId: flow.flowId, pipelineId: "p", nodeId: "AGENT", subflowKey: null, kind: "fresh" });

    await executor.reviveAgentKindOrphan(flow);

    assert.equal(warnCalls.length, 1);
    assert.match(warnCalls[0], /resultJson write failed \(no-such-invocation\)/);
    const final = store.get(flow.flowId);
    assert.equal(final.status, "failed", "must fail outright, not dead-end downstream with 'no next node'");
    assert.match(final.statusReason, /resultJson write failed \(no-such-invocation\)/);
  });

  it("still advances (does not fail) when writeResult reports a race — a concurrent writer already committed real content", async () => {
    const pipeline = makePipeline({
      nodes: {
        AGENT: {
          kind: "agent", agent: "scout", session: "agent:scout:orphan-session",
          promptBuilder: () => "original prompt — must NOT be sent on revival",
          apply: (state, output) => ({ agentReply: output.text }),
        },
      },
      edges: { __start__: "AGENT", AGENT: (state) => (state.agentReply ? "__end__" : null) },
    });
    const fakeRunTurn = async () => ({ text: "already submitted, done", runId: "run-1" });
    const store = makeStore([]);
    const pipelinesRef = { value: new Map([["p", pipeline]]) };
    const baseSpy = makeSpyNodeExecutionsStore();
    // Same honest simulation as the main-loop version of this test above —
    // the loser gets {ok:false, reason:"race"}, but the CONCURRENT writer's
    // own call actually won and stored the real content.
    const spy = {
      ...baseSpy,
      writeResult: async (invocationId, result) => {
        await baseSpy.writeResult(invocationId, result);
        return { ok: false, reason: "race" };
      },
    };
    const executor = createExecutor({
      api, store, pipelinesRef, logger,
      ensurePipelinesLoaded: null, runTurn: fakeRunTurn,
      failureStore: null, nodeHealthStore: null, notifyFlowEnteredWaiting: null,
      sessionIndex: null, nodeExecutionsStore: spy,
      admissionQueueStore: makeAdmissionQueueStore(),
    });
    const flow = { flowId: "orphan-3", revision: 5, lastStep: "AGENT", status: "running", stateJson: { pipelineId: "p" }, waitJson: null };
    store._map.set(flow.flowId, flow);
    await spy.enter({ flowId: flow.flowId, pipelineId: "p", nodeId: "AGENT", subflowKey: null, kind: "fresh" });

    await executor.reviveAgentKindOrphan(flow);

    assert.equal(store.get(flow.flowId)?.status, "succeeded", "a race loser must not fail the flow — the winner's write already has real data");
  });
});

// ─── nodeEmitter.node.executing ───────────────────────────────────────────────

// process.nextTick inside an async microtask chain fires only after all
// currently-queued microtasks complete — i.e. after `await runFlow()` resolves.
// A setImmediate flush ensures those nextTick callbacks drain before asserting.
const flushNextTick = () => new Promise((r) => setImmediate(r));

describe("runFlow — nodeEmitter.node.executing", () => {
  it("emits node.executing with correct flowId, nodeId, pipelineId when a node is entered", async () => {
    const pipeline = makePipeline({
      nodes: { WORK: { kind: "code", run: () => ({ done: true }) } },
      edges: { __start__: "WORK", WORK: "__end__" },
    });
    const { executor } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");

    const emitted = [];
    const capture = (p) => { if (p.flowId === r.flowId) emitted.push(p); };
    nodeEmitter.on("node.executing", capture);
    await executor.runFlow(r.flowId);
    await flushNextTick();
    nodeEmitter.off("node.executing", capture);

    assert.equal(emitted.length, 1, "should fire exactly once for the single WORK node");
    assert.equal(emitted[0].flowId, r.flowId);
    assert.equal(emitted[0].nodeId, "WORK");
    assert.equal(emitted[0].pipelineId, "p");
    assert.equal(emitted[0].parentInvocationId, null);
  });

  it("emits once per node visited in a multi-node pipeline", async () => {
    const pipeline = makePipeline({
      nodes: {
        A: { kind: "code", run: () => ({}) },
        B: { kind: "code", run: () => ({}) },
      },
      edges: { __start__: "A", A: "B", B: "__end__" },
    });
    const { executor } = makeExecutor({ p: pipeline });
    const r = await executor.start("p");

    const nodeIds = [];
    const capture = ({ flowId, nodeId }) => { if (flowId === r.flowId) nodeIds.push(nodeId); };
    nodeEmitter.on("node.executing", capture);
    await executor.runFlow(r.flowId);
    await flushNextTick();
    nodeEmitter.off("node.executing", capture);

    assert.deepEqual(nodeIds, ["A", "B"]);
  });
});

// ── invokePipeline — singleton-busy ──────────────────────────────────────────

// invokePipeline translates a queued admission record into the
// invoked:false/reason:"singleton-busy" shape callers already depend on.
// Like the file it lives in, this block drives the real createExecutor(),
// start() and admissionQueueStore rather than the nodeCtx surface alone.
function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeSingletonBusyHarness() {
  const dbPath = tmpPath("invoke-singleton");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const admissionQueueStore = createAdmissionQueueStore({ dbPath, getLimit: getGlobalConcurrencyLimit });
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, admissionQueueStore, logger });

  // This fixture deliberately omits allowConcurrent. The per-pipeline
  // singleton gate in executor.js's start() fires on `allowConcurrent ===
  // false` only, so omitting the field leaves this pipeline unconstrained by
  // that gate and competing for the shared global limit alone (set by the
  // test below). Setting `allowConcurrent: false` here would change what the
  // test exercises: whenever the gate fired it would leave start() with
  // status "skipped" rather than "queued", and it is the queued→singleton-busy
  // translation above that this test asserts on.
  const singletonTarget = {
    id: "singleton_target",
    state: {},
    nodes: { NOOP: { kind: "code", run: () => ({ NOOP: { ran: true } }) } },
    edges: { __start__: "NOOP", NOOP: "__end__" },
  };
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: {
      CALL_TWICE: {
        kind: "code",
        run: async (state, ctx) => {
          const first = await ctx.invokePipeline("singleton_target");
          const second = await ctx.invokePipeline("singleton_target");
          return { CALL_TWICE: { first, second } };
        },
      },
    },
    edges: { __start__: "CALL_TWICE", CALL_TWICE: "__end__" },
  };
  const pipelinesRef = {
    value: new Map([
      ["test_parent", parentPipeline],
      ["singleton_target", singletonTarget],
    ]),
  };

  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    runTurn: null,
    failureStore: null,
    nodeHealthStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex: null,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore,
  });
  resumeParentRef.resume = executor.resume;

  return { runStore, nodeExecutionsStore, store, executor };
}

describe("nodeCtx.invokePipeline — singleton-busy when the shared pool is full", () => {
  let harness;
  afterEach(() => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
    resetGlobalConcurrency();
  });

  it("second call to an already-active singleton target returns invoked:false, reason:singleton-busy — does not throw", async () => {
    // limit=2, not 1 — under the shared GLOBAL pool, the PARENT
    // flow (test_parent) itself now holds one of the slots for its own
    // entire run (kind:"flow-start", same pool every kind draws from), so
    // a limit of 1 would make even the FIRST invokePipeline call busy.
    // limit=2 leaves exactly one free slot for a target flow — enough for
    // the first call to succeed and the second (a genuinely concurrent 2nd
    // instance of the SAME target pipeline) to find the pool full.
    resetGlobalConcurrency(2);
    harness = await makeSingletonBusyHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await executor.runFlow(record.flowId);

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded", "the parent node must not have thrown out of CALL_TWICE.run");

    const flowState = await assembleState(flow.flowId, { runStore, nodeExecutionsStore, baseState: flow.stateJson });
    const { first, second } = flowState.CALL_TWICE;
    assert.equal(first.invoked, true, "first call against an idle singleton target must succeed");
    assert.equal(second.invoked, false);
    assert.equal(second.reason, "singleton-busy");
    assert.match(second.message, /is at its concurrency limit/i);
  });
});
