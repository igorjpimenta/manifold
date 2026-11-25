/**
 * Engine-mechanics E2E for assembleState(), one test per node
 * shape, matching this suite's own established convention (see
 * executor-subflow-children-e2e.test.js):
 * a real createExecutor() driving a real dispatch loop against isolated tmp
 * sqlite stores, asserting assembleState()'s output against the SAME flow's
 * real stateJson rather than a hand-built mock of listResultsForFlow.
 *
 * nodeExecutions.writeResult() records results for the ordinary code/agent
 * success branch (executor.js's main loop) — human-gate's own
 * *completion* write goes through that same branch once its pendingSelector
 * clears, but a gate's approvedIds/rejectedIds (written by gate-resolver.js
 * directly onto stateJson, never through a node's own stateUpdate) and
 * dispatchSubflowChildren's parent-side write (always stateUpdate:{}, per
 * the design doc's corrected "SCORE is itself a subflow" resolution) are
 * NOT node-keyed results — assembleState() correctly has no way to see them.
 * Several tests below assert that gap explicitly, as documented current
 * behavior.
 *
 * Isolated-sandbox discipline:
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForGateOpen, waitForTerminal } from "../../helpers/wait-for-event.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";
import { assembleState } from "../../../engine/executor-helpers.js";

// Always-admits-immediately fake — admissionQueueStore is required by createExecutor's
// subflow-children paths. These tests don't exercise queue behavior.
function makeAdmissionQueueStore(overrides = {}) {
  return {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
    ...overrides,
  };
}

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function makeHarness(pipelineDef) {
  const dbPath = tmpPath("e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const pipelinesRef = { value: new Map(Object.entries(pipelineDef)) };

  const admissionQueueStore = makeAdmissionQueueStore();
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

  const gateResolver = createGateResolver({
    api: { logger },
    store, executor, runStore, nodeExecutionsStore, admissionQueueStore,
    pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
  });

  return { runStore, nodeExecutionsStore, store, executor, gateResolver, pipelinesRef };
}

describe("assembleState() against a real dispatch loop, per node shape", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("ordinary sequential code nodes: assembled state matches real stateJson key-for-key", async () => {
    harness = await makeHarness({
      test_linear: {
        id: "test_linear",
        state: {},
        nodes: {
          A: { kind: "code", run: () => ({ A: { v: 1 } }) },
          B: { kind: "code", run: (state) => ({ B: { v: state.A.v + 1 } }) },
          C: { kind: "code", run: () => ({ C: { done: true } }) },
        },
        edges: { __start__: "A", A: "B", B: "C", C: "__end__" },
      },
    });
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_linear", {}, { kind: "test" });
    await executor.runFlow(record.flowId);

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");

    // state_json is entry-only — A/B/C never land
    // there at all. assembleState() is the only source for a node's own
    // output; this test proves stateJson has nothing to compare against
    // and that assembleState()
    // alone is still correct.
    assert.equal(flow.stateJson.A, undefined, "state_json must not carry node output");
    assert.equal(flow.stateJson.B, undefined);
    assert.equal(flow.stateJson.C, undefined);

    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.equal(assembled.A.v, 1);
    assert.equal(assembled.B.v, 2);
    assert.equal(assembled.C.done, true);
  });

  test("loop-back (same node re-dispatched): assembled state reflects the LATEST visit, not the first", async () => {
    harness = await makeHarness({
      test_loop: {
        id: "test_loop",
        state: {},
        nodes: {
          LOOP: { kind: "code", run: (state) => ({ LOOP: { count: (state.LOOP?.count ?? 0) + 1 } }) },
        },
        edges: { __start__: "LOOP", LOOP: (state) => (state.LOOP.count < 3 ? "LOOP" : "__end__") },
      },
    });
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_loop", {}, { kind: "test" });
    // Unlike the other tests in this file, LOOP revisits the SAME nodeId
    // within one runFlow() call — isFreshNodeEntry only forces "fresh" on a
    // genuinely new node, so the 2nd/3rd dispatch falls through to whatever
    // `kind` runFlow() itself was called with. The real call sites (agent/
    // cli.js, routes/operator.js, dashboard/server.js) always pass "fresh"
    // after start() — match that convention rather than the no-arg form the
    // other tests in this file get away with (they never revisit a node).
    await executor.runFlow(record.flowId, "fresh");

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");
    // state_json is entry-only — LOOP's count never lands there
    // at all. assembleState() (below) is the only place it survives.
    assert.equal(flow.stateJson.LOOP, undefined, "state_json must not carry node output");

    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.equal(assembled.LOOP.count, 3, "assembleState must not accidentally return the 1st or 2nd pass's row");

    const results = await nodeExecutionsStore.listResultsForFlow(record.flowId);
    assert.equal(results.filter((r) => r.nodeId === "LOOP").length, 1, "3 visits dedup to exactly 1 row per listResultsForFlow's contract");
  });

  test("human-gate: both gates' records land in gate_records, never in state_json or assembleState() — across TWO gates in sequence", async () => {
    harness = await makeHarness({
      test_gates: {
        id: "test_gates",
        state: {},
        nodes: {
          GATE_1: {
            kind: "human-gate",
            pendingSelector: (state) => (state.approvedIds?.includes("g1") ? [] : ["g1"]),
            approvalRequestBuilder: () => ({ title: "Approve g1?", description: "", layout: [] }),
          },
          GATE_2: {
            kind: "human-gate",
            pendingSelector: (state) => (state.approvedIds?.includes("g2") ? [] : ["g2"]),
            approvalRequestBuilder: () => ({ title: "Approve g2?", description: "", layout: [] }),
          },
        },
        edges: { __start__: "GATE_1", GATE_1: "GATE_2", GATE_2: "__end__" },
      },
    });
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_gates", {}, { kind: "test" });
    await executor.runFlow(record.flowId);
    let flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "blocked");
    assert.equal(flow.waitJson.nodeId, "GATE_1");

    const gate2Open = waitForGateOpen((p) => p.flowId === record.flowId && p.nodeId === "GATE_2");
    await gateResolver.recordItemDecision({ flowId: record.flowId, itemId: "g1", decision: "approve", source: "ws" });
    await gate2Open;
    flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "blocked");
    assert.equal(flow.waitJson.nodeId, "GATE_2", "GATE_1 cleared, now blocked on GATE_2");

    const parentDone = waitForTerminal((p) => p.flowId === record.flowId);
    await gateResolver.recordItemDecision({ flowId: record.flowId, itemId: "g2", decision: "approve", source: "ws" });
    await parentDone;
    flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");

    // gate_records is the correctness-safe source for gate state (a
    // shared-key snapshot on stateJson goes stale across sibling gates —
    // see its own MIGRATIONS v11 comment); stateJson carries no gate
    // snapshot. Both gates' own point-lookup rows are the whole story.
    const gate1 = await nodeExecutionsStore.getGateRecord(record.flowId, "GATE_1");
    const gate2 = await nodeExecutionsStore.getGateRecord(record.flowId, "GATE_2");
    assert.ok(gate1?.consumedAt, "GATE_1's own record survives independently, keyed by its own nodeId");
    assert.ok(gate2?.consumedAt);

    assert.equal(flow.stateJson.gates, undefined, "state_json must never carry gate data");
    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.equal(assembled.gates, undefined, "assembleState has no gates key either — gate_records is the only source now");
  });

  test("known gap: approvedIds/rejectedIds are gate-resolver bookkeeping, not any node's resultJson — assembleState correctly omits them", async () => {
    harness = await makeHarness({
      test_gate_bookkeeping: {
        id: "test_gate_bookkeeping",
        state: {},
        nodes: {
          GATE: {
            kind: "human-gate",
            pendingSelector: (state) => (state.approvedIds?.includes("x") ? [] : ["x"]),
            approvalRequestBuilder: () => ({ title: "Approve x?", description: "", layout: [] }),
          },
        },
        edges: { __start__: "GATE", GATE: "__end__" },
      },
    });
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_gate_bookkeeping", {}, { kind: "test" });
    await executor.runFlow(record.flowId);
    const settled = waitForTerminal((p) => p.flowId === record.flowId);
    await gateResolver.recordItemDecision({ flowId: record.flowId, itemId: "x", decision: "approve", source: "ws" });
    await settled;

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");
    assert.deepEqual(flow.stateJson.approvedIds, ["x"], "sanity: gate-resolver really does write this to real stateJson");

    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.equal(assembled.approvedIds, undefined, "documented gap — gate bookkeeping is not carried into assembled state");
  });

  test("subflow (childFlows:true): assembled state carries _subflowGroups, no synthesized state[nodeId] for the subflow's own key", async () => {
    const CONNECT_NODE = {
      kind: "subflow",
      childFlows: true,
      selector: (state) => state.shortlist ?? [],
      itemKey: "id",
      flow: {
        nodes: { STAMP: { kind: "code", run: () => ({ STAMP: { done: true } }) } },
        edges: { __start__: "STAMP", STAMP: "__end__" },
      },
    };
    harness = await makeHarness({
      test_subflow_parent: {
        id: "test_subflow_parent",
        state: {},
        nodes: { CONNECT: CONNECT_NODE },
        edges: { __start__: "CONNECT", CONNECT: "__end__" },
      },
      "test_subflow_parent::CONNECT": {
        id: "test_subflow_parent::CONNECT",
        state: {},
        nodes: CONNECT_NODE.flow.nodes,
        edges: CONNECT_NODE.flow.edges,
      },
    });
    const { executor, runStore, nodeExecutionsStore, store } = harness;

    const record = await executor.start("test_subflow_parent", {}, { kind: "test" });
    const seed = await store.get(record.flowId);
    await store.setRunning({ flowId: record.flowId, expectedRevision: seed.revision, lastStep: seed.lastStep, stateJson: { ...seed.stateJson, shortlist: [{ id: "abc" }] } });
    const parentSettled = waitForTerminal((p) => p.flowId === record.flowId);
    await executor.runFlow(record.flowId);
    await parentSettled;

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded", "single child, no gate — parent auto-resumes past CONNECT once its only child is terminal");

    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.ok(assembled._subflowGroups?.CONNECT?.abc, "subflow result surfaces only via _subflowGroups, same mechanism enrichWithSubflows already uses");
    assert.equal(assembled.CONNECT, undefined, "CONNECT's own dispatchSubflowChildren always writes stateUpdate:{} — there is no flat state[nodeId] to assemble");
  });

  test("a result well over 64KB is assembled in full — no size limit applies", async () => {
    harness = await makeHarness({
      test_oversize: {
        id: "test_oversize",
        state: {},
        nodes: {
          BIG: { kind: "code", run: () => ({ BIG: { blob: "x".repeat(70_000) } }) },
        },
        edges: { __start__: "BIG", BIG: "__end__" },
      },
    });
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_oversize", {}, { kind: "test" });
    await executor.runFlow(record.flowId);

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");
    // state_json is entry-only — BIG never lands there regardless of
    // size. assembleState() (below) is the only place a node's own output
    // survives.
    assert.equal(flow.stateJson.BIG, undefined, "state_json must not carry node output");

    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.equal(assembled.BIG._resultTooLarge, undefined, "no cap left to trigger the marker");
    assert.equal(assembled.BIG.blob, "x".repeat(70_000), "the full oversized payload survives, unmodified");
  });

  test("a result well over the size cap, flat small+bulk fields: assembled state keeps ALL fields, including the bulk one", async () => {
    // A flat (unwrapped) node result mixing tiny scalars
    // (accessToken, bidderId) with a large array (listings) must keep
    // EVERY field when assembled — the tiny fields
    // a later node (SUBMIT_CODE) depends on must survive alongside
    // the bulk one. With no size cap, nothing gets dropped at all.
    harness = await makeHarness({
      test_oversize_mixed: {
        id: "test_oversize_mixed",
        state: {},
        nodes: {
          SCAN: {
            kind: "code",
            run: () => ({
              accessToken: "tok-abc123",
              bidderId: 987654,
              scanMode: "ok",
              listings: Array.from({ length: 2000 }, (_, i) => ({ id: i, text: "x".repeat(50) })),
            }),
          },
        },
        edges: { __start__: "SCAN", SCAN: "__end__" },
      },
    });
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_oversize_mixed", {}, { kind: "test" });
    await executor.runFlow(record.flowId);

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");

    const assembled = await assembleState(record.flowId, { runStore, nodeExecutionsStore });
    assert.equal(assembled.accessToken, "tok-abc123");
    assert.equal(assembled.bidderId, 987654);
    assert.equal(assembled.scanMode, "ok");
    assert.equal(assembled.listings.length, 2000, "the bulk field survives in full too — no size-based dropping left");
    assert.equal(assembled._resultDroppedFields, undefined);
  });
});
