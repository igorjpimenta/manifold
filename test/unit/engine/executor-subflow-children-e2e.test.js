/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors nyxen_linkedin-prospect's CONNECT
 * (DEDUP_CHECK → DRAFT_NOTE → GATE → STAMP_APPROVED/STAMP_REJECTED →
 * SEND_CR_CODE → STAMP_SENT), but with every "code"/"agent" sub-node
 * replaced by a trivial no-op stand-in — zero real filesystem, subprocess,
 * or agent dispatch I/O anywhere in this fixture. This proves the
 * loop → real children → gate-blocks-the-child → resume-the-parent
 * plumbing works for CONNECT's exact edge shape BEFORE any real send
 * script or agent is ever involved (that fidelity is covered by the real
 * node functions' own mocked-I/O unit tests under workspace/.pipelines/
 * test/unit/nyxen_linkedin-prospect/nodes/connect/).
 *
 * Isolated-sandbox discipline (per the incident writeup):
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForGateOpen, waitForTerminal, waitForStatus } from "../../helpers/wait-for-event.js";
import { waitForChildFlowIds, waitForTerminalStatus } from "../../helpers/wait-for-children.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";
import { getAllSubflowGroups } from "../../../state/subflow-state.js";
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

// CONNECT's exact edge topology, sub-nodes replaced by no-op stand-ins.
// GATE's edge reads state.approvedIds/rejectedIds, same as the real
// nodes/connect/index.js post-Milestone-7b rewrite.
function makeConnectLikeNode({ sendMode = "ok" } = {}) {
  return {
    kind: "subflow",
    childFlows: true,
    selector: (state) => state.shortlist ?? [],
    itemKey: "linkedin-id",
    flow: {
      nodes: {
        DEDUP_CHECK: { kind: "code", run: (state) => ({ DEDUP_CHECK: { status: "fresh" } }) },
        DRAFT_NOTE: { kind: "code", run: (state) => ({ DRAFT_NOTE: { text: "hi" } }) },
        GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const itemId = state.item?.["linkedin-id"];
            if (!itemId) return [];
            const decided = new Set([...(state.approvedIds ?? []), ...(state.rejectedIds ?? [])]);
            return decided.has(itemId) ? [] : [itemId];
          },
          approvalRequestBuilder: (state, itemId) => ({ title: `Approve ${itemId}?`, description: "", layout: [] }),
        },
        STAMP_APPROVED: { kind: "code", run: (state) => ({ STAMP_APPROVED: { stampedAt: "t" } }) },
        STAMP_REJECTED: { kind: "code", run: (state) => ({ STAMP_REJECTED: { rejectedAt: "t" } }) },
        SEND_CR_CODE: { kind: "code", run: (state) => ({ SEND_CR_CODE: { mode: sendMode } }) },
        SEND_CR_AGENT: { kind: "code", run: (state) => ({ SEND_CR_AGENT: { ok: true } }) },
        STAMP_SENT: { kind: "code", run: (state) => ({ STAMP_SENT: { stampedAt: "t" } }) },
      },
      edges: {
        __start__: "DEDUP_CHECK",
        DEDUP_CHECK: (state) => state.DEDUP_CHECK.status === "already-sent" ? "__end__" : "DRAFT_NOTE",
        DRAFT_NOTE: (state) => state.DRAFT_NOTE?._error ? "__end__" : "GATE",
        GATE: (state) => state.approvedIds?.includes(state.item?.["linkedin-id"]) ? "STAMP_APPROVED" : "STAMP_REJECTED",
        STAMP_APPROVED: "SEND_CR_CODE",
        STAMP_REJECTED: "__end__",
        SEND_CR_CODE: (state) => {
          const mode = state.SEND_CR_CODE.mode;
          if (mode === "agent_required") return "SEND_CR_AGENT";
          if (mode === "ok") return "STAMP_SENT";
          return "__end__";
        },
        SEND_CR_AGENT: (state) => state.SEND_CR_AGENT.ok ? "STAMP_SENT" : "__end__",
        STAMP_SENT: "__end__",
      },
    },
  };
}

async function makeHarness({ sendMode = "ok" } = {}) {
  const runStore = await createRunStore({ dbPath: tmpPath("e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const CONNECT_NODE = makeConnectLikeNode({ sendMode });
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { CONNECT: CONNECT_NODE },
    edges: { __start__: "CONNECT", CONNECT: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::CONNECT",
    state: {},
    nodes: CONNECT_NODE.flow.nodes,
    edges: CONNECT_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::CONNECT", childPipeline]]) };

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

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "CONNECT");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

describe("childFlows:true CONNECT-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("single item: real child spawned, child's own GATE blocks the child (not the parent)", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ "linkedin-id": "abc", name: "Jane" }]);
    await executor.runFlow(record.flowId);

    const parent = await runStore.get(record.flowId);
    assert.equal(parent.status, "blocked");
    assert.equal(parent.waitJson.kind, "children-pending");

    // The child's own runFlow is fire-and-forget dispatched via setImmediate
    // from dispatchLoopChildren — poll until it reaches its own GATE. A
    // fixed flush() flakes under full-suite CI load (the assertion fires
    // while the child is still running); the state store already announces
    // the transition, so wait for the condition that matters.
    const kids = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "CONNECT", 1);
    assert.equal(kids.length, 1);
    const child = await waitForStatus(runStore, kids[0], (r) => r.status === "blocked");
    assert.equal(child.pipelineId, "test_parent::CONNECT");
    assert.equal(child.waitJson.kind, "human-gate");
    assert.equal(child.waitJson.items[0].itemId, "abc");
  });

  test("approving the child's gate resumes ONLY the child, drives it through STAMP_APPROVED→SEND_CR_CODE→STAMP_SENT to terminal, then auto-resumes the parent to succeeded", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ "linkedin-id": "abc" }]);
    await executor.runFlow(record.flowId);
    const [childFlowId] = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "CONNECT", 1);
    await waitForStatus(runStore, childFlowId, (r) => r.status === "blocked");

    const result = await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "abc", decision: "approve", source: "cli" });
    assert.equal(result.ok, true);
    const child = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.STAMP_APPROVED?.stampedAt, "t");
    assert.equal(childState.SEND_CR_CODE?.mode, "ok");
    assert.equal(childState.STAMP_SENT?.stampedAt, "t");

    const parent = await waitForTerminalStatus(runStore, record.flowId);
    assert.equal(parent.status, "succeeded", "parent must auto-resume past CONNECT once its only child is terminal");
  });

  test("rejecting the child's gate routes to STAMP_REJECTED, never touches SEND_CR_CODE", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ "linkedin-id": "abc" }]);
    await executor.runFlow(record.flowId);
    const [childFlowId] = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "CONNECT", 1);
    await waitForStatus(runStore, childFlowId, (r) => r.status === "blocked");

    const childSettled1 = waitForTerminal((q) => q.flowId === childFlowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "abc", decision: "reject", source: "cli" });
    await childSettled1;

    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.ok(childState.STAMP_REJECTED);
    assert.equal(childState.SEND_CR_CODE, undefined);
  });

  test("two items: parent stays blocked until BOTH siblings are terminal, resolving out of order", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ "linkedin-id": "abc" }, { "linkedin-id": "def" }]);
    await executor.runFlow(record.flowId);

    const kids = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "CONNECT", 2);
    assert.equal(kids.length, 2);
    const byItem = {};
    for (const flowId of kids) {
      const c = await waitForStatus(runStore, flowId, (r) => r.status === "blocked");
      byItem[c.waitJson.items[0].itemId] = flowId;
    }

    await gateResolver.recordItemDecision({ flowId: byItem.def, itemId: "def", decision: "approve", source: "cli" });
    assert.equal((await runStore.get(record.flowId)).status, "blocked", "one sibling (abc) still pending — parent must not resume yet");

    await gateResolver.recordItemDecision({ flowId: byItem.abc, itemId: "abc", decision: "reject", source: "cli" });
    assert.equal((await waitForTerminalStatus(runStore, record.flowId)).status, "succeeded", "last sibling now terminal — parent must auto-resume");
  });

  test("SEND_CR_CODE mode=agent_required routes through SEND_CR_AGENT before STAMP_SENT", async () => {
    harness = await makeHarness({ sendMode: "agent_required" });
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ "linkedin-id": "abc" }]);
    await executor.runFlow(record.flowId);
    const [childFlowId] = await waitForChildFlowIds(nodeExecutionsStore, runStore, record.flowId, "CONNECT", 1);
    await waitForStatus(runStore, childFlowId, (r) => r.status === "blocked");

    const childSettled2 = waitForTerminal((q) => q.flowId === childFlowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "abc", decision: "approve", source: "cli" });
    await childSettled2;

    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.ok(childState.SEND_CR_AGENT?.ok);
    assert.ok(childState.STAMP_SENT);
  });

  test("state/subflow-state.js's getAllSubflowGroups reports the group as migrated, normalized like an old branch row", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ "linkedin-id": "abc" }]);
    // The child's runFlow is dispatched fire-and-forget, so a fixed sleep
    // and hoping it had reached its gate is unreliable: under CI load it
    // hadn't, and the assertion read an undefined waitJson.
    // gate.opened on any flow other than the
    // parent is exactly the condition being waited for.
    const childAtGate = waitForGateOpen((q) => q.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childAtGate;

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.CONNECT);
    const row = groups.CONNECT.abc;
    assert.ok(row, "keyed by branchKey, same shape old branchService rows used");
    assert.equal(row.status, "running", "blocked-on-gate child folds into 'running' to match the old model");
    assert.equal(row.awaiting?.requestPayload?.kind, "human-gate");
    assert.ok(row.flowId);
  });
});

// Seeds shortlist onto the just-created parent flow's stateJson before
// driving it — executor.start()'s args merge under initialState.args, not
// top-level state, so this writes directly via store.setRunning to land
// state.shortlist where CONNECT's selector reads it.
async function store_seedShortlist(harness, flowId, shortlist) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, shortlist },
  });
}
