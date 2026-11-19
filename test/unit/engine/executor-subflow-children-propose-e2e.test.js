/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors freelance_workana-proposal's PROPOSE
 * (DEDUP_CHECK → DRAFT → [GAP_GATE ↔ SETTLE_GAP_ANSWER ↔ DRAFT]* → GATE →
 * STAMP/STAMP_REJECT → SUBMIT), with every sub-node replaced by a trivial
 * no-op stand-in — zero real filesystem, subprocess, or agent dispatch I/O
 * anywhere in this fixture. GAP_GATE and GATE's pendingSelector/edge logic
 * are NOT stubbed — they use the real gapGateItemId helper, since proving
 * the round-scoping collision fix actually works end-to-end (not just as
 * a pure-function unit test) is the whole point of this file. This proves
 * the loop-back → real second invocation-worth-of-executions →
 * GAP_GATE-then-GATE-both-present → parent-resume plumbing works for
 * PROPOSE's exact edge shape, before any real draft/submit script or
 * agent is ever involved (that fidelity is covered by the real node
 * functions' own mocked-I/O unit tests under workspace/.pipelines/
 * test/unit/freelance_workana-proposal/nodes/propose/).
 *
 * SUBMIT was a real kind:"fallback" node (code-then-agent, two entries)
 * here until an earlier rewrite retired that mechanism entirely — now a
 * plain single "code" stand-in like every other node in this fixture;
 * the real cascade-migration coverage lives
 * in workspace/.pipelines/test/unit/freelance_workana-proposal/nodes/
 * propose/submit/.
 *
 * Isolated-sandbox discipline (per the incident writeup):
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { waitForGateOpen, waitForTerminal, waitForStatus } from "../../helpers/wait-for-event.js";
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

// Real round-scoping logic (same shape as
// workspace/.pipelines/freelance_workana-proposal/nodes/propose/gap-gate-id.js)
// — deliberately reimplemented here rather than imported, since this test
// lives in a different package (extensions/manifold) than the pipeline
// file. Kept byte-for-byte equivalent so the mechanics this test proves
// actually match what the real pipeline does.
function gapGateItemId(state) {
  const listingId = state.item?.id;
  if (!listingId) return null;
  const round = state.DRAFT?._gapRounds ?? 1;
  return `${listingId}:gap:${round}`;
}

function makeProposeLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "id",
    selector: (state) => state.shortlist ?? [],
    flow: {
      nodes: {
        DEDUP_CHECK: { kind: "code", run: (state) => ({ DEDUP_CHECK: { alreadySent: false } }) },
        DRAFT: {
          kind: "code",
          run: (state) => {
            const priorRounds = state.DRAFT?._gapRounds ?? 0;
            const gapRoundsNeeded = state.item?.gapRoundsNeeded ?? 0;
            if (priorRounds < gapRoundsNeeded) {
              return { DRAFT: { gapQuestions: [`Q${priorRounds + 1}?`], _gapRounds: priorRounds + 1 } };
            }
            return { DRAFT: { text: "draft text", draftPath: "/fake/draft.md" } };
          },
        },
        GAP_GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const id = gapGateItemId(state);
            if (!id) return [];
            if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
            return [id];
          },
          approvalRequestBuilder: (state) => ({ title: `gap round ${state.DRAFT?._gapRounds}`, description: "", layout: [] }),
        },
        SETTLE_GAP_ANSWER: { kind: "code", run: (state) => ({ SETTLE_GAP_ANSWER: {} }) },
        GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const id = state.item?.id;
            if (!id) return [];
            if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
            return [id];
          },
          approvalRequestBuilder: (state) => ({ title: `Approve ${state.item?.id}?`, description: "", layout: [] }),
        },
        STAMP_REJECT: { kind: "code", run: (state) => ({ STAMP_REJECT: {} }) },
        STAMP: { kind: "code", run: (state) => ({ STAMP: { token: "t" } }) },
        SUBMIT: {
          kind: "code",
          run: (state) => ({ SUBMIT: { submitMode: "ok", ok: true } }),
        },
      },
      edges: {
        __start__: "DEDUP_CHECK",
        DEDUP_CHECK: (state) => state.DEDUP_CHECK?.alreadySent ? "__end__" : "DRAFT",
        DRAFT: (state) => state.DRAFT?.gapQuestions ? "GAP_GATE" : (state.DRAFT?._error ? "__end__" : "GATE"),
        GAP_GATE: () => "SETTLE_GAP_ANSWER",
        SETTLE_GAP_ANSWER: () => "DRAFT",
        GATE: (state) => state.approvedIds?.includes(state.item?.id) ? "STAMP" : "STAMP_REJECT",
        STAMP_REJECT: () => "__end__",
        STAMP: (state) => state.STAMP?._error ? "__end__" : "SUBMIT",
        SUBMIT: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const dbPath = tmpPath("propose-e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const PROPOSE_NODE = makeProposeLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { PROPOSE: PROPOSE_NODE },
    edges: { __start__: "PROPOSE", PROPOSE: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::PROPOSE",
    state: {},
    nodes: PROPOSE_NODE.flow.nodes,
    edges: PROPOSE_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::PROPOSE", childPipeline]]) };

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
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "PROPOSE");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

// Retained only for waits with no corresponding emitter. Anything waiting on a
// flow reaching a state now uses waitForGateOpen/waitForTerminal instead — a
// fixed sleep is both too long on a fast machine and too short on a loaded
// runner, which is why this constant kept getting bumped and flaked anyway.
function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedShortlist(harness, flowId, shortlist) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, shortlist },
  });
}

describe("childFlows:true PROPOSE-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("no gap needed: child blocks at GATE (not GAP_GATE), approve drives it to terminal, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ id: "job-1", gapRoundsNeeded: 0 }]);
    const childBlocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childBlocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked");
    assert.equal(child.waitJson.kind, "human-gate");
    assert.equal(child.waitJson.items[0].itemId, "job-1", "must block at GATE with the plain item id, not a gap-scoped one");

    const settledWait = waitForTerminal((p) => p.flowId === childFlowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-1", decision: "approve", source: "cli" });
    await settledWait;

    const settled = await runStore.get(childFlowId);
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.STAMP?.token, "t");
    assert.equal(settledState.SUBMIT?.ok, true);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("one gap round: child blocks at GAP_GATE first, then at the REAL GATE after the loop-back — the collision-fix regression", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ id: "job-2", gapRoundsNeeded: 1 }]);
    const childBlocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childBlocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    let child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked");
    assert.equal(child.waitJson.items[0].itemId, "job-2:gap:1", "must block at GAP_GATE with the round-scoped id");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-2:gap:1", decision: "approve", editedText: "the answer", source: "cli" });
    await waitForStatus(runStore, childFlowId,
      (r) => r.status === "blocked" && r.waitJson?.items?.[0]?.itemId === "job-2");

    // Loop-back completed: DRAFT re-ran with priorRounds=1 >= gapRoundsNeeded=1,
    // so it now proceeds to the REAL GATE — must actually present, not be
    // silently auto-skipped because "job-2" was mistaken for already-decided.
    child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked", "child must still be blocked — now at the real GATE, not terminal");
    assert.equal(child.waitJson.items[0].itemId, "job-2", "GATE must present with the plain item id, unaffected by GAP_GATE's round-1 approval");

    const settledWait = waitForTerminal((p) => p.flowId === childFlowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-2", decision: "approve", source: "cli" });
    await settledWait;

    const settled = await runStore.get(childFlowId);
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.STAMP?.token, "t");
  });

  test("two gap rounds: each round's decision is distinct and non-colliding", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ id: "job-3", gapRoundsNeeded: 2 }]);
    const childBlocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childBlocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    assert.equal((await runStore.get(childFlowId)).waitJson.items[0].itemId, "job-3:gap:1");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-3:gap:1", decision: "approve", editedText: "answer 1", source: "cli" });
    await waitForStatus(runStore, childFlowId,
      (r) => r.status === "blocked" && r.waitJson?.items?.[0]?.itemId === "job-3:gap:2");

    const afterRound1 = await runStore.get(childFlowId);
    assert.equal(afterRound1.status, "blocked");
    assert.equal(afterRound1.waitJson.items[0].itemId, "job-3:gap:2", "round 2 must present its own distinct round-scoped id");
    assert.deepEqual(afterRound1.stateJson.approvedIds, ["job-3:gap:1"], "round 1's id is the only one decided so far");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-3:gap:2", decision: "approve", editedText: "answer 2", source: "cli" });
    await waitForStatus(runStore, childFlowId,
      (r) => r.status === "blocked" && r.waitJson?.items?.[0]?.itemId === "job-3");

    const afterRound2 = await runStore.get(childFlowId);
    assert.equal(afterRound2.status, "blocked");
    assert.equal(afterRound2.waitJson.items[0].itemId, "job-3", "now the real GATE, unaffected by either gap round");
    assert.deepEqual([...afterRound2.stateJson.approvedIds].sort(), ["job-3:gap:1", "job-3:gap:2"].sort());
  });

  test("reject at GATE — STAMP_REJECT reached, SUBMIT never dispatched", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ id: "job-4", gapRoundsNeeded: 0 }]);
    const childBlocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childBlocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const settledWait = waitForTerminal((p) => p.flowId === childFlowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-4", decision: "reject", source: "cli" });
    await settledWait;

    const settled = await runStore.get(childFlowId);
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.ok(settledState.STAMP_REJECT);
    assert.equal(settledState.SUBMIT, undefined);
  });

  test("state/subflow-state.js's getAllSubflowGroups resolves row.data.SUBMIT the same way it resolves any other node's data", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedShortlist(harness, record.flowId, [{ id: "job-6", gapRoundsNeeded: 0 }]);
    const childBlocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childBlocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-6", decision: "approve", source: "cli" });
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.PROPOSE);
    const row = groups.PROPOSE["job-6"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.SUBMIT?.ok, true);
    assert.equal(row.data.STAMP?.token, "t");
  });
});
