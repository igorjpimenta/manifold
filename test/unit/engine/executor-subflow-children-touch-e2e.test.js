/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors freelance_workana-touch-cadence's
 * TOUCH (DRAFT_NUDGE → GATE → SEND_NUDGE → STAMP_TOUCH / STAMP_TOUCH_
 * REJECT), the simplest of the three pipelines migrated so far — no
 * loop-back, exactly one human-gate node. Every "code"/"agent" sub-node
 * is a trivial no-op stand-in — zero real filesystem, subprocess, or
 * agent dispatch I/O anywhere in this fixture. GATE's pendingSelector/
 * edge logic is NOT stubbed away — it uses the real plain-item-id shape
 * TOUCH's own GATE uses (no round-scoping helper needed here, unlike
 * PROPOSE's GAP_GATE). SEND_NUDGE was a real kind:"fallback" node here
 * until an earlier rewrite retired that mechanism entirely — now a
 * plain "code" stand-in like every other
 * node in this fixture; the real cascade-migration coverage lives in
 * workspace/.pipelines/test/unit/freelance_workana-touch-cadence/nodes/
 * touch/send-nudge.test.js.
 *
 * Isolated-sandbox discipline (per the incident writeup):
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForGateOpen, waitForTerminal, waitForStatus } from "../../helpers/wait-for-event.js";
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

function makeTouchLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "jobId",
    selector: (state) => state.candidates ?? [],
    flow: {
      nodes: {
        DRAFT_NUDGE: {
          kind: "code",
          run: (state) => ({ DRAFT_NUDGE: { text: "draft text", draftPath: "/fake/nudge.md" } }),
        },
        GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const id = state.item?.jobId;
            if (!id) return [];
            if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
            return [id];
          },
          approvalRequestBuilder: (state) => ({ title: `Touch nudge for ${state.item?.jobId}?`, description: "", layout: [] }),
        },
        SEND_NUDGE: {
          kind: "code",
          run: (state) => ({ SEND_NUDGE: { mode: "ok", ok: true, replyPath: "/fake/sent.md" } }),
        },
        STAMP_TOUCH: {
          kind: "code",
          run: (state) => ({ STAMP_TOUCH: { stamped: state.SEND_NUDGE?.mode === "ok", touchCount: 2 } }),
        },
        STAMP_TOUCH_REJECT: {
          kind: "code",
          run: (state) => ({ STAMP_TOUCH_REJECT: { rejectedAt: "t", _skipped: true } }),
        },
      },
      edges: {
        __start__: "DRAFT_NUDGE",
        DRAFT_NUDGE: (state) => state.DRAFT_NUDGE?._error ? "__end__" : "GATE",
        GATE: (state) => state.approvedIds?.includes(state.item?.jobId) ? "SEND_NUDGE" : "STAMP_TOUCH_REJECT",
        SEND_NUDGE: () => "STAMP_TOUCH",
        STAMP_TOUCH: () => "__end__",
        STAMP_TOUCH_REJECT: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("touch-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("touch-e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const TOUCH_NODE = makeTouchLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { TOUCH: TOUCH_NODE },
    edges: { __start__: "TOUCH", TOUCH: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::TOUCH",
    state: {},
    nodes: TOUCH_NODE.flow.nodes,
    edges: TOUCH_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::TOUCH", childPipeline]]) };

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
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "TOUCH");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

// Raised above the sibling e2e files' tighter default — a full-suite
// concurrent run showed the tighter margin flakes under load.
function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedCandidates(harness, flowId, candidates) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, candidates },
  });
}

describe("childFlows:true TOUCH-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("approve: child blocks at GATE with the plain item id, approve drives it through SEND_NUDGE/STAMP_TOUCH to terminal, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ jobId: "job-1", nextTouchNumber: 2 }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked");
    assert.equal(child.waitJson.kind, "human-gate");
    assert.equal(child.waitJson.items[0].itemId, "job-1", "must block at GATE with the plain item id — TOUCH has no round-scoping");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-1", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.SEND_NUDGE?.mode, "ok");
    assert.equal(settledState.STAMP_TOUCH?.stamped, true);
    assert.equal(settledState.STAMP_TOUCH_REJECT, undefined);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("reject at GATE — STAMP_TOUCH_REJECT reached, SEND_NUDGE/STAMP_TOUCH never dispatched", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ jobId: "job-2", nextTouchNumber: 1 }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-2", decision: "reject", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.ok(settledState.STAMP_TOUCH_REJECT);
    assert.equal(settledState.SEND_NUDGE, undefined);
    assert.equal(settledState.STAMP_TOUCH, undefined);
  });

  test("state/subflow-state.js's getAllSubflowGroups resolves row.data.SEND_NUDGE/STAMP_TOUCH the same way it resolves any other node's data", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ jobId: "job-4", nextTouchNumber: 3 }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-4", decision: "approve", source: "cli" });
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.TOUCH);
    const row = groups.TOUCH["job-4"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.SEND_NUDGE?.mode, "ok");
    assert.equal(row.data.STAMP_TOUCH?.stamped, true);
  });
});
