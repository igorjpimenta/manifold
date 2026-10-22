/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors nyxen_linkedin-touch's TOUCH
 * (DRAFT_TOUCH → GATE → STAMP_APPROVED/STAMP_REJECTED → ROUTE_CHANNEL →
 * SEND_LINKEDIN_DM_CODE/(_AGENT)/SEND_EMAIL → STAMP_SENT). One
 * human-gate, no fallback node, no loop-back — the simplest topology in
 * this whole migration series in terms of engine novelty, but the
 * largest branch node COUNT (9), and the only one with a 3-way channel
 * fan-out after the gate. Named "touch2" to avoid colliding with the
 * existing executor-loop-children-touch-e2e.test.js, which covers the
 * unrelated freelance_workana-touch-cadence pipeline's own TOUCH loop.
 *
 * Every sub-node is a trivial no-op stand-in — zero real filesystem,
 * subprocess, or agent dispatch I/O anywhere in this fixture.
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
    itemKey: "linkedin-id",
    selector: (state) => state.candidates ?? [],
    flow: {
      nodes: {
        DRAFT_TOUCH: {
          kind: "code",
          run: (state) => ({ DRAFT_TOUCH: { text: "draft text", draftPath: "/fake/touch.md" } }),
        },
        GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const id = state.item?.["linkedin-id"];
            if (!id) return [];
            if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
            return [id];
          },
          approvalRequestBuilder: (state) => ({ title: `Touch for ${state.item?.["linkedin-id"]}`, description: "", layout: [] }),
        },
        STAMP_APPROVED: {
          kind: "code",
          run: (state) => ({ STAMP_APPROVED: { stampedAt: "t", approvalToken: "tok" } }),
        },
        STAMP_REJECTED: {
          kind: "code",
          run: (state) => ({ STAMP_REJECTED: { rejectedAt: "t" } }),
        },
        ROUTE_CHANNEL: {
          kind: "code",
          run: (state) => ({ ROUTE_CHANNEL: { channel: state.item?.channel ?? "linkedin", reason: "test" } }),
        },
        SEND_LINKEDIN_DM_CODE: {
          kind: "code",
          run: (state) => {
            if (state.item?.dmMode === "agent_required") {
              return { SEND_LINKEDIN_DM_CODE: { mode: "agent_required", reason: "dom-regression" } };
            }
            return { SEND_LINKEDIN_DM_CODE: { mode: "ok", sentAt: "t" } };
          },
        },
        SEND_LINKEDIN_DM_AGENT: {
          kind: "code",
          run: (state) => ({ SEND_LINKEDIN_DM_AGENT: { ok: true, sentAt: "t" } }),
        },
        SEND_EMAIL: {
          kind: "code",
          run: (state) => ({ SEND_EMAIL: { mode: "manual-pending", sentAt: "t" } }),
        },
        STAMP_SENT: {
          kind: "code",
          run: (state) => ({ STAMP_SENT: { stampedAt: "t", newStage: "touched-once" } }),
        },
      },
      edges: {
        __start__: "DRAFT_TOUCH",
        DRAFT_TOUCH: (state) => state.DRAFT_TOUCH?._error ? "__end__" : "GATE",
        GATE: (state) => state.approvedIds?.includes(state.item?.["linkedin-id"]) ? "STAMP_APPROVED" : "STAMP_REJECTED",
        STAMP_APPROVED: () => "ROUTE_CHANNEL",
        STAMP_REJECTED: () => "__end__",
        ROUTE_CHANNEL: (state) => {
          const channel = state.ROUTE_CHANNEL.channel;
          if (channel === "linkedin") return "SEND_LINKEDIN_DM_CODE";
          if (channel === "email") return "SEND_EMAIL";
          return "__end__";
        },
        SEND_LINKEDIN_DM_CODE: (state) => {
          const mode = state.SEND_LINKEDIN_DM_CODE.mode;
          if (mode === "agent_required") return "SEND_LINKEDIN_DM_AGENT";
          if (mode === "ok") return "STAMP_SENT";
          return "__end__";
        },
        SEND_LINKEDIN_DM_AGENT: (state) => (state.SEND_LINKEDIN_DM_AGENT.ok ? "STAMP_SENT" : "__end__"),
        SEND_EMAIL: (state) => ["ok", "manual-pending"].includes(state.SEND_EMAIL.mode) ? "STAMP_SENT" : "__end__",
        STAMP_SENT: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("touch2-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("touch2-e2e-execs"));
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
    getWorkingSubagent: null,
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

function flush(ms = 40) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedCandidates(harness, flowId, candidates) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, candidates },
  });
}

describe("childFlows:true TOUCH-shaped loop (nyxen_linkedin-touch), full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("approve, linkedin channel, code send ok — child blocks at GATE with the plain linkedin-id, approve drives it through ROUTE_CHANNEL/SEND_LINKEDIN_DM_CODE/STAMP_SENT to succeeded", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ "linkedin-id": "p-1", channel: "linkedin" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked");
    assert.equal(child.waitJson.kind, "human-gate");
    assert.equal(child.waitJson.items[0].itemId, "p-1", "must block at GATE with the plain item id — TOUCH has no round-scoping");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "p-1", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.STAMP_APPROVED?.stampedAt, "t");
    assert.equal(settledState.SEND_LINKEDIN_DM_CODE?.mode, "ok");
    assert.equal(settledState.STAMP_SENT?.stampedAt, "t");
    assert.equal(settledState.STAMP_REJECTED, undefined);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("approve, linkedin channel, code returns agent_required — SEND_LINKEDIN_DM_AGENT dispatches, then STAMP_SENT", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ "linkedin-id": "p-2", channel: "linkedin", dmMode: "agent_required" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "p-2", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.SEND_LINKEDIN_DM_CODE?.mode, "agent_required");
    assert.equal(settledState.SEND_LINKEDIN_DM_AGENT?.ok, true);
    assert.equal(settledState.STAMP_SENT?.stampedAt, "t");
  });

  test("approve, email channel — routes to SEND_EMAIL, then STAMP_SENT (SEND_LINKEDIN_DM_CODE/AGENT never dispatch)", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ "linkedin-id": "p-3", channel: "email" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "p-3", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.SEND_EMAIL?.mode, "manual-pending");
    assert.equal(settledState.STAMP_SENT?.stampedAt, "t");
    assert.equal(settledState.SEND_LINKEDIN_DM_CODE, undefined);
    assert.equal(settledState.SEND_LINKEDIN_DM_AGENT, undefined);
  });

  test("reject at GATE — STAMP_REJECTED reached, ROUTE_CHANNEL/SEND_*/STAMP_SENT never dispatched", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ "linkedin-id": "p-4", channel: "linkedin" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "p-4", decision: "reject", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.ok(settledState.STAMP_REJECTED);
    assert.equal(settledState.STAMP_APPROVED, undefined);
    assert.equal(settledState.ROUTE_CHANNEL, undefined);
    assert.equal(settledState.STAMP_SENT, undefined);
  });

  test("state/subflow-state.js's getAllSubflowGroups resolves row.data.STAMP_APPROVED/STAMP_SENT the same way it resolves any other node's data", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedCandidates(harness, record.flowId, [{ "linkedin-id": "p-5", channel: "linkedin" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "p-5", decision: "approve", source: "cli" });
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.TOUCH);
    const row = groups.TOUCH["p-5"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.STAMP_APPROVED?.stampedAt, "t");
    assert.equal(row.data.STAMP_SENT?.stampedAt, "t");
  });
});
