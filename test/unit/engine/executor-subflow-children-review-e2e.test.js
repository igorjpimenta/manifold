/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors system_memory-synthesis's REVIEW
 * (GATE → APPLY → __end__, GATE's edge unconditional — APPLY itself reads
 * the decision). Second-simplest gate-bearing topology after TOUCH's: one
 * human-gate node, no loop-back, no fallback. GATE/APPLY are trivial
 * no-op stand-ins — zero real gbrain/filesystem I/O anywhere in this
 * fixture (that fidelity is covered by the real node functions' own
 * mocked-I/O unit tests under workspace/.pipelines/test/unit/
 * system_memory-synthesis/nodes/review/).
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

function makeReviewLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "id",
    selector: (state) => state.proposals ?? [],
    flow: {
      nodes: {
        GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const id = state.item?.id;
            if (!id) return [];
            if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
            return [id];
          },
          approvalRequestBuilder: (state) => ({ title: `Review ${state.item?.id}?`, description: "", layout: [] }),
        },
        APPLY: {
          kind: "code",
          run: (state) => {
            const proposal = state.item;
            const decided = state.approvedIds?.includes(proposal.id);
            if (!decided) return { APPLY: { applied: false } };
            const editedBody = state.editedTexts?.[proposal.id] ?? null;
            return { APPLY: { applied: true, slug: proposal.id, body: editedBody ?? proposal.body } };
          },
        },
      },
      edges: {
        __start__: "GATE",
        GATE: () => "APPLY",
        APPLY: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("review-e2e-runs"), legacyDbPath: tmpPath("review-e2e-runs-legacy") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("review-e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const REVIEW_NODE = makeReviewLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { REVIEW: REVIEW_NODE },
    edges: { __start__: "REVIEW", REVIEW: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::REVIEW",
    state: {},
    nodes: REVIEW_NODE.flow.nodes,
    edges: REVIEW_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::REVIEW", childPipeline]]) };

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

  return { runStore, nodeExecutionsStore, store, executor, gateResolver };
}

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "REVIEW");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

// 60ms, not 20ms — matches executor-loop-children-reply-e2e.test.js's own
// bump: a full-suite concurrent run showed the tighter margin flakes under
// load (a real, if rare, timing flake — not a fixture bug).
function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedProposals(harness, flowId, proposals) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, proposals },
  });
}

describe("childFlows:true REVIEW-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("approve: child blocks at GATE with the plain item id, approve drives it through APPLY to terminal, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProposals(harness, record.flowId, [{ id: "slug-1", body: "Original body." }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked");
    assert.equal(child.waitJson.kind, "human-gate");
    assert.equal(child.waitJson.items[0].itemId, "slug-1");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "slug-1", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.APPLY?.applied, true);
    assert.equal(settledState.APPLY?.slug, "slug-1");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("reject at GATE — APPLY still runs (unconditional edge), but reports applied:false", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProposals(harness, record.flowId, [{ id: "slug-2", body: "Original body." }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "slug-2", decision: "reject", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded", "REVIEW's GATE edge is unconditional — APPLY runs either way");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.deepEqual(settledState.APPLY, { applied: false });
  });

  test("approve with edited text — APPLY observes state.editedTexts[item.id]", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProposals(harness, record.flowId, [{ id: "slug-3", body: "Original body." }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({
      flowId: childFlowId, itemId: "slug-3", decision: "approve",
      editedText: "The operator's edited replacement.", source: "cli",
    });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.APPLY?.applied, true);
    assert.equal(settledState.APPLY?.body, "The operator's edited replacement.");
  });

  test("state/subflow-state.js's getAllSubflowGroups resolves row.data.APPLY the same way it resolves any other node's data", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProposals(harness, record.flowId, [{ id: "slug-4", body: "Original body." }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "slug-4", decision: "approve", source: "cli" });
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.REVIEW);
    const row = groups.REVIEW["slug-4"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.APPLY?.applied, true);
  });
});
