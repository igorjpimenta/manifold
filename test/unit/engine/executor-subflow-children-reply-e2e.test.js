/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors freelance_workana-reply's REPLY
 * (FETCH_THREAD → DRAFT_REPLY → GATE → SEND_REPLY → VERIFY_SEND /
 * STAMP_REJECT), the largest branch topology migrated so far — one
 * human-gate, and a node (ENSURE_INFRA) reached via a normal EDGE-LEVEL
 * LOOP-BACK (SEND_REPLY → ENSURE_INFRA → SEND_REPLY). Every sub-node is
 * a trivial no-op stand-in — zero real filesystem, subprocess, or
 * agent dispatch I/O anywhere in this fixture.
 *
 * ENSURE_INFRA was a real kind:"fallback" node here until an earlier
 * rewrite retired that mechanism entirely — now a plain "code" stand-in
 * like every other node in this fixture; the real cascade-migration
 * coverage lives in
 * workspace/.pipelines/test/unit/freelance_workana-reply/nodes/reply/
 * ensure-infra.test.js.
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

const SEND_INFRA_MAX_RETRIES = 5;

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

function makeReplyLikeNode({ sendFailsInfraPersistently = false } = {}) {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "jobId",
    selector: (state) => state.needsReply ?? [],
    flow: {
      nodes: {
        FETCH_THREAD: {
          kind: "code",
          run: (state) => ({ FETCH_THREAD: { ok: true, messages: [] } }),
        },
        DRAFT_REPLY: {
          kind: "code",
          run: (state) => ({ DRAFT_REPLY: { text: "draft text", draftPath: "/fake/reply.md" } }),
        },
        GATE: {
          kind: "human-gate",
          pendingSelector: (state) => {
            const id = state.item?.jobId;
            if (!id) return [];
            if (state.approvedIds?.includes(id) || state.rejectedIds?.includes(id)) return [];
            return [id];
          },
          approvalRequestBuilder: (state) => ({ title: `Reply to ${state.item?.jobId}`, description: "", layout: [] }),
        },
        SEND_REPLY: {
          kind: "code",
          run: (state) => {
            const retries = state.SEND_REPLY?._infraRetries ?? 0;
            const stillFailing = sendFailsInfraPersistently || retries < 2;
            if (stillFailing) {
              const nextInfraRetries = retries + 1;
              if (nextInfraRetries < SEND_INFRA_MAX_RETRIES) {
                return { SEND_REPLY: { mode: "blocked", blockedReason: "infra-unavailable: CDP dead", _infraRetries: nextInfraRetries } };
              }
              return { SEND_REPLY: { mode: "blocked", blockedReason: "infra-unavailable: CDP dead", _infraRetries: nextInfraRetries, _error: `gave up after ${nextInfraRetries} infra retries` } };
            }
            return { SEND_REPLY: { mode: "ok", ok: true } };
          },
        },
        ENSURE_INFRA: {
          kind: "code",
          run: (state) => ({ ENSURE_INFRA: { loginMode: "ok" } }),
        },
        VERIFY_SEND: {
          kind: "code",
          run: (state) => ({ VERIFY_SEND: { mode: "passthrough" } }),
        },
        STAMP_REJECT: {
          kind: "code",
          run: (state) => ({ STAMP_REJECT: { stamped: true, _skipped: true } }),
        },
      },
      edges: {
        __start__: "FETCH_THREAD",
        FETCH_THREAD: () => "DRAFT_REPLY",
        DRAFT_REPLY: (state) => state.DRAFT_REPLY?._error ? "__end__" : "GATE",
        GATE: (state) => state.approvedIds?.includes(state.item?.jobId) ? "SEND_REPLY" : "STAMP_REJECT",
        SEND_REPLY: (state) => {
          const r = state.SEND_REPLY;
          if (r?.mode === "blocked" && /^infra-unavailable/.test(r.blockedReason ?? "") && (r._infraRetries ?? 0) < SEND_INFRA_MAX_RETRIES) {
            return "ENSURE_INFRA";
          }
          return "VERIFY_SEND";
        },
        ENSURE_INFRA: () => "SEND_REPLY", // the loop-back under test
        VERIFY_SEND: () => "__end__",
        STAMP_REJECT: () => "__end__",
      },
    },
  };
}

async function makeHarness({ sendFailsInfraPersistently = false } = {}) {
  const runStore = await createRunStore({ dbPath: tmpPath("reply-e2e-runs"), legacyDbPath: tmpPath("reply-e2e-runs-legacy") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("reply-e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const REPLY_NODE = makeReplyLikeNode({ sendFailsInfraPersistently });
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { REPLY: REPLY_NODE },
    edges: { __start__: "REPLY", REPLY: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::REPLY",
    state: {},
    nodes: REPLY_NODE.flow.nodes,
    edges: REPLY_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::REPLY", childPipeline]]) };

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
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "REPLY");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

// Default bumped above the sibling e2e files' 20-30ms — REPLY's branch has
// one more hop before GATE (FETCH_THREAD → DRAFT_REPLY → GATE) than
// TOUCH's template, and a full-suite concurrent run showed this margin
// mattered under load (a real, if rare, flake — not a fixture bug).
function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedNeedsReply(harness, flowId, needsReply) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, needsReply },
  });
}

describe("childFlows:true REPLY-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("approve, no infra failure — child blocks at GATE with the plain jobId, approve drives it through SEND_REPLY/VERIFY_SEND to succeeded, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedNeedsReply(harness, record.flowId, [{ jobId: "job-1", inboundText: "hi" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked");
    assert.equal(child.waitJson.kind, "human-gate");
    assert.equal(child.waitJson.items[0].itemId, "job-1", "must block at GATE with the plain item id — REPLY has no round-scoping");

    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-1", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.SEND_REPLY?.mode, "ok");
    assert.equal(settledState.VERIFY_SEND?.mode, "passthrough");
    assert.equal(settledState.STAMP_REJECT, undefined);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("reject at GATE — STAMP_REJECT reached, SEND_REPLY/VERIFY_SEND never dispatched", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedNeedsReply(harness, record.flowId, [{ jobId: "job-2", inboundText: "hi" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-2", decision: "reject", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.ok(settledState.STAMP_REJECT);
    assert.equal(settledState.SEND_REPLY, undefined);
    assert.equal(settledState.VERIFY_SEND, undefined);
  });

  test("SEND_REPLY fails infra-unavailable twice, then succeeds — ENSURE_INFRA dispatches and loops back correctly each time, _infraRetries increments across the loop", async () => {
    harness = await makeHarness();
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedNeedsReply(harness, record.flowId, [{ jobId: "job-3", inboundText: "hi" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-3", decision: "approve", source: "cli" });
    const settled = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(settled.status, "succeeded", "must recover after 2 real ENSURE_INFRA dispatches and reach a terminal state, not hang or fail");
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    assert.equal(settledState.SEND_REPLY?.mode, "ok");
    assert.equal(settledState.VERIFY_SEND?.mode, "passthrough");
  });

  test("infra-unavailable persisting past the retry cap — the loop terminates (routes past ENSURE_INFRA to VERIFY_SEND) rather than looping indefinitely, carrying the escalation _error as a failure-surface marker", async () => {
    harness = await makeHarness({ sendFailsInfraPersistently: true });
    const { executor, runStore, gateResolver, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedNeedsReply(harness, record.flowId, [{ jobId: "job-4", inboundText: "hi" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-4", decision: "approve", source: "cli" });
    await flush(150);

    const settled = await runStore.get(childFlowId);
    const settledState = await assembleState(settled.flowId, { runStore, nodeExecutionsStore, baseState: settled.stateJson });
    // The cap only stops the ENSURE_INFRA loop-back — a returned _error
    // field is a failure-surface marker (read by the dashboard/failures
    // queue via flow_node_executions), not something the engine itself
    // interprets to fail the flow. Reaching a real terminal status here
    // (not still "running"/"blocked") is the actual proof the loop
    // terminates instead of retrying forever.
    assert.equal(settledState.SEND_REPLY?._infraRetries, SEND_INFRA_MAX_RETRIES);
    assert.match(settledState.SEND_REPLY?._error ?? "", /gave up after/);
    assert.equal(settled.status, "succeeded", "the cap must stop the ENSURE_INFRA loop-back and let the edge fall through to VERIFY_SEND — not hang");
    assert.equal(settledState.VERIFY_SEND?.mode, "passthrough");
  });

  test("state/subflow-state.js's getAllSubflowGroups resolves row.data.SEND_REPLY/STAMP_REJECT the same way it resolves any other node's data", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, gateResolver } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedNeedsReply(harness, record.flowId, [{ jobId: "job-5", inboundText: "hi" }]);
    const _blocked = waitForGateOpen((p) => p.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await _blocked;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    await gateResolver.recordItemDecision({ flowId: childFlowId, itemId: "job-5", decision: "approve", source: "cli" });
    await flush();

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.REPLY);
    const row = groups.REPLY["job-5"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.SEND_REPLY?.mode, "ok");
    assert.equal(row.data.VERIFY_SEND?.mode, "passthrough");
  });
});
