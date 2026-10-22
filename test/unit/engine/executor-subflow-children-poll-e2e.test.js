/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors freelance_workana-poll-inbox's POLL
 * (PERSIST_UPDATE → (INVOKE_REPLY | __end__) → (STAMP_NOTIFIED | __end__)),
 * no gate, no fallback. Every sub-node is a trivial kind:"code" stand-in.
 *
 * THE LOAD-BEARING CASE this file exists to prove: INVOKE_REPLY is the
 * first migrated node in this whole effort that uses the engine's real
 * kind:"code" 2-arg contract, run(state, ctx) — ctx.invokePipeline fires a
 * real second flow. Tier 1's unit test only proves INVOKE_REPLY's own
 * logic against a HAND-BUILT STUB ctx; it cannot catch a regression in the
 * real dispatchLoopChildren → runFlow → nodeCtx wiring itself. So this
 * file's stand-in INVOKE_REPLY node calls the REAL ctx.invokePipeline
 * against a real, trivial no-op target pipeline registered directly in
 * this harness's own pipelinesRef Map — proving a genuine childFlows
 * child gets the exact same nodeCtx a root-level node would.
 *
 * Isolated-sandbox discipline (per the incident writeup):
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForTerminal } from "../../helpers/wait-for-event.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
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

function makePollLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "jobId",
    selector: (state) => state.probeResults ?? [],
    flow: {
      nodes: {
        PERSIST_UPDATE: {
          kind: "code",
          run: (state) => {
            const item = state.item;
            if (item.probeError) return { PERSIST_UPDATE: { _error: "probe error", persisted: false, needsReply: false } };
            return { PERSIST_UPDATE: { persisted: true, needsReply: !!item.needsReply } };
          },
        },
        INVOKE_REPLY: {
          kind: "code",
          run: async (state, ctx) => {
            const item = state.item;
            let result;
            try {
              result = await ctx.invokePipeline(item.replyTarget ?? "test_reply_target", { needsReply: [{ jobId: item.jobId }] });
            } catch (err) {
              return { INVOKE_REPLY: { invoked: false, reason: `invokePipeline threw: ${err?.message ?? err}`, _error: err?.message ?? String(err) } };
            }
            if (!result?.invoked) return { INVOKE_REPLY: { invoked: false, reason: result?.reason ?? "not invoked" } };
            return { INVOKE_REPLY: { invoked: true, flowId: result.flowId, target: result.target } };
          },
        },
        STAMP_NOTIFIED: {
          kind: "code",
          run: () => ({ STAMP_NOTIFIED: { stamped: true } }),
        },
      },
      edges: {
        __start__: "PERSIST_UPDATE",
        PERSIST_UPDATE: (state) => {
          if (state.PERSIST_UPDATE?._error) return "__end__";
          return state.PERSIST_UPDATE?.needsReply ? "INVOKE_REPLY" : "__end__";
        },
        INVOKE_REPLY: (state) => state.INVOKE_REPLY?.invoked ? "STAMP_NOTIFIED" : "__end__",
        STAMP_NOTIFIED: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("poll-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("poll-e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const POLL_NODE = makePollLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { POLL: POLL_NODE },
    edges: { __start__: "POLL", POLL: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::POLL",
    state: {},
    nodes: POLL_NODE.flow.nodes,
    edges: POLL_NODE.flow.edges,
  };
  // Real, trivial no-op target pipeline for ctx.invokePipeline to fire
  // against — the whole point of this E2E is proving a childFlows child
  // gets a REAL, working nodeCtx, not a stubbed one.
  const replyTargetPipeline = {
    id: "test_reply_target",
    state: {},
    nodes: { NOOP: { kind: "code", run: () => ({ NOOP: { ran: true } }) } },
    edges: { __start__: "NOOP", NOOP: "__end__" },
  };
  const pipelinesRef = {
    value: new Map([
      ["test_parent", parentPipeline],
      ["test_parent::POLL", childPipeline],
      ["test_reply_target", replyTargetPipeline],
    ]),
  };

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
    admissionQueueStore: makeAdmissionQueueStore(),
  });
  resumeParentRef.resume = executor.resume;

  return { runStore, nodeExecutionsStore, store, executor, pipelinesRef };
}

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "POLL");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

function flush(ms = 30) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function store_seedProbeResults(harness, flowId, probeResults) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, probeResults },
  });
}

describe("childFlows:true POLL-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("needsReply:false — routes straight to __end__, INVOKE_REPLY/STAMP_NOTIFIED never dispatch", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProbeResults(harness, record.flowId, [{ jobId: "job-1", needsReply: false }]);
    const childDone1 = waitForTerminal((q) => q.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childDone1;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.PERSIST_UPDATE?.persisted, true);
    assert.equal(childState.INVOKE_REPLY, undefined);
    assert.equal(childState.STAMP_NOTIFIED, undefined);
  });

  test("PERSIST_UPDATE._error — routes to __end__ regardless of needsReply", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProbeResults(harness, record.flowId, [{ jobId: "job-2", needsReply: true, probeError: "timeout" }]);
    const childDone2 = waitForTerminal((q) => q.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childDone2;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.PERSIST_UPDATE?._error, "probe error");
    assert.equal(childState.INVOKE_REPLY, undefined);
  });

  test("full happy path — INVOKE_REPLY fires a REAL ctx.invokePipeline, real second flow_runs row spawned and succeeds, STAMP_NOTIFIED runs", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProbeResults(harness, record.flowId, [{ jobId: "job-3", needsReply: true }]);
    await executor.runFlow(record.flowId);
    await flush(50);

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.INVOKE_REPLY?.invoked, true);
    assert.equal(childState.STAMP_NOTIFIED?.stamped, true);

    // The real proof: a second, independent flow_runs row exists for the
    // target pipeline, spawned via the child's own real ctx.invokePipeline
    // call — not a stub. This is only possible if a childFlows child gets
    // the identical nodeCtx a root-level node would.
    const targetFlowId = childState.INVOKE_REPLY.flowId;
    assert.ok(targetFlowId, "INVOKE_REPLY must report the real spawned flowId");
    await flush(50);
    const targetFlow = await runStore.get(targetFlowId);
    assert.ok(targetFlow, "the real target pipeline flow must actually exist in runStore");
    assert.equal(targetFlow.status, "succeeded");
    assert.equal(targetFlow.pipelineId, "test_reply_target");

    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });

  test("unregistered target pipeline — invokePipeline throws synchronously, caught by INVOKE_REPLY, routes to __end__", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedProbeResults(harness, record.flowId, [{ jobId: "job-4", needsReply: true, replyTarget: "test_reply_target_missing" }]);
    const childDone3 = waitForTerminal((q) => q.flowId !== record.flowId);
    await executor.runFlow(record.flowId);
    await childDone3;

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.INVOKE_REPLY?.invoked, false);
    assert.match(childState.INVOKE_REPLY?._error, /unknown pipeline: test_reply_target_missing/);
    assert.equal(childState.STAMP_NOTIFIED, undefined);
  });
});
