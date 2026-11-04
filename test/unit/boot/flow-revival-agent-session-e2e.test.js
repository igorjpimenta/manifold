/**
 * E2E: boot/flow-revival.js's agent-vs-fresh-dispatch classification, driven
 * against a real executor + real state-store + real session-index (all
 * tmp-sqlite, no mocks beyond the subagent runtime boundary itself). Proves
 * the two paths discussed while designing this — a flow genuinely
 * interrupted mid-turn gets revived with the continuation prompt (REVIVAL_MSG,
 * same session), a flow that was only ever queued waiting for a dispatch
 * slot and never actually started gets a fresh dispatch (its own
 * promptBuilder output, regardless of the target node being agent-kind) —
 * against the real state_json / flow_sessions / flow_node_invocations
 * schema, not a hand-rolled mock of the classification logic.
 *
 * The one mock is the agent runtime itself (`runTurn`) — same
 * convention as executor.test.js's fallback-circuit-breaker suite — since
 * we're proving flow-revival's classification and executor's dispatch
 * wiring, not exercising a real LLM call.
 *
 * Isolated-sandbox discipline:
 * independent tmp-sqlite run-store/node-executions/session-index/state-store
 * instances, never ~/.openclaw/runtime.sqlite; fixture pipeline registered
 * directly in an in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createSessionIndex } from "../../../state/session-index.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { reviveOrphanFlows } from "../../../boot/flow-revival.js";
import { REVIVAL_MSG } from "../../../engine/revival-messages.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Always-admits-immediately fake — admissionQueueStore is required by createExecutor's
// subflow-children paths. These tests don't exercise queue behavior
// (covered by admission-queue-store.test.js) — just the always-admits fake is needed.
function makeAdmissionQueueStore(overrides = {}) {
  return {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
    ...overrides,
  };
}

// Fixture pipeline: a single agent-kind node. promptBuilder's output is
// distinguishable from REVIVAL_MSG so a test can tell which one the fake
// subagent actually received.
function makeFixturePipeline() {
  return {
    id: "test_agent_fixture",
    state: {},
    nodes: {
      AGENT_STEP: {
        kind: "agent",
        agent: "scout",
        session: "agent:scout:fixture-session",
        promptBuilder: () => "FRESH_PROMPT: score this listing",
        apply: (state, reply) => ({ AGENT_STEP: { reply } }),
      },
    },
    edges: { __start__: "AGENT_STEP", AGENT_STEP: "__end__" },
  };
}

async function makeHarness({ runTurn } = {}) {
  const runStore = await createRunStore({ dbPath: tmpPath("fr-agent-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("fr-agent-e2e-execs"));
  const sessionIndex = await createSessionIndex({ dbPath: tmpPath("fr-agent-e2e-sessions") });
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const pipeline = makeFixturePipeline();
  const pipelinesRef = { value: new Map([[pipeline.id, pipeline]]) };

  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    runTurn,
    failureStore: null,
    nodeHealthStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore: makeAdmissionQueueStore(),
  });
  resumeParentRef.resume = executor.resume;

  return { runStore, nodeExecutionsStore, sessionIndex, store, executor, pipelinesRef };
}

async function waitForStatus(harness, flowId, statuses, { timeoutMs = 3000, pollMs = 10 } = {}) {
  const list = Array.isArray(statuses) ? statuses : [statuses];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const flow = await harness.runStore.get(flowId);
    if (flow && list.includes(flow.status)) return flow;
    await sleep(pollMs);
  }
  return harness.runStore.get(flowId);
}

describe("flow-revival + session-index — agent-node revival classification (fixture pipeline)", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); harness?.sessionIndex?.close(); });

  test("a flow interrupted mid-turn (session already recorded) is revived with the continuation prompt, same session", async () => {
    const messagesReceived = [];
    let releaseHang = null;
    const runTurn = async ({ sessionKey, message }) => {
      messagesReceived.push({ sessionKey, message });
      // Hang here — simulating the gateway process dying mid-turn, after
      // sessionIndex.record() already fired (confirmed ordering: record()
      // happens before this await, at the top of dispatchAgentWithRetry)
      // but before the turn ever resolves.
      await new Promise((resolve) => { releaseHang = resolve; });
      return { text: "done", runId: "run-1" };
    };
    harness = await makeHarness({ runTurn });
    const { executor, runStore, sessionIndex } = harness;

    const record = await executor.start("test_agent_fixture", {}, { kind: "test" });
    executor.runFlow(record.flowId, "fresh").catch(() => {}); // never awaited to completion — simulates the crash

    // Wait for the dispatch to actually reach the (now-hanging) subagent
    // call — proves sessionIndex.record() has already fired by this point.
    const deadline = Date.now() + 2000;
    while (messagesReceived.length === 0 && Date.now() < deadline) await sleep(10);
    assert.equal(messagesReceived.length, 1, "the fresh dispatch must have reached the subagent boundary");
    assert.equal(messagesReceived[0].message, "FRESH_PROMPT: score this listing", "the ORIGINAL dispatch uses promptBuilder, not REVIVAL_MSG");

    const flowRow = await runStore.get(record.flowId);
    assert.equal(flowRow.status, "running", "still 'running' — the node dispatch never resolved");
    assert.equal(await sessionIndex.hasSessionForNode(record.flowId, "AGENT_STEP"), true, "session record exists — dispatch genuinely started");

    // Now drive the real revival pass against this "orphan" (never actually
    // crashed the process — but from the DB's point of view, indistinguishable
    // from a real crash: status='running', session recorded, node never resolved).
    const result = await reviveOrphanFlows({
      all: [flowRow],
      pipelines: harness.pipelinesRef.value,
      store: harness.store,
      logger: { info: () => {}, warn: () => {} },
      executor,
      runStore,
      nodeExecutionsStore: harness.nodeExecutionsStore,
      sessionIndex,
    });

    assert.equal(result.revivedAgent, 1, "classified as a genuine in-flight interruption");
    assert.equal(result.revived, 1);

    // The revival's OWN dispatch attempt (reviveAgentKindOrphan) reaches the
    // subagent again, this time hanging on messagesReceived[1] — confirms it
    // sent REVIVAL_MSG to the SAME session as the original attempt.
    const deadline2 = Date.now() + 2000;
    while (messagesReceived.length < 2 && Date.now() < deadline2) await sleep(10);
    assert.equal(messagesReceived.length, 2);
    assert.equal(messagesReceived[1].message, REVIVAL_MSG, "revival sends the continuation prompt, not a fresh promptBuilder call");
    assert.equal(messagesReceived[1].sessionKey, messagesReceived[0].sessionKey, "same deterministic session — this IS the continuation, not a new conversation");

    releaseHang?.();
  });

  test("a flow only ever queued for a dispatch slot (no session recorded) gets a fresh dispatch instead", async () => {
    const messagesReceived = [];
    const runTurn = async ({ sessionKey, message }) => {
      messagesReceived.push({ sessionKey, message });
      return { text: "done", runId: "run-1" };
    };
    harness = await makeHarness({ runTurn });
    const { store, runStore, sessionIndex, executor } = harness;

    // Never went through executor.start()/runFlow() at all — the queued row
    // is written directly, a flow queued on an in-node resource wait before
    // AGENT_STEP ran. No dispatch was ever attempted, so no session record
    // exists. lastStep stays "__start__"; the pending target node is
    // carried by waitJson.nodeId, not lastStep.
    const flow = await store.create({ pipelineId: "test_agent_fixture", initialState: {} });
    await store.setQueued({
      flowId: flow.flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
      stateJson: flow.stateJson,
      waitJson: { kind: "agent-semaphore", nodeId: "AGENT_STEP" },
      statusReason: "queued: waiting on a resource (test fixture)",
    });

    assert.equal(await sessionIndex.hasSessionForNode(flow.flowId, "AGENT_STEP"), false, "sanity: no session recorded yet");

    const flowRow = await runStore.get(flow.flowId);
    const result = await reviveOrphanFlows({
      all: [flowRow],
      pipelines: harness.pipelinesRef.value,
      store,
      logger: { info: () => {}, warn: () => {} },
      executor,
      runStore,
      nodeExecutionsStore: harness.nodeExecutionsStore,
      sessionIndex,
    });

    assert.equal(result.revivedAgent, 0, "classified as never-started, NOT a genuine interruption");
    assert.equal(result.revived, 1);

    const final = await waitForStatus(harness, flow.flowId, ["succeeded", "failed"]);
    assert.equal(final.status, "succeeded");
    assert.equal(messagesReceived.length, 1);
    assert.equal(messagesReceived[0].message, "FRESH_PROMPT: score this listing", "fresh dispatch used promptBuilder, not the continuation prompt — nothing to continue");
  });
});
