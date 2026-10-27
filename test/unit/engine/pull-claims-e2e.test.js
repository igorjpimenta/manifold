// Pull-claim park → claim → report → resume E2E (PULL-1) on a real
// executor with an agent node: real sqlite run/node-execution
// stores on isolated tmp files, real state-store, always-admit admission.
// No dispatch runtime is wired anywhere in this harness: reaching `blocked`
// on agent-claim proves the pull branch parked, and claim → report → resume
// completes the run.
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { reportPullClaim } from "../../../engine/pull-claims.js";
import { assembleState } from "../../../engine/executor-helpers.js";

const logger = { info: () => {}, warn: () => {}, error: () => {} };
const TRIPLE = { harness: "harness-1", agent: "agent-1", session: "session-1" };

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("pull-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("pull-e2e-execs"));
  const resumeParentRef = { resume: null };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });
  const admissionQueueStore = {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
    releaseByFlow: async () => ({ released: true, promoted: null }),
  };
  const emitted = [];
  const pipelinesRef = {
    value: new Map([
      ["test_pull", {
        id: "test_pull",
        state: {},
        nodes: {
          ENTER: { kind: "code", run: () => ({ ENTER: { ok: true } }) },
          WORK: { kind: "agent", agent: "test-agent" },
          DONE: { kind: "code", run: () => ({ DONE: { ok: true } }) },
        },
        edges: {
          __start__: "ENTER",
          ENTER: "WORK",
          WORK: (state) => (state.WORK?.done ? "DONE" : null),
          DONE: () => "__end__",
        },
      }],
    ]),
  };
  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    failureStore: null,
    nodeHealthStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex: null,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore,
    emit: async (kind, payload) => { emitted.push({ kind, payload }); return { ok: true, kind }; },
  });
  resumeParentRef.resume = executor.resume;
  return { runStore, nodeExecutionsStore, store, executor, admissionQueueStore, emitted };
}

async function waitForStatus(runStore, flowId, want, timeoutMs = 15000) {
  const start = Date.now();
  for (;;) {
    const flow = await runStore.get(flowId);
    if (flow?.status === want) return flow;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${want} (last: ${flow?.status})`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

describe("pull-claim park → claim → report → resume E2E", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("agent node parks, emits, and resumes to succeeded after a report", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, store, admissionQueueStore, emitted } = harness;

    const record = await executor.start("test_pull", {}, { kind: "test" });
    await executor.runFlow(record.flowId, "fresh");

    const parked = await runStore.get(record.flowId);
    assert.equal(parked.status, "blocked");
    assert.equal(parked.waitJson?.kind, "agent-claim");
    assert.equal(parked.waitJson?.nodeId, "WORK");
    const ticket = parked.waitJson?.ticket;
    assert.ok(typeof ticket === "string" && ticket.length > 0);
    const invocation = await nodeExecutionsStore.getLatestInvocationForNode(record.flowId, "WORK", null);
    assert.equal(ticket, invocation.id);
    const parkEvent = emitted.find((e) => e.kind === "agent.claim.parked");
    assert.deepEqual(parkEvent?.payload, { flowId: record.flowId, ticket, nodeId: "WORK" });

    const claim = await nodeExecutionsStore.claimNext({ ...TRIPLE });
    assert.equal(claim.ticket, ticket);

    const api = { runtime: {}, config: {} };
    const reported = await reportPullClaim(
      { nodeExecutionsStore, store, admissionQueueStore, runStore, executor, logger, api },
      { ticket, ...TRIPLE, result: { WORK: { done: true } } },
    );
    assert.equal(reported.ok, true);
    assert.equal(reported.queued, false);

    const terminal = await waitForStatus(runStore, record.flowId, "succeeded");
    assert.equal(terminal.status, "succeeded");
    const enriched = await assembleState(record.flowId, { runStore, nodeExecutionsStore, baseState: terminal.stateJson });
    assert.equal(enriched.WORK?.done, true);
    assert.equal(enriched.DONE?.ok, true);
    assert.equal((await nodeExecutionsStore.getClaim(ticket)).claimState, "applied");
  });

  test("an impostor report is refused and the flow stays parked", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;

    const record = await executor.start("test_pull", {}, { kind: "test" });
    await executor.runFlow(record.flowId, "fresh");
    const parked = await runStore.get(record.flowId);
    const ticket = parked.waitJson?.ticket;
    await nodeExecutionsStore.claimNext({ ...TRIPLE });

    const api = { runtime: {}, config: {} };
    await assert.rejects(
      () => reportPullClaim(
        { nodeExecutionsStore, store, admissionQueueStore, runStore, executor, logger, api },
        { ticket, ...TRIPLE, agent: "impostor", result: { WORK: { done: true } } },
      ),
      /mismatch/,
    );
    assert.equal((await runStore.get(record.flowId)).status, "blocked");
    assert.equal((await nodeExecutionsStore.getClaim(ticket)).claimState, "claimed");
  });
});
