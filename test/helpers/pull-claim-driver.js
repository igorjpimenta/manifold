// pull-claim-driver.js — shared claim-style driver for pull-first tests.
//
// Park → claim → report → resume against a real executor: real sqlite
// run/node-execution stores on isolated tmp files, real state-store,
// always-admit admission. No subagent runtime exists anywhere in this
// path — an agent node that tried to push-dispatch would fail the flow,
// so reaching `blocked` on `agent-claim` proves the pull branch ran.
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../state/run-store.js";
import { createNodeExecutionsStore } from "../../state/node-executions.js";
import { createStateStore } from "../../state/state-store.js";
import { createExecutor } from "../../engine/executor.js";
import { reportPullClaim } from "../../engine/pull-claims.js";

export const TRIPLE = { harness: "harness-1", agent: "agent-1", session: "session-1" };
export const silentLogger = { info: () => {}, warn: () => {}, error: () => {} };

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

export async function makeClaimHarness(pipelines, { logger = silentLogger } = {}) {
  const runStore = await createRunStore({ dbPath: tmpPath("claim-driver-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("claim-driver-execs"));
  const resumeParentRef = { resume: null };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });
  const admissionQueueStore = {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
    releaseByFlow: async () => ({ released: true, promoted: null }),
  };
  const emitted = [];
  const pipelinesRef = { value: new Map(Object.entries(pipelines)) };
  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    failureStore: null,
    notifyFlowEnteredWaiting: null,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore,
    emit: async (kind, payload) => { emitted.push({ kind, payload }); return { ok: true, kind }; },
  });
  resumeParentRef.resume = executor.resume;
  return {
    runStore, nodeExecutionsStore, store, executor, admissionQueueStore, emitted,
    close() { runStore?.close(); nodeExecutionsStore?.close(); },
  };
}

export async function waitForStatus(runStore, flowId, want, timeoutMs = 15000) {
  const start = Date.now();
  for (;;) {
    const flow = await runStore.get(flowId);
    if (flow?.status === want) return flow;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${want} (last: ${flow?.status})`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

// Start a pipeline and drive it until the agent node parks. Returns the
// parked flow row plus its ticket.
export async function startAndPark(harness, pipelineId, args = {}) {
  const { executor, runStore } = harness;
  const record = await executor.start(pipelineId, args, { kind: "test" });
  await executor.runFlow(record.flowId, "fresh");
  const parked = await runStore.get(record.flowId);
  if (parked.status !== "blocked" || parked.waitJson?.kind !== "agent-claim") {
    throw new Error(`expected a parked agent-claim, got status=${parked.status} wait=${parked.waitJson?.kind}`);
  }
  return { flowId: record.flowId, parked, ticket: parked.waitJson.ticket, nodeId: parked.waitJson.nodeId };
}

// Claim the ticket and report a result through the real report path,
// resuming the parked flow.
export async function claimAndReport(harness, ticket, result, triple = TRIPLE) {
  const { nodeExecutionsStore, store, admissionQueueStore, runStore, executor } = harness;
  const claim = await nodeExecutionsStore.claimNext({ ...triple });
  if (claim.ticket !== ticket) throw new Error(`claimed ${claim.ticket}, expected ${ticket}`);
  return reportPullClaim(
    { nodeExecutionsStore, store, admissionQueueStore, runStore, executor, logger: silentLogger, api: { runtime: {}, config: {} } },
    { ticket, ...triple, result },
  );
}
