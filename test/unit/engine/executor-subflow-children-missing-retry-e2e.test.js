import { test, describe, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { mkdir, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";

// operator-retry reads the getNodeExecutionsStore() singleton, and static imports hoist above mock.module — redirect the singleton and import everything after the mock registers.
const realNodeExecutions = await import("../../../state/node-executions.js");
let sharedNodeExecutionsStore = null;
await mock.module("../../../state/node-executions.js", {
  exports: { ...realNodeExecutions, getNodeExecutionsStore: () => sharedNodeExecutionsStore },
});

const { createNodeExecutionsStore } = realNodeExecutions;
const { loadPipelines } = await import("../../../engine/loader.js");
const { createRunStore } = await import("../../../state/run-store.js");
const { createFailureStore } = await import("../../../state/failure-store.js");
const { createStateStore } = await import("../../../state/state-store.js");
const { createExecutor } = await import("../../../engine/executor.js");
const { createRetryFailure } = await import("../../../routes/operator-retry.js");
const { assembleState } = await import("../../../engine/executor-helpers.js");

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

const INCIDENT_PARENT_PIPELINE = `
export default {
  id: "test_incident_parent",
  state: { candidates: [] },
  nodes: {
    SCORE: {
      kind: "subflow",
      selector: (state) => state.candidates ?? [],
      mode: "any",
      flow: {
        nodes: {
          INNER: { kind: "code", run: async (state) => ({ INNER: { ok: true } }) },
        },
        edges: { __start__: "INNER", INNER: "__end__" },
      },
    },
  },
  edges: { __start__: "SCORE", SCORE: "__end__" },
};
`;

async function makeHarness() {
  const pipelinesDir = path.join(os.tmpdir(), `gf-incident-e2e-${randomUUID()}`);
  await mkdir(path.join(pipelinesDir, "test_incident_parent"), { recursive: true });
  await writeFile(path.join(pipelinesDir, "test_incident_parent", "index.js"), INCIDENT_PARENT_PIPELINE, "utf8");

  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const loaded = await loadPipelines(pipelinesDir, logger);
  assert.ok(loaded.has("test_incident_parent::SCORE"), "sanity: loader minted the synthetic child pipeline");
  const pipelinesRef = { value: loaded };

  const dbPath = tmpPath("incident-e2e");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  sharedNodeExecutionsStore = nodeExecutionsStore;
  const failureStore = await createFailureStore({ dbPath, nodeExecutionsStore });
  const resumeParentRef = { resume: null };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, pipelinesRef, failureStore, logger });

  const admissionQueueStore = makeAdmissionQueueStore();
  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store,
    pipelinesRef,
    ensurePipelinesLoaded: null,
    logger,
    runTurn: null,
    failureStore,
    nodeHealthStore: null,
    notifyFlowEnteredWaiting: null,
    sessionIndex: null,
    nodeExecutionsStore,
    runStore,
    admissionQueueStore,
  });
  resumeParentRef.resume = executor.resume;

  const { retryFailureBranch } = createRetryFailure({
    store, readStore: runStore, failureStore, pipelinesRef, executor, runStore, ownerSessionKey: "test-owner", admissionQueueStore, logger,
  });

  return { pipelinesDir, runStore, nodeExecutionsStore, failureStore, store, executor, pipelinesRef, retryFailureBranch };
}

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "SCORE");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

async function seedCandidates(harness, flowId, candidates) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, candidates },
  });
}

function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("subflow child pipeline transiently missing, then Retry on the parent", () => {
  let harness;
  afterEach(async () => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
    if (harness?.pipelinesDir) await rm(harness.pipelinesDir, { recursive: true, force: true });
  });

  test("child fails with 'pipeline not loaded' and no failure_node when the synthetic entry is missing; parent auto-fails subflow-mode-not-satisfied", async () => {
    harness = await makeHarness();
    const { executor, runStore, failureStore, pipelinesRef } = harness;

    const savedChildPipeline = pipelinesRef.value.get("test_incident_parent::SCORE");
    pipelinesRef.value.delete("test_incident_parent::SCORE");

    const record = await executor.start("test_incident_parent", {}, { kind: "test" });
    await seedCandidates(harness, record.flowId, [{ id: "job-1" }]);
    await executor.runFlow(record.flowId);
    await flush();

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "failed");
    assert.match(child.statusReason ?? "", /pipeline not loaded: test_incident_parent::SCORE/);

    const childFailures = await failureStore.list({ flowId: childFlowId });
    assert.equal(childFailures.length, 1);
    assert.equal(childFailures[0].failure_node, null, "failed before any node dispatched — no invocation to derive a failure_node from");
    assert.equal(childFailures[0].error_type, null);

    const parent = await runStore.get(record.flowId);
    assert.equal(parent.status, "failed");
    assert.match(parent.statusReason ?? "", /subflow "SCORE" \(mode:"any"\) not satisfied/);
    const parentFailures = await failureStore.list({ flowId: record.flowId });
    assert.equal(parentFailures.length, 1);
    assert.equal(parentFailures[0].error_type, "subflow-mode-not-satisfied");

    pipelinesRef.value.set("test_incident_parent::SCORE", savedChildPipeline);
  });

  test("Retry on the parent's failure now recovers the run end-to-end, instead of 409ing 'can't determine retry target'", async () => {
    harness = await makeHarness();
    const { executor, runStore, failureStore, pipelinesRef, retryFailureBranch } = harness;

    const savedChildPipeline = pipelinesRef.value.get("test_incident_parent::SCORE");
    pipelinesRef.value.delete("test_incident_parent::SCORE");

    const record = await executor.start("test_incident_parent", {}, { kind: "test" });
    await seedCandidates(harness, record.flowId, [{ id: "job-1" }]);
    await executor.runFlow(record.flowId);
    await flush();

    assert.equal((await runStore.get(record.flowId)).status, "failed", "sanity: parent failed same as the previous test");

    pipelinesRef.value.set("test_incident_parent::SCORE", savedChildPipeline);

    const [parentFailure] = await failureStore.list({ flowId: record.flowId });
    assert.equal(parentFailure.error_type, "subflow-mode-not-satisfied");

    const result = await retryFailureBranch({ id: parentFailure.id, via: "test-tap-retry-on-parent" });
    assert.equal(result.code, "ok", `expected ok, got: ${JSON.stringify(result)}`);
    assert.equal(result.mode, "cascade-redirect");
    assert.equal(result.results.length, 1);
    assert.equal(result.results[0].code, "ok");
    assert.equal(result.results[0].mode, "flow-restart", "no failure_node — must fall back to a whole-flow __start__ retry, not 409");

    await flush();

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded", "child actually ran this time — the pipeline resolved");
    const childState = await assembleState(childFlowId, { runStore, nodeExecutionsStore: harness.nodeExecutionsStore });
    assert.equal(childState?.INNER?.ok, true);

    const parent = await runStore.get(record.flowId);
    assert.equal(parent.status, "succeeded", "parent auto-resumed once the retried child satisfied mode:\"any\"");
  });
});
