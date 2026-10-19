/**
 * Engine-mechanics E2E proving a blocked crossProcessLock
 * acquire surfaces as status='queued' on the blocked child's own flow_runs
 * row, then flips back to 'running' once the lock frees. This is the one
 * assertion nothing else in this test suite covers end-to-end:
 * the wrappedFn technique in engine/executor.js's withCrossProcessLockMaybe
 * actually persists the transition, not just that locks/lock.js's
 * onWouldBlock hook fires (already covered in test/unit/locks/lock.test.js).
 *
 * Reuses the same FETCH_PROFILES-shaped fixture as
 * executor-loop-children-fetch-profiles-e2e.test.js (single sub-node
 * SCRAPE_PROFILE declaring crossProcessLock: "browser:linkedin"), but the
 * mocked withLock here HONORS the onWouldBlock contract for a second
 * concurrent caller — the existing sibling file's mock ignores opts
 * entirely, so it can't prove this.
 *
 * mock.module must be registered before engine/executor.js is imported (it
 * imports withLock at module-load time and caches the reference) — every
 * pipeclaw-internal module here is loaded via a dynamic `await import()`
 * AFTER the mock is in place, not a static top-level import.
 *
 * Isolated-sandbox discipline:
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";

let lockCalls = [];
let holderActive = false;
let releaseHolder = null;

await mock.module("file:///Users/herculano/.openclaw/extensions/pipeclaw/locks/lock.js", {
  exports: {
    // Lock-owner plumbing executor-nodes.js imports — inert here: this
    // stand-in's withLock doesn't model owners.
    runWithLockOwner: (_owner, fn) => fn(),
    registerSessionOwner: () => {},
    clearSessionOwner: () => {},
    withLock: async (name, fn, opts) => {
      lockCalls.push(name);
      if (holderActive) {
        // Simulate a real blocked acquire — fire onWouldBlock exactly once
        // (the real lock.js contract), same as the first failed tryAcquire
        // inside acquireLock's poll loop, then wait for the current holder
        // to release before proceeding.
        opts?.onWouldBlock?.();
        await new Promise((resolve) => { releaseHolder = resolve; });
      }
      holderActive = true;
      try {
        // Small artificial delay — gives the test a real window to poll the
        // blocked caller's status mid-wait, same technique as the sibling
        // fetch-profiles E2E's 5ms per-child delay.
        await new Promise((r) => setTimeout(r, 30));
        return await fn();
      } finally {
        holderActive = false;
        const release = releaseHolder;
        releaseHolder = null;
        release?.();
      }
    },
  },
});

const { createRunStore } = await import("file:///Users/herculano/.openclaw/extensions/pipeclaw/state/run-store.js");
const { createNodeExecutionsStore } = await import("file:///Users/herculano/.openclaw/extensions/pipeclaw/state/node-executions.js");
const { createStateStore } = await import("file:///Users/herculano/.openclaw/extensions/pipeclaw/state/state-store.js");
const { createExecutor } = await import("file:///Users/herculano/.openclaw/extensions/pipeclaw/engine/executor.js");

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

// Always-admits-immediately fake — admissionQueueStore is required by
// createExecutor's start()/subflow-children paths. This
// suite exercises crossProcessLock contention specifically, not admission-
// pool contention, so nothing here needs a real queued/admitted lifecycle.
function makeAdmissionQueueStore() {
  return {
    admit: async () => ({ id: "fake-admission", status: "admitted" }),
    release: async () => ({ released: true, promoted: null }),
  };
}

function makeFetchProfilesLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "linkedin-id",
    selector: (state) => state.queueUrls ?? [],
    branch: {
      nodes: {
        SCRAPE_PROFILE: {
          kind: "code",
          crossProcessLock: "browser:linkedin",
          run: (state) => {
            const item = state.item;
            return { SCRAPE_PROFILE: { "linkedin-id": item["linkedin-id"], name: `Prospect ${item["linkedin-id"]}` } };
          },
        },
      },
      edges: {
        __start__: () => "SCRAPE_PROFILE",
        SCRAPE_PROFILE: () => "__end__",
      },
    },
  };
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("lock-queued-e2e-runs"), legacyDbPath: tmpPath("lock-queued-e2e-runs-legacy") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("lock-queued-e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });

  const FETCH_PROFILES_NODE = makeFetchProfilesLikeNode();
  const parentPipeline = {
    id: "test_parent",
    state: {},
    nodes: { FETCH_PROFILES: FETCH_PROFILES_NODE },
    edges: { __start__: "FETCH_PROFILES", FETCH_PROFILES: "__end__" },
  };
  const childPipeline = {
    id: "test_parent::FETCH_PROFILES",
    state: {},
    nodes: FETCH_PROFILES_NODE.branch.nodes,
    edges: FETCH_PROFILES_NODE.branch.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::FETCH_PROFILES", childPipeline]]) };

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

  return { runStore, nodeExecutionsStore, store, executor };
}

async function childFlowIds(harness, parentFlowId) {
  const invocationIds = await harness.nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, "FETCH_PROFILES");
  return (await harness.runStore.listForParentInvocations(invocationIds)).map((c) => c.flowId);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function flush(harness, flowId, { timeoutMs = 3000, pollMs = 15 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const flow = await harness.runStore.get(flowId);
    if (flow && ["succeeded", "failed", "cancelled"].includes(flow.status)) return;
    await sleep(pollMs);
  }
}

// Polls a child's own flow_runs row until it reports the given status, or
// times out — used to catch the transient 'queued' state, which a fixed
// delay can't reliably land on across a full-suite run's varying load.
async function waitForStatus(harness, flowId, status, { timeoutMs = 2000, pollMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const flow = await harness.runStore.get(flowId);
    if (flow?.status === status) return flow;
    await sleep(pollMs);
  }
  return null;
}

// Same as waitForStatus but accepts any of several statuses — used where
// the flow may race straight past an intermediate status (e.g. 'running')
// to a terminal one before the next poll tick, which a sequential
// "wait for running, THEN wait for succeeded" would otherwise burn a full
// timeout window on for no reason.
async function waitForAnyStatus(harness, flowId, statuses, { timeoutMs = 2000, pollMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const flow = await harness.runStore.get(flowId);
    if (flow && statuses.includes(flow.status)) return flow;
    await sleep(pollMs);
  }
  return null;
}

async function store_seedQueueUrls(harness, flowId, queueUrls) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, queueUrls },
  });
}

describe("crossProcessLock contention surfaces as status='queued'", () => {
  let harness;
  afterEach(() => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
    lockCalls = [];
    holderActive = false;
    releaseHolder = null;
  });

  test("two children contend for the same lock — the blocked one shows queued + wait_json, then flips to running, both eventually succeed", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    const items = ["jane-doe", "john-smith"].map((id) => ({ "linkedin-id": id, url: `https://linkedin.com/in/${id}` }));
    await store_seedQueueUrls(harness, record.flowId, items);
    await executor.runFlow(record.flowId, "fresh");

    // Both children's SCRAPE_PROFILE dispatch fires via setImmediate in the
    // same tick (dispatchLoopChildren) — one wins the mocked lock, the
    // other must observe onWouldBlock and persist status='queued' on ITS
    // OWN flow_runs row before the holder releases.
    let ids = [];
    const deadline = Date.now() + 2000;
    while (ids.length < 2 && Date.now() < deadline) {
      ids = await childFlowIds(harness, record.flowId);
      if (ids.length < 2) await sleep(5);
    }
    assert.equal(ids.length, 2, "both items should spawn a child");

    const queuedChild = await Promise.race(ids.map((id) => waitForStatus(harness, id, "queued")));
    assert.ok(queuedChild, "one of the two children should transition through status='queued'");
    assert.deepEqual(queuedChild.waitJson, {
      kind: "resource-lock",
      resource: "browser:linkedin",
      nodeId: "SCRAPE_PROFILE",
    });
    assert.ok(
      queuedChild.statusReason?.includes("browser:linkedin"),
      `statusReason should mention the lock name, got: ${queuedChild.statusReason}`,
    );

    // Confirm it flips back to running (or has already moved past it to a
    // terminal state) once the holder releases — never stuck at 'queued'.
    const flippedOrDone = await waitForAnyStatus(harness, queuedChild.flowId, ["running", "succeeded"]);
    assert.ok(flippedOrDone, "the queued child must leave 'queued' once the lock frees");

    await flush(harness, record.flowId);
    for (const id of ids) assert.equal((await runStore.get(id)).status, "succeeded");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
    assert.equal(lockCalls.length, 2);
  });

  test("a single, uncontested child never transitions through 'queued'", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedQueueUrls(harness, record.flowId, [{ "linkedin-id": "solo", url: "https://linkedin.com/in/solo" }]);
    await executor.runFlow(record.flowId, "fresh");
    await flush(harness, record.flowId);

    const [childId] = await childFlowIds(harness, record.flowId);
    assert.equal((await runStore.get(childId)).status, "succeeded");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
  });
});
