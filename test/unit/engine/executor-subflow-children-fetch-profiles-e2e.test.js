/**
 * Engine-mechanics E2E for a real childFlows:true
 * loop node whose topology mirrors the freelance prospecting pipeline's
 * FETCH_PROFILES (single sub-node SCRAPE_PROFILE, no gate, no
 * fallback) — same simplest topology as EVICT_STALE/EVICT_GATES/SCORE.
 *
 * The load-bearing assertion for THIS test: FETCH_PROFILES declares
 * `crossProcessLock: "acct:linkedin"` on the real sub-node,
 * SCRAPE_PROFILE. No prior childFlows E2E in this series exercises
 * crossProcessLock at all (confirmed via grep). Under childFlows,
 * executor-subflow-children.js's dispatchLoopChildren() spawns every
 * selected child via `setImmediate(() => runFlow(childFlowId))` in the
 * SAME tick — a genuine concurrent fire-and-forget dispatch, not a
 * sequential await-per-item loop.
 * This test proves crossProcessLock on SCRAPE_PROFILE is (a) actually
 * invoked once PER CHILD (not once for the whole node dispatch), and
 * (b) genuinely serializes concurrent
 * children against each other when the lock provides real mutual
 * exclusion — using an instrumented in-memory stand-in for
 * the lock service's withLock that queues callers via promise chaining
 * (the same mutual-exclusion contract the real disk-backed lock
 * provides), never touching the real ~/.openclaw/locks/*.lock files.
 *
 * The lock service is injected via createExecutor's `locks` param (the
 * same seam the harness plugin uses in production) — no module mocking.
 *
 * Isolated-sandbox discipline (per the incident writeup):
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";

let lockCalls = [];
let concurrentInLock = 0;
let maxConcurrentInLock = 0;
let lockChain = Promise.resolve();

import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { getAllSubflowGroups } from "../../../state/subflow-state.js";
import { assembleState } from "../../../engine/executor-helpers.js";

// Stand-in lock service: promise-chained mutual exclusion, owner plumbing
// inert (this stand-in's withLock doesn't model owners).
const lockStubs = {
  runWithLockOwner: (_owner, fn) => fn(),
  registerSessionOwner: () => {},
  clearSessionOwner: () => {},
  withLock: (name, fn) => {
      lockCalls.push(name);
      const run = async () => {
        concurrentInLock++;
        maxConcurrentInLock = Math.max(maxConcurrentInLock, concurrentInLock);
        try {
          // Artificial delay — gives two concurrently-dispatched children a
          // real window to overlap if this stand-in did NOT serialize them,
          // so maxConcurrentInLock staying at 1 is a genuine proof, not a
          // coincidence of synchronous execution.
          await new Promise((r) => setTimeout(r, 5));
          return await fn();
        } finally {
          concurrentInLock--;
        }
      };
      // Chain onto the prior call regardless of its outcome — the same
      // "only one holder at a time, everyone else queues" contract the
      // real disk-backed lock provides via acquireLock's poll loop.
      const next = lockChain.then(run, run);
      lockChain = next.catch(() => {});
      return next;
    },
};

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

function makeFetchProfilesLikeNode() {
  return {
    kind: "subflow",
    childFlows: true,
    itemKey: "linkedin-id",
    selector: (state) => state.queueUrls ?? [],
    flow: {
      nodes: {
        SCRAPE_PROFILE: {
          kind: "code",
          crossProcessLock: "acct:linkedin",
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
  const runStore = await createRunStore({ dbPath: tmpPath("fetch-profiles-e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("fetch-profiles-e2e-execs"));
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
    nodes: FETCH_PROFILES_NODE.flow.nodes,
    edges: FETCH_PROFILES_NODE.flow.edges,
  };
  const pipelinesRef = { value: new Map([["test_parent", parentPipeline], ["test_parent::FETCH_PROFILES", childPipeline]]) };

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
    admissionQueueStore: makeAdmissionQueueStore(),
    locks: lockStubs,
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

// Polls until the parent flow reaches a terminal status, instead of a fixed
// short delay — this test's mock withLock adds a real per-child
// artificial delay (see above) on top of normal dispatch overhead, which a
// fixed flush() isn't reliably enough time for
// under a full-suite run's concurrent load. Bounded so a genuine hang still
// fails loudly rather than hanging the suite.
async function flush(harness, flowId, { timeoutMs = 3000, pollMs = 15 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const flow = await harness.runStore.get(flowId);
    if (flow && ["succeeded", "failed", "cancelled"].includes(flow.status)) return;
    await sleep(pollMs);
  }
}

async function store_seedQueueUrls(harness, flowId, queueUrls) {
  const flow = await harness.store.get(flowId);
  await harness.store.setRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep,
    stateJson: { ...flow.stateJson, queueUrls },
  });
}

describe("childFlows:true FETCH_PROFILES-shaped loop, full engine E2E", () => {
  let harness;
  afterEach(() => {
    harness?.runStore?.close();
    harness?.nodeExecutionsStore?.close();
    lockCalls = [];
    concurrentInLock = 0;
    maxConcurrentInLock = 0;
    lockChain = Promise.resolve();
  });

  test("single item — child spawns, SCRAPE_PROFILE runs under the lock, parent auto-resumes", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedQueueUrls(harness, record.flowId, [{ "linkedin-id": "jane-doe", url: "https://linkedin.com/in/jane-doe" }]);
    await executor.runFlow(record.flowId);
    await flush(harness, record.flowId);

    const [childFlowId] = await childFlowIds(harness, record.flowId);
    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "succeeded");
    const childState = await assembleState(child.flowId, { runStore, nodeExecutionsStore, baseState: child.stateJson });
    assert.equal(childState.SCRAPE_PROFILE?.name, "Prospect jane-doe");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
    assert.deepEqual(lockCalls, ["acct:linkedin"]);
  });

  test("multiple items dispatched concurrently via setImmediate — crossProcessLock acquired once PER CHILD, never more than 1 concurrent holder", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    const items = ["a", "b", "c", "d"].map((id) => ({ "linkedin-id": id, url: `https://linkedin.com/in/${id}` }));
    await store_seedQueueUrls(harness, record.flowId, items);
    await executor.runFlow(record.flowId);
    await flush(harness, record.flowId);

    const ids = await childFlowIds(harness, record.flowId);
    assert.equal(ids.length, 4, "all 4 selected URLs spawn a child, no fan-out limiter");
    for (const id of ids) assert.equal((await runStore.get(id)).status, "succeeded");
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");

    // The mechanical proof: one withLock("acct:linkedin") call per real
    // child dispatch — the OLD simple-mode behavior acquired the lock ONCE
    // for the whole node's entire item batch, not per item.
    assert.equal(lockCalls.length, 4);
    assert.ok(lockCalls.every((n) => n === "acct:linkedin"));
    // The concurrency proof: despite dispatchLoopChildren() firing all 4
    // children's runFlow() via setImmediate in the same tick, the lock's
    // real mutual-exclusion contract (faithfully simulated by the
    // promise-chained stand-in above) never let more than one child's
    // SCRAPE_PROFILE body run at once.
    assert.equal(maxConcurrentInLock, 1);
  });

  test("no queued URLs — zero children spawned, lock never acquired, parent still succeeds", async () => {
    harness = await makeHarness();
    const { executor, runStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedQueueUrls(harness, record.flowId, []);
    await executor.runFlow(record.flowId);
    await flush(harness, record.flowId);

    assert.equal((await childFlowIds(harness, record.flowId)).length, 0);
    assert.equal((await runStore.get(record.flowId)).status, "succeeded");
    assert.deepEqual(lockCalls, []);
  });

  test("getAllSubflowGroups resolves under groups.FETCH_PROFILES", async () => {
    harness = await makeHarness();
    const { executor, runStore, nodeExecutionsStore } = harness;

    const record = await executor.start("test_parent", {}, { kind: "test" });
    await store_seedQueueUrls(harness, record.flowId, [{ "linkedin-id": "jane-doe", url: "https://linkedin.com/in/jane-doe" }]);
    await executor.runFlow(record.flowId);
    await flush(harness, record.flowId);

    const groups = await getAllSubflowGroups({ flowId: record.flowId, runStore, nodeExecutionsStore });
    assert.ok(groups.FETCH_PROFILES);
    const row = groups.FETCH_PROFILES["jane-doe"];
    assert.ok(row);
    assert.equal(row.status, "succeeded");
    assert.equal(row.data.SCRAPE_PROFILE?.name, "Prospect jane-doe");
  });
});
