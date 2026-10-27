/**
 * Option B "needs:" — engine-mechanics E2E, matching this suite's own
 * established convention: a real createExecutor() driving a real dispatch loop
 * against isolated tmp-sqlite stores, proving lazy needs: resolution (via
 * ctx.needs(varName), NOT a bulk pre-fetch) actually works through the
 * live engine — not just the mocked-store unit coverage in
 * test/unit/engine/needs-resolver.test.js.
 *
 * Scenario: a root pipeline's SCAN (code) node runs, then PROPOSE (a
 * childFlows:true subflow) spawns a child whose own DRAFT node declares
 * needs: {scan: {hops: 1, nodeId: "SCAN"}} and calls
 * `await ctx.needs("scan")` to read SCAN's real output from its parent
 * (the root flow) without it being cloned into the child's initial state
 * at spawn — and without the engine ever pre-resolving it before run().
 *
 * Isolated-sandbox discipline:
 * independent tmp-sqlite run-store/node-executions/state-store instances,
 * never ~/.openclaw/runtime.sqlite; pipelines registered directly in an
 * in-memory Map, never touching workspace/.pipelines/.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import { waitForTerminal, waitForStatus } from "../../helpers/wait-for-event.js";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createExecutor } from "../../../engine/executor.js";
import { createGateResolver } from "../../../gates/gate-resolver.js";

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

function flush(ms = 60) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function makeHarness(pipelineDef) {
  const runStore = await createRunStore({ dbPath: tmpPath("e2e-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("e2e-execs"));
  const resumeParentRef = { resume: null };
  const logger = { info: () => {}, warn: () => {}, error: () => {} };
  const store = createStateStore({}, "test-owner", { runStore, nodeExecutionsStore, resumeParentRef, logger });
  const pipelinesRef = { value: new Map(Object.entries(pipelineDef)) };

  const executor = createExecutor({
    api: { runtime: {}, config: {} },
    store, pipelinesRef, ensurePipelinesLoaded: null, logger,
    notifyFlowEnteredWaiting: null, sessionIndex: null,
    nodeExecutionsStore, runStore,
    admissionQueueStore: makeAdmissionQueueStore(),
  });
  resumeParentRef.resume = executor.resume;

  const gateResolver = createGateResolver({
    api: { logger }, store, executor, runStore, nodeExecutionsStore,
    pipelineLookup: (pipelineId) => pipelinesRef.value.get(pipelineId),
  });

  return { runStore, nodeExecutionsStore, store, executor, gateResolver, pipelinesRef };
}

let draftCalls;
function buildPipelines() {
  draftCalls = [];
  const PROPOSE_NODE = {
    kind: "subflow",
    childFlows: true,
    mode: "any",
    selector: (state) => state.shortlist ?? [],
    itemKey: "id",
    flow: {
      nodes: {
        DRAFT: {
          kind: "code",
          needs: { scan: { hops: 1, nodeId: "SCAN" } },
          run: async (state, ctx) => {
            const scan = await ctx.needs("scan"); // throws NodeBlocked if not ready — push only happens once resolved
            draftCalls.push(scan);
            return { DRAFT: { text: `drafted from ${JSON.stringify(scan)}` } };
          },
        },
      },
      edges: { __start__: "DRAFT", DRAFT: "__end__" },
    },
  };
  return {
    test_needs_parent: {
      id: "test_needs_parent",
      state: {},
      nodes: {
        // B4: shortlist must come from a real node's own
        // stateUpdate, not a raw store.setRunning() injection — a
        // manually-seeded, non-node-output field is exactly what
        // state_json narrowing now correctly stops carrying
        // forward. This mirrors how a real pipeline would actually shape
        // it: whatever node runs before a childFlows subflow returns the
        // selector's input as part of its own output.
        SCAN: { kind: "code", run: () => ({ SCAN: { listings: ["a", "b"] }, shortlist: [{ id: "x" }] }) },
        PROPOSE: PROPOSE_NODE,
      },
      edges: { __start__: "SCAN", SCAN: "PROPOSE", PROPOSE: "__end__" },
    },
    "test_needs_parent::PROPOSE": {
      id: "test_needs_parent::PROPOSE",
      state: {},
      nodes: PROPOSE_NODE.flow.nodes,
      edges: PROPOSE_NODE.flow.edges,
    },
  };
}

describe('Option B "needs:" — real dispatch loop, cross-boundary hops:1 read', () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("happy path: SCAN completes before PROPOSE spawns its child — DRAFT resolves needs:{hops:1,SCAN} immediately and succeeds", async () => {
    harness = await makeHarness(buildPipelines());
    const { executor, runStore } = harness;

    const record = await executor.start("test_needs_parent", {}, { kind: "test" });
    await executor.runFlow(record.flowId, "fresh");
    await flush();

    const parent = await runStore.get(record.flowId);
    assert.equal(parent.status, "succeeded", "SCAN already complete by the time PROPOSE dispatches — nothing should ever block");

    assert.equal(draftCalls.length, 1, "DRAFT's run() must have been called exactly once — never dispatched while pending");
    assert.deepEqual(draftCalls[0], { listings: ["a", "b"] }, "DRAFT read the real parent SCAN value via _needs, not a cloned copy");
  });

  test("not-ready: DRAFT blocks with needs-not-ready (never calls run()) while its declared dependency is still in flight, then resolves once it completes", async () => {
    harness = await makeHarness(buildPipelines());
    const { executor, runStore, store, nodeExecutionsStore } = harness;

    // Manufacture "SCAN dispatched, not yet complete" directly — the normal
    // edge graph always finishes SCAN before PROPOSE can spawn a child, so
    // this is the deterministic way to exercise the not-ready branch
    // end-to-end through DRAFT's own real dispatch rather than only via
    // needs-resolver.test.js's mocked-store coverage.
    const record = await executor.start("test_needs_parent", {}, { kind: "test" });
    const enterResult = await nodeExecutionsStore.enter({ flowId: record.flowId, pipelineId: "test_needs_parent", nodeId: "SCAN", subflowKey: null, kind: "fresh" });
    const seed = await store.get(record.flowId);
    // Route straight to PROPOSE without ever running SCAN's own dispatch —
    // its invocation row above exists (resultWrittenAt still null) but no
    // resultJson was ever written, matching a real "dispatched, still
    // running" node.
    await store.setRunning({ flowId: record.flowId, expectedRevision: seed.revision, lastStep: "SCAN", stateJson: { ...seed.stateJson, shortlist: [{ id: "x" }] } });
    await executor.runFlow(record.flowId, "retry");
    // "retry" kind + lastStep:"SCAN" makes the loop resolve SCAN's OWN edge
    // next, not re-enter SCAN — redirect by asserting the parent's own
    // dispatch reached PROPOSE and spawned a genuinely blocked child.
    await flush();

    const parent = await runStore.get(record.flowId);
    // PROPOSE's own mode:"any" fan-out never resumes the parent until its
    // child is terminal — the child itself is what's blocked here.
    const invocationIds = await nodeExecutionsStore.listInvocationIdsForNode(record.flowId, "PROPOSE");
    assert.ok(invocationIds.length >= 1, "PROPOSE must have spawned its one child despite SCAN not being complete — needs: blocking is DRAFT's problem, not PROPOSE's");
    const children = await runStore.listForParentInvocations(invocationIds);
    assert.equal(children.length, 1);
    const childFlowId = children[0].flowId;

    const child = await runStore.get(childFlowId);
    assert.equal(child.status, "blocked", "DRAFT must block rather than dispatch against a still-pending SCAN");
    assert.equal(child.waitJson.kind, "needs-not-ready");
    assert.equal(child.waitJson.needed[0], "SCAN");
    assert.equal(draftCalls.length, 0, "DRAFT's run() must NEVER have been called while its declared need was pending");

    // Now complete SCAN for real and resume the child — DRAFT must re-check
    // and succeed with the now-real value. resume()/runFlow() is a no-op on
    // a still-'blocked' flow by design (see child-resume-trigger.js's own
    // comment on flipToRunningAndResume) — every real caller flips status
    // to 'running' via the store first; mirror that here rather than
    // calling resume() directly.
    await nodeExecutionsStore.writeResult(enterResult.invocationId, { SCAN: { listings: ["late"] } });
    const blockedChild = await store.get(childFlowId);
    await store.setRunning({ flowId: childFlowId, expectedRevision: blockedChild.revision, lastStep: blockedChild.lastStep, stateJson: blockedChild.stateJson });
    await executor.resume(childFlowId);
    const childAfter = await waitForStatus(runStore, childFlowId, (r) => r.status === "succeeded");
    assert.equal(childAfter.status, "succeeded");
    assert.equal(draftCalls.length, 1, "DRAFT's run() must have been called exactly once now that its need resolved");
    assert.deepEqual(draftCalls[0], { listings: ["late"] });
  });
});

// ── entry-based needs: reaching an ancestor's own entry/bookkeeping field
// (runId), not a node output — real pipeline shape: PROPOSE spawns SUBMIT
// as a NESTED subflow (mirrors freelance_upwork-proposal's own
// PROPOSE > SUBMIT nesting), so SUBMIT's own node is 2 hops from the root.

let submitCalls;
function buildNestedPipelines() {
  submitCalls = [];
  const SUBMIT_NODE = {
    kind: "subflow",
    childFlows: true,
    mode: "any",
    selector: (state) => [{ id: "only" }],
    itemKey: "id",
    flow: {
      nodes: {
        SUBMIT_CODE: {
          kind: "code",
          // hops:2 from SUBMIT_CODE's own flow: 1 hop reaches PROPOSE's own
          // flow (its immediate parent), 2 hops reaches the true root —
          // exactly the shape submit-code.js's real state.runId read has.
          needs: { runId: { hops: 2, entry: "runId" } },
          run: async (state, ctx) => {
            const runId = await ctx.needs("runId");
            submitCalls.push(runId);
            return { SUBMIT_CODE: { token: `token-for-${runId}` } };
          },
        },
      },
      edges: { __start__: "SUBMIT_CODE", SUBMIT_CODE: "__end__" },
    },
  };
  const PROPOSE_NODE = {
    kind: "subflow",
    childFlows: true,
    mode: "any",
    selector: (state) => state.shortlist ?? [],
    itemKey: "id",
    flow: {
      nodes: { SUBMIT: SUBMIT_NODE },
      edges: { __start__: "SUBMIT", SUBMIT: "__end__" },
    },
  };
  return {
    test_needs_nested: {
      id: "test_needs_nested",
      state: {},
      nodes: { PROPOSE: PROPOSE_NODE },
      edges: { __start__: "PROPOSE", PROPOSE: "__end__" },
    },
    "test_needs_nested::PROPOSE": {
      id: "test_needs_nested::PROPOSE",
      state: {},
      nodes: PROPOSE_NODE.flow.nodes,
      edges: PROPOSE_NODE.flow.edges,
    },
    "test_needs_nested::PROPOSE::SUBMIT": {
      id: "test_needs_nested::PROPOSE::SUBMIT",
      state: {},
      nodes: SUBMIT_NODE.flow.nodes,
      edges: SUBMIT_NODE.flow.edges,
    },
  };
}

describe('Option B "needs:" — entry-based hops:2 read (root\'s own runId, real PROPOSE>SUBMIT nesting shape)', () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("SUBMIT_CODE, nested two levels deep, resolves the TRUE ROOT's own runId — not PROPOSE's inherited copy, not undefined", async () => {
    harness = await makeHarness(buildNestedPipelines());
    const { executor, runStore, store } = harness;

    const record = await executor.start("test_needs_nested", {}, { kind: "test" });
    const rootFlow = await runStore.get(record.flowId);
    const realRunId = rootFlow.stateJson.runId;
    assert.ok(realRunId, "sanity: the root flow really does have its own runId, set once at start()");

    const seed = await store.get(record.flowId);
    await store.setRunning({ flowId: record.flowId, expectedRevision: seed.revision, lastStep: seed.lastStep, stateJson: { ...seed.stateJson, shortlist: [{ id: "x" }] } });
    await executor.runFlow(record.flowId, "fresh");
    await flush(120); // two levels of fire-and-forget child dispatch to settle

    const parent = await runStore.get(record.flowId);
    assert.equal(parent.status, "succeeded");
    assert.equal(submitCalls.length, 1, "SUBMIT_CODE's run() must have been called exactly once");
    assert.equal(submitCalls[0], realRunId, "resolved value must be the TRUE ROOT's runId, reached by walking 2 hops from the doubly-nested SUBMIT_CODE");
  });
});

// ── hops:0 (same-flow). Two
// shapes: nodeId-based (a sibling's own resultJson, no walk at all) and
// config-based (pipeline-declared static `state:`, read live off the
// pipeline definition, never off stateJson). The config case reproduces
// the actual system_memory-synthesis regression: a pipeline-declared field
// (sourceFilter) that is NOT a node output and NOT in ENTRY_FIELDS, so it's
// dropped from stateJson the moment the flow persists past its first node
// — confirmed here by asserting the persisted row really doesn't carry it,
// while ctx.needs() still resolves it correctly regardless.

let sameFlowCalls;
function buildSameFlowPipeline() {
  sameFlowCalls = [];
  return {
    test_needs_hop0: {
      id: "test_needs_hop0",
      state: { sourceFilter: { includeAgentSelfRediscovery: true } },
      nodes: {
        // nodeId-based needs always unwraps via parsed[nodeId]
        // (needs-resolver.js:135) — a node must wrap its own return as
        // {[ownNodeId]: value} to be readable that way, regardless of
        // hops. WALK does that here; EXTRACT does not (system_memory-
        // synthesis's real WALK returns flat fields instead, which is
        // exactly why its own EXTRACT migration left `sessions` on a
        // plain state.sessions read rather than nodeId-based needs).
        WALK: { kind: "code", run: () => ({ WALK: { sessions: ["s1", "s2"] } }) },
        EXTRACT: {
          kind: "code",
          needs: {
            walkResult: { hops: 0, nodeId: "WALK" },
            sourceFilter: { hops: 0, config: "sourceFilter" },
          },
          run: async (state, ctx) => {
            const [walkResult, sourceFilter] = await Promise.all([ctx.needs("walkResult"), ctx.needs("sourceFilter")]);
            sameFlowCalls.push({ sessions: walkResult.sessions, sourceFilter });
            return { extracted: true };
          },
        },
      },
      edges: { __start__: "WALK", WALK: "EXTRACT", EXTRACT: "__end__" },
    },
  };
}

describe('Option B "needs:" — hops:0 (same-flow), both nodeId and config shapes', () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("EXTRACT resolves WALK's real sessions AND the pipeline's declared sourceFilter, even though neither survives in the persisted stateJson after WALK", async () => {
    harness = await makeHarness(buildSameFlowPipeline());
    const { executor, runStore } = harness;

    const record = await executor.start("test_needs_hop0", {}, { kind: "test" });
    await executor.runFlow(record.flowId, "fresh");
    await flush();

    const flow = await runStore.get(record.flowId);
    assert.equal(flow.status, "succeeded");
    assert.equal(sameFlowCalls.length, 1, "EXTRACT's run() must have been called exactly once");
    assert.deepEqual(sameFlowCalls[0].sessions, ["s1", "s2"], "resolved WALK's own resultJson via nodeId-based needs at hops:0, no walk");
    assert.deepEqual(sameFlowCalls[0].sourceFilter, { includeAgentSelfRediscovery: true }, "resolved the pipeline-declared config live off the definition, not off stateJson");

    // Regression guard: confirm sourceFilter genuinely is NOT in the
    // persisted stateJson by the time EXTRACT dispatches — proving
    // ctx.needs() is doing real work here, not just reading a field that
    // happened to survive anyway (which is exactly how the original bug
    // went unnoticed for the implicit `state.sourceFilter` read it replaced).
    assert.equal(flow.stateJson?.sourceFilter, undefined, "sourceFilter must be absent from persisted stateJson — the whole point of routing this through config-type needs() instead");
  });
});
