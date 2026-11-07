/**
 * Unit tests for state/state-store.js — currently scoped to the
 * resume-trigger wiring: finish/skip/fail/cancel must call maybeResumeParent
 * when a terminal child completes the last non-terminal sibling under its loop
 * node, and must stay a safe no-op when nodeExecutionsStore/resumeParentRef
 * aren't wired in (today's real index.js call sites always wire them, but
 * every other test harness in this repo constructs createStateStore without
 * them, so that path must never throw).
 */
import { test, describe, afterEach, after } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createStateStore } from "../../../state/state-store.js";
import { resolveOrCreateChild } from "../../../engine/spawn-child.js";
import { gateEmitter, flowEmitter } from "../../../events.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

// In-memory fake for node-executions — matches the surface used by
// resolveOrCreateChild (findOrCreateInvocation) and maybeResumeParent
// (listInvocationIdsForNode, getInvocation). node-executions.js is not yet
// migrated to async, so using the real store here would trigger a dangling
// async error from openDb() returning a Promise into sync code.
function makeFakeNodeExecutions() {
  const byKey = new Map();  // `${flowId}:${nodeId}:${subflowKey}` → record
  const byId  = new Map();  // invocationId → record
  const byFN  = new Map();  // `${flowId}:${nodeId}` → invocationId[]

  return {
    findOrCreateInvocation({ flowId, pipelineId, nodeId, subflowKey }) {
      const key = `${flowId}:${nodeId}:${subflowKey}`;
      if (!byKey.has(key)) {
        const id = randomUUID();
        const rec = { id, flowId, nodeId, subflowKey };
        byKey.set(key, rec);
        byId.set(id, rec);
        const fnKey = `${flowId}:${nodeId}`;
        if (!byFN.has(fnKey)) byFN.set(fnKey, []);
        byFN.get(fnKey).push(id);
      }
      return byKey.get(key).id;
    },
    listInvocationIdsForNode(flowId, nodeId) {
      return byFN.get(`${flowId}:${nodeId}`) ?? [];
    },
    getInvocation(id) { return byId.get(id) ?? null; },
    queryErrorsByFlow() { return []; },
    close() {},
  };
}

async function makeHarness({ withResumeHook = true } = {}) {
  const runStore = await createRunStore({ dbPath: tmpPath("state-store-runs") });
  const nodeExecutionsStore = makeFakeNodeExecutions();
  const resumed = [];
  const resumeParentRef = { resume: withResumeHook ? async (flowId) => { resumed.push(flowId); } : null };
  const store = createStateStore({}, "test-owner", {
    runStore, nodeExecutionsStore, resumeParentRef, logger: { warn: () => {} },
  });
  return { runStore, nodeExecutionsStore, store, resumed };
}

// process.nextTick-flushing helper — maybeResumeParent runs as a fire-and-
// forget promise inside triggerParentResume, so tests need one microtask
// turn before asserting on its side effects.
function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

describe("createStateStore — create() pipelineId integrity", () => {
  let runStore;
  afterEach(() => { runStore?.close(); });

  // Regression test for a real incident: the child-spawn path built
  // initialState from a deep clone of the PARENT's own state, which already
  // carried its own `pipelineId` field. create() originally spread
  // `{ pipelineId, ...initialState }` — initialState's own pipelineId won,
  // silently redirecting the new flow onto the WRONG pipeline. Caught by
  // live end-to-end testing (tens of thousands of runaway rows in minutes),
  // never shipped — but the underlying footgun in create() itself needed
  // fixing, not just the one caller that happened to trip it.
  test("the intended pipelineId always wins, even when initialState carries its own (different) pipelineId field", async () => {
    runStore = await createRunStore({ dbPath: tmpPath("create-pid-runs") });
    const store = createStateStore({}, "test-owner", { runStore });

    const record = await store.create({
      pipelineId: "intended_pipeline",
      initialState: { pipelineId: "wrong_pipeline_from_cloned_parent_state", foo: "bar" },
    });

    assert.equal(record.pipelineId, "intended_pipeline");
    assert.equal(record.stateJson.pipelineId, "intended_pipeline");
    assert.equal(record.stateJson.foo, "bar", "other initialState fields still come through");

    const persisted = await store.get(record.flowId);
    assert.equal(persisted.pipelineId, "intended_pipeline", "row's own pipeline_id column is correct");
    assert.equal(persisted.stateJson.pipelineId, "intended_pipeline", "stateJson.pipelineId is what runFlow() actually reads to resolve the pipeline definition");
  });
});

describe("createStateStore — resume-trigger wiring", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  // A real flow_runs row, blocked (as dispatchLoopChildren would leave it)
  // — maybeResumeParent's flipToRunningAndResume needs a genuine row to
  // flip back to 'running' before calling resume() (resume() is just
  // runFlow(), whose own first check returns immediately for a flow still
  // in status='blocked').
  async function makeBlockedParent(store) {
    const parent = await store.create({ pipelineId: "p", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    return parent.flowId;
  }

  test("finish() on the last non-terminal sibling wakes the parent", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, resumed } = harness;
    const parentFlowId = await makeBlockedParent(store);
    const { childFlowId: child1 } = await resolveOrCreateChild({
      flowId: parentFlowId, pipelineId: "p", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "p::LOOP", initialState: {}, store, runStore, nodeExecutionsStore,
    });
    const { childFlowId: child2 } = await resolveOrCreateChild({
      flowId: parentFlowId, pipelineId: "p", nodeId: "LOOP", subflowKey: "item-2",
      childPipelineId: "p::LOOP", initialState: {}, store, runStore, nodeExecutionsStore,
    });

    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });
    await flush();
    assert.equal(resumed.length, 0, "one sibling still running — must not resume yet");

    await store.finish({ flowId: child2, expectedRevision: 0, stateJson: {} });
    await flush();
    assert.deepEqual(resumed, [parentFlowId]);
    assert.equal((await runStore.get(parentFlowId)).status, "running", "parent must be flipped back to running");
  });

  test("fail() also fires the trigger (terminal, not just success)", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, resumed } = harness;
    const parentFlowId = await makeBlockedParent(store);
    const { childFlowId: child1 } = await resolveOrCreateChild({
      flowId: parentFlowId, pipelineId: "p", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "p::LOOP", initialState: {}, store, runStore, nodeExecutionsStore,
    });

    await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom" });
    await flush();
    assert.deepEqual(resumed, [parentFlowId]);
  });

  test("cancel() also fires the trigger", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, resumed } = harness;
    const parentFlowId = await makeBlockedParent(store);
    const { childFlowId: child1 } = await resolveOrCreateChild({
      flowId: parentFlowId, pipelineId: "p", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "p::LOOP", initialState: {}, store, runStore, nodeExecutionsStore,
    });

    await store.cancel({ flowId: child1, stateJson: {} });
    await flush();
    assert.deepEqual(resumed, [parentFlowId]);
  });

  test("cancel({reason}) threads through to statusReason on the run row", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parentFlowId = await makeBlockedParent(store);
    const { childFlowId: child1 } = await resolveOrCreateChild({
      flowId: parentFlowId, pipelineId: "p", nodeId: "LOOP", subflowKey: "item-1",
      childPipelineId: "p::LOOP", initialState: {}, store, runStore, nodeExecutionsStore,
    });

    await store.cancel({ flowId: child1, stateJson: {}, reason: "cancelled by operator" });
    assert.equal((await runStore.get(child1)).statusReason, "cancelled by operator");
  });

  test("finish() on an ordinary (non-child) flow never touches the trigger", async () => {
    harness = await makeHarness();
    const { store, resumed } = harness;
    const flow = await store.create({ pipelineId: "some_pipe", initialState: {} });

    await store.finish({ flowId: flow.flowId, expectedRevision: 0, stateJson: {} });
    await flush();
    assert.equal(resumed.length, 0);
  });

  test("stays a safe no-op when nodeExecutionsStore/resumeParentRef aren't wired in", async () => {
    // harness NOT used here — we deliberately omit nodeExecutionsStore/resumeParentRef
    const runStore = await createRunStore({ dbPath: tmpPath("state-store-bare-runs") });
    try {
      const store = createStateStore({}, "test-owner", { runStore });
      const flow = await store.create({ pipelineId: "some_pipe", initialState: {} });
      await assert.doesNotReject(() => store.finish({ flowId: flow.flowId, expectedRevision: 0, stateJson: {} }));
      await flush();
    } finally {
      runStore.close();
    }
  });
});

// ── triggerAdmissionRelease — promotion wake routing ──────────────────────────
//
// wakePromotedRelease (engine/admission-promotion-resume.js) only drives
// promotions of a kind the engine admits (flow-start/child-dispatch). These
// tests exercise that routing through the real public API (store.finish),
// not by calling the private triggerAdmissionRelease directly.
describe("createStateStore — triggerAdmissionRelease promotion routing", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); });

  // A fake admissionQueueStore whose releaseByFlow reports a caller-supplied
  // promotion only for kind:"child-dispatch" (triggerAdmissionRelease's own
  // Promise.all releases both "flow-start" and "child-dispatch" for the
  // finishing flow, and picks whichever result carries a promotion — arbitrary
  // which of the two we attach it to for these tests).
  function makeFakeAdmissionQueueStore(promoted) {
    return {
      async releaseByFlow({ kind }) {
        if (kind === "child-dispatch") return { released: true, promoted };
        return { released: false, promoted: null };
      },
    };
  }

  async function makeRoutingHarness(promoted) {
    const runStore = await createRunStore({ dbPath: tmpPath("state-store-routing-runs") });
    const resumed = [];
    const warnings = [];
    const resumeParentRef = { resume: async (flowId) => { resumed.push(flowId); } };
    const admissionQueueStore = makeFakeAdmissionQueueStore(promoted);
    const store = createStateStore({}, "test-owner", {
      runStore, resumeParentRef, admissionQueueStore, logger: { warn: (msg) => warnings.push(msg) },
    });
    return { runStore, store, resumed, warnings };
  }

  test("a promotion of a kind the engine doesn't admit (a legacy agent-dispatch row) is never resumed — no resume call, no misleading warning", async () => {
    const otherFlowId = randomUUID();
    harness = await makeRoutingHarness({ id: randomUUID(), kind: "agent-dispatch", flowId: otherFlowId, nodeId: "SCORE_LLM" });
    const { store, resumed, warnings } = harness;
    const flow = await store.create({ pipelineId: "p", initialState: {} });

    await store.finish({ flowId: flow.flowId, expectedRevision: 0, stateJson: {} });
    await flush();

    assert.equal(resumed.includes(otherFlowId), false, "a non-run kind has no flow run to flip and resume");
    assert.equal(warnings.length, 0, "a skipped legacy promotion must never look like a failed resume");
  });

  test("a child-dispatch promotion pointing at a real blocked flow IS routed through and resumes it", async () => {
    const runStore = await createRunStore({ dbPath: tmpPath("state-store-routing-runs") });
    harness = { runStore };
    // Seed the promotion TARGET first, via its own admission-free store —
    // this flow must already exist and be blocked before the store-under-
    // test's admissionQueueStore starts reporting it as promoted.
    const seedStore = createStateStore({}, "test-owner", { runStore });
    const target = await seedStore.create({ pipelineId: "p2", initialState: {} });
    await seedStore.setWaiting({
      flowId: target.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });

    const resumed = [];
    const warnings = [];
    const admissionQueueStore = makeFakeAdmissionQueueStore({ id: randomUUID(), kind: "child-dispatch", flowId: target.flowId, nodeId: "LOOP" });
    const store = createStateStore({}, "test-owner", {
      runStore, resumeParentRef: { resume: async (flowId) => resumed.push(flowId) },
      admissionQueueStore, logger: { warn: (msg) => warnings.push(msg) },
    });
    const flow = await store.create({ pipelineId: "p", initialState: {} });

    await store.finish({ flowId: flow.flowId, expectedRevision: 0, stateJson: {} });
    await flush();

    assert.deepEqual(resumed, [target.flowId]);
    assert.equal(warnings.length, 0);
    assert.equal((await runStore.get(target.flowId)).status, "running");
  });

  test("a child-dispatch promotion that genuinely fails to resume (target flow doesn't exist) still logs the warning — real leaks stay diagnosable", async () => {
    const missingFlowId = randomUUID();
    harness = await makeRoutingHarness({ id: randomUUID(), kind: "child-dispatch", flowId: missingFlowId, nodeId: "LOOP" });
    const { store, resumed, warnings } = harness;
    const flow = await store.create({ pipelineId: "p", initialState: {} });

    await store.finish({ flowId: flow.flowId, expectedRevision: 0, stateJson: {} });
    await flush();

    assert.equal(resumed.length, 0);
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /did not resume: parent_not_found/);
  });
});

// ── Structured emitter wiring ─────────────────────────────────────────────────

describe("createStateStore — structured emitters", () => {
  let runStore;
  after(() => { runStore?.close(); });

  async function makeSimpleHarness() {
    runStore = await createRunStore({ dbPath: tmpPath("emitter-runs") });
    const store = createStateStore({}, "test-owner", { runStore, logger: { warn: () => {} } });
    const flow = await store.create({ pipelineId: "team_test-pipe", initialState: {} });
    return { store, flowId: flow.flowId };
  }

  test("setWaiting with kind=human-gate emits gate.opened on gateEmitter", async () => {
    const { store, flowId } = await makeSimpleHarness();
    const items = [{ itemId: "item-1", requestPayload: { title: "Approve?" } }];
    let emitted = null;
    gateEmitter.once("gate.opened", (p) => { emitted = p; });
    await store.setWaiting({
      flowId, expectedRevision: 0, lastStep: "PROPOSE",
      stateJson: { pipelineId: "team_test-pipe" },
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main", items },
      blockingNodeId: "PROPOSE", statusReason: null,
    });
    await flush();
    assert.ok(emitted !== null, "gate.opened must have fired");
    assert.equal(emitted.flowId, flowId);
    assert.deepEqual(emitted.items, items);
    assert.ok(typeof emitted.updatedAt === "number" && emitted.updatedAt > 0, "gate.opened must carry updatedAt");
    gateEmitter.removeAllListeners("gate.opened");
  });

  test("setWaiting with non-gate kind does NOT emit gate.opened", async () => {
    const { store, flowId } = await makeSimpleHarness();
    let fired = false;
    gateEmitter.once("gate.opened", () => { fired = true; });
    await store.setWaiting({
      flowId, expectedRevision: 0, lastStep: "WAIT",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" },
      blockingNodeId: "LOOP", statusReason: null,
    });
    await flush();
    assert.equal(fired, false, "gate.opened must not fire for non-human-gate waits");
    gateEmitter.removeAllListeners("gate.opened");
  });

  test("setRunning emits flow.step.changed on flowEmitter", async () => {
    const { store, flowId } = await makeSimpleHarness();
    let emitted = null;
    flowEmitter.once("flow.step.changed", (p) => { emitted = p; });
    await store.setRunning({
      flowId, expectedRevision: 0, lastStep: "EXECUTE",
      stateJson: { pipelineId: "team_test-pipe" },
    });
    await flush();
    assert.ok(emitted !== null, "flow.step.changed must have fired");
    assert.equal(emitted.flowId, flowId);
    assert.equal(emitted.lastStep, "EXECUTE");
    assert.equal(emitted.status, "running");
    flowEmitter.removeAllListeners("flow.step.changed");
  });

  test("finish() emits flow.terminal with status=succeeded", async () => {
    const { store, flowId } = await makeSimpleHarness();
    let emitted = null;
    flowEmitter.once("flow.terminal", (p) => { emitted = p; });
    await store.finish({ flowId, expectedRevision: 0, stateJson: {} });
    await flush();
    assert.ok(emitted !== null, "flow.terminal must have fired");
    assert.equal(emitted.flowId, flowId);
    assert.equal(emitted.status, "succeeded");
    flowEmitter.removeAllListeners("flow.terminal");
  });

  test("fail() emits flow.terminal with status=failed", async () => {
    const { store, flowId } = await makeSimpleHarness();
    let emitted = null;
    flowEmitter.once("flow.terminal", (p) => { emitted = p; });
    await store.fail({ flowId, expectedRevision: 0, stateJson: {}, statusReason: "oops" });
    await flush();
    assert.equal(emitted?.status, "failed");
    flowEmitter.removeAllListeners("flow.terminal");
  });

  test("cancel() emits flow.terminal with status=cancelled", async () => {
    const { store, flowId } = await makeSimpleHarness();
    let emitted = null;
    flowEmitter.once("flow.terminal", (p) => { emitted = p; });
    await store.cancel({ flowId, stateJson: {} });
    await flush();
    assert.equal(emitted?.status, "cancelled");
    flowEmitter.removeAllListeners("flow.terminal");
  });
});
