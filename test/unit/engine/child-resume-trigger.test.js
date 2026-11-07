/**
 * Unit tests for engine/child-resume-trigger.js — the "wake the parent
 * once every sibling child is terminal" mechanism. Uses real sqlite-backed
 * run-store.js/node-executions.js instances (tmp files) plus
 * engine/spawn-child.js to build genuine parent/child pairs, since the
 * logic under test is a join across both tables' real schemas.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { resolveOrCreateChild } from "../../../engine/spawn-child.js";
import { maybeResumeParent, areSiblingsAllTerminal, flipToRunningAndResume, admitAndSetRunning, subflowOutcomeSatisfied } from "../../../engine/child-resume-trigger.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { drainDetachedResumes } from "../../../engine/admission-promotion-resume.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const runStore = await createRunStore({ dbPath: tmpPath("resume-trigger-runs") });
  const nodeExecutionsStore = await createNodeExecutionsStore(tmpPath("resume-trigger-execs"));
  const store = createStateStore({}, "test-owner", { runStore });
  return { runStore, nodeExecutionsStore, store };
}

// Spawns `count` children of a synthetic parent flow under one loop node
// ("LOOP"), returning their childFlowIds.
async function spawnChildren(harness, parentFlowId, count) {
  const { runStore, nodeExecutionsStore, store } = harness;
  const ids = [];
  for (let i = 0; i < count; i++) {
    const { childFlowId } = await resolveOrCreateChild({
      flowId: parentFlowId, pipelineId: "parent_pipe", nodeId: "LOOP",
      subflowKey: `item-${i}`, childPipelineId: "parent_pipe::LOOP", initialState: { i },
      store, runStore, nodeExecutionsStore,
    });
    ids.push(childFlowId);
  }
  return ids;
}

describe("areSiblingsAllTerminal", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("false when no invocations exist for the group", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore } = harness;
    const result = await areSiblingsAllTerminal({ flowId: randomUUID(), nodeId: "LOOP", runStore, nodeExecutionsStore });
    assert.equal(result.allTerminal, false);
    assert.equal(result.reason, "no_siblings");
  });

  test("false when a sibling is still running", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parentFlowId = randomUUID();
    const [child1, child2] = await spawnChildren(harness, parentFlowId, 2);
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });
    // child2 left running

    const result = await areSiblingsAllTerminal({ flowId: parentFlowId, nodeId: "LOOP", runStore, nodeExecutionsStore });
    assert.equal(result.allTerminal, false);
    assert.equal(result.reason, "siblings_pending");
  });

  test("true when every sibling has gone terminal (mixed outcomes)", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parentFlowId = randomUUID();
    const [child1, child2, child3] = await spawnChildren(harness, parentFlowId, 3);
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });
    await store.fail({ flowId: child2, expectedRevision: 0, stateJson: {}, statusReason: "boom" });
    await store.skip({ flowId: child3, expectedRevision: 0, stateJson: {} });

    const result = await areSiblingsAllTerminal({ flowId: parentFlowId, nodeId: "LOOP", runStore, nodeExecutionsStore });
    assert.equal(result.allTerminal, true);
  });
});

describe("maybeResumeParent", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("resumes the parent when the finishing child is the last non-terminal sibling", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    // A real flow_runs row, blocked (as dispatchLoopChildren would leave it)
    // — maybeResumeParent must flip this back to 'running' before calling
    // resume(), since resume() is just runFlow() and runFlow's own first
    // check returns immediately for a still-blocked flow.
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    const parentFlowId = parent.flowId;
    await store.setWaiting({
      flowId: parentFlowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1, child2] = await spawnChildren(harness, parentFlowId, 2);
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });
    await store.finish({ flowId: child2, expectedRevision: 0, stateJson: {} });

    let resumedWith = null;
    const resume = async (flowId) => { resumedWith = flowId; };

    const result = await maybeResumeParent({ flowId: child2, runStore, nodeExecutionsStore, resume, logger: null });

    assert.equal(result.resumed, true);
    assert.equal(result.parentFlowId, parentFlowId);
    assert.equal(resumedWith, parentFlowId);
    assert.equal((await runStore.get(parentFlowId)).status, "running", "parent must be flipped back to running before resume() is called");
  });

  test("does not resume while a sibling is still pending", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parentFlowId = randomUUID();
    const [child1] = await spawnChildren(harness, parentFlowId, 2); // second sibling left running
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });

    let called = false;
    const resume = async () => { called = true; };

    const result = await maybeResumeParent({ flowId: child1, runStore, nodeExecutionsStore, resume, logger: null });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "siblings_pending");
    assert.equal(called, false);
  });

  test("no-ops for a flow that isn't a loop-spawned child", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const rootFlow = await store.create({ pipelineId: "some_pipe", initialState: {} });

    let called = false;
    const resume = async () => { called = true; };

    const result = await maybeResumeParent({ flowId: rootFlow.flowId, runStore, nodeExecutionsStore, resume, logger: null });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "not_a_child");
    assert.equal(called, false);
  });

  test("no-ops when no resume function is provided (dormant hook)", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore } = harness;
    const result = await maybeResumeParent({ flowId: randomUUID(), runStore, nodeExecutionsStore, resume: null, logger: null });
    assert.equal(result.resumed, false);
    assert.equal(result.reason, "no_resume_fn");
  });

  test("catches and reports a throwing resume function rather than throwing itself", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    const parentFlowId = parent.flowId;
    await store.setWaiting({
      flowId: parentFlowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1] = await spawnChildren(harness, parentFlowId, 1);
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });

    const resume = async () => { throw new Error("resume exploded"); };
    const warnings = [];
    const logger = { warn: (msg) => warnings.push(msg) };

    const result = await maybeResumeParent({ flowId: child1, runStore, nodeExecutionsStore, resume, logger });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "resume_threw");
    assert.equal(warnings.length, 1);
  });
});

describe("subflowOutcomeSatisfied", () => {
  test('mode:"any" — satisfied with at least one success, out of a mixed batch', () => {
    const children = [{ status: "failed" }, { status: "succeeded" }, { status: "failed" }];
    assert.equal(subflowOutcomeSatisfied("any", children), true);
  });

  test('mode:"any" — not satisfied when every child failed', () => {
    const children = [{ status: "failed" }, { status: "cancelled" }];
    assert.equal(subflowOutcomeSatisfied("any", children), false);
  });

  test('mode:"all" — satisfied only when every child succeeded', () => {
    assert.equal(subflowOutcomeSatisfied("all", [{ status: "succeeded" }, { status: "succeeded" }]), true);
    assert.equal(subflowOutcomeSatisfied("all", [{ status: "succeeded" }, { status: "failed" }]), false);
  });

  test("unrecognized/missing mode fails open (never blocks a resume)", () => {
    assert.equal(subflowOutcomeSatisfied(undefined, [{ status: "failed" }]), true);
    assert.equal(subflowOutcomeSatisfied("bogus", [{ status: "failed" }]), true);
  });
});

// Stage 1.5: a subflow node's
// `mode` decides whether terminal children resume the parent or fail it.
describe("maybeResumeParent — Stage 1.5 mode enforcement", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  function pipelinesRefWithMode(mode) {
    return { value: new Map([["parent_pipe", { nodes: { LOOP: { mode } } }]]) };
  }

  test('mode:"any" — every child failed: fails the parent via failFlow, does not resume', async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1, child2] = await spawnChildren(harness, parent.flowId, 2);
    await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom1" });
    await store.fail({ flowId: child2, expectedRevision: 0, stateJson: {}, statusReason: "boom2" });

    let resumeCalled = false;
    const resume = async () => { resumeCalled = true; };
    const failFlowCalls = [];
    const failFlow = (args) => { failFlowCalls.push(args); return store.fail(args); };

    const result = await maybeResumeParent({
      flowId: child2, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("any"), resume, failFlow, logger: null,
    });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "mode_not_satisfied");
    assert.equal(result.failed, true);
    assert.equal(resumeCalled, false, "resume() must not be called when mode is not satisfied");
    assert.equal(failFlowCalls.length, 1);
    assert.match(failFlowCalls[0].statusReason, /mode:"any"/);
    assert.equal((await runStore.get(parent.flowId)).status, "failed");
  });

  test('mode:"any" — at least one child succeeded: resumes normally, failFlow never called', async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1, child2] = await spawnChildren(harness, parent.flowId, 2);
    await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom" });
    await store.finish({ flowId: child2, expectedRevision: 0, stateJson: {} });

    let resumedWith = null;
    const resume = async (flowId) => { resumedWith = flowId; };
    let failFlowCalled = false;
    const failFlow = () => { failFlowCalled = true; };

    const result = await maybeResumeParent({
      flowId: child2, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("any"), resume, failFlow, logger: null,
    });

    assert.equal(result.resumed, true);
    assert.equal(resumedWith, parent.flowId);
    assert.equal(failFlowCalled, false);
  });

  test('mode:"all" — one child failed among successes: fails the parent', async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1, child2] = await spawnChildren(harness, parent.flowId, 2);
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });
    await store.fail({ flowId: child2, expectedRevision: 0, stateJson: {}, statusReason: "boom" });

    let resumeCalled = false;
    const resume = async () => { resumeCalled = true; };
    const failFlow = (args) => store.fail(args);

    const result = await maybeResumeParent({
      flowId: child2, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("all"), resume, failFlow, logger: null,
    });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "mode_not_satisfied");
    assert.equal(resumeCalled, false);
    assert.equal((await runStore.get(parent.flowId)).status, "failed");
  });

  test('mode:"all" — every child succeeded: resumes normally', async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1] = await spawnChildren(harness, parent.flowId, 1);
    await store.finish({ flowId: child1, expectedRevision: 0, stateJson: {} });

    let resumedWith = null;
    const resume = async (flowId) => { resumedWith = flowId; };

    const result = await maybeResumeParent({
      flowId: child1, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("all"), resume, logger: null,
    });

    assert.equal(result.resumed, true);
    assert.equal(resumedWith, parent.flowId);
  });

  // A mode-not-satisfied failure gets a REAL failure_events row, not just a
  // bare flow_runs.status
  // write. Note: `invocation` inside maybeResumeParent is the CHILD's own
  // PER-ITEM invocation (spawn-child.js keys it by the item's subflowKey) —
  // recordEngineFailure must target the subflow node's OWN dispatch
  // invocation (subflowKey:null) instead, which these tests create
  // explicitly via nodeExecutionsStore.write() to mimic a real dispatch.
  describe("maybeResumeParent — records a real failure for mode-not-satisfied", () => {
    test("calls failureStore.recordEngineFailure against the subflow node's OWN invocation/execution, not the per-item one", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store } = harness;
      const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
      const ownWrite = await nodeExecutionsStore.write({
        flowId: parent.flowId, nodeId: "LOOP", enteredAt: 100, exitedAt: 110, kind: "fresh",
      });
      await nodeExecutionsStore.flush(); // write() defers through batch-writer — drain before maybeResumeParent reads it back
      await store.setWaiting({
        flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
        stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
      });
      const [child1, child2] = await spawnChildren(harness, parent.flowId, 2);
      await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom1" });
      await store.fail({ flowId: child2, expectedRevision: 0, stateJson: {}, statusReason: "boom2" });

      const recordCalls = [];
      const failureStore = { recordEngineFailure: async (args) => { recordCalls.push(args); return { recorded: 1 }; } };
      const failFlow = (args) => store.fail(args);
      const resume = async () => {};

      const result = await maybeResumeParent({
        flowId: child2, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("any"),
        resume, failFlow, failureStore, logger: null,
      });

      assert.equal(result.reason, "mode_not_satisfied");
      assert.equal(recordCalls.length, 1);
      assert.equal(recordCalls[0].flowId, parent.flowId);
      assert.equal(recordCalls[0].pipelineId, "parent_pipe");
      assert.equal(recordCalls[0].failureNode, "LOOP");
      assert.match(recordCalls[0].message, /mode:"any"/);
      assert.equal(recordCalls[0].errorType, "subflow-mode-not-satisfied");
      assert.equal(recordCalls[0].executionId, ownWrite.executionId, "the LOOP node's own execution, not a per-item one");
      assert.notEqual(recordCalls[0].invocationId, undefined);
    });

    test("does not call recordEngineFailure when failFlow's CAS write doesn't apply", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store } = harness;
      const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
      await nodeExecutionsStore.write({ flowId: parent.flowId, nodeId: "LOOP", enteredAt: 100, exitedAt: 110, kind: "fresh" });
      await store.setWaiting({
        flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
        stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
      });
      const [child1, child2] = await spawnChildren(harness, parent.flowId, 2);
      await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom1" });
      await store.fail({ flowId: child2, expectedRevision: 0, stateJson: {}, statusReason: "boom2" });

      const recordCalls = [];
      const failureStore = { recordEngineFailure: async (args) => { recordCalls.push(args); return { recorded: 1 }; } };
      const failFlow = async () => ({ applied: false, code: "revision_mismatch" });
      const resume = async () => {};

      const result = await maybeResumeParent({
        flowId: child2, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("any"),
        resume, failFlow, failureStore, logger: null,
      });

      assert.equal(result.failed, false);
      assert.equal(recordCalls.length, 0, "no failure_events row for a status write that never actually applied");
    });

    test("no failureStore provided — mode-not-satisfied still fails the parent, no throw (optional dependency)", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store } = harness;
      const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
      await nodeExecutionsStore.write({ flowId: parent.flowId, nodeId: "LOOP", enteredAt: 100, exitedAt: 110, kind: "fresh" });
      await store.setWaiting({
        flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
        stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
      });
      const [child1, child2] = await spawnChildren(harness, parent.flowId, 2);
      await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom1" });
      await store.fail({ flowId: child2, expectedRevision: 0, stateJson: {}, statusReason: "boom2" });

      const failFlow = (args) => store.fail(args);
      const result = await maybeResumeParent({
        flowId: child2, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("any"),
        resume: async () => {}, failFlow, logger: null, // no failureStore
      });

      assert.equal(result.reason, "mode_not_satisfied");
      assert.equal((await runStore.get(parent.flowId)).status, "failed");
    });
  });

  test("no pipelinesRef provided — falls back to unconditional resume (pre-Stage-1.5 behavior)", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1] = await spawnChildren(harness, parent.flowId, 1);
    await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom" });

    let resumedWith = null;
    const resume = async (flowId) => { resumedWith = flowId; };

    const result = await maybeResumeParent({ flowId: child1, runStore, nodeExecutionsStore, resume, logger: null });

    assert.equal(result.resumed, true, "no pipelinesRef means mode can't be looked up — resumes unconditionally, same as before Stage 1.5");
    assert.equal(resumedWith, parent.flowId);
  });

  test("mode not satisfied but no failFlow provided — logs a warning and resumes anyway (fails open, not closed)", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "children pending",
    });
    const [child1] = await spawnChildren(harness, parent.flowId, 1);
    await store.fail({ flowId: child1, expectedRevision: 0, stateJson: {}, statusReason: "boom" });

    let resumedWith = null;
    const resume = async (flowId) => { resumedWith = flowId; };
    const warnings = [];
    const logger = { warn: (msg) => warnings.push(msg) };

    const result = await maybeResumeParent({
      flowId: child1, runStore, nodeExecutionsStore, pipelinesRef: pipelinesRefWithMode("all"), resume, logger,
    });

    assert.equal(result.resumed, true);
    assert.equal(resumedWith, parent.flowId);
    assert.equal(warnings.length, 1);
  });
});

describe("flipToRunningAndResume", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("flips a blocked flow to running, clears wait_json, then calls resume()", async () => {
    harness = await makeHarness();
    const { runStore, store } = harness;
    const parent = await store.create({ pipelineId: "p", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "LOOP" }, statusReason: "x",
    });

    let resumedWith = null;
    const result = await flipToRunningAndResume({
      parentFlowId: parent.flowId, runStore, resume: async (id) => { resumedWith = id; }, logger: null,
    });

    assert.equal(result.resumed, true);
    assert.equal(resumedWith, parent.flowId);
    const row = await runStore.get(parent.flowId);
    assert.equal(row.status, "running");
    assert.equal(row.waitJson, null);
    assert.equal(row.lastStep, "START", "lastStep is preserved so runFlow() re-resolves the same edge");
  });

  test("returns parent_not_found for a flowId with no flow_runs row", async () => {
    harness = await makeHarness();
    const { runStore } = harness;
    let called = false;
    const result = await flipToRunningAndResume({
      parentFlowId: randomUUID(), runStore, resume: async () => { called = true; }, logger: null,
    });
    assert.equal(result.resumed, false);
    assert.equal(result.reason, "parent_not_found");
    assert.equal(called, false);
  });

  test("returns flip_failed and does not call resume() when the parent is already terminal", async () => {
    harness = await makeHarness();
    const { runStore, store } = harness;
    const parent = await store.create({ pipelineId: "p", initialState: {} });
    await store.finish({ flowId: parent.flowId, expectedRevision: 0, stateJson: {} });

    let called = false;
    const result = await flipToRunningAndResume({
      parentFlowId: parent.flowId, runStore, resume: async () => { called = true; }, logger: null,
    });
    assert.equal(result.resumed, false);
    assert.equal(result.reason, "flip_failed:terminal");
    assert.equal(called, false);
  });
});

// Regression for the "orphaned promotion" bug class (see engine/admission-
// promotion-resume.js's header): a CAS-lost cleanup release() here always
// holds a genuinely 'admitted' row, so it can promote a DIFFERENT flow's
// queued row from the shared combined pool as a side effect — discarding
// that promotion strands the promoted flow exactly like the boot-time leak
// sweep once did (fixed separately in boot/flow-revival.js's own admission-
// leak release, which drives wakePromotedRelease on the same release).
describe("flipToRunningAndResume — CAS-lost-after-admitted release drives resumePromotedAdmission", () => {
  test("resumes the different flow promoted by this call's own cleanup release", async () => {
    const parentFlowId = "parent-flow";
    const rows = {
      [parentFlowId]: { flowId: parentFlowId, revision: 1, status: "blocked", lastStep: null, stateJson: {}, parentInvocationId: null },
      "other-flow": { flowId: "other-flow", revision: 5, status: "queued", lastStep: null, stateJson: {}, parentInvocationId: null },
    };
    const runStore = {
      get: async (id) => rows[id] ?? null,
      casSetRunning: async (id, args) => {
        if (id === parentFlowId) {
          // Simulate a real concurrent waiter showing up for the pool's one
          // free slot right as this CAS is about to lose the race — by the
          // time the cleanup below releases parentFlowId's own admission,
          // "other-flow" is the oldest queued entry and gets promoted.
          await admissionQueueStore.admit({ kind: "flow-start", flowId: "other-flow" });
          return { applied: false, code: "test_forced_cas_loss" };
        }
        rows[id] = { ...rows[id], status: "running", revision: rows[id].revision + 1, lastStep: args.lastStep, stateJson: args.stateJson, waitJson: null };
        return { applied: true };
      },
    };
    const dbPath = path.join(os.tmpdir(), `child-resume-trigger-cas-lost-promote-${randomUUID()}.sqlite`);
    const admissionQueueStore = createAdmissionQueueStore({ dbPath, getLimit: () => 1 });
    await admissionQueueStore.ready();

    const resumed = [];
    const result = await flipToRunningAndResume({
      parentFlowId, runStore, resume: async (id) => { resumed.push(id); },
      logger: { info() {}, warn() {} }, admissionQueueStore, api: {},
    });

    assert.equal(result.resumed, false, "this call's own flip lost the CAS race");
    assert.equal(result.reason, "flip_failed:test_forced_cas_loss");
    await drainDetachedResumes();
    assert.deepEqual(resumed, ["other-flow"], "the flow promoted by the cleanup release must be resumed, not just have its admission flipped");
    assert.equal(rows["other-flow"].status, "running");
  });
});

describe("admitAndSetRunning — CAS-lost-after-admitted release drives resumePromotedAdmission", () => {
  test("resumes the different flow promoted by this call's own cleanup release", async () => {
    const flowId = "target-flow";
    const rows = {
      "other-flow": { flowId: "other-flow", revision: 5, status: "queued", lastStep: null, stateJson: {}, parentInvocationId: null },
    };
    const runStore = {
      get: async (id) => rows[id] ?? null,
      casSetRunning: async (id, args) => {
        rows[id] = { ...rows[id], status: "running", revision: (rows[id]?.revision ?? 0) + 1, lastStep: args.lastStep, stateJson: args.stateJson, waitJson: null };
        return { applied: true };
      },
    };
    const dbPath = path.join(os.tmpdir(), `admit-and-set-running-cas-lost-promote-${randomUUID()}.sqlite`);
    const admissionQueueStore = createAdmissionQueueStore({ dbPath, getLimit: () => 1 });
    await admissionQueueStore.ready();

    const store = {
      setRunning: async () => {
        // Simulate a real concurrent waiter showing up for the pool's one
        // free slot right as this CAS is about to lose the race.
        await admissionQueueStore.admit({ kind: "flow-start", flowId: "other-flow" });
        return { applied: false, code: "test_forced_cas_loss" };
      },
    };

    const resumed = [];
    const result = await admitAndSetRunning({
      flowId, expectedRevision: 1, lastStep: null, stateJson: {}, parentInvocationId: null,
      store, admissionQueueStore, runStore, resume: async (id) => { resumed.push(id); },
      logger: { info() {}, warn() {} }, api: {},
    });

    assert.equal(result.applied, false, "this call's own setRunning lost the CAS race");
    await drainDetachedResumes();
    assert.deepEqual(resumed, ["other-flow"], "the flow promoted by the cleanup release must be resumed, not just have its admission flipped");
    assert.equal(rows["other-flow"].status, "running");
  });
});

// A 'failed' parent (Stage 1.5's mode-not-satisfied path) can be resurrected
// once the fan-out it failed on
// later becomes satisfied — but only via the repairGuard the caller
// supplies, and only when that guard confirms the settling child's
// spawning invocation is still the current one for its node/subflowKey.
describe("flipToRunningAndResume — repair propagation", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("repairs a 'failed' parent back to running when the repairGuard's invocation is still current", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "p", initialState: {} });
    const invocationId = await nodeExecutionsStore.findOrCreateInvocation({
      flowId: parent.flowId, nodeId: "SUBMIT", subflowKey: "only",
    });
    await store.fail({ flowId: parent.flowId, expectedRevision: 0, stateJson: {}, statusReason: "boom" });
    assert.equal((await runStore.get(parent.flowId)).status, "failed");

    let resumedWith = null;
    const result = await flipToRunningAndResume({
      parentFlowId: parent.flowId, runStore, resume: async (id) => { resumedWith = id; }, logger: null,
      repairGuard: { nodeExecutionsStore, nodeId: "SUBMIT", subflowKey: "only", expectedInvocationId: invocationId },
    });

    assert.equal(result.resumed, true);
    assert.equal(resumedWith, parent.flowId);
    const row = await runStore.get(parent.flowId);
    assert.equal(row.status, "running");
  });

  test("does not repair when a fresh invocation has superseded the one the repairGuard expects (stale settlement)", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "p", initialState: {} });
    const staleInvocationId = await nodeExecutionsStore.findOrCreateInvocation({
      flowId: parent.flowId, nodeId: "SUBMIT", subflowKey: "only",
    });
    await store.fail({ flowId: parent.flowId, expectedRevision: 0, stateJson: {}, statusReason: "boom" });
    // The node gets re-entered independently (a fresh top-level retry, a
    // loop-back) — a new visit_seq invocation now owns this identity.
    await nodeExecutionsStore.findOrCreateInvocation({
      flowId: parent.flowId, nodeId: "SUBMIT", subflowKey: "only", forceNew: true,
    });

    let called = false;
    const result = await flipToRunningAndResume({
      parentFlowId: parent.flowId, runStore, resume: async () => { called = true; }, logger: null,
      repairGuard: { nodeExecutionsStore, nodeId: "SUBMIT", subflowKey: "only", expectedInvocationId: staleInvocationId },
    });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "repair_stale_invocation");
    assert.equal(called, false);
    assert.equal((await runStore.get(parent.flowId)).status, "failed", "a rejected repair must leave the parent exactly as it was");
  });

  test("never repairs 'succeeded'/'cancelled'/'skipped' parents, even with a repairGuard provided", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;

    for (const terminalize of [
      (flowId) => store.finish({ flowId, expectedRevision: 0, stateJson: {} }),
      (flowId) => store.cancel({ flowId, stateJson: {} }),
      (flowId) => store.skip({ flowId, expectedRevision: 0, stateJson: {} }),
    ]) {
      const parent = await store.create({ pipelineId: "p", initialState: {} });
      const invocationId = await nodeExecutionsStore.findOrCreateInvocation({
        flowId: parent.flowId, nodeId: "SUBMIT", subflowKey: "only",
      });
      await terminalize(parent.flowId);
      const statusBefore = (await runStore.get(parent.flowId)).status;
      assert.notEqual(statusBefore, "failed");

      let called = false;
      const result = await flipToRunningAndResume({
        parentFlowId: parent.flowId, runStore, resume: async () => { called = true; }, logger: null,
        repairGuard: { nodeExecutionsStore, nodeId: "SUBMIT", subflowKey: "only", expectedInvocationId: invocationId },
      });

      assert.equal(result.resumed, false);
      assert.equal(result.reason, "flip_failed:terminal");
      assert.equal(called, false);
      assert.equal((await runStore.get(parent.flowId)).status, statusBefore);
    }
  });

  test("a 'failed' parent with no repairGuard supplied (legacy callers) keeps the pre-repair behavior", async () => {
    harness = await makeHarness();
    const { runStore, store } = harness;
    const parent = await store.create({ pipelineId: "p", initialState: {} });
    await store.fail({ flowId: parent.flowId, expectedRevision: 0, stateJson: {}, statusReason: "boom" });

    let called = false;
    const result = await flipToRunningAndResume({
      parentFlowId: parent.flowId, runStore, resume: async () => { called = true; }, logger: null,
    });

    assert.equal(result.resumed, false);
    assert.equal(result.reason, "flip_failed:terminal");
    assert.equal(called, false);
  });
});

// End-to-end through maybeResumeParent: a real single-item-cascade child
// retried and re-settled must repair its once-failed parent automatically,
// with no code path specific to "this is a repair" — the same
// triggerParentResume hook that handles ordinary first-time settlement.
describe("maybeResumeParent — repair propagation end to end", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  function pipelinesRefWithMode(mode) {
    return { value: new Map([["parent_pipe", { nodes: { SUBMIT: { mode } } }]]) };
  }

  test("a retried single-item cascade child that now succeeds resurrects its 'failed' parent", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store } = harness;
    const parent = await store.create({ pipelineId: "parent_pipe", initialState: {} });
    await store.setWaiting({
      flowId: parent.flowId, expectedRevision: 0, lastStep: "START",
      stateJson: {}, waitJson: { kind: "children-pending", nodeId: "SUBMIT" }, statusReason: "children pending",
    });
    const { childFlowId } = await resolveOrCreateChild({
      flowId: parent.flowId, pipelineId: "parent_pipe", nodeId: "SUBMIT",
      subflowKey: "only", childPipelineId: "parent_pipe::SUBMIT", initialState: {},
      store, runStore, nodeExecutionsStore,
    });

    // First attempt fails — Stage 1.5 fails the parent since mode:"all"
    // isn't satisfied yet.
    const failFlow = (args) => store.fail(args);
    await store.fail({ flowId: childFlowId, expectedRevision: 0, stateJson: {}, statusReason: "infra blip" });
    const firstResult = await maybeResumeParent({
      flowId: childFlowId, runStore, nodeExecutionsStore,
      pipelinesRef: pipelinesRefWithMode("all"), resume: async () => {}, failFlow, logger: null,
    });
    assert.equal(firstResult.reason, "mode_not_satisfied");
    assert.equal((await runStore.get(parent.flowId)).status, "failed");

    // The child gets retried (flipped back to running by whatever retry
    // mechanism — simulated directly here) and this time succeeds.
    const beforeRetry = await runStore.get(childFlowId);
    const unblocked = await runStore.forceBlock(childFlowId, {
      expectedRevision: beforeRetry.revision, stateJson: beforeRetry.stateJson, statusReason: null, ownerKey: beforeRetry.ownerKey,
    });
    assert.equal(unblocked.applied, true);
    await runStore.casSetRunning(childFlowId, { expectedRevision: beforeRetry.revision + 1, lastStep: null, stateJson: {} });
    await store.finish({ flowId: childFlowId, expectedRevision: beforeRetry.revision + 2, stateJson: {} });

    let resumedWith = null;
    const secondResult = await maybeResumeParent({
      flowId: childFlowId, runStore, nodeExecutionsStore,
      pipelinesRef: pipelinesRefWithMode("all"), resume: async (id) => { resumedWith = id; }, failFlow, logger: null,
    });

    assert.equal(secondResult.resumed, true);
    assert.equal(resumedWith, parent.flowId);
    assert.equal((await runStore.get(parent.flowId)).status, "running");
  });
});
