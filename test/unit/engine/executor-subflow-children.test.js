/**
 * Unit tests for engine/executor-subflow-children.js — its
 * childFlows:true real-child dispatch. Uses real sqlite-backed
 * run-store.js/node-executions.js/state-store.js instances (tmp files),
 * matching the pattern in spawn-child.test.js/child-resume-trigger.test.js,
 * since the logic under test is a real join across both tables' schemas.
 */
import { test, describe, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createStateStore } from "../../../state/state-store.js";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { dispatchSubflowChildren } from "../../../engine/executor-subflow-children.js";
import { getGlobalConcurrencyLimit, _resetForTests as resetChildFanoutCap } from "../../../engine/global-concurrency.js";

function tmpPath(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

async function makeHarness() {
  const dbPath = tmpPath("loop-children");
  const runStore = await createRunStore({ dbPath });
  const nodeExecutionsStore = await createNodeExecutionsStore(dbPath);
  // Real admissionQueueStore, same shared getGlobalConcurrencyLimit()
  // singleton every test here already drives via resetChildFanoutCap(n) —
  // dispatchSubflowChildren has a hard dependency on a real store now, not
  // a fake, since these tests exercise the actual admit/queue/promote cycle
  // end to end (same rationale as admission-queue-store.test.js's own
  // "real tmp-sqlite, not mocks" convention).
  const admissionQueueStore = createAdmissionQueueStore({ dbPath, getLimit: getGlobalConcurrencyLimit });
  const store = createStateStore({}, "test-owner", { runStore, admissionQueueStore });
  return { runStore, nodeExecutionsStore, store, admissionQueueStore };
}

// The parent run row exists in production before its node fans out.
async function dispatch(args) {
  await args.runStore.insert({ flowId: args.flowId, status: "running" });
  return dispatchSubflowChildren(args);
}

function allowAllBudgetGate() {
  return { skip: false, pause: false };
}

// Collects every childFlowId passed to a fake runFlow, without actually
// driving it — dispatchSubflowChildren only needs to fire-and-forget it.
function makeRunFlowSpy() {
  const calls = [];
  const runFlow = async (flowId) => { calls.push(flowId); };
  return { runFlow, calls };
}

function makeLogger() {
  const warns = [];
  return { warns, logger: { info: () => {}, warn: (m) => warns.push(m) } };
}

// setImmediate-flushing helper — dispatchSubflowChildren fires runFlow via
// setImmediate, not inline.
function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

describe("dispatchSubflowChildren", () => {
  let harness;
  afterEach(() => { harness?.runStore?.close(); harness?.nodeExecutionsStore?.close(); });

  test("0 items: returns {stateUpdate:{}} without touching any store", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [] };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    assert.deepEqual(result, { stateUpdate: {} });
    assert.equal(calls.length, 0);
  });

  test("first entry: creates one child per item, dispatches runFlow for each, returns children-pending", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }] };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    assert.equal(result.stateUpdate && typeof result.stateUpdate, "object");
    assert.equal(result.waitJson.kind, "children-pending");
    assert.equal(result.waitJson.nodeId, "LOOP");
    assert.equal(result.waitJson.childFlowIds.length, 2);
    assert.equal(result.reentrant, true);

    await flush();
    assert.equal(calls.length, 2, "runFlow should be fire-and-forget dispatched for each newly created child");

    // Real children actually exist, parented correctly.
    for (const childFlowId of result.waitJson.childFlowIds) {
      const child = await runStore.get(childFlowId);
      assert.ok(child, "child flow_runs row should exist");
      assert.ok(child.parentInvocationId, "child should carry a parentInvocationId");
      assert.equal(child.pipelineId, "p::LOOP");
      assert.ok(child.stateJson.item?.id, "child state should carry the loop item");
    }
  });

  // buildChildInitialState does not clone the parent's state; a
  // child's initialState is exactly item/itemIndex/parentItem. Anything
  // else an inner node needs from an ancestor flow goes through the
  // explicit needs: mechanism (engine/needs-resolver.js) rather than
  // riding along in the spawn-time state.
  test("child initialState carries ONLY item/itemIndex/parentItem — no parent state rides along", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }] };
    const state = { pipelineId: "p", shortlist: ["a", "b"], _subflowGroups: { x: 1 } };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state,
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    const child = await runStore.get(result.waitJson.childFlowIds[0]);
    assert.deepEqual(child.stateJson.item, { id: "item-1" });
    assert.equal(child.stateJson.itemIndex, 0);
    assert.equal(child.stateJson.parentItem, undefined, "state.item was absent on the parent, so parentItem is too");
    assert.equal(child.stateJson.shortlist, undefined, "parent's shortlist must not ride along");
    // pipelineId IS present — but it's the CHILD's own synthetic id (set by
    // spawn-child.js, not buildChildInitialState), never the parent's.
    assert.equal(child.stateJson.pipelineId, "p::LOOP");
    assert.equal(child.stateJson._subflowGroups, undefined);
    assert.deepEqual(Object.keys(child.stateJson).sort(), ["item", "itemIndex", "pipelineId"], "no parent-cloned field survives the spawn beyond what spawn-child.js itself sets");
  });

  // A single-item retry-cascade node
  // (selector: () => [{id:"only"}], structuring a code -> infra-
  // recovery -> agent-fallback chain, not a real per-item fan-out): the
  // child's state.item is this node's own placeholder, so parentItem
  // preserves the parent branch's real item (e.g. a listing/candidate/
  // conversation, as SUBMIT_CODE bidding on
  // https://www.workana.com/messages/bid/<id> does) specifically so a
  // single-item cascade's own inner nodes can still reach it.
  test("parentItem carries the PARENT branch's real item, even when this node's own selector returns a placeholder — the single-item retry-cascade case", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    // The parent branch's own real item — e.g. PROPOSE's per-listing item,
    // already resolved before SUBMIT (a single-item cascade) is dispatched.
    const realListing = { id: "desenvolvimento-de-sistema-real", url: "https://example.com/job", score: 91 };
    const node = { selector: () => [{ id: "only" }] };
    const state = { pipelineId: "p", item: realListing };

    const result = await dispatch({
      node, nodeId: "SUBMIT", flowId, state,
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    const child = await runStore.get(result.waitJson.childFlowIds[0]);
    assert.deepEqual(child.stateJson.item, { id: "only" }, "this node's own selector item still wins for state.item — cascade structure is unchanged");
    assert.deepEqual(child.stateJson.parentItem, realListing, "parentItem must carry the real parent item untouched, for the cascade's inner nodes to read");
  });

  test("parentItem is undefined when the parent branch itself had no item — a real top-level fan-out (e.g. PROPOSE), not a nested cascade", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "listing-a" }, { id: "listing-b" }] };
    const state = { pipelineId: "p" }; // no .item — this IS the top-level fan-out

    const result = await dispatch({
      node, nodeId: "PROPOSE", flowId, state,
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    for (const childFlowId of result.waitJson.childFlowIds) {
      const child = await runStore.get(childFlowId);
      assert.equal(child.stateJson.parentItem, undefined, "no parent item existed to carry forward — must not fabricate one");
    }
  });

  // The parent's own state already carries its own
  // `pipelineId` field (every flow's stateJson does). Cloning that whole
  // state into the child's initialState — without stripping pipelineId —
  // would let it override the intended child pipeline id once merged in
  // state-store.js's create(), making the child re-resolve and re-run the
  // PARENT pipeline from __start__ instead of its own `.branch` — an
  // unbounded recursive fan-out (each "child" spawning more children of the
  // whole parent pipeline again). This test pins the call site.
  test("child's stateJson.pipelineId is the CHILD pipeline id, never the parent's own", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }] };
    // The parent's own state — like every real flow's stateJson — already
    // carries its own pipelineId.
    const state = { pipelineId: "parent_pipeline" };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state,
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    const child = await runStore.get(result.waitJson.childFlowIds[0]);
    assert.equal(child.pipelineId, "parent_pipeline::LOOP");
    assert.equal(child.stateJson.pipelineId, "parent_pipeline::LOOP", "stateJson.pipelineId is what runFlow() actually reads — must be the child pipeline, not the parent's");
  });

  test("re-entry with all children terminal returns {stateUpdate:{}} — parent advances", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }] };

    const first = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });
    for (const childFlowId of first.waitJson.childFlowIds) {
      await store.finish({ flowId: childFlowId, expectedRevision: 0, stateJson: {} });
    }

    const second = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });
    assert.deepEqual(second, { stateUpdate: {} });
  });

  test("re-entry with one child still pending returns children-pending again, without creating duplicate children", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }] };

    const first = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });
    await store.finish({ flowId: first.waitJson.childFlowIds[0], expectedRevision: 0, stateJson: {} });
    // second child left "running"
    await flush();
    calls.length = 0;

    const second = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });
    assert.equal(second.waitJson.kind, "children-pending");
    assert.equal(second.waitJson.childFlowIds.length, 2);
    assert.deepEqual([...second.waitJson.childFlowIds].sort(), [...first.waitJson.childFlowIds].sort());

    await flush();
    assert.equal(calls.length, 0, "no new runFlow dispatch for already-existing children");

    const allChildren = await runStore.listForParentInvocations(
      await nodeExecutionsStore.listInvocationIdsForNode(flowId, "LOOP"),
    );
    assert.equal(allChildren.length, 2, "no duplicate children created on re-entry");
  });

  test("budget-gate pause short-circuits before any child is created", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }] };
    const maybeBudgetGate = () => ({ skip: false, pause: true, blockedInfo: { reason: "budget" } });

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate, runFlow, logger, admissionQueueStore,
    });

    assert.deepEqual(result, { blockedInfo: { reason: "budget" } });
    assert.equal(calls.length, 0);
    assert.equal((await nodeExecutionsStore.listInvocationIdsForNode(flowId, "LOOP")).length, 0);
  });

  test("budget-gate skip returns {stateUpdate:{}} without creating children", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }] };
    const maybeBudgetGate = () => ({ skip: true, pause: false });

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate, runFlow, logger, admissionQueueStore,
    });

    assert.deepEqual(result, { stateUpdate: {} });
    assert.equal(calls.length, 0);
  });

  test("honors node.itemKey for subflowKey derivation", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { itemKey: "linkedin-id", selector: () => [{ "linkedin-id": "abc123", name: "Someone" }] };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    const invocationId = (await runStore.get(result.waitJson.childFlowIds[0])).parentInvocationId;
    const invocation = await nodeExecutionsStore.getInvocation(invocationId);
    assert.equal(invocation.subflowKey, "abc123");
  });

  test("falls back to .id/.listingId/String(item) when itemKey is not declared", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ listingId: "job-42" }] };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    const invocationId = (await runStore.get(result.waitJson.childFlowIds[0])).parentInvocationId;
    const invocation = await nodeExecutionsStore.getInvocation(invocationId);
    assert.equal(invocation.subflowKey, "job-42");
  });

  test("an item producing an empty subflowKey is skipped with a warning, not a crash", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger, warns } = makeLogger();
    const flowId = randomUUID();
    const node = { itemKey: "missing", selector: () => [{ id: "item-1" }] };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    assert.equal(result.waitJson.childFlowIds.length, 0);
    assert.equal(warns.length, 1);
  });

  // node.concurrency: participation in the shared
  // combined admission pool (state/admission-queue-store.js), NOT scoped to
  // any one pipeline or subflow node — a boolean PARTICIPATION switch, not a
  // per-node number. Every test here resets the module-level singleton
  // limit first (_resetForTests), since it's shared process-wide state.
  describe("node.concurrency — single gateway-wide cap", () => {
    afterEach(() => { resetChildFanoutCap(); });

    test("caps live dispatch: only the first N children (by selector order) run, the rest are created but queued", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      resetChildFanoutCap(2);
      const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }, { id: "item-3" }, { id: "item-4" }] };

      const result = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      // All 4 children get real rows — the cap governs dispatch, not creation.
      assert.equal(result.waitJson.childFlowIds.length, 4);
      await flush();
      assert.equal(calls.length, 2, "only 2 of the 4 children should have been handed to runFlow");

      const rows = await Promise.all(result.waitJson.childFlowIds.map((id) => runStore.get(id)));
      const running = rows.filter((r) => r.status === "running");
      const queued = rows.filter((r) => r.status === "queued");
      assert.equal(running.length, 2, "first 2 (by selector order) stay running/dispatched");
      assert.equal(queued.length, 2, "remaining 2 are demoted to queued instead of dispatched");
      for (const q of queued) {
        assert.equal(q.waitJson?.kind, "child-dispatch");
      }
      assert.equal(running.map((r) => r.stateJson.item.id).sort().join(","), "item-1,item-2");
      assert.equal(queued.map((r) => r.stateJson.item.id).sort().join(","), "item-3,item-4");
    });

    test("concurrency:false opts a node out entirely — unbounded even when the global budget is already exhausted", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      resetChildFanoutCap(1);
      const node = { concurrency: false, selector: () => Array.from({ length: 5 }, (_, i) => ({ id: `item-${i}` })) };

      const result = await dispatch({
        node, nodeId: "EVICT_GATES", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      await flush();
      assert.equal(calls.length, 5, "every child dispatches immediately — this node opted out of the shared budget");
      const rows = await Promise.all(result.waitJson.childFlowIds.map((id) => runStore.get(id)));
      assert.ok(rows.every((r) => r.status === "running"));
    });

    test("concurrency unset behaves exactly like concurrency:true — participates in the shared budget by default", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      resetChildFanoutCap(2);
      const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }, { id: "item-3" }] };

      const result = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      await flush();
      assert.equal(calls.length, 2);
      const rows = await Promise.all(result.waitJson.childFlowIds.map((id) => runStore.get(id)));
      assert.equal(rows.filter((r) => r.status === "queued").length, 1, "the field being absent must behave identically to concurrency:true, not concurrency:false");
    });

    // The entire point of collapsing to a single global cap: two totally
    // unrelated subflow nodes — different pipelines, different nodeIds —
    // draw from the exact same shared budget.
    test("the budget is shared across different pipelines and different subflow nodes, not scoped per pipeline or per node", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow } = makeRunFlowSpy();
      const { logger } = makeLogger();
      resetChildFanoutCap(2);

      const flowIdA = randomUUID();
      const resultA = await dispatch({
        node: { selector: () => [{ id: "a-1" }, { id: "a-2" }] },
        nodeId: "SCORE", flowId: flowIdA, state: { pipelineId: "pipeline-a" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });
      await flush();
      const aRows = await Promise.all(resultA.waitJson.childFlowIds.map((id) => runStore.get(id)));
      assert.equal(aRows.filter((r) => r.status === "running").length, 2, "pipeline-a's own 2 items exactly fill the shared cap of 2");

      const flowIdB = randomUUID();
      const resultB = await dispatch({
        node: { selector: () => [{ id: "b-1" }] },
        nodeId: "FETCH_PROFILES", flowId: flowIdB, state: { pipelineId: "pipeline-b" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });
      await flush();

      const bRow = await runStore.get(resultB.waitJson.childFlowIds[0]);
      assert.equal(bRow.status, "queued", "a completely different pipeline's child still competes for the SAME global budget, already exhausted by pipeline-a");
    });

    // A child "blocked" on its own human gate consumes zero ongoing
    // resources — must not occupy a concurrency slot, or a gated sibling
    // could starve the cap for however long it sits waiting on an
    // operator's Telegram tap. Deliberately different from executor.js's
    // root-level start() check, which DOES count "blocked" — that's
    // guarding logical exclusivity (don't start a 2nd pipeline run while
    // the 1st is paused), not resource contention, so the same 3-status
    // filter doesn't transfer here.
    test("a sibling blocked on its own human gate does not occupy a slot — a new child still dispatches immediately", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      resetChildFanoutCap(1);
      const node = { selector: () => [{ id: "item-1" }] };

      const first = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });
      const [item1FlowId] = first.waitJson.childFlowIds;
      await flush();
      assert.equal((await runStore.get(item1FlowId)).status, "running");

      // Simulate item-1 reaching an internal GATE inside its own child flow.
      const item1 = await store.get(item1FlowId);
      await store.setWaiting({
        flowId: item1FlowId, expectedRevision: item1.revision, lastStep: "GATE",
        blockingNodeId: "GATE", stateJson: item1.stateJson, waitJson: { kind: "human-gate", items: [] },
      });
      assert.equal((await runStore.get(item1FlowId)).status, "blocked");

      calls.length = 0;
      const second = await dispatch({
        node: { selector: () => [{ id: "item-1" }, { id: "item-2" }] },
        nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });
      await flush();

      assert.equal(calls.length, 1, "item-2 must dispatch — item-1 being blocked (not running) doesn't count against the cap");
      const item2FlowId = second.waitJson.childFlowIds.find((id) => id !== item1FlowId);
      assert.equal((await runStore.get(item2FlowId)).status, "running", "item-2 must NOT be queued");
    });

    test("item count at or under the cap: nothing gets queued", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      resetChildFanoutCap(3);
      const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }] };

      const result = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      await flush();
      assert.equal(calls.length, 2);
      const rows = await Promise.all(result.waitJson.childFlowIds.map((id) => runStore.get(id)));
      assert.ok(rows.every((r) => r.status === "running"));
    });

    test("re-entry: already-queued children are left untouched, not re-queued or duplicated", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      resetChildFanoutCap(1);
      const node = { selector: () => [{ id: "item-1" }, { id: "item-2" }] };

      const first = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });
      await flush();
      calls.length = 0;

      const second = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });
      await flush();

      assert.equal(calls.length, 0, "re-entry must not re-dispatch or re-evaluate already-created children");
      assert.deepEqual([...second.waitJson.childFlowIds].sort(), [...first.waitJson.childFlowIds].sort());
      const rows = await Promise.all(second.waitJson.childFlowIds.map((id) => runStore.get(id)));
      assert.equal(rows.filter((r) => r.status === "running").length, 1);
      assert.equal(rows.filter((r) => r.status === "queued").length, 1);
      const allChildren = await runStore.listForParentInvocations(
        await nodeExecutionsStore.listInvocationIdsForNode(flowId, "SCORE"),
      );
      assert.equal(allChildren.length, 2, "no duplicate children created on re-entry");
    });
  });

  test("the child invocation's nodeId is the loop node's own id", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "item-1" }] };

    const result = await dispatch({
      node, nodeId: "LOOP", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
    });

    const invocationId = (await runStore.get(result.waitJson.childFlowIds[0])).parentInvocationId;
    const invocation = await nodeExecutionsStore.getInvocation(invocationId);
    assert.equal(invocation.nodeId, "LOOP");
  });

  // A cascade-shape subflow (flow.kind:
  // "cascade" — ENSURE_LOGIN/SUBMIT/SEND_REPLY's code-then-agent-fallback
  // shape) must never be gated at its own subflow-entry — that would hold
  // the whole flow for approval before its free code leg even gets a
  // chance to resolve the item without spending anything. Real spend stays
  // covered by dispatchAgentEntry's independent gate on the agent leg, if
  // the cascade ever reaches it — out of scope for dispatchSubflowChildren
  // itself, so not exercised here.
  test("cascade-shape node (flow.kind:'cascade') skips the subflow-entry budget gate entirely", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = {
      selector: () => [{ id: "only" }],
      flow: { kind: "cascade", nodes: {}, edges: {} },
    };
    const failIfCalled = () => { throw new Error("maybeBudgetGate must not be called for a cascade-shape node"); };

    const result = await dispatch({
      node, nodeId: "ENSURE_LOGIN", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: failIfCalled, runFlow, logger, admissionQueueStore,
    });

    // The gate was skipped entirely, so the free code leg still dispatches
    // — a real child is created, same as the ungated path.
    assert.equal(result.waitJson.childFlowIds.length, 1);
    await flush();
    assert.equal(calls.length, 1);
    const child = await runStore.get(result.waitJson.childFlowIds[0]);
    assert.ok(child, "child flow should still be created even though the gate was skipped");
  });

  // Companion regression guard: a real multi-item childFlows fan-out (no
  // flow.kind at all — SCORE's shape) must be COMPLETELY unaffected by the
  // cascade guard above; it keeps gating at its own subflow entry exactly
  // as before.
  test("non-cascade node (no flow.kind, e.g. SCORE's real fan-out shape) still gates at subflow entry", async () => {
    harness = await makeHarness();
    const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
    const { runFlow, calls } = makeRunFlowSpy();
    const { logger } = makeLogger();
    const flowId = randomUUID();
    const node = { selector: () => [{ id: "listing-1" }, { id: "listing-2" }] };

    let gateCalled = false;
    const lockedGate = () => {
      gateCalled = true;
      return { pause: true, blockedInfo: { _kind: "budget-gate-approval", reason: "daily budget cap reached" } };
    };

    const result = await dispatch({
      node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
      store, runStore, nodeExecutionsStore, maybeBudgetGate: lockedGate, runFlow, logger, admissionQueueStore,
    });

    assert.equal(gateCalled, true, "the gate must still be consulted for a real fan-out node");
    assert.deepEqual(result, { blockedInfo: { _kind: "budget-gate-approval", reason: "daily budget cap reached" } });
    assert.equal(calls.length, 0, "no children should spawn once the subflow-entry gate pauses");
  });

  // node.dispatchGuard — the atomic cross-flow recheck (state/node-
  // executions.js's `guard` param, threaded through spawn-child.js).
  // Two parent
  // flow-start runs whose FILTER scans overlapped both decided to dispatch
  // PROPOSE for the same listings before either had committed anything —
  // node.selector's own bulk decision can't see a sibling parent's
  // uncommitted work, so the guard has to run right at commit time instead.
  describe("node.dispatchGuard", () => {
    test("a rejected item is excluded from children-pending, never admitted, never runFlow'd", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      const node = {
        selector: () => [{ id: "listing-1" }, { id: "listing-2" }],
        dispatchGuard: async (item) => item.id !== "listing-1",
      };

      const result = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      assert.equal(result.waitJson.childFlowIds.length, 1, "the guarded-out item must not appear in childFlowIds at all");
      await flush();
      assert.equal(calls.length, 1, "only the allowed item should ever reach runFlow");

      const { rows } = await nodeExecutionsStore._db.execute({
        sql: "SELECT subflow_key FROM flow_node_invocations WHERE flow_id = ? AND node_id = ?",
        args: [flowId, "SCORE"],
      });
      assert.deepEqual(rows.map((r) => r.subflow_key).sort(), ["listing-2"], "no invocation row should exist for the rejected item either");
    });

    test("every item rejected: no children created, but the node stays reentrant with an empty children-pending wait (areSiblingsAllTerminal's own no_siblings case, unmodified) rather than falsely reporting done", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      const node = {
        selector: () => [{ id: "listing-1" }, { id: "listing-2" }],
        dispatchGuard: async () => false,
      };

      const result = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      assert.equal(result.reentrant, true, "re-entry will re-run selector+guard, self-healing once the other parent's claim resolves");
      assert.deepEqual(result.waitJson, { kind: "children-pending", nodeId: "SCORE", childFlowIds: [] });
      await flush();
      assert.equal(calls.length, 0, "nothing should ever reach runFlow when every item was rejected");
    });

    test("no dispatchGuard declared: every item dispatches, exactly today's behavior", async () => {
      harness = await makeHarness();
      const { runStore, nodeExecutionsStore, store, admissionQueueStore } = harness;
      const { runFlow, calls } = makeRunFlowSpy();
      const { logger } = makeLogger();
      const flowId = randomUUID();
      const node = { selector: () => [{ id: "listing-1" }, { id: "listing-2" }] };

      const result = await dispatch({
        node, nodeId: "SCORE", flowId, state: { pipelineId: "p" },
        store, runStore, nodeExecutionsStore, maybeBudgetGate: allowAllBudgetGate, runFlow, logger, admissionQueueStore,
      });

      assert.equal(result.waitJson.childFlowIds.length, 2);
    });
  });
});
