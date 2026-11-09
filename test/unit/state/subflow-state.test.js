/**
 * Unit tests for state/subflow-state.js
 *
 * Both getAllSubflowGroups/getSubflowGroup read flow_runs children via
 * nodeExecutionsStore.listSubflowNodeIdsForFlow.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getSubflowGroup, getAllSubflowGroups } from "../../../state/subflow-state.js";

// ── fake run-store / node-executions-store ─────────────────────────────────

// An invocation's own nodeId IS the group/loop-node id
// (see subflow-state.js's normalizeChildAsSubflowRow comment) — fixtures below
// use `nodeId` as the group id.
function fakeStores({ invocations = [], children = [] } = {}) {
  const nodeExecutionsStore = {
    listInvocationIdsForNode: (flowId, nodeId) =>
      invocations.filter((i) => i.flowId === flowId && i.nodeId === nodeId).map((i) => i.id),
    listSubflowNodeIdsForFlow: (flowId) =>
      [...new Set(invocations.filter((i) => i.flowId === flowId).map((i) => i.nodeId))],
    getInvocation: (id) => invocations.find((i) => i.id === id) ?? null,
  };
  const runStore = {
    listForParentInvocations: (ids) => children.filter((c) => ids.includes(c.parentInvocationId)),
  };
  return { runStore, nodeExecutionsStore };
}

// ── getAllSubflowGroups — base behavior ───────────────────────────────────────

describe("getAllSubflowGroups — base behavior", () => {
  test("returns empty object when the flow has no subflow nodes", async () => {
    const { runStore, nodeExecutionsStore } = fakeStores();
    assert.deepEqual(await getAllSubflowGroups({ flowId: "flow-1", runStore, nodeExecutionsStore }), {});
  });
});

// ── new-model (child flow_runs) path ────────────────────────────────────────

describe("getSubflowGroup — new model (real flow_runs children)", () => {
  test("returns real flow_runs children for a migrated group", async () => {
    const flowId = randomUUID();
    const { runStore, nodeExecutionsStore } = fakeStores({
      invocations: [{ id: "inv-1", flowId, nodeId: "group1", subflowKey: "item-1" }],
      children: [{ flowId: "child-1", parentInvocationId: "inv-1", status: "succeeded", lastStep: "__end__", stateJson: { ok: true }, createdAt: 1, updatedAt: 2, endedAt: 3 }],
    });

    const result = await getSubflowGroup({ flowId, groupId: "group1", runStore, nodeExecutionsStore });
    assert.deepEqual(Object.keys(result), ["item-1"]);
    assert.equal(result["item-1"].step, "__end__");
    assert.equal(result["item-1"].status, "succeeded");
    assert.equal(result["item-1"].flowId, "child-1");
  });

  test("maps status/data/timing fields onto the old row shape", async () => {
    const flowId = randomUUID();
    const { runStore, nodeExecutionsStore } = fakeStores({
      invocations: [{ id: "inv-1", flowId, nodeId: "group1", subflowKey: "item-1" }],
      children: [{
        flowId: "child-1", parentInvocationId: "inv-1", status: "failed",
        lastStep: "SUBMIT", stateJson: { foo: "bar" }, createdAt: 100, updatedAt: 200, endedAt: 300,
      }],
    });

    const result = await getSubflowGroup({ flowId, groupId: "group1", runStore, nodeExecutionsStore });
    assert.equal(result["item-1"].status, "failed");
    assert.equal(result["item-1"].step, "SUBMIT");
    assert.equal(result["item-1"].startedAt, 100);
    assert.equal(result["item-1"].updatedAt, 200);
    assert.equal(result["item-1"].failedAt, 300);
    assert.equal(result["item-1"].doneAt, null);
    assert.deepEqual(result["item-1"].data, { foo: "bar" });
  });

  test("folds 'blocked' status into 'running', matching the old model", async () => {
    const flowId = randomUUID();
    const { runStore, nodeExecutionsStore } = fakeStores({
      invocations: [{ id: "inv-1", flowId, nodeId: "group1", subflowKey: "item-1" }],
      children: [
        { flowId: "child-1", parentInvocationId: "inv-1", status: "blocked", waitJson: { kind: "human-gate" }, currentStep: "GATE", lastStep: "DRAFT", updatedAt: 5 },
      ],
    });

    const result = await getSubflowGroup({ flowId, groupId: "group1", runStore, nodeExecutionsStore });
    assert.equal(result["item-1"].status, "running");
  });

  // Phase 4: unlike 'blocked', 'queued'
  // has no old-model analogue to fold into "running" — it's an internal-
  // resource wait (an admission slot), not a gate,
  // so it's passed through unchanged along with its reason text.
  test("passes 'queued' status through unchanged, with statusReason carried onto the row", async () => {
    const flowId = randomUUID();
    const { runStore, nodeExecutionsStore } = fakeStores({
      invocations: [{ id: "inv-2", flowId, nodeId: "group1", subflowKey: "item-2" }],
      children: [
        {
          flowId: "child-2", parentInvocationId: "inv-2", status: "queued", lastStep: "DRAFT", updatedAt: 5,
          waitJson: { kind: "admission-slot", nodeId: "SCRAPE_PROFILE" },
          statusReason: "queued: waiting for an admission slot (node SCRAPE_PROFILE)",
        },
      ],
    });

    const result = await getSubflowGroup({ flowId, groupId: "group1", runStore, nodeExecutionsStore });
    assert.equal(result["item-2"].status, "queued");
    assert.equal(result["item-2"].statusReason, "queued: waiting for an admission slot (node SCRAPE_PROFILE)");
    // A queued child's wait_json is not gate-shaped — it must not populate
    // `awaiting` (reserved for human-gate payloads with a subNodeId).
    assert.equal(result["item-2"].awaiting, null);
  });

  test("derives 'awaiting' from wait_json only when blocked", async () => {
    const flowId = randomUUID();
    const { runStore, nodeExecutionsStore } = fakeStores({
      invocations: [{ id: "inv-1", flowId, nodeId: "group1", subflowKey: "item-1" }],
      children: [{ flowId: "child-1", parentInvocationId: "inv-1", status: "blocked", waitJson: { kind: "human-gate", requestPayload: { title: "x" } }, currentStep: "GATE", updatedAt: 9 }],
    });

    const result = await getSubflowGroup({ flowId, groupId: "group1", runStore, nodeExecutionsStore });
    assert.ok(result["item-1"].awaiting);
    assert.equal(result["item-1"].awaiting.subNodeId, "GATE");
    assert.equal(result["item-1"].awaiting.requestedAt, 9);
  });

  test("returns empty object when no children have been spawned yet for the group", async () => {
    const flowId = randomUUID();
    const { runStore, nodeExecutionsStore } = fakeStores({
      invocations: [],
      children: [],
    });

    const result = await getSubflowGroup({ flowId, groupId: "group1", runStore, nodeExecutionsStore });
    assert.deepEqual(result, {});
  });
});
