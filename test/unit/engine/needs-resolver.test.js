import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { resolveOneNeed, resolveOneEntry, resolveOneConfig, makeNeedsCtx } from "../../../engine/needs-resolver.js";

function makePipelinesRef(pipelines = {}) {
  return { value: new Map(Object.entries(pipelines)) };
}

function makeStore({ flows = {}, invocations = {}, results = {}, subflowInvocationIds = {}, subflowChildren = {} } = {}) {
  return {
    runStore: {
      get: async (flowId) => flows[flowId] ?? null,
      listForParentInvocations: async (ids) => ids.flatMap((id) => subflowChildren[id] ?? []),
    },
    nodeExecutionsStore: {
      getInvocation: async (id) => invocations[id] ?? null,
      getResultForNode: async (flowId, nodeId) => results[`${flowId}:${nodeId}`] ?? null,
      listInvocationIdsForNode: async (flowId, nodeId) => subflowInvocationIds[`${flowId}:${nodeId}`] ?? [],
    },
  };
}

describe("resolveOneNeed — ordinary (non-subflow) targets", () => {
  it("hops:1, ready — resolves the immediate parent's completed result", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: 1000, resultJson: JSON.stringify({ SCAN: { listings: [1, 2] } }), resultTooLargeBytes: null } },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: { listings: [1, 2] } });
  });

  it("hops:1, not-ready — invocation row exists but resultWrittenAt is still null", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: null, resultJson: null, resultTooLargeBytes: null } },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-ready");
  });

  it("hops:1, not-found — no invocation row for that node at all", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent" } },
      invocations: { "inv-parent": { flowId: "parent" } },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-found");
  });

  it("hops:2 — walks two levels (parent -> grandparent) correctly", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: {
        grandchild: { parentInvocationId: "inv-child" },
        child: { parentInvocationId: "inv-root-SCAN" },
      },
      invocations: {
        "inv-child": { flowId: "child" },
        "inv-root-SCAN": { flowId: "root" },
      },
      results: { "root:SCAN": { resultWrittenAt: 1000, resultJson: JSON.stringify({ SCAN: { ok: true } }), resultTooLargeBytes: null } },
    });
    const result = await resolveOneNeed({ flowId: "grandchild", hops: 2, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: { ok: true } });
  });

  it("hops exceeds real ancestor depth — walked off the top, not-found", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { root: { parentInvocationId: null } },
    });
    const result = await resolveOneNeed({ flowId: "root", hops: 1, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-found");
  });

  it("a row with resultJson non-null AND resultTooLargeBytes set resolves the real surviving value", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: {
        "parent:SCAN": {
          resultWrittenAt: 1000,
          resultJson: JSON.stringify({ SCAN: { ok: true }, _resultDroppedFields: ["listings"] }),
          resultTooLargeBytes: 165000,
        },
      },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: { ok: true } });
  });

  it("a corrupted resultJson row resolves as not-found, not a thrown error", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: 1000, resultJson: "{not valid", resultTooLargeBytes: null } },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-found");
  });
});

describe("resolveOneNeed — hops:0 (same-flow)", () => {
  it("ready — resolves the declaring node's OWN flow's completed sibling result, no walk", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { parentInvocationId: "inv-parent-SCAN" } },
      results: { "self:SCAN": { resultWrittenAt: 1000, resultJson: JSON.stringify({ SCAN: { listings: [1, 2] } }), resultTooLargeBytes: null } },
    });
    const result = await resolveOneNeed({ flowId: "self", hops: 0, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: { listings: [1, 2] } });
  });

  it("not-ready — same-flow sibling node hasn't completed yet", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: {} },
      results: { "self:SCAN": { resultWrittenAt: null, resultJson: null, resultTooLargeBytes: null } },
    });
    const result = await resolveOneNeed({ flowId: "self", hops: 0, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-ready");
  });

  it("not-found — same-flow sibling node hasn't dispatched at all", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({ flows: { self: {} } });
    const result = await resolveOneNeed({ flowId: "self", hops: 0, nodeId: "SCAN", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-found");
  });
});

describe("resolveOneNeed — subflow-sourced targets", () => {
  it("ready — every sibling under the target subflow node is terminal", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCORE" } },
      invocations: {
        "inv-parent-SCORE": { flowId: "parent" },
        "item-inv-1": { subflowKey: "listing-1" },
      },
      subflowInvocationIds: { "parent:SCORE": ["item-inv-1"] },
      subflowChildren: { "item-inv-1": [{ flowId: "grandchild-1", parentInvocationId: "item-inv-1", status: "succeeded", stateJson: { DRAFT: {} } }] },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCORE", runStore, nodeExecutionsStore });
    assert.equal(result.status, "ready");
    assert.ok(result.value["listing-1"], "resolves to the same {[subflowKey]: row} aggregate shape _subflowGroups already exposes");
  });

  it("not-ready — a sibling is still running (not yet terminal)", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCORE" } },
      invocations: {
        "inv-parent-SCORE": { flowId: "parent" },
        "item-inv-1": { subflowKey: "listing-1" },
      },
      subflowInvocationIds: { "parent:SCORE": ["item-inv-1"] },
      subflowChildren: { "item-inv-1": [{ flowId: "grandchild-1", parentInvocationId: "item-inv-1", status: "running" }] },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCORE", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-ready");
  });

  it("not-found — the target subflow node has spawned zero children (hasn't dispatched at all)", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent" } },
      invocations: { "inv-parent": { flowId: "parent" } },
    });
    const result = await resolveOneNeed({ flowId: "child", hops: 1, nodeId: "SCORE", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-found");
  });
});

describe("resolveOneEntry — an ancestor flow's own entry/bookkeeping field, not a node output", () => {
  it("ready — resolves the immediate parent's runId, set once at that flow's own start()", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: {
        child: { parentInvocationId: "inv-parent" },
        parent: { stateJson: { runId: "run-abc-123" } },
      },
      invocations: { "inv-parent": { flowId: "parent" } },
    });
    const result = await resolveOneEntry({ flowId: "child", hops: 1, entry: "runId", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: "run-abc-123" });
  });

  it("ready even when the value is undefined — an entry field's absence is a legitimate value, not a not-ready state", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: {
        child: { parentInvocationId: "inv-parent" },
        parent: { stateJson: {} },
      },
      invocations: { "inv-parent": { flowId: "parent" } },
    });
    const result = await resolveOneEntry({ flowId: "child", hops: 1, entry: "cost", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: undefined });
  });

  it("hops:2 — walks two levels to reach the true root's own runId, not the immediate parent's inherited copy", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: {
        grandchild: { parentInvocationId: "inv-child" },
        child: { parentInvocationId: "inv-root" },
        root: { stateJson: { runId: "run-root-999" } },
      },
      invocations: {
        "inv-child": { flowId: "child" },
        "inv-root": { flowId: "root" },
      },
    });
    const result = await resolveOneEntry({ flowId: "grandchild", hops: 2, entry: "runId", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: "run-root-999" });
  });

  it("not-found — walked off the top (fewer real ancestors than declared)", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { root: { parentInvocationId: null } },
    });
    const result = await resolveOneEntry({ flowId: "root", hops: 1, entry: "runId", runStore, nodeExecutionsStore });
    assert.equal(result.status, "not-found");
  });

  it("hops:0 — resolves the declaring node's OWN flow's entry field, no walk", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { stateJson: { runId: "run-self-1" } } },
    });
    const result = await resolveOneEntry({ flowId: "self", hops: 0, entry: "runId", runStore, nodeExecutionsStore });
    assert.deepEqual(result, { status: "ready", value: "run-self-1" });
  });
});

describe("resolveOneConfig — pipeline-declared static config (index.js's `state:` block), read live off the loaded pipeline definition, never off stateJson", () => {
  it("hops:0, ready — resolves a config key off the declaring node's own flow's pipeline", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { stateJson: { pipelineId: "system_memory-synthesis" } } },
    });
    const pipelinesRef = makePipelinesRef({
      "system_memory-synthesis": { state: { sourceFilter: { includeAgentSelfRediscovery: true } } },
    });
    const result = await resolveOneConfig({ flowId: "self", hops: 0, config: "sourceFilter", runStore, nodeExecutionsStore, pipelinesRef });
    assert.deepEqual(result, { status: "ready", value: { includeAgentSelfRediscovery: true } });
  });

  it("hops:1, ready — walks to the parent flow's pipeline before reading config", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: {
        child: { parentInvocationId: "inv-parent" },
        parent: { stateJson: { pipelineId: "outer" } },
      },
      invocations: { "inv-parent": { flowId: "parent" } },
    });
    const pipelinesRef = makePipelinesRef({ outer: { state: { minClusterSize: 3 } } });
    const result = await resolveOneConfig({ flowId: "child", hops: 1, config: "minClusterSize", runStore, nodeExecutionsStore, pipelinesRef });
    assert.deepEqual(result, { status: "ready", value: 3 });
  });

  it("ready with undefined value — an unrecognized config key is a legitimate value, not a not-found state (no schema on `state:` to validate against)", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { stateJson: { pipelineId: "p1" } } },
    });
    const pipelinesRef = makePipelinesRef({ p1: { state: { known: 1 } } });
    const result = await resolveOneConfig({ flowId: "self", hops: 0, config: "typo", runStore, nodeExecutionsStore, pipelinesRef });
    assert.deepEqual(result, { status: "ready", value: undefined });
  });

  it("not-found — the target flow's pipeline isn't currently loaded in pipelinesRef", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { stateJson: { pipelineId: "not-loaded" } } },
    });
    const result = await resolveOneConfig({ flowId: "self", hops: 0, config: "x", runStore, nodeExecutionsStore, pipelinesRef: makePipelinesRef({}) });
    assert.equal(result.status, "not-found");
  });

  it("not-found — walked off the top", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { root: { parentInvocationId: null } },
    });
    const result = await resolveOneConfig({ flowId: "root", hops: 1, config: "x", runStore, nodeExecutionsStore, pipelinesRef: makePipelinesRef({}) });
    assert.equal(result.status, "not-found");
  });
});

describe("makeNeedsCtx — the ctx.needs(varName) accessor a node's own code actually calls", () => {
  it("returns the resolved value when ready", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: 1000, resultJson: JSON.stringify({ SCAN: { listings: [1] } }), resultTooLargeBytes: null } },
    });
    const node = { needs: { scan: { hops: 1, nodeId: "SCAN" } } };
    const needs = makeNeedsCtx({ node, flowId: "child", runStore, nodeExecutionsStore });
    assert.deepEqual(await needs("scan"), { listings: [1] });
  });

  it("throws NodeBlocked with _kind:'needs-not-ready' when the declared need isn't ready — not a bare NodeBlocked, which the engine treats as a hard failure", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: null, resultJson: null, resultTooLargeBytes: null } },
    });
    const node = { needs: { scan: { hops: 1, nodeId: "SCAN" } } };
    const needs = makeNeedsCtx({ node, flowId: "child", runStore, nodeExecutionsStore });
    await assert.rejects(
      () => needs("scan"),
      (err) => {
        assert.equal(err.name, "NodeBlocked");
        assert.equal(err._blocked._kind, "needs-not-ready");
        assert.deepEqual(err._blocked.needed, ["SCAN"]);
        return true;
      },
    );
  });

  it("throws a plain Error (not NodeBlocked) for a varName that was never declared in .needs — an author typo, not a wait state", async () => {
    const node = { needs: { scan: { hops: 1, nodeId: "SCAN" } } };
    const needs = makeNeedsCtx({ node, flowId: "child", runStore: {}, nodeExecutionsStore: {} });
    await assert.rejects(() => needs("typo"), (err) => {
      assert.notEqual(err.name, "NodeBlocked");
      return true;
    });
  });

  it("lazy: never touches the store at all if the returned function is never called", async () => {
    let called = false;
    const runStore = { get: async () => { called = true; return null; } };
    const node = { needs: { scan: { hops: 1, nodeId: "SCAN" } } };
    makeNeedsCtx({ node, flowId: "child", runStore, nodeExecutionsStore: {} });
    assert.equal(called, false, "constructing the accessor must not itself resolve anything");
  });

  it("fields: picks a subset object with just those keys", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: 1000, resultJson: JSON.stringify({ SCAN: { listings: [1, 2], filtered: [1], bono: 5 } }), resultTooLargeBytes: null } },
    });
    const node = { needs: { picked: { hops: 1, nodeId: "SCAN", fields: ["listings", "bono"] } } };
    const needs = makeNeedsCtx({ node, flowId: "child", runStore, nodeExecutionsStore });
    assert.deepEqual(await needs("picked"), { listings: [1, 2], bono: 5 });
  });

  it("fields: a single-element array still resolves to a dict, never unwrapped to a bare value", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { child: { parentInvocationId: "inv-parent-SCAN" } },
      invocations: { "inv-parent-SCAN": { flowId: "parent" } },
      results: { "parent:SCAN": { resultWrittenAt: 1000, resultJson: JSON.stringify({ SCAN: { listings: [1, 2], filtered: [1] } }), resultTooLargeBytes: null } },
    });
    const node = { needs: { listings: { hops: 1, nodeId: "SCAN", fields: ["listings"] } } };
    const needs = makeNeedsCtx({ node, flowId: "child", runStore, nodeExecutionsStore });
    const result = await needs("listings");
    assert.deepEqual(result, { listings: [1, 2] }, "always a dict, even for one field — no bare-array/bare-scalar mode");
  });

  it("entry-based spec: resolves an ancestor's own runId, not a node output", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: {
        child: { parentInvocationId: "inv-parent" },
        parent: { stateJson: { runId: "run-xyz" } },
      },
      invocations: { "inv-parent": { flowId: "parent" } },
    });
    const node = { needs: { runId: { hops: 1, entry: "runId" } } };
    const needs = makeNeedsCtx({ node, flowId: "child", runStore, nodeExecutionsStore });
    assert.equal(await needs("runId"), "run-xyz");
  });

  it("config-based spec at hops:0: resolves a pipeline-declared static config field off the live pipeline definition", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { stateJson: { pipelineId: "system_memory-synthesis" } } },
    });
    const pipelinesRef = makePipelinesRef({
      "system_memory-synthesis": { state: { sourceFilter: { includeAgentSelfRediscovery: false } } },
    });
    const node = { needs: { sourceFilter: { hops: 0, config: "sourceFilter" } } };
    const needs = makeNeedsCtx({ node, flowId: "self", runStore, nodeExecutionsStore, pipelinesRef });
    assert.deepEqual(await needs("sourceFilter"), { includeAgentSelfRediscovery: false });
  });

  it("config-based spec: not-found throws NodeBlocked with _kind:'needs-not-ready' and the config key in `needed`, same shape as nodeId/entry", async () => {
    const { runStore, nodeExecutionsStore } = makeStore({
      flows: { self: { stateJson: { pipelineId: "not-loaded" } } },
    });
    const node = { needs: { sourceFilter: { hops: 0, config: "sourceFilter" } } };
    const needs = makeNeedsCtx({ node, flowId: "self", runStore, nodeExecutionsStore, pipelinesRef: makePipelinesRef({}) });
    await assert.rejects(
      () => needs("sourceFilter"),
      (err) => {
        assert.equal(err.name, "NodeBlocked");
        assert.equal(err._blocked._kind, "needs-not-ready");
        assert.deepEqual(err._blocked.needed, ["sourceFilter"]);
        return true;
      },
    );
  });
});
