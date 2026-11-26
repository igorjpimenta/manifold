import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  resolveEdge,
  isTerminal,
  isBlocked,
  isQueued,
  mergeForConflict,
  withEditableTextDefault,
  shortSummary,
  deepClone,
  detectBlockedSentinel,
  usesNamedParams,
  detectSkippedSentinel,
  validateGateLayout,
  isFreshNodeEntry,
  describeWaitFallback,
  assembleState,
} from "../../../engine/executor-helpers.js";

describe("resolveEdge", () => {
  it("returns string edge directly", () => {
    assert.equal(resolveEdge({ a: "b" }, "a", {}), "b");
  });

  it("calls function edge with state", () => {
    const edges = { step: (s) => s.go ? "yes" : "no" };
    assert.equal(resolveEdge(edges, "step", { go: true }), "yes");
    assert.equal(resolveEdge(edges, "step", { go: false }), "no");
  });

  it("returns null for missing edge", () => {
    assert.equal(resolveEdge({}, "missing", {}), null);
  });

  it("returns null and does not throw when function edge throws", () => {
    const edges = { bad: () => { throw new Error("oops"); } };
    assert.equal(resolveEdge(edges, "bad", {}, { logger: { warn: () => {} }, flowId: "00000000" }), null);
  });

  it("a 1-param edge is invoked with exactly 1 argument, even when ctx.error is present", () => {
    let receivedArgCount = "not called";
    // Plain `function`, not arrow: `arguments` must be this function's own binding.
    const edges = { step: function (s) { receivedArgCount = arguments.length; return "next"; } };
    resolveEdge(edges, "step", {}, { error: { message: "boom", errorType: "infra_unavailable" } });
    assert.equal(receivedArgCount, 1);
  });

  it("a 2-param edge receives ctx.error as its 2nd argument", () => {
    let receivedError;
    const edges = { step: (s, error) => { receivedError = error; return "next"; } };
    const err = { message: "boom", errorType: "infra_unavailable" };
    resolveEdge(edges, "step", {}, { error: err });
    assert.deepEqual(receivedError, err);
  });

  it("a 2-param edge receives null when ctx has no error (success path)", () => {
    let receivedError = "not called";
    const edges = { step: (s, error) => { receivedError = error; return "next"; } };
    resolveEdge(edges, "step", {}, { logger: null, flowId: "x" });
    assert.equal(receivedError, null);
  });

  it("a 2-param edge receives null when ctx itself is omitted", () => {
    let receivedError = "not called";
    const edges = { step: (s, error) => { receivedError = error; return "next"; } };
    resolveEdge(edges, "step", {});
    assert.equal(receivedError, null);
  });

  it("FOOTGUN: a default value on the 2nd param collapses fn.length back to 1 — no error ever passed", () => {
    let receivedError = "not called";
    const edges = { step: (s, error = "default") => { receivedError = error; return "next"; } };
    resolveEdge(edges, "step", {}, { error: { message: "boom", errorType: "infra_unavailable" } });
    assert.equal(receivedError, "default");
  });
});

describe("isTerminal", () => {
  it("returns true for succeeded, failed, cancelled, skipped", () => {
    for (const s of ["succeeded", "failed", "cancelled", "skipped"]) {
      assert.equal(isTerminal(s), true, s);
    }
  });
  it("returns false for running, waiting, blocked", () => {
    for (const s of ["running", "waiting", "blocked"]) {
      assert.equal(isTerminal(s), false, s);
    }
  });
});

describe("isBlocked", () => {
  it("returns true for blocked", () => {
    assert.equal(isBlocked("blocked"), true);
  });
  // "waiting" is never persisted — gate pauses write "blocked" — so it's non-blocked like any other status.
  it("returns false for running, waiting, and terminal statuses", () => {
    for (const s of ["running", "waiting", "succeeded", "failed"]) {
      assert.equal(isBlocked(s), false, s);
    }
  });
});

describe("isQueued", () => {
  it("returns true for queued", () => {
    assert.equal(isQueued("queued"), true);
  });
  it("returns false for running, blocked, and terminal statuses", () => {
    for (const s of ["running", "blocked", "succeeded", "failed"]) {
      assert.equal(isQueued(s), false, s);
    }
  });
});

describe("isFreshNodeEntry", () => {
  it("returns true for the first-ever entry to any node (no prior invocation at all)", () => {
    assert.equal(isFreshNodeEntry(null, "DRAFT"), true);
    assert.equal(isFreshNodeEntry(undefined, "DRAFT"), true);
  });

  it("returns true for a genuine loop-back — the last dispatched node differs from the one being entered", () => {
    assert.equal(isFreshNodeEntry("GATE", "DRAFT"), true);
  });

  it("returns false when the last dispatched node IS the one being entered (continuation)", () => {
    // Operator retry and reentrant loop-wait both land here: findCurrentStep still resolves to the node's own id.
    assert.equal(isFreshNodeEntry("DRAFT", "DRAFT"), false);
  });
});

describe("mergeForConflict", () => {
  it("returns mine when fresh is not an object", () => {
    assert.equal(mergeForConflict(null, { a: 1 }).a, 1);
  });

  it("mine values take precedence over fresh for scalar keys", () => {
    assert.deepEqual(mergeForConflict({ a: 1 }, { a: 2 }), { a: 2 });
  });

  it("keeps fresh keys that mine doesn't have", () => {
    const result = mergeForConflict({ a: 1, extra: "fresh" }, { a: 2 });
    assert.equal(result.extra, "fresh");
  });

  it("deep-merges nested objects", () => {
    const fresh = { slots: { A: { done: true }, B: { done: false } } };
    const mine  = { slots: { A: { done: true }, B: { done: true } } };
    const result = mergeForConflict(fresh, mine);
    assert.equal(result.slots.A.done, true);
    assert.equal(result.slots.B.done, true);
  });
});

describe("withEditableTextDefault", () => {
  it("returns payload unchanged when editableText is already set", () => {
    const p = { editableText: "existing" };
    assert.equal(withEditableTextDefault(p, { DRAFT: { text: "draft" } }), p);
  });

  it("fills editableText from last branch data text", () => {
    const result = withEditableTextDefault({ title: "Approve?" }, { DRAFT: { text: "hello world" } });
    assert.equal(result.editableText, "hello world");
  });

  it("returns payload unchanged when no text candidate found", () => {
    const p = { title: "X" };
    const result = withEditableTextDefault(p, { DATA: { score: 5 } });
    assert.equal(result.editableText, undefined);
  });

  it("returns payload unchanged when payload is null", () => {
    assert.equal(withEditableTextDefault(null, {}), null);
  });
});

describe("shortSummary", () => {
  it("returns empty string for falsy input", () => {
    assert.equal(shortSummary(""), "");
    assert.equal(shortSummary(null), "");
  });

  it("returns first line of multi-line text", () => {
    assert.equal(shortSummary("first\nsecond"), "first");
  });

  it("truncates at 240 chars with ellipsis", () => {
    const long = "x".repeat(250);
    const result = shortSummary(long);
    assert.ok(result.endsWith("..."), result.slice(-10));
    assert.equal(result.length, 240);
  });

  it("returns text unchanged if under 240 chars", () => {
    const s = "short text";
    assert.equal(shortSummary(s), s);
  });
});

describe("deepClone", () => {
  it("clones a nested object", () => {
    const orig = { a: { b: [1, 2] } };
    const clone = deepClone(orig);
    clone.a.b.push(3);
    assert.equal(orig.a.b.length, 2);
  });

  it("produces equal structure", () => {
    const orig = { x: 1, y: { z: true } };
    assert.deepEqual(deepClone(orig), orig);
  });
});

describe("detectBlockedSentinel", () => {
  it("returns _blocked when present", () => {
    const block = { reason: "oops", needed: [], hint: "" };
    assert.deepEqual(detectBlockedSentinel({ _blocked: block }), block);
  });
  it("returns null for regular state update", () => {
    assert.equal(detectBlockedSentinel({ count: 5 }), null);
  });
  it("returns null for null input", () => {
    assert.equal(detectBlockedSentinel(null), null);
  });
  it("returns null when _blocked is not an object", () => {
    assert.equal(detectBlockedSentinel({ _blocked: "string" }), null);
  });
});

describe("usesNamedParams", () => {
  it("returns true for destructured single-param function", () => {
    assert.equal(usesNamedParams(({ state, output }) => state), true);
  });
  it("returns false for positional param function", () => {
    assert.equal(usesNamedParams((state, output) => state), false);
  });
  it("returns false for non-function", () => {
    assert.equal(usesNamedParams(null), false);
  });
  it("returns false for function with more than 1 param", () => {
    assert.equal(usesNamedParams(({ a }, b) => a), false);
  });
});

describe("detectSkippedSentinel", () => {
  it("returns _skipped when present", () => {
    const skip = { reason: "already done" };
    assert.deepEqual(detectSkippedSentinel({ _skipped: skip }), skip);
  });
  it("returns null for regular update", () => {
    assert.equal(detectSkippedSentinel({ count: 1 }), null);
  });
  it("returns null for null input", () => {
    assert.equal(detectSkippedSentinel(null), null);
  });
});

describe("validateGateLayout", () => {
  it("does nothing for non-array or empty items", () => {
    assert.doesNotThrow(() => validateGateLayout(undefined));
    assert.doesNotThrow(() => validateGateLayout(null));
    assert.doesNotThrow(() => validateGateLayout([]));
  });

  it("does nothing when layout has no deck kind at all", () => {
    const items = [{ itemId: "a", requestPayload: { layout: [
      { kind: "field", id: "f1", label: "L", type: "text", value: "v", editable: true },
      { kind: "field-group", id: "g1", fields: [] },
    ] } }];
    assert.doesNotThrow(() => validateGateLayout(items));
  });

  it("throws when requestPayload/layout is absent", () => {
    assert.throws(() => validateGateLayout([{ itemId: "a" }]), /layout must be an array/);
    assert.throws(() => validateGateLayout([{ itemId: "b", requestPayload: {} }]), /layout must be an array/);
  });

  it("throws when a deck is mixed with another layout kind", () => {
    const items = [{ itemId: "a", requestPayload: { layout: [
      { kind: "deck", id: "d1", questions: [{ question: "Q?" }] },
      { kind: "field", id: "f1", label: "L", type: "text", value: "v", editable: true },
    ] } }];
    assert.throws(() => validateGateLayout(items), /mixes kind:"deck"/);
  });

  it("throws when deck.questions is missing or empty", () => {
    const missing = [{ itemId: "a", requestPayload: { layout: [{ kind: "deck", id: "d1" }] } }];
    assert.throws(() => validateGateLayout(missing), /non-empty questions array/);
    const empty = [{ itemId: "a", requestPayload: { layout: [{ kind: "deck", id: "d1", questions: [] }] } }];
    assert.throws(() => validateGateLayout(empty), /non-empty questions array/);
  });

  it("throws when a question is missing a non-empty question string", () => {
    const items = [{ itemId: "a", requestPayload: { layout: [
      { kind: "deck", id: "d1", questions: [{ question: "" }] },
    ] } }];
    assert.throws(() => validateGateLayout(items), /missing a non-empty "question" string/);
    const noQuestionKey = [{ itemId: "a", requestPayload: { layout: [
      { kind: "deck", id: "d1", questions: [{ hint: "h" }] },
    ] } }];
    assert.throws(() => validateGateLayout(noQuestionKey), /missing a non-empty "question" string/);
  });

  it("throws when a question's hint is present but not a string", () => {
    const items = [{ itemId: "a", requestPayload: { layout: [
      { kind: "deck", id: "d1", questions: [{ question: "Q?", hint: 123 }] },
    ] } }];
    assert.throws(() => validateGateLayout(items), /non-string "hint"/);
  });

  it("passes for a well-formed single-item deck", () => {
    const items = [{ itemId: "a", requestPayload: { layout: [
      { kind: "deck", id: "d1", questions: [{ question: "Q1?", hint: "h" }, { question: "Q2?" }] },
    ] } }];
    assert.doesNotThrow(() => validateGateLayout(items));
  });

  it("validates each item independently across a multi-item batch", () => {
    const items = [
      { itemId: "good", requestPayload: { layout: [{ kind: "deck", id: "d1", questions: [{ question: "Q?" }] }] } },
      { itemId: "bad", requestPayload: { layout: [
        { kind: "deck", id: "d2", questions: [{ question: "Q?" }] },
        { kind: "field", id: "f1", label: "L", type: "text", value: "v" },
      ] } },
    ];
    assert.throws(() => validateGateLayout(items), /item bad:/);
  });
});

describe("describeWaitFallback", () => {
  it("human-gate: pluralizes correctly for multiple pending items", () => {
    const waitJson = { kind: "human-gate", nodeId: "GATE", pending: ["a", "b", "c"] };
    assert.equal(describeWaitFallback(waitJson, "GATE"), "awaiting 3 approvals at GATE");
  });

  it("human-gate: singular for exactly one pending item", () => {
    const waitJson = { kind: "human-gate", nodeId: "GATE", pending: ["a"] };
    assert.equal(describeWaitFallback(waitJson, "GATE"), "awaiting 1 approval at GATE");
  });

  it("human-gate: falls back to items.length when pending is absent", () => {
    const waitJson = { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "a" }, { itemId: "b" }] };
    assert.equal(describeWaitFallback(waitJson, "GATE"), "awaiting 2 approvals at GATE");
  });

  it("children-pending: pluralizes correctly for multiple children", () => {
    const waitJson = { kind: "children-pending", nodeId: "LOOP", childFlowIds: ["c1", "c2", "c3"] };
    assert.equal(describeWaitFallback(waitJson, "LOOP"), "awaiting 3 child flows at LOOP");
  });

  it("children-pending: singular for exactly one child", () => {
    const waitJson = { kind: "children-pending", nodeId: "LOOP", childFlowIds: ["c1"] };
    assert.equal(describeWaitFallback(waitJson, "LOOP"), "awaiting 1 child flow at LOOP");
  });

  it("children-pending: zero when childFlowIds is missing", () => {
    const waitJson = { kind: "children-pending", nodeId: "LOOP" };
    assert.equal(describeWaitFallback(waitJson, "LOOP"), "awaiting 0 child flows at LOOP");
  });

  it("falls back to the generic template for an unrecognised kind", () => {
    const waitJson = { kind: "something-new", nodeId: "MYSTERY" };
    assert.equal(describeWaitFallback(waitJson, "MYSTERY"), "blocked at MYSTERY");
  });

  it("falls back to the generic template when waitJson is null/undefined", () => {
    assert.equal(describeWaitFallback(null, "NODE"), "blocked at NODE");
    assert.equal(describeWaitFallback(undefined, "NODE"), "blocked at NODE");
  });
});

function resultRow(nodeId, { resultWrittenAt = 1000, resultJson = null, resultTooLargeBytes = null } = {}) {
  return { nodeId, resultWrittenAt, resultJson, resultTooLargeBytes };
}

function makeStore({ results = [], subflowNodeIds = [], invocationIdsByNode = {}, invocations = {}, children = [] } = {}) {
  return {
    nodeExecutionsStore: {
      listResultsForFlow: async () => results,
      listSubflowNodeIdsForFlow: async () => subflowNodeIds,
      listInvocationIdsForNode: async (flowId, nodeId) => invocationIdsByNode[nodeId] ?? [],
      getInvocation: async (id) => invocations[id] ?? null,
    },
    runStore: {
      listForParentInvocations: async () => children,
    },
  };
}

describe("assembleState", () => {
  it("empty flow — no invocations at all — returns {}", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({ results: [] });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.deepEqual(state, {});
  });

  it("merges each node's own resultJson via a shallow spread", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [
        resultRow("PRIME", { resultJson: JSON.stringify({}) }),
        resultRow("DRAFT", { resultJson: JSON.stringify({ DRAFT: { text: "hello" } }) }),
      ],
    });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.deepEqual(state, { DRAFT: { text: "hello" } });
  });

  it("a later node's update overwrites an earlier one's same-named key, matching replay order", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [
        resultRow("A", { resultJson: JSON.stringify({ x: 1 }) }),
        resultRow("B", { resultJson: JSON.stringify({ x: 2 }) }),
      ],
    });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.equal(state.x, 2, "B ran after A in listResultsForFlow's own order — its value must win");
  });

  it("a row with resultWrittenAt: null (dispatched, not yet complete) is skipped entirely — not merged as undefined/null", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [resultRow("SCAN", { resultWrittenAt: null, resultJson: null })],
    });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.deepEqual(state, {});
  });

  it("a genuinely-null result (resultWrittenAt set, resultJson literally 'null') merges as a no-op", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [
        resultRow("GATE", { resultJson: JSON.stringify({}) }),
        resultRow("STAMP", { resultWrittenAt: 2000, resultJson: "null" }),
      ],
    });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.deepEqual(state, {});
  });

  it("a corrupted resultJson row is skipped, not fatal to the rest of assembly", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [
        resultRow("BAD", { resultJson: "{not valid json" }),
        resultRow("GOOD", { resultJson: JSON.stringify({ GOOD: { ok: true } }) }),
      ],
    });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.deepEqual(state, { GOOD: { ok: true } });
  });

  it("_subflowGroups is attached when getAllSubflowGroups finds real groups", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [resultRow("PROPOSE", { resultJson: JSON.stringify({}) })],
      subflowNodeIds: ["PROPOSE"],
      invocationIdsByNode: { PROPOSE: ["item-inv-1"] },
      invocations: { "item-inv-1": { subflowKey: "listing-1" } },
      children: [{ flowId: "child-1", parentInvocationId: "item-inv-1", status: "succeeded", stateJson: { DRAFT: {} } }],
    });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.ok(state._subflowGroups, "PROPOSE has a real spawned child — _subflowGroups must be attached");
    assert.ok(state._subflowGroups.PROPOSE["listing-1"], "keyed by the child's own subflowKey, same shape getAllSubflowGroups already returns");
  });

  it("no _subflowGroups key at all when there are no subflow groups", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({ results: [resultRow("PRIME", { resultJson: "{}" })] });
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore });
    assert.equal("_subflowGroups" in state, false);
  });

  it("missing nodeExecutionsStore/runStore entirely — degrades to {}, does not throw", async () => {
    const state = await assembleState("flow-1", {});
    assert.deepEqual(state, {});
  });

  it("baseState's entry/bookkeeping fields (pipelineId, args, cost, approvedIds) pass through untouched — no node's resultJson ever produces them", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [resultRow("DRAFT", { resultJson: JSON.stringify({ DRAFT: { text: "hi" } }) })],
    });
    const baseState = { pipelineId: "test_pipeline", args: { a: 1 }, cost: 0.02, approvedIds: ["x"] };
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore, baseState });
    assert.equal(state.pipelineId, "test_pipeline");
    assert.deepEqual(state.args, { a: 1 });
    assert.equal(state.cost, 0.02);
    assert.deepEqual(state.approvedIds, ["x"]);
    assert.deepEqual(state.DRAFT, { text: "hi" });
  });

  it("a resultJson value overlays baseState's own copy of the same key — resultJson wins", async () => {
    const { nodeExecutionsStore, runStore } = makeStore({
      results: [resultRow("DRAFT", { resultJson: JSON.stringify({ DRAFT: { text: "fresher" } }) })],
    });
    const baseState = { DRAFT: { text: "stale" } };
    const state = await assembleState("flow-1", { runStore, nodeExecutionsStore, baseState });
    assert.deepEqual(state.DRAFT, { text: "fresher" });
  });
});
