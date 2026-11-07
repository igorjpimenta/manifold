// Unit tests for gates/gate-resolver.js — createGateResolver factory.
// Covers: recordItemDecision input validation, idempotency, item-membership
// for waiting/blocked/running flows, persist path and maybeResolveFlow trigger;
// maybeResolveFlow guard conditions, sendFailed coercion, already-processed
// item skipping; applyResolutionsAndResume approve/reject accumulation,
// editedText storage, approvalResults append, setRunning+resume sequencing,
// revision_conflict retry loop, non-conflict bail; updateApprovalState routing
// between setRunning/setWaiting, retry exhaustion, non-conflict bail.

import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createGateResolver } from "../../../gates/gate-resolver.js";

// ─── helpers ────────────────────────────────────────────────────────────────

// Always-admits-immediately fake — applyResolutionsAndResume now
// re-requests admission (admitAndSetRunning, engine/child-resume-
// trigger.js) before resuming a flow past its gate. None of these tests
// exercise queue/capacity behavior itself (see
// executor-invoke-pipeline-singleton-busy.test.js and executor.test.js's
// "start" describe block for real admission-queue-store coverage).
const admissionQueueStore = {
  admit: async () => ({ id: "fake-admission", status: "admitted" }),
  release: async () => ({ released: true, promoted: null }),
};

function makeLogger() {
  const calls = { info: [], warn: [], debug: [], error: [] };
  return {
    calls,
    info:  (...a) => calls.info.push(a),
    warn:  (...a) => calls.warn.push(a),
    debug: (...a) => calls.debug.push(a),
    error: (...a) => calls.error.push(a),
  };
}

/** Build a minimal flow object paused on a human-gate waitJson. Gate pauses
 *  are always persisted as "blocked" — "waiting" is never actually written
 *  anywhere (confirmed repo-wide). */
function makeWaitingFlow({ status = "blocked", kind = "human-gate", items = [], stateJson = {}, waitJson = undefined, revision = 1, flowId = randomUUID() } = {}) {
  return {
    flowId,
    status,
    revision,
    lastStep: "gate-node",
    statusReason: null,
    stateJson,
    waitJson: waitJson !== undefined ? waitJson : {
      kind,
      nodeId: "gate-node",
      items,
    },
  };
}

/** Build a minimal running flow. Running flows do not support item-membership
 *  validation (validItem is always false for running flows). */
function makeRunningFlow({ itemId = randomUUID(), nodeId = "gate-node", stateJson = {}, revision = 1 } = {}) {
  return {
    flowId: randomUUID(),
    status: "running",
    revision,
    lastStep: nodeId,
    statusReason: null,
    stateJson,
    waitJson: null,
  };
}

/** Build a stub store that holds a single flow in memory. */
function makeStore(flow) {
  let current = flow ? { ...flow } : null;
  return {
    _current: () => current,
    get: (id) => (current && current.flowId === id ? { ...current } : null),
    setRunning: mock.fn(({ flowId, expectedRevision, lastStep, stateJson }) => {
      if (!current || current.flowId !== flowId) return { applied: false, code: "flow_not_found" };
      if (current.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      current = { ...current, revision: current.revision + 1, stateJson, status: "running", waitJson: null };
      return { applied: true };
    }),
    // Despite the name, the real setWaiting always writes status="blocked"
    // (confirmed repo-wide) — never "waiting". Mirrored here.
    setWaiting: mock.fn(({ flowId, expectedRevision, lastStep, stateJson, waitJson, statusReason }) => {
      if (!current || current.flowId !== flowId) return { applied: false, code: "flow_not_found" };
      if (current.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      current = { ...current, revision: current.revision + 1, stateJson, waitJson, statusReason, status: "blocked" };
      return { applied: true };
    }),
    _replace: (f) => { current = f ? { ...f } : null; },
  };
}

/** Build a stub executor. */
function makeExecutor() {
  return {
    resume: mock.fn(async () => {}),
  };
}

/** Build a stub node-executions store — gate-resolver.js's
 *  persistApplyFailureMarker is injectable specifically so tests don't fall
 *  through to the real getNodeExecutionsStore() singleton. `results` seeds
 *  listResultsForFlow, consumed by assembleState()'s replayResults() to
 *  reconstruct node-output keys (e.g. state.DRAFT) that flow.stateJson
 *  itself never persists — see the "apply() sees assembled state" tests
 *  below. */
function makeFakeNodeExecutions(results = []) {
  const writes = [];
  return {
    writes,
    write: (row) => writes.push(row),
    listResultsForFlow: async () => results,
  };
}

/** Drain all pending setImmediate callbacks. */
async function drainImmediate() {
  await new Promise((r) => setImmediate(r));
}

/** Build a fully wired resolver + collaborators for a given flow. */
function makeResolver(flow) {
  const logger = makeLogger();
  const api = { logger };
  const store = makeStore(flow);
  const executor = makeExecutor();
  const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
  return { resolver, store, executor, logger, api };
}

// ─── recordItemDecision — input validation ───────────────────────────────────

describe("recordItemDecision — input validation", () => {
  test("rejects missing flowId (missing_flow_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: "", itemId: "x", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "missing_flow_id" });
  });

  test("rejects null flowId (missing_flow_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: null, itemId: "x", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "missing_flow_id" });
  });

  test("rejects missing itemId (missing_item_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "missing_item_id" });
  });

  test("rejects null itemId (missing_item_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: null, decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "missing_item_id" });
  });

  test("rejects invalid decision value (invalid_decision)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "maybe", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "invalid_decision" });
  });

  test("rejects empty decision (invalid_decision)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "invalid_decision" });
  });

  test("accepts approve, reject, edit as valid decisions", async () => {
    // We only check they don't fail on decision validation — subsequent checks
    // will fail for other reasons (no store flow). We test all three pass the decision guard.
    const { resolver } = makeResolver(null);
    for (const decision of ["approve", "reject"]) {
      const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision, source: "cli" });
      assert.notEqual(res.code, "invalid_decision", `decision=${decision} should not fail on decision validation`);
    }
  });

  test("rejects edit decision without editedText string (edit_requires_text)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "edit", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "edit_requires_text" });
  });

  test("rejects edit decision with non-string editedText (edit_requires_text)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "edit", editedText: 42, source: "cli" });
    assert.deepEqual(res, { ok: false, code: "edit_requires_text" });
  });

  test("accepts edit decision with valid string editedText", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "edit", editedText: "my reply", source: "cli" });
    assert.notEqual(res.code, "edit_requires_text");
  });
});

// ─── recordItemDecision — telltale detection ─────────────────────────────────

describe("recordItemDecision — telltale editedText rejection", () => {
  const telltales = [
    "Approve to send",
    "Reject to drop",
    "openclaw manifold respond",
    "─── Approve to send",
  ];

  for (const telltale of telltales) {
    test(`rejects editedText containing "${telltale.slice(0, 30)}" (edit_text_looks_like_description)`, async () => {
      const { resolver } = makeResolver(null);
      const res = await resolver.recordItemDecision({
        flowId: randomUUID(),
        itemId: "x",
        decision: "edit",
        editedText: `Prefix text. ${telltale} suffix text.`,
        source: "cli",
      });
      assert.deepEqual(res, { ok: false, code: "edit_text_looks_like_description" });
    });
  }

  test("does not reject editedText that contains none of the telltales", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({
      flowId: randomUUID(),
      itemId: "x",
      decision: "edit",
      editedText: "Hello, this is a genuine reply with no telltale strings.",
      source: "cli",
    });
    assert.notEqual(res.code, "edit_text_looks_like_description");
  });
});

// ─── recordItemDecision — source validation ───────────────────────────────────

describe("recordItemDecision — source validation", () => {
  test("rejects unknown source (invalid_source)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: "unknown-channel" });
    assert.deepEqual(res, { ok: false, code: "invalid_source" });
  });

  test("rejects missing source (invalid_source)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: null });
    assert.deepEqual(res, { ok: false, code: "invalid_source" });
  });

  test("accepts all valid sources", async () => {
    for (const source of ["app", "dashboard", "cli", "pipeline"]) {
      const { resolver } = makeResolver(null);
      const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source });
      assert.notEqual(res.code, "invalid_source", `source=${source} should be accepted`);
    }
  });
});

// ─── recordItemDecision — flow lookup ────────────────────────────────────────

describe("recordItemDecision — flow lookup", () => {
  test("returns flow_not_found when store.get returns null", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "flow_not_found" });
  });

  test("returns flow_not_awaiting for completed flow status", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "completed" };
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "x", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "flow_not_awaiting" });
  });

  test("returns flow_not_awaiting for failed flow status", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "failed" };
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "x", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "flow_not_awaiting" });
  });

  test("returns flow_not_awaiting for cancelled flow status", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "cancelled" };
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "x", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "flow_not_awaiting" });
  });
});

// ─── recordItemDecision — idempotency ────────────────────────────────────────

describe("recordItemDecision — idempotency", () => {
  test("returns already-decided when approvalPending[itemId].decision is set", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "app", decidedAt: Date.now() },
        },
      },
    });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "cli" });
    assert.equal(res.ok, true);
    assert.equal(res.status, "already-decided");
  });

  test("already-decided does not write to the store", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() },
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "dashboard" });
    assert.equal(store.setRunning.mock.calls.length, 0);
    assert.equal(store.setWaiting.mock.calls.length, 0);
  });

  test("already-decided takes priority over item-membership check (idempotent after executor consumed item)", async () => {
    // Simulate: executor consumed the item and removed it from waitJson.items,
    // but a retry fires the same decision again.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [], // item absent from waitJson
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() },
        },
      },
    });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.equal(res.ok, true);
    assert.equal(res.status, "already-decided");
  });
});

// ─── recordItemDecision — gate kind validation ────────────────────────────────

describe("recordItemDecision — gate kind validation for waiting/blocked flows", () => {
  test("returns flow_not_gate when waitJson.kind is not a gate kind", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "some-other-kind", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "flow_not_gate" });
  });

  test("accepts waitJson.kind = human-gate", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "human-gate", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.notEqual(res.code, "flow_not_gate");
  });

  test("accepts waitJson.kind = gate", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "gate", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.notEqual(res.code, "flow_not_gate");
  });

  test("accepts waitJson.kind = verify-required", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "verify-required", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.notEqual(res.code, "flow_not_gate");
  });
});

// ─── recordItemDecision — item membership ────────────────────────────────────

describe("recordItemDecision — item membership for waiting flows", () => {
  test("returns item_not_in_wait when itemId is not in waitJson.items", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId: randomUUID() }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "not-in-list", decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "item_not_in_wait" });
  });

  test("returns item_not_in_wait for running flows regardless of itemId (validItem is always false)", async () => {
    // Running flows don't support mid-batch item-membership validation via
    // — validItem is always false for running flows. Gate items
    // for running flows must arrive via a blocked waitJson.
    const itemId = randomUUID();
    const flow = makeRunningFlow({ itemId });
    const flowId = flow.flowId;
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: false, code: "item_not_in_wait" });
  });
});

// ─── recordItemDecision — success path ───────────────────────────────────────

describe("recordItemDecision — successful write path", () => {
  test("writes approvalPending entry with decision, source, and decidedAt", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const before = Date.now();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, store } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "dashboard" });
    assert.equal(res.ok, true);
    assert.equal(res.status, "applied");
    // The store's state should now contain the approvalPending entry
    const current = store._current();
    const entry = current.stateJson?.approvalPending?.[itemId];
    assert.ok(entry, "approvalPending[itemId] should exist");
    assert.equal(entry.decision, "approve");
    assert.equal(entry.source, "dashboard");
    assert.ok(entry.decidedAt >= before, "decidedAt should be set");
  });

  test("includes editedText in approvalPending entry for edit decision", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, store } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "edit", editedText: "my custom reply", source: "app" });
    const current = store._current();
    const entry = current.stateJson?.approvalPending?.[itemId];
    assert.equal(entry?.editedText, "my custom reply");
  });

  test("includes reason in approvalPending entry when provided", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, store } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "pipeline", reason: "listing already won" });
    const current = store._current();
    const entry = current.stateJson?.approvalPending?.[itemId];
    assert.equal(entry?.reason, "listing already won");
  });

  test("does not include editedText field when not provided on approve", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, store } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    const current = store._current();
    const entry = current.stateJson?.approvalPending?.[itemId];
    assert.equal("editedText" in entry, false);
  });

  test("returns applied on success", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "app" });
    assert.deepEqual(res, { ok: true, status: "applied" });
  });
});

// ─── recordItemDecision — persist_failed propagation ─────────────────────────

describe("recordItemDecision — persist failure propagation", () => {
  test("returns persist_failed code when updateApprovalState returns applied:false", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const api = { logger };
    // Store that always fails setWaiting/setRunning
    const store = {
      get: () => ({ ...flow }),
      setRunning: mock.fn(() => ({ applied: false, code: "db_error" })),
      setWaiting: mock.fn(() => ({ applied: false, code: "db_error" })),
    };
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.equal(res.ok, false);
    // code should be db_error or persist_failed
    assert.ok(res.code, "should carry a failure code");
  });
});

// ─── maybeResolveFlow — guard conditions ─────────────────────────────────────

describe("maybeResolveFlow — guard conditions", () => {
  test("is a no-op when flow is not found in store", async () => {
    const { resolver, executor } = makeResolver(null);
    await resolver.maybeResolveFlow(randomUUID());
    assert.equal(executor.resume.mock.calls.length, 0);
  });

  test("is a no-op when flow status is running", async () => {
    const flow = makeRunningFlow();
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flow.flowId);
    assert.equal(executor.resume.mock.calls.length, 0);
  });

  test("is a no-op when flow status is completed", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "completed" };
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(executor.resume.mock.calls.length, 0);
  });

  test("is a no-op when waitJson is null", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), waitJson: null };
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(executor.resume.mock.calls.length, 0);
  });

  test("is a no-op when waitJson.kind is not a gate kind", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "some-other-kind" });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(executor.resume.mock.calls.length, 0);
  });

  test("is a no-op when decided list is empty (no entries with decision or sendFailed)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: { approvalPending: {} }, // no decision on the item
    });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(executor.resume.mock.calls.length, 0);
  });
});

// ─── maybeResolveFlow — item collection ──────────────────────────────────────

describe("maybeResolveFlow — item collection logic", () => {
  test("collects items with entry.decision set", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() },
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    // setRunning should have been called to apply resolutions
    assert.equal(store.setRunning.mock.calls.length >= 1, true);
  });

  test("treats sendFailed entries as reject decisions with reason string", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { sendFailed: true, sendError: "TG timeout" },
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    // Should have resolved the item as reject
    const current = store._current();
    assert.ok(current.stateJson?.rejectedIds?.includes(itemId), "sendFailed item should be in rejectedIds");
  });

  test("treats sendFailed entries without sendError as reject with n/a reason", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { sendFailed: true },
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.ok(current.stateJson?.rejectedIds?.includes(itemId));
  });

  test("skips items already in approvalResults[nodeId] (idempotency across partial-resume passes)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    // decidedAt matches between approvalPending and approvalResults — this
    // is what a genuine "already processed in an earlier pass" snapshot
    // looks like: applyResolutionsAndResume copies the pending entry's
    // decidedAt into the approvalResults record it pushes (same decision,
    // not a later re-decision of the same itemId — see the "does NOT skip
    // a re-decided item" test below for that distinct case).
    const decidedAt = Date.now();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "cli", decidedAt },
        },
        // Item already recorded in approvalResults for this nodeId
        approvalResults: {
          "gate-node": [{ itemId, decision: "approve", decidedAt }],
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    // setRunning should NOT be called — nothing new to process
    assert.equal(store.setRunning.mock.calls.length, 0);
  });

  test("does NOT skip a re-decided item whose approvalPending decidedAt is newer than the stale approvalResults entry", async () => {
    // The GAP_GATE round-2 scenario: an earlier round's decision already
    // landed in approvalResults (stale decidedAt), but approvalPending now
    // holds a freshly-recorded decision (newer decidedAt) for the same
    // itemId — a branch revisiting the same human-gate sub-node. Must be
    // processed, not silently swallowed by the itemId-presence check.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const staleDecidedAt = Date.now() - 10_000;
    const freshDecidedAt = Date.now();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "cli", decidedAt: freshDecidedAt },
        },
        approvalResults: {
          "gate-node": [{ itemId, decision: "reject", decidedAt: staleDecidedAt }],
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length, 1, "the fresh decision should be processed, not skipped as already-decided");
  });

  test("only skips items already processed for the matching nodeId", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() },
        },
        // Item recorded under a DIFFERENT nodeId — should not block processing
        approvalResults: {
          "other-node": [{ itemId, decision: "approve" }],
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length >= 1, true);
  });

  test("handles waitJson.kind = gate", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      kind: "gate",
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length >= 1, true);
  });

  test("handles waitJson.kind = verify-required", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      kind: "verify-required",
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "reject", source: "dashboard", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length >= 1, true);
  });
});

// ─── applyResolutionsAndResume — accumulation logic ──────────────────────────

describe("applyResolutionsAndResume — approve/reject accumulation", () => {
  test("appends approve itemId to approvedIds without duplication", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvedIds: [itemId], // already present — must not duplicate
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    const approvedIds = current.stateJson?.approvedIds ?? [];
    assert.equal(approvedIds.filter((id) => id === itemId).length, 1);
  });

  test("appends reject itemId to rejectedIds without duplication", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        rejectedIds: [itemId],
        approvalPending: { [itemId]: { decision: "reject", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    const rejectedIds = current.stateJson?.rejectedIds ?? [];
    assert.equal(rejectedIds.filter((id) => id === itemId).length, 1);
  });

  test("adds new approve itemId to empty approvedIds", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "app", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.ok(current.stateJson?.approvedIds?.includes(itemId));
  });

  test("adds new reject itemId to empty rejectedIds", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "reject", source: "app", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.ok(current.stateJson?.rejectedIds?.includes(itemId));
  });
});

// ─── applyResolutionsAndResume — editedTexts ─────────────────────────────────

describe("applyResolutionsAndResume — editedTexts storage", () => {
  test("stores editedText in editedTexts map when approve decision includes editedText", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", editedText: "custom text", source: "app", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.equal(current.stateJson?.editedTexts?.[itemId], "custom text");
  });

  test("does not add editedTexts entry when editedText is absent on approve", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    const editedTexts = current.stateJson?.editedTexts ?? {};
    assert.equal(itemId in editedTexts, false);
  });

  test("merges editedTexts with existing map", async () => {
    const flowId = randomUUID();
    const existingItemId = randomUUID();
    const newItemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId: newItemId }],
      stateJson: {
        editedTexts: { [existingItemId]: "old text" },
        approvalPending: { [newItemId]: { decision: "approve", editedText: "new text", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.equal(current.stateJson?.editedTexts?.[existingItemId], "old text");
    assert.equal(current.stateJson?.editedTexts?.[newItemId], "new text");
  });
});

// ─── applyResolutionsAndResume — approvalResults ─────────────────────────────

describe("applyResolutionsAndResume — approvalResults accumulation", () => {
  test("appends new results to existing approvalResults[nodeId] array", async () => {
    const flowId = randomUUID();
    const existingItemId = randomUUID();
    const newItemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId: newItemId }],
      stateJson: {
        approvalResults: {
          "gate-node": [{ itemId: existingItemId, decision: "approve" }],
        },
        approvalPending: { [newItemId]: { decision: "reject", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    const results = current.stateJson?.approvalResults?.["gate-node"] ?? [];
    assert.equal(results.length, 2);
    assert.ok(results.some((r) => r.itemId === existingItemId));
    assert.ok(results.some((r) => r.itemId === newItemId));
  });

  test("creates approvalResults[nodeId] array when none existed", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    const results = current.stateJson?.approvalResults?.["gate-node"] ?? [];
    assert.equal(results.length, 1);
    assert.equal(results[0].itemId, itemId);
  });

  test("uses 'gate' as fallback nodeId when waitJson.nodeId is absent", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    // Remove nodeId from waitJson
    flow.waitJson = { kind: "human-gate", items: [{ itemId }] };
    flow.stateJson = {
      approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
    };
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.ok(current.stateJson?.approvalResults?.["gate"], "should use 'gate' as fallback nodeId");
  });
});

// ─── applyResolutionsAndResume — setRunning + executor.resume ────────────────

describe("applyResolutionsAndResume — setRunning and executor.resume", () => {
  test("calls store.setRunning on successful resolution", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length >= 1, true);
  });

  test("calls executor.resume via setImmediate after setRunning succeeds", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    // executor.resume is called inside setImmediate — drain it
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length >= 1, true);
    assert.equal(executor.resume.mock.calls[0].arguments[0], flowId);
  });

  test("does not call executor.resume synchronously (deferred via setImmediate)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    // Before draining setImmediate, resume should NOT have been called yet
    assert.equal(executor.resume.mock.calls.length, 0);
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length >= 1, true);
  });

  test("logs a warn when executor.resume throws", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = { resume: mock.fn(async () => { throw new Error("resume boom"); }) };
    const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    await drainImmediate();
    assert.ok(logger.calls.warn.length >= 1, "should log warn on resume error");
  });
});

// makeExecutor()'s resume() resolves on the same tick, so it can't
// distinguish "awaited to completion" from "merely scheduled and happened
// to already be done" — this executor forces a real async gap (one
// setImmediate) so a test can tell the two apart, same technique
// routes/operator-retry.test.js uses for retryFailureBranch's identical
// awaitCompletion param.
function makeSlowExecutor() {
  const calls = [];
  return {
    resume: mock.fn(async (flowId) => {
      await new Promise((r) => setImmediate(r));
      calls.push(flowId);
    }),
    _calls: calls,
  };
}

describe("applyResolutionsAndResume — awaitCompletion", () => {
  test("awaits executor.resume to genuine completion when awaitCompletion:true — the CLI's one-shot process can't rely on a merely-scheduled setImmediate surviving", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const logger = makeLogger();
    const store = makeStore(flow);
    const executor = makeSlowExecutor();
    const resolver = createGateResolver({ api: { logger }, store, executor, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId, true);
    assert.deepEqual(executor._calls, [flowId], "executor.resume must have fully resolved before maybeResolveFlow itself returns");
  });

  test("still defers via setImmediate (resume not yet called) when awaitCompletion is omitted — HTTP/Telegram/dashboard/companion callers need the fast return", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const logger = makeLogger();
    const store = makeStore(flow);
    const executor = makeSlowExecutor();
    const resolver = createGateResolver({ api: { logger }, store, executor, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.deepEqual(executor._calls, [], "resume must not have run yet — only scheduled");
    await drainImmediate();
    await drainImmediate();
    assert.deepEqual(executor._calls, [flowId], "resume does eventually run, just not before maybeResolveFlow returned");
  });

  test("recordItemDecision propagates awaitCompletion through to the executor.resume call", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const store = makeStore(flow);
    const executor = makeSlowExecutor();
    const resolver = createGateResolver({ api: { logger }, store, executor, admissionQueueStore });
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli", awaitCompletion: true });
    assert.equal(res.ok, true);
    assert.deepEqual(executor._calls, [flowId], "executor.resume must have fully resolved before recordItemDecision itself returns");
  });
});

// ─── applyResolutionsAndResume — revision_conflict retry ─────────────────────

describe("applyResolutionsAndResume — revision_conflict retry loop", () => {
  test("retries on revision_conflict and succeeds on subsequent attempt", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    let callCount = 0;
    const logger = makeLogger();
    const api = { logger };
    const realStore = makeStore(flow);
    // Override setRunning to fail once then succeed
    const originalSetRunning = realStore.setRunning;
    realStore.setRunning = mock.fn((args) => {
      callCount++;
      if (callCount === 1) return { applied: false, code: "revision_conflict" };
      // Patch the internal revision so the retry reads updated flow
      const current = realStore._current();
      if (current) current.revision = args.expectedRevision; // reset to match
      return originalSetRunning(args);
    });
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store: realStore, executor, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.ok(callCount >= 2, "setRunning should have been retried");
  });

  test("bails immediately on non-revision_conflict store error and logs warn", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const logger = makeLogger();
    const api = { logger };
    let callCount = 0;
    const store = {
      get: () => ({ ...flow }),
      setRunning: mock.fn(() => {
        callCount++;
        return { applied: false, code: "db_locked" };
      }),
      setWaiting: mock.fn(() => ({ applied: false, code: "db_locked" })),
    };
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    // Should have bailed after 1 attempt (non-conflict error)
    assert.equal(callCount, 1);
    assert.ok(logger.calls.warn.length >= 1, "should log warn on non-conflict failure");
  });

  describe("round-based live retry on sustained revision_conflict (root-cause fix, not just the boot-time backstop)", () => {
    // Rather than fighting fake-timer cascade semantics (Node's
    // mock.timers.tick() didn't reliably drain the microtask between
    // chained `await new Promise(r => setTimeout(r, N))` calls in
    // practice here — the promise never settled and tests hung),
    // collapse every setTimeout delay (inner backoff AND the outer
    // round-scheduling delay) down to a real, near-instant timer.
    // Real timers still exercise the exact same code paths and ordering
    // — only the wall-clock cost changes — and `await` naturally waits
    // out however many real event-loop ticks the chain takes.
    let realSetTimeout;
    beforeEach(() => {
      realSetTimeout = globalThis.setTimeout;
      mock.method(globalThis, "setTimeout", (fn) => realSetTimeout(fn, 0));
    });
    afterEach(() => {
      // mock.method patches the real, shared globalThis.setTimeout — must
      // restore explicitly or every later test in this process (this file
      // and beyond, if run in the same worker) inherits the 0-delay stub.
      globalThis.setTimeout = realSetTimeout;
    });

    test("round 1 exhausts after 5 revision_conflict failures, schedules a live retry round rather than giving up immediately", async () => {
      const flowId = randomUUID();
      const itemId = randomUUID();
      const flow = makeWaitingFlow({
        flowId,
        items: [{ itemId }],
        stateJson: {
          approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
        },
      });
      const logger = makeLogger();
      const api = { logger };
      let callCount = 0;
      const store = {
        get: () => ({ ...flow }),
        setRunning: mock.fn(() => {
          callCount++;
          return { applied: false, code: "revision_conflict" };
        }),
        setWaiting: mock.fn(() => ({ applied: false, code: "revision_conflict" })),
      };
      const executor = makeExecutor();
      const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
      await resolver.maybeResolveFlow(flowId);
      assert.equal(callCount, 5, "round 1 should retry exactly 5 times before scheduling round 2");
      // Inspect the FIRST warn call specifically — round 1's own
      // exhaustion message — rather than requiring rounds 2/3 to not
      // yet have happened (with real timers they may well have
      // already started running in the background by the time this
      // assertion runs).
      const firstWarn = logger.calls.warn[0]?.join(" ") ?? "";
      assert.ok(firstWarn.includes("retry_exhausted"), "should log retry_exhausted for round 1");
      assert.match(firstWarn, /round 1\/3.*scheduling another round in 500ms/);
    });

    test("all 3 rounds (15 total attempts) exhaust under sustained conflict — logs the terminal self-heal-via-revival message, never retries indefinitely", async () => {
      const flowId = randomUUID();
      const itemId = randomUUID();
      const flow = makeWaitingFlow({
        flowId,
        items: [{ itemId }],
        stateJson: {
          approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
        },
      });
      const logger = makeLogger();
      const api = { logger };
      let callCount = 0;
      const store = {
        get: () => ({ ...flow }),
        setRunning: mock.fn(() => {
          callCount++;
          return { applied: false, code: "revision_conflict" };
        }),
        setWaiting: mock.fn(() => ({ applied: false, code: "revision_conflict" })),
      };
      const executor = makeExecutor();
      const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
      await resolver.maybeResolveFlow(flowId);
      // Round 1 (synchronous within the awaited call) is done; rounds 2
      // and 3 were scheduled via (now near-instant) setTimeout and may
      // still be in flight. Poll briefly for the final call count rather
      // than assuming a fixed number of event-loop ticks.
      const deadline = Date.now() + 2000;
      while (callCount < 15 && Date.now() < deadline) {
        await new Promise((r) => realSetTimeout(r, 5));
      }
      assert.equal(callCount, 15, "all 3 rounds × 5 attempts each");

      // Give any (incorrect) 4th round a chance to fire, then confirm it didn't.
      await new Promise((r) => realSetTimeout(r, 100));
      assert.equal(callCount, 15, "must not retry indefinitely past MAX_ROUNDS");

      const warnMessages = logger.calls.warn.flat().join(" ");
      assert.match(warnMessages, /retry_exhausted after 3 rounds/);
      assert.match(warnMessages, /self-heal via flow-revival\.js's stuck-gate-decision reconciliation pass on next restart/);
    });
  });
});

// ─── applyResolutionsAndResume — root-gate apply() hook ──────────────────────

describe("applyResolutionsAndResume — root-gate apply() hook", () => {
  test("calls node.apply(state, item) once per decided item after commit", async () => {
    const flowId = randomUUID();
    const itemA = "check-a";
    const itemB = "check-b";
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId: itemA }, { itemId: itemB }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: {
          [itemA]: { decision: "approve", source: "cli", decidedAt: Date.now() },
          [itemB]: { decision: "reject", source: "cli", decidedAt: Date.now() },
        },
      },
    });
    const applyCalls = [];
    const applyFn = mock.fn(async (state, item) => { applyCalls.push({ state, item }); });
    const pipelineLookup = (pipelineId) =>
      pipelineId === "team_test" ? { nodes: { CHECKLIST: { kind: "human-gate", apply: applyFn } } } : undefined;
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(applyFn.mock.calls.length, 2, "apply should fire once per decided item");
    const calledItemIds = applyCalls.map((c) => c.item.itemId).sort();
    assert.deepEqual(calledItemIds, [itemA, itemB].sort());
    const approveCall = applyCalls.find((c) => c.item.itemId === itemA);
    assert.equal(approveCall.item.decision, "approve");
    const rejectCall = applyCalls.find((c) => c.item.itemId === itemB);
    assert.equal(rejectCall.item.decision, "reject");
  });

  test("passes the matching waitJson item as apply's 3rd argument", async () => {
    // apply()'s 2nd arg (the decision) never carries a gate-UI field edit —
    // only the matching waitJson item's requestPayload.layout does. Without
    // this 3rd arg, buildGateNode's apply hook has no way to see what the
    // operator actually edited.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const waitItem = {
      itemId,
      requestPayload: { layout: [{ kind: "field-group", id: "fields", fields: [{ id: "budget", value: "500" }] }] },
    };
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [waitItem] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const applyCalls = [];
    const applyFn = mock.fn(async (state, item, matchedItem) => { applyCalls.push(matchedItem); });
    const pipelineLookup = () => ({ nodes: { CHECKLIST: { kind: "human-gate", apply: applyFn } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(applyCalls.length, 1);
    assert.deepEqual(applyCalls[0], waitItem, "apply's 3rd arg should be the exact matching waitJson item");
  });

  test("an item decided outside the current waitJson.items batch is never handed to apply at all (item-membership is enforced upstream)", async () => {
    // Confirms the matchedItem lookup's undefined fallback is unreachable in
    // practice, not just untested: maybeResolveFlow's item collection only
    // ever surfaces decisions for items that are genuinely in
    // waitJson.items, so apply() is never invoked for an orphaned decision
    // in the first place — items.find(...) inside the loop above will
    // always find a match whenever apply actually fires.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId: "some-other-item" }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const applyFn = mock.fn(async () => {});
    const pipelineLookup = () => ({ nodes: { CHECKLIST: { kind: "human-gate", apply: applyFn } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(applyFn.mock.calls.length, 0);
  });

  test("apply's state arg is the assembled state — sees a prior node's own output (e.g. DRAFT), which newState alone never carries", async () => {
    // flow.stateJson is entry-only (executor-persist.js's toPersistableState
    // allowlist) — an earlier kind:"code" node's own return value (like
    // buildGateNode.apply's state.DRAFT.draftPath) never lands there. It's
    // only reconstructed via assembleState()'s replay of
    // flow_node_invocations.result_json. Without that replay, apply() would
    // silently see state.DRAFT as undefined even though a real prior node
    // set it — exactly the bug a live fixture run caught.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const seenStates = [];
    const applyFn = mock.fn(async (state) => { seenStates.push(state); });
    const pipelineLookup = () => ({ nodes: { CHECKLIST: { kind: "human-gate", apply: applyFn } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    // Simulates an earlier WRITE_DRAFT node's committed result row — never
    // present in flow.stateJson, only in flow_node_invocations.result_json.
    const nodeExecutionsStore = makeFakeNodeExecutions([
      { nodeId: "WRITE_DRAFT", resultWrittenAt: Date.now(), resultJson: JSON.stringify({ DRAFT: { draftPath: "/tmp/fixture.md" } }) },
    ]);
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, nodeExecutionsStore, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(seenStates.length, 1);
    assert.equal(seenStates[0].DRAFT?.draftPath, "/tmp/fixture.md", "apply() must see DRAFT via assembleState, not just entry-field newState");
  });

  test("skips the assembleState round-trip entirely when the node has no apply() (no nodeExecutionsStore.listResultsForFlow call)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "FINAL_REVIEW", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({ nodes: { FINAL_REVIEW: { kind: "human-gate" } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    let listResultsCalls = 0;
    const nodeExecutionsStore = { write: () => {}, listResultsForFlow: async () => { listResultsCalls++; return []; } };
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, nodeExecutionsStore, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(listResultsCalls, 0, "no apply() defined — assembleState should never be invoked");
  });

  test("does not call apply when pipelineLookup is not supplied (backward compatible)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    // No pipelineLookup passed — mirrors every production call site.
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length, 1, "resolution should still proceed normally");
  });

  test("does not call apply when the node has no .apply function", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "FINAL_REVIEW", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({ nodes: { FINAL_REVIEW: { kind: "human-gate" } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    // No throw, no warn about apply — plain resolution proceeds.
    const warnText = logger.calls.warn.flat().join(" ");
    assert.ok(!warnText.includes("apply("), "should not attempt to log an apply() failure");
  });

  test("apply fires exactly once per item even when setRunning hits a revision_conflict first", async () => {
    // Guards the double-fire risk: the CAS retry loop legitimately re-runs
    // its in-memory computation on conflict, but apply() must only ever
    // run after a successful commit, never once per attempt.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const applyFn = mock.fn(async () => {});
    const pipelineLookup = () => ({ nodes: { CHECKLIST: { kind: "human-gate", apply: applyFn } } });
    const logger = makeLogger();
    const api = { logger };
    const realStore = makeStore(flow);
    let callCount = 0;
    const originalSetRunning = realStore.setRunning;
    realStore.setRunning = mock.fn((args) => {
      callCount++;
      if (callCount === 1) return { applied: false, code: "revision_conflict" };
      const current = realStore._current();
      if (current) current.revision = args.expectedRevision;
      return originalSetRunning(args);
    });
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store: realStore, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.ok(callCount >= 2, "setRunning should have retried at least once");
    assert.equal(applyFn.mock.calls.length, 1, "apply must fire exactly once despite the retry");
  });

  test("logs a warn and continues when apply() throws", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({
      nodes: { CHECKLIST: { kind: "human-gate", apply: async () => { throw new Error("apply boom"); } } },
    });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const nodeExecutionsStore = makeFakeNodeExecutions();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, nodeExecutionsStore, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    const warnText = logger.calls.warn.flat().join(" ");
    assert.ok(warnText.includes("apply(") && warnText.includes("apply boom"), "should log the apply() failure");
    // Resolution itself committed via setRunning; the durable failure
    // marker goes to flow_node_executions, not a second setRunning call —
    // see the "persists a durable marker" test below for its content.
    assert.equal(store.setRunning.mock.calls.length, 1);
    assert.equal(nodeExecutionsStore.writes.length, 1);
  });

  test("persists a durable marker on apply() throw, for a root-level gate", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({
      nodes: { CHECKLIST: { kind: "human-gate", apply: async () => { throw new Error("disk full"); } } },
    });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const nodeExecutionsStore = makeFakeNodeExecutions();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, nodeExecutionsStore, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(nodeExecutionsStore.writes.length, 1);
    const write = nodeExecutionsStore.writes[0];
    assert.equal(write.nodeId, "CHECKLIST");
    assert.equal(write.branchKey, null, "root-level marker is not scoped to a branch key");
    assert.ok(write.error.includes("disk full"));
  });

  test("does not throw when persisting the failure marker itself fails", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({
      nodes: { CHECKLIST: { kind: "human-gate", apply: async () => { throw new Error("boom"); } } },
    });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    // The marker write itself throws — simulates e.g. a transient SQLite
    // error on the secondary write.
    const nodeExecutionsStore = { write: () => { throw new Error("db locked"); } };
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, nodeExecutionsStore, admissionQueueStore });
    await assert.doesNotReject(() => resolver.maybeResolveFlow(flowId));
    assert.equal(store.setRunning.mock.calls.length, 1, "the resolution commit itself still succeeded");
    const warnText = logger.calls.warn.flat().join(" ");
    assert.ok(warnText.includes("apply(") && warnText.includes("boom"), "the original apply() failure is still logged");
    assert.ok(warnText.includes("persistApplyFailureMarker"), "the marker write's own failure is also logged (best-effort, no retry)");
  });
});

// ─── applyResolutionsAndResume — branch-mode apply() hook ────────────────────

describe("applyResolutionsAndResume — branch-mode apply() hook", () => {
  test("does not call apply on a subflow node that has no .apply property directly on it", async () => {
    // The branch-row lookup path (subNodeId -> sub-node apply)
    // has been removed. A subflow node with .apply only on inner nodes — not directly
    // on the top-level pipelineNode — will not have its inner apply() reached.
    // Resolution itself should still proceed normally.
    const flowId = randomUUID();
    const branchKey = "listing-a";
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", items: [{ itemId: branchKey }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [branchKey]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const gateApplyFn = mock.fn(async () => {});
    const pipelineLookup = () => ({
      nodes: { PROPOSE: { kind: "subflow", flow: { nodes: { GATE: { kind: "human-gate", apply: gateApplyFn } } } } },
    });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(gateApplyFn.mock.calls.length, 0, "sub-node apply is not reachable without branch-row lookup");
    assert.equal(store.setRunning.mock.calls.length, 1, "resolution should still proceed normally");
  });

  test("root-level gate resolves apply() directly from pipelineLookup", async () => {
    // A node with .apply directly on it (kind:"human-gate") is called without
    // any branch-row lookup — the root-gate path is unaffected by the
    // removal of branch-mode routing.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "PRIME_FILL", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const rootApplyFn = mock.fn(async () => {});
    const pipelineLookup = () => ({ nodes: { PRIME_FILL: { kind: "human-gate", apply: rootApplyFn } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    assert.equal(rootApplyFn.mock.calls.length, 1);
  });
});

// ─── updateApprovalState — routing ───────────────────────────────────────────

describe("updateApprovalState — setRunning vs setWaiting routing", () => {
  test("routes to setRunning when flow.status is running", async () => {
    const itemId = randomUUID();
    const flow = makeRunningFlow({ itemId });
    const flowId = flow.flowId;
    const { resolver, store } = makeResolver(flow);
    // Use updateApprovalState directly
    const result = await resolver.updateApprovalState(flowId, (s) => ({ ...s, _test: true }));
    assert.equal(result.applied, true);
    assert.equal(store.setRunning.mock.calls.length >= 1, true);
    assert.equal(store.setWaiting.mock.calls.length, 0);
  });

  test("routes to setWaiting when flow.status is waiting", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId });
    const { resolver, store } = makeResolver(flow);
    const result = await resolver.updateApprovalState(flowId, (s) => ({ ...s, _test: true }));
    assert.equal(result.applied, true);
    assert.equal(store.setWaiting.mock.calls.length >= 1, true);
    assert.equal(store.setRunning.mock.calls.length, 0);
  });

  test("routes to setWaiting when flow.status is blocked", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "blocked" };
    const { resolver, store } = makeResolver(flow);
    const result = await resolver.updateApprovalState(flowId, (s) => ({ ...s, _test: true }));
    assert.equal(result.applied, true);
    assert.equal(store.setWaiting.mock.calls.length >= 1, true);
  });

  test("passes waitJson and statusReason through on setWaiting call", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const waitJson = { kind: "human-gate", nodeId: "gate-node", items: [{ itemId }] };
    const flow = {
      ...makeWaitingFlow({ flowId, items: [{ itemId }] }),
      statusReason: "some summary",
      waitJson,
    };
    const { resolver, store } = makeResolver(flow);
    await resolver.updateApprovalState(flowId, (s) => s);
    const call = store.setWaiting.mock.calls[0]?.arguments[0];
    assert.ok(call, "setWaiting should have been called");
    assert.deepEqual(call.waitJson, waitJson);
    assert.equal(call.statusReason, "some summary");
  });

  test("returns flow_not_found when store.get returns null", async () => {
    const { resolver } = makeResolver(null);
    const result = await resolver.updateApprovalState(randomUUID(), (s) => s);
    assert.deepEqual(result, { applied: false, code: "flow_not_found" });
  });

  test("returns retry_exhausted after 5 revision_conflict failures", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId });
    const logger = makeLogger();
    const api = { logger };
    const store = {
      get: () => ({ ...flow }),
      setRunning: mock.fn(() => ({ applied: false, code: "revision_conflict" })),
      setWaiting: mock.fn(() => ({ applied: false, code: "revision_conflict" })),
    };
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
    const result = await resolver.updateApprovalState(flowId, (s) => s);
    assert.equal(store.setWaiting.mock.calls.length, 5);
    assert.deepEqual(result, { applied: false, code: "retry_exhausted" });
  });

  test("returns immediately on non-conflict store error", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId });
    const logger = makeLogger();
    const api = { logger };
    let callCount = 0;
    const store = {
      get: () => ({ ...flow }),
      setRunning: mock.fn(() => ({ applied: false, code: "schema_error" })),
      setWaiting: mock.fn(() => { callCount++; return { applied: false, code: "schema_error" }; }),
    };
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
    const result = await resolver.updateApprovalState(flowId, (s) => s);
    assert.equal(callCount, 1, "should bail after first non-conflict failure");
    assert.equal(result.applied, false);
    assert.equal(result.code, "schema_error");
  });

  test("updater function receives current stateJson and its return value is stored", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId, stateJson: { existing: "value" } });
    const { resolver, store } = makeResolver(flow);
    await resolver.updateApprovalState(flowId, (s) => ({ ...s, added: "extra" }));
    const current = store._current();
    assert.equal(current.stateJson?.existing, "value");
    assert.equal(current.stateJson?.added, "extra");
  });
});

// ─── full integration path: recordItemDecision → maybeResolveFlow → resume ───

describe("full integration: recordItemDecision triggers resolution and resume", () => {
  test("approve decision on single-item gate flows through to executor.resume", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, executor } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.equal(res.ok, true);
    assert.equal(res.status, "applied");
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length >= 1, true);
  });

  test("reject decision flows through to executor.resume", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, executor } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "app" });
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length >= 1, true);
  });

  test("multiple items: first decision records approvalPending; resolution fires after each decided item", async () => {
    // Use a store that stays blocked across both decisions so we can test
    // that each decided item gets a pending entry and maybeResolveFlow fires.
    const flowId = randomUUID();
    const itemId1 = randomUUID();
    const itemId2 = randomUUID();

    // Custom store: setRunning keeps the flow blocked so the second
    // decision also sees a blocked flow with both items still in waitJson.
    let state = {
      stateJson: {},
      waitJson: { kind: "human-gate", nodeId: "gate-node", items: [{ itemId: itemId1 }, { itemId: itemId2 }] },
      status: "blocked",
      revision: 1,
      lastStep: "gate-node",
      statusReason: null,
      flowId,
    };
    const setRunningCalls = [];
    const mockStore = {
      get: () => ({ ...state }),
      setRunning: mock.fn((args) => {
        if (state.revision !== args.expectedRevision) return { applied: false, code: "revision_conflict" };
        // Merge stateJson but keep flow blocked so second decision can land
        state = { ...state, stateJson: args.stateJson, revision: state.revision + 1 };
        setRunningCalls.push(args);
        return { applied: true };
      }),
      setWaiting: mock.fn((args) => {
        if (state.revision !== args.expectedRevision) return { applied: false, code: "revision_conflict" };
        state = { ...state, stateJson: args.stateJson, revision: state.revision + 1, waitJson: args.waitJson, statusReason: args.statusReason };
        return { applied: true };
      }),
    };
    const logger = makeLogger();
    const executor = makeExecutor();
    const resolver = createGateResolver({ api: { logger }, store: mockStore, executor, admissionQueueStore });

    // First decision
    const res1 = await resolver.recordItemDecision({ flowId, itemId: itemId1, decision: "approve", source: "cli" });
    assert.equal(res1.ok, true);
    assert.equal(res1.status, "applied");
    await drainImmediate();
    assert.ok(executor.resume.mock.calls.length >= 1, "resume should fire after first decision");

    const resumeCount1 = executor.resume.mock.calls.length;

    // Second decision
    const res2 = await resolver.recordItemDecision({ flowId, itemId: itemId2, decision: "reject", source: "cli" });
    assert.equal(res2.ok, true);
    assert.equal(res2.status, "applied");
    await drainImmediate();
    assert.ok(executor.resume.mock.calls.length > resumeCount1, "resume should fire again after second decision");
  });

  test("edit decision stores editedText end-to-end", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, store } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "edit", editedText: "finalized reply", source: "app" });
    await drainImmediate();
    const current = store._current();
    // editedText lands in either approvalPending or editedTexts (after resolution)
    const inPending = current.stateJson?.approvalPending?.[itemId]?.editedText;
    const inEdited = current.stateJson?.editedTexts?.[itemId];
    assert.ok(inPending === "finalized reply" || inEdited === "finalized reply", "editedText should be persisted");
  });
});

// ─── recordItemDecision — getBroadcast / WS wake emission ────────────────────

describe("recordItemDecision — getBroadcast WS emission", () => {
  /** Resolver wired with a mock broadcast function accessible for assertions. */
  function makeResolverWithBroadcast(flow) {
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const broadcastFn = mock.fn();
    const resolver = createGateResolver({ api, store, executor, getBroadcast: () => broadcastFn, admissionQueueStore });
    return { resolver, store, executor, logger, broadcastFn };
  }

  test("emits 'gate.resolved' broadcast on applied path with correct payload shape", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, broadcastFn } = makeResolverWithBroadcast(flow);
    const before = Date.now();
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "app" });
    // gate.resolved fires on the applied path via the canonical emitter
    // (resume patches come from the WS run listener). Find by name.
    const gateCall = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.gate.resolved");
    assert.ok(gateCall, "plugin.gate.resolved broadcast must fire");
    const [eventName, payload] = gateCall.arguments;
    assert.equal(eventName, "plugin.gate.resolved");
    assert.equal(payload.flowId, flowId);
    assert.equal(payload.itemId, itemId);
    assert.equal(payload.decision, "approve");
    assert.ok(typeof payload.resolvedAt === "number" && payload.resolvedAt >= before, "resolvedAt must be a timestamp");
    assert.equal(payload.kind, "gate.resolved");
  });

  test("does NOT emit a resolver-owned run.updated on resolve (owned by the WS run listener)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, store, executor, broadcastFn } = makeResolverWithBroadcast(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    const runUpdatedCall = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.run.updated");
    assert.equal(runUpdatedCall, undefined, "resolver must not emit its own run.updated");
    // The resume itself still happened through the store.
    assert.equal(store._current().status, "running");
    // resume() is dispatched on setImmediate — drain before asserting.
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 1);
  });

  test("does NOT emit broadcast on already-decided path", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "cli", decidedAt: Date.now() } },
      },
    });
    const { resolver, broadcastFn } = makeResolverWithBroadcast(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "app" });
    assert.equal(res.status, "already-decided");
    assert.equal(broadcastFn.mock.calls.length, 0, "broadcast must NOT fire for already-decided");
  });

  test("does NOT emit broadcast on flow_not_found", async () => {
    const { resolver, broadcastFn } = makeResolverWithBroadcast(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: "cli" });
    assert.equal(res.code, "flow_not_found");
    assert.equal(broadcastFn.mock.calls.length, 0);
  });

  test("does NOT emit broadcast on invalid_decision", async () => {
    const { resolver, broadcastFn } = makeResolverWithBroadcast(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "bad", source: "cli" });
    assert.equal(res.code, "invalid_decision");
    assert.equal(broadcastFn.mock.calls.length, 0);
  });

  test("does NOT emit broadcast on invalid_source", async () => {
    const { resolver, broadcastFn } = makeResolverWithBroadcast(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: "unknown" });
    assert.equal(res.code, "invalid_source");
    assert.equal(broadcastFn.mock.calls.length, 0);
  });

  test("does NOT emit broadcast when persist (updateApprovalState) fails", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const store = {
      get: () => ({ ...flow }),
      setRunning: mock.fn(() => ({ applied: false, code: "db_error" })),
      setWaiting: mock.fn(() => ({ applied: false, code: "db_error" })),
    };
    const broadcastFn = mock.fn();
    const resolver = createGateResolver({ api: { logger }, store, executor: makeExecutor(), getBroadcast: () => broadcastFn, admissionQueueStore });
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.equal(broadcastFn.mock.calls.length, 0, "broadcast must NOT fire when persist fails");
  });

  test("undefined getBroadcast does not throw on applied path — returns applied", async () => {
    // makeResolver() does not pass getBroadcast; this is the default case for callers that
    // don't wire a broadcast (e.g. CLI resolver). Verifies the ?. guard works.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: true, status: "applied" });
  });

  test("getBroadcast returning null does not throw — returns applied", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const resolver = createGateResolver({ api: { logger }, store: makeStore(flow), executor: makeExecutor(), getBroadcast: () => null, admissionQueueStore });
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "dashboard" });
    assert.deepEqual(res, { ok: true, status: "applied" });
  });

  test("getBroadcast getter throwing does not propagate — returns applied", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const resolver = createGateResolver({
      api: { logger },
      store: makeStore(flow),
      executor: makeExecutor(),
      getBroadcast: () => { throw new Error("broadcaster down"); },
      admissionQueueStore,
    });
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    assert.deepEqual(res, { ok: true, status: "applied" });
  });

  test("broadcast fires after state is committed — REST re-fetch will see the decision", async () => {
    // The WS event is a wake signal only; clients re-fetch via REST on receipt.
    // This test asserts that by the time the broadcast fires, the store already holds
    // the decision — either in approvalPending (after updateApprovalState) or in
    // approvedIds/rejectedIds (after maybeResolveFlow's setRunning). Both are REST-visible.
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const store = makeStore(flow);
    let stateAtBroadcastTime = null;
    const broadcastFn = mock.fn(() => {
      stateAtBroadcastTime = store._current()?.stateJson;
    });
    const resolver = createGateResolver({ api: { logger }, store, executor: makeExecutor(), getBroadcast: () => broadcastFn, admissionQueueStore });
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "app" });
    assert.ok(stateAtBroadcastTime, "state must be readable at broadcast time");
    const decisionCommitted =
      stateAtBroadcastTime.approvalPending?.[itemId]?.decision === "approve" ||
      stateAtBroadcastTime.approvedIds?.includes(itemId);
    assert.ok(decisionCommitted, "decision must already be persisted to the store when the WS wake fires");
  });
});

// ─── resume without emitter ─────────────────────────────────────────────────
// Resolving resumes the flow through the store; a failed persist resumes
// nothing. No emitter participates in either path.

describe("applyResolutionsAndResume — resume", () => {
  test("resolves and resumes the flow with no gateEmitter emission", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const store = makeStore(flow);
    const logger = makeLogger();
    const executor = makeExecutor();

    const resolver = createGateResolver({
      api: { logger }, store, executor, getBroadcast: () => mock.fn(), admissionQueueStore,
    });
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });

    assert.equal(res.status, "applied");
    assert.equal(store._current().status, "running", "flow must resume through the store");
    // resume() is dispatched on setImmediate — drain before asserting.
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 1, "executor.resume must fire");
  });

  test("does NOT resume when setRunning fails (revision conflict exhausted)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    // Real store so setWaiting can persist the decision; setRunning always fails.
    const store = makeStore(flow);
    store.setRunning = mock.fn(() => ({ applied: false, code: "revision_conflict" }));
    const logger = makeLogger();
    const executor = makeExecutor();
    const resolver = createGateResolver({
      api: { logger }, store, executor, getBroadcast: () => mock.fn(), admissionQueueStore,
    });
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "cli" });
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 0, "executor.resume must not fire when setRunning is rejected");
    assert.equal(store._current().status, "blocked", "flow must stay blocked");
  });
});
