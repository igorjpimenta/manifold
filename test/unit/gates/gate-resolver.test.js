
import { test, describe, beforeEach, afterEach, mock } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createGateResolver } from "../../../gates/gate-resolver.js";


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
    setWaiting: mock.fn(({ flowId, expectedRevision, lastStep, stateJson, waitJson, statusReason }) => {
      if (!current || current.flowId !== flowId) return { applied: false, code: "flow_not_found" };
      if (current.revision !== expectedRevision) return { applied: false, code: "revision_conflict" };
      current = { ...current, revision: current.revision + 1, stateJson, waitJson, statusReason, status: "blocked" };
      return { applied: true };
    }),
    _replace: (f) => { current = f ? { ...f } : null; },
  };
}


function makeExecutor() {
  return {
    resume: mock.fn(async () => {}),
  };
}

function makeFakeNodeExecutions(results = []) {
  const writes = [];
  return {
    writes,
    write: (row) => writes.push(row),
    listResultsForFlow: async () => results,
  };
}


async function drainImmediate() {
  await new Promise((r) => setImmediate(r));
}


function makeResolver(flow) {
  const logger = makeLogger();
  const api = { logger };
  const store = makeStore(flow);
  const executor = makeExecutor();
  const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
  return { resolver, store, executor, logger, api };
}


describe("recordItemDecision — input validation", () => {
  test("rejects missing flowId (missing_flow_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: "", itemId: "x", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "missing_flow_id" });
  });

  test("rejects null flowId (missing_flow_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: null, itemId: "x", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "missing_flow_id" });
  });

  test("rejects missing itemId (missing_item_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "missing_item_id" });
  });

  test("rejects null itemId (missing_item_id)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: null, decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "missing_item_id" });
  });

  test("rejects invalid decision value (invalid_decision)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "maybe", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "invalid_decision" });
  });

  test("rejects empty decision (invalid_decision)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "invalid_decision" });
  });

  test("accepts approve, reject, edit as valid decisions", async () => {
    const { resolver } = makeResolver(null);
    for (const decision of ["approve", "reject"]) {
      const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision, source: "ws" });
      assert.notEqual(res.code, "invalid_decision", `decision=${decision} should not fail on decision validation`);
    }
  });

  test("rejects edit decision without editedText string (edit_requires_text)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "edit", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "edit_requires_text" });
  });

  test("rejects edit decision with non-string editedText (edit_requires_text)", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "edit", editedText: 42, source: "ws" });
    assert.deepEqual(res, { ok: false, code: "edit_requires_text" });
  });

  test("accepts edit decision with valid string editedText", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "edit", editedText: "my reply", source: "ws" });
    assert.notEqual(res.code, "edit_requires_text");
  });
});


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
    for (const source of ["ws", "dashboard", "mcp", "pipeline"]) {
      const { resolver } = makeResolver(null);
      const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source });
      assert.notEqual(res.code, "invalid_source", `source=${source} should be accepted`);
    }
  });
});


describe("recordItemDecision — flow lookup", () => {
  test("returns flow_not_found when store.get returns null", async () => {
    const { resolver } = makeResolver(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "flow_not_found" });
  });

  test("returns flow_not_awaiting for completed flow status", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "completed" };
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "x", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "flow_not_awaiting" });
  });

  test("returns flow_not_awaiting for failed flow status", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "failed" };
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "x", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "flow_not_awaiting" });
  });

  test("returns flow_not_awaiting for cancelled flow status", async () => {
    const flowId = randomUUID();
    const flow = { ...makeWaitingFlow({ flowId }), status: "cancelled" };
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "x", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "flow_not_awaiting" });
  });
});


describe("recordItemDecision — idempotency", () => {
  test("returns already-decided when approvalPending[itemId].decision is set", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() },
        },
      },
    });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "ws" });
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
          [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() },
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "dashboard" });
    assert.equal(store.setRunning.mock.calls.length, 0);
    assert.equal(store.setWaiting.mock.calls.length, 0);
  });

  test("already-decided takes priority over item-membership check (idempotent after executor consumed item)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() },
        },
      },
    });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.equal(res.ok, true);
    assert.equal(res.status, "already-decided");
  });
});


describe("recordItemDecision — gate kind validation for waiting/blocked flows", () => {
  test("returns flow_not_gate when waitJson.kind is not a gate kind", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "some-other-kind", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "flow_not_gate" });
  });

  test("accepts waitJson.kind = human-gate", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "human-gate", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.notEqual(res.code, "flow_not_gate");
  });

  test("accepts waitJson.kind = gate", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "gate", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.notEqual(res.code, "flow_not_gate");
  });

  test("accepts waitJson.kind = verify-required", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, kind: "verify-required", items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.notEqual(res.code, "flow_not_gate");
  });
});


describe("recordItemDecision — item membership for waiting flows", () => {
  test("returns item_not_in_wait when itemId is not in waitJson.items", async () => {
    const flowId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId: randomUUID() }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId: "not-in-list", decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "item_not_in_wait" });
  });

  test("returns item_not_in_wait for running flows regardless of itemId (validItem is always false)", async () => {
    const itemId = randomUUID();
    const flow = makeRunningFlow({ itemId });
    const flowId = flow.flowId;
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: false, code: "item_not_in_wait" });
  });
});


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
    await resolver.recordItemDecision({ flowId, itemId, decision: "edit", editedText: "my custom reply", source: "ws" });
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    const current = store._current();
    const entry = current.stateJson?.approvalPending?.[itemId];
    assert.equal("editedText" in entry, false);
  });

  test("returns applied on success", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "ws" });
    assert.deepEqual(res, { ok: true, status: "applied" });
  });
});


describe("recordItemDecision — persist failure propagation", () => {
  test("returns persist_failed code when updateApprovalState returns applied:false", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const logger = makeLogger();
    const api = { logger };
    const store = {
      get: () => ({ ...flow }),
      setRunning: mock.fn(() => ({ applied: false, code: "db_error" })),
      setWaiting: mock.fn(() => ({ applied: false, code: "db_error" })),
    };
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, admissionQueueStore });
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.equal(res.ok, false);
    assert.ok(res.code, "should carry a failure code");
  });
});


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
      stateJson: { approvalPending: {} },
    });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(executor.resume.mock.calls.length, 0);
  });
});


describe("maybeResolveFlow — item collection logic", () => {
  test("collects items with entry.decision set", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() },
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
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
    const decidedAt = Date.now();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "ws", decidedAt },
        },
        approvalResults: {
          "gate-node": [{ itemId, decision: "approve", decidedAt }],
        },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    assert.equal(store.setRunning.mock.calls.length, 0);
  });

  test("does NOT skip a re-decided item whose approvalPending decidedAt is newer than the stale approvalResults entry", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const staleDecidedAt = Date.now() - 10_000;
    const freshDecidedAt = Date.now();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: {
          [itemId]: { decision: "approve", source: "ws", decidedAt: freshDecidedAt },
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
          [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() },
        },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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


describe("applyResolutionsAndResume — approve/reject accumulation", () => {
  test("appends approve itemId to approvedIds without duplication", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvedIds: [itemId],
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "reject", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "reject", source: "ws", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.ok(current.stateJson?.rejectedIds?.includes(itemId));
  });
});


describe("applyResolutionsAndResume — editedTexts storage", () => {
  test("stores editedText in editedTexts map when approve decision includes editedText", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", editedText: "custom text", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [newItemId]: { decision: "approve", editedText: "new text", source: "ws", decidedAt: Date.now() } },
      },
    });
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.equal(current.stateJson?.editedTexts?.[existingItemId], "old text");
    assert.equal(current.stateJson?.editedTexts?.[newItemId], "new text");
  });
});


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
        approvalPending: { [newItemId]: { decision: "reject", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
    flow.waitJson = { kind: "human-gate", items: [{ itemId }] };
    flow.stateJson = {
      approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
    };
    const { resolver, store } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
    const current = store._current();
    assert.ok(current.stateJson?.approvalResults?.["gate"], "should use 'gate' as fallback nodeId");
  });
});


describe("applyResolutionsAndResume — setRunning and executor.resume", () => {
  test("calls store.setRunning on successful resolution", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    const { resolver, executor } = makeResolver(flow);
    await resolver.maybeResolveFlow(flowId);
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws", awaitCompletion: true });
    assert.equal(res.ok, true);
    assert.deepEqual(executor._calls, [flowId], "executor.resume must have fully resolved before recordItemDecision itself returns");
  });
});


describe("applyResolutionsAndResume — revision_conflict retry loop", () => {
  test("retries on revision_conflict and succeeds on subsequent attempt", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      items: [{ itemId }],
      stateJson: {
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    let callCount = 0;
    const logger = makeLogger();
    const api = { logger };
    const realStore = makeStore(flow);
    const originalSetRunning = realStore.setRunning;
    realStore.setRunning = mock.fn((args) => {
      callCount++;
      if (callCount === 1) return { applied: false, code: "revision_conflict" };
      const current = realStore._current();
      if (current) current.revision = args.expectedRevision;
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
    assert.equal(callCount, 1);
    assert.ok(logger.calls.warn.length >= 1, "should log warn on non-conflict failure");
  });

  describe("round-based live retry on sustained revision_conflict (root-cause retry, not just the boot-time backstop)", () => {
    let realSetTimeout;
    beforeEach(() => {
      realSetTimeout = globalThis.setTimeout;
      mock.method(globalThis, "setTimeout", (fn) => realSetTimeout(fn, 0));
    });
    afterEach(() => {
      globalThis.setTimeout = realSetTimeout;
    });

    test("round 1 exhausts after 5 revision_conflict failures, schedules a live retry round rather than giving up immediately", async () => {
      const flowId = randomUUID();
      const itemId = randomUUID();
      const flow = makeWaitingFlow({
        flowId,
        items: [{ itemId }],
        stateJson: {
          approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
          approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
      const deadline = Date.now() + 2000;
      while (callCount < 15 && Date.now() < deadline) {
        await new Promise((r) => realSetTimeout(r, 5));
      }
      assert.equal(callCount, 15, "all 3 rounds × 5 attempts each");

      await new Promise((r) => realSetTimeout(r, 100));
      assert.equal(callCount, 15, "must not retry indefinitely past MAX_ROUNDS");

      const warnMessages = logger.calls.warn.flat().join(" ");
      assert.match(warnMessages, /retry_exhausted after 3 rounds/);
      assert.match(warnMessages, /self-heal via flow-revival\.js's stuck-gate-decision reconciliation pass on next restart/);
    });
  });
});


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
          [itemA]: { decision: "approve", source: "ws", decidedAt: Date.now() },
          [itemB]: { decision: "reject", source: "ws", decidedAt: Date.now() },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId: "some-other-item" }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    const seenStates = [];
    const applyFn = mock.fn(async (state) => { seenStates.push(state); });
    const pipelineLookup = () => ({ nodes: { CHECKLIST: { kind: "human-gate", apply: applyFn } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({ nodes: { FINAL_REVIEW: { kind: "human-gate" } } });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, admissionQueueStore });
    await resolver.maybeResolveFlow(flowId);
    const warnText = logger.calls.warn.flat().join(" ");
    assert.ok(!warnText.includes("apply("), "should not attempt to log an apply() failure");
  });

  test("apply fires exactly once per item even when setRunning hits a revision_conflict first", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "CHECKLIST", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    const pipelineLookup = () => ({
      nodes: { CHECKLIST: { kind: "human-gate", apply: async () => { throw new Error("boom"); } } },
    });
    const logger = makeLogger();
    const api = { logger };
    const store = makeStore(flow);
    const executor = makeExecutor();
    const nodeExecutionsStore = { write: () => { throw new Error("db locked"); } };
    const resolver = createGateResolver({ api, store, executor, pipelineLookup, nodeExecutionsStore, admissionQueueStore });
    await assert.doesNotReject(() => resolver.maybeResolveFlow(flowId));
    assert.equal(store.setRunning.mock.calls.length, 1, "the resolution commit itself still succeeded");
    const warnText = logger.calls.warn.flat().join(" ");
    assert.ok(warnText.includes("apply(") && warnText.includes("boom"), "the original apply() failure is still logged");
    assert.ok(warnText.includes("persistApplyFailureMarker"), "the marker write's own failure is also logged (best-effort, no retry)");
  });
});


describe("applyResolutionsAndResume — branch-mode apply() hook", () => {
  test("does not call apply on a subflow node that has no .apply property directly on it", async () => {
    const flowId = randomUUID();
    const branchKey = "listing-a";
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", items: [{ itemId: branchKey }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [branchKey]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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
    assert.equal(gateApplyFn.mock.calls.length, 0, "inner apply() is not reachable from the top-level pipelineNode");
    assert.equal(store.setRunning.mock.calls.length, 1, "resolution should still proceed normally");
  });

  test("root-level gate resolves apply() directly from pipelineLookup", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({
      flowId,
      waitJson: { kind: "human-gate", nodeId: "PRIME_FILL", items: [{ itemId }] },
      stateJson: {
        pipelineId: "team_test",
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
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


describe("updateApprovalState — setRunning vs setWaiting routing", () => {
  test("routes to setRunning when flow.status is running", async () => {
    const itemId = randomUUID();
    const flow = makeRunningFlow({ itemId });
    const flowId = flow.flowId;
    const { resolver, store } = makeResolver(flow);
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


describe("full integration: recordItemDecision triggers resolution and resume", () => {
  test("approve decision on single-item gate flows through to executor.resume", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver, executor } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "reject", source: "ws" });
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length >= 1, true);
  });

  test("multiple items: first decision records approvalPending; resolution fires after each decided item", async () => {
    const flowId = randomUUID();
    const itemId1 = randomUUID();
    const itemId2 = randomUUID();

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

    const res1 = await resolver.recordItemDecision({ flowId, itemId: itemId1, decision: "approve", source: "ws" });
    assert.equal(res1.ok, true);
    assert.equal(res1.status, "applied");
    await drainImmediate();
    assert.ok(executor.resume.mock.calls.length >= 1, "resume should fire after first decision");

    const resumeCount1 = executor.resume.mock.calls.length;

    const res2 = await resolver.recordItemDecision({ flowId, itemId: itemId2, decision: "reject", source: "ws" });
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "edit", editedText: "finalized reply", source: "ws" });
    await drainImmediate();
    const current = store._current();
    const inPending = current.stateJson?.approvalPending?.[itemId]?.editedText;
    const inEdited = current.stateJson?.editedTexts?.[itemId];
    assert.ok(inPending === "finalized reply" || inEdited === "finalized reply", "editedText should be persisted");
  });
});


describe("recordItemDecision — getBroadcast WS emission", () => {
  
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    const runUpdatedCall = broadcastFn.mock.calls.find(c => c.arguments[0] === "plugin.run.updated");
    assert.equal(runUpdatedCall, undefined, "resolver must not emit its own run.updated");
    assert.equal(store._current().status, "running");
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
        approvalPending: { [itemId]: { decision: "approve", source: "ws", decidedAt: Date.now() } },
      },
    });
    const { resolver, broadcastFn } = makeResolverWithBroadcast(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.equal(res.status, "already-decided");
    assert.equal(broadcastFn.mock.calls.length, 0, "broadcast must NOT fire for already-decided");
  });

  test("does NOT emit broadcast on flow_not_found", async () => {
    const { resolver, broadcastFn } = makeResolverWithBroadcast(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "approve", source: "ws" });
    assert.equal(res.code, "flow_not_found");
    assert.equal(broadcastFn.mock.calls.length, 0);
  });

  test("does NOT emit broadcast on invalid_decision", async () => {
    const { resolver, broadcastFn } = makeResolverWithBroadcast(null);
    const res = await resolver.recordItemDecision({ flowId: randomUUID(), itemId: "x", decision: "bad", source: "ws" });
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.equal(broadcastFn.mock.calls.length, 0, "broadcast must NOT fire when persist fails");
  });

  test("undefined getBroadcast does not throw on applied path — returns applied", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const { resolver } = makeResolver(flow);
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
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
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.deepEqual(res, { ok: true, status: "applied" });
  });

  test("broadcast fires after state is committed — REST re-fetch will see the decision", async () => {
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
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    assert.ok(stateAtBroadcastTime, "state must be readable at broadcast time");
    const decisionCommitted =
      stateAtBroadcastTime.approvalPending?.[itemId]?.decision === "approve" ||
      stateAtBroadcastTime.approvedIds?.includes(itemId);
    assert.ok(decisionCommitted, "decision must already be persisted to the store when the WS wake fires");
  });
});


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
    const res = await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });

    assert.equal(res.status, "applied");
    assert.equal(store._current().status, "running", "flow must resume through the store");
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 1, "executor.resume must fire");
  });

  test("does NOT resume when setRunning fails (revision conflict exhausted)", async () => {
    const flowId = randomUUID();
    const itemId = randomUUID();
    const flow = makeWaitingFlow({ flowId, items: [{ itemId }] });
    const store = makeStore(flow);
    store.setRunning = mock.fn(() => ({ applied: false, code: "revision_conflict" }));
    const logger = makeLogger();
    const executor = makeExecutor();
    const resolver = createGateResolver({
      api: { logger }, store, executor, getBroadcast: () => mock.fn(), admissionQueueStore,
    });
    await resolver.recordItemDecision({ flowId, itemId, decision: "approve", source: "ws" });
    await drainImmediate();
    assert.equal(executor.resume.mock.calls.length, 0, "executor.resume must not fire when setRunning is rejected");
    assert.equal(store._current().status, "blocked", "flow must stay blocked");
  });
});
