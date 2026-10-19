import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createLiveBridge } from "../../../ios/live-bridge.js";
import { gateEmitter, nodeEmitter, flowEmitter, failureEmitter } from "../../../events.js";

// ── helpers ───────────────────────────────────────────────────────────────────

// Flush all pending microtasks so async event handlers complete before asserts.
function flush() {
  return new Promise((r) => setImmediate(r));
}

function makeBroadcastCapture() {
  const broadcasts = [];
  return { broadcasts, relay: { broadcast: (msg) => broadcasts.push(msg) } };
}

function makeRunStore(flows = []) {
  return {
    listActive: async () => flows,
    get: async (id) => flows.find((f) => f.flowId === id) ?? null,
    listForParentInvocations: async () => [],
  };
}

function makeNodeExecutionsStore(overrides = {}) {
  return {
    listSubflowNodeIdsForFlow: async () => [],
    listInvocationIdsForNode: async () => [],
    getInvocation: async () => null,
    findCurrentStep: async () => null,
    getLatestInvocationInvokedAt: async () => null,
    ...overrides,
  };
}

function makeFlow(overrides = {}) {
  return {
    flowId: "12345678-0000-0000-0000-000000000001",
    status: "running",
    lastStep: "NODE_A",
    stateJson: { pipelineId: "team_test-pipe" },
    waitJson: null,
    endedAt: null,
    updatedAt: Date.now(),
    createdAt: Date.now(),
    ...overrides,
  };
}

function makeGateFlow(overrides = {}) {
  return makeFlow({
    status: "blocked",
    waitJson: {
      kind: "human-gate",
      nodeId: "PROPOSE",
      responder: "main",
      items: [
        { itemId: "item-1", requestPayload: { title: "Approve?" } },
      ],
    },
    ...overrides,
  });
}

function makeBridge(opts = {}) {
  const { relay, broadcasts } = makeBroadcastCapture();
  const bridge = createLiveBridge({
    relay,
    pipelineLookup:    null,
    agentLookup:       null,
    nodeAgentLookup:   opts.nodeAgentLookup   ?? null,
    nodeDiscussLookup: null,
    enableAgentRunning: opts.enableAgentRunning ?? false,
    apnsPushApproval:  opts.apnsPushApproval  ?? null,
    apnsPushFailure:   opts.apnsPushFailure   ?? null,
    runStore:          opts.runStore          ?? makeRunStore(),
    nodeExecutionsStore: opts.nodeExecutionsStore ?? makeNodeExecutionsStore(),
    logger: null,
  });
  return { bridge, broadcasts, relay };
}

// Emit gate.opened and wait for the async handler to complete
async function emitGateOpened(payload) {
  gateEmitter.emit("gate.opened", payload);
  await flush();
}

// ── gate.opened ───────────────────────────────────────────────────────────────

describe("createLiveBridge: gate.opened", () => {
  it("broadcasts AgentApproval.start for a new gate item", async () => {
    const { bridge, broadcasts } = makeBridge();
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Approve?" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    assert.equal(broadcasts.length, 1);
    assert.equal(broadcasts[0].type, "live_activity.start");
    assert.equal(broadcasts[0].activityType, "AgentApproval");
    assert.equal(broadcasts[0].activityId, "agent-approval:12345678-0000-0000-0000-000000000001:item-1");
    await bridge.stop();
  });

  it("emittedAtMs on the broadcast reflects flow_node_invocations.invoked_at for the gate node", async () => {
    // Sole source of truth — see gate-projector.js's blockedAtMs comment.
    const invokedAt = Date.now() - 7_200_000; // 2h ago
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getLatestInvocationInvokedAt: async () => invokedAt,
    });
    const { bridge, broadcasts } = makeBridge({ nodeExecutionsStore });
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Approve?" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    assert.equal(broadcasts.length, 1);
    assert.equal(broadcasts[0].emittedAtMs, invokedAt, "emittedAtMs must equal flow_node_invocations.invoked_at for the gate node");
    await bridge.stop();
  });

  it("dedup — second gate.opened for the same item does not re-broadcast", async () => {
    const { bridge, broadcasts } = makeBridge();
    const payload = {
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Approve?" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    };
    await emitGateOpened(payload);
    const countAfterFirst = broadcasts.length;
    await emitGateOpened(payload);
    assert.equal(broadcasts.length, countAfterFirst, "second emit must not re-broadcast");
    await bridge.stop();
  });

  it("skips items where approvalPending already has a decision", async () => {
    const { bridge, broadcasts } = makeBridge();
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Already decided" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: {
        pipelineId: "team_test-pipe",
        approvalPending: { "item-1": { decision: "approved" } },
      },
    });
    assert.equal(broadcasts.length, 0, "decided item must not produce a broadcast");
    await bridge.stop();
  });

  // "dismisses open PipelineProgress when gate opens for the same flow"
  // removed — PipelineProgress dismissal-on-gate-open was live-bridge.js's
  // own openProgress-Map bookkeeping, which no longer exists.
  // pipeclaw.run.* (status: "blocked" once a gate opens) now drives this
  // instead, and the dismissal happens client-side in
  // EventConsumer.applyRunningPipelinePatch — any non-"running" status
  // removes the entry.

  it("suppresses APNs when _suppressApns is true", async () => {
    const calls = [];
    const { bridge } = makeBridge({ apnsPushApproval: (a) => calls.push(a) });
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Approve?" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
      _suppressApns: true,
    });
    await Promise.resolve(); // flush apnsPushApproval's fire-and-forget Promise
    assert.equal(calls.length, 0, "APNs must be suppressed on startup seed");
    await bridge.stop();
  });

  it("calls apnsPushApproval once per item when at or below coalesce threshold", async () => {
    const calls = [];
    const { bridge } = makeBridge({ apnsPushApproval: (a) => calls.push(a) });
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [
        { itemId: "item-1", requestPayload: { title: "A" } },
        { itemId: "item-2", requestPayload: { title: "B" } },
      ],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    await Promise.resolve(); // flush APNs fire-and-forget
    assert.equal(calls.length, 2);
    assert.ok(calls[0].item, "item field expected (not items batch)");
    await bridge.stop();
  });

  it("coalesces into one batch push when items exceed threshold", async () => {
    const calls = [];
    const { bridge } = makeBridge({ apnsPushApproval: (a) => calls.push(a) });
    const items = [1, 2, 3, 4].map((n) => ({
      itemId: `item-${n}`, requestPayload: { title: `Item ${n}` },
    }));
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "FILL",
      items,
      waitJson: { kind: "human-gate", nodeId: "FILL", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    await Promise.resolve();
    assert.equal(calls.length, 1, "should coalesce into one batch push");
    assert.equal(calls[0].items?.length, 4, "batch payload should carry all items");
    assert.equal(calls[0].item, undefined, "single-item field must be absent in batch mode");
    await bridge.stop();
  });
});

// ── gate.closed ───────────────────────────────────────────────────────────────

describe("createLiveBridge: gate.closed", () => {
  it("broadcasts AgentApproval.end for every open item on the closed flow", async () => {
    const flowId = "12345678-0000-0000-0000-000000000001";
    const { bridge, broadcasts } = makeBridge();
    await emitGateOpened({
      flowId, nodeId: "PROPOSE",
      items: [
        { itemId: "item-1", requestPayload: { title: "A" } },
        { itemId: "item-2", requestPayload: { title: "B" } },
      ],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    const beforeClose = broadcasts.length;

    gateEmitter.emit("gate.closed", { flowId, nodeId: "PROPOSE" });
    const ends = broadcasts.slice(beforeClose).filter((b) => b.type === "live_activity.end");
    assert.equal(ends.length, 2, "one end per open item");
    assert.ok(ends.every((b) => b.activityId.startsWith("agent-approval:")));
    await bridge.stop();
  });

  it("does not touch open items belonging to a different flow", async () => {
    const flowA = "12345678-0000-0000-0000-000000000001";
    const flowB = "12345678-0000-0000-0000-000000000002";
    const { bridge, broadcasts } = makeBridge();
    await emitGateOpened({
      flowId: flowA, nodeId: "PROPOSE",
      items: [{ itemId: "item-a", requestPayload: { title: "A" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    gateEmitter.emit("gate.closed", { flowId: flowB, nodeId: "PROPOSE" });
    const ends = broadcasts.filter((b) => b.type === "live_activity.end");
    assert.equal(ends.length, 0, "closing flowB must not end flowA's items");
    await bridge.stop();
  });
});

// flow.step.changed no longer has any listener in live-bridge.js —
// PipelineProgress's live-push half moved to pipeclaw.run.* (index.js's
// buildRunPatch, driven by flowStateEmitter directly, no per-step
// dedup bookkeeping needed the way the old openProgress Map required).
// This whole describe block (4 tests asserting PipelineProgress.start/
// update broadcasts from this event, plus a "same nodeId must not
// re-broadcast" dedup test and a "child flows are skipped" test) tested
// behavior that's gone by design. The equivalent coverage
// (dedup-by-revision, child-flow exclusion) now belongs against
// index.js's buildRunPatch/flowStateEmitter listener, not this file.

// ── flow.terminal ─────────────────────────────────────────────────────────────

describe("createLiveBridge: flow.terminal", () => {
  // "broadcasts PipelineProgress.end when the flow was open" removed —
  // same reason as flow.step.changed above. flow.terminal's remaining
  // (unchanged) job is ending open AgentRunning entries for the flow.

  it("does not broadcast if nothing is tracked for the flow", async () => {
    const { bridge, broadcasts } = makeBridge();
    flowEmitter.emit("flow.terminal", {
      flowId: "12345678-0000-0000-0000-000000000099",
      status: "succeeded",
      stateJson: {},
    });
    await flush();
    assert.equal(broadcasts.length, 0, "no broadcast for a flow with no open AgentRunning entries");
    await bridge.stop();
  });
});

// ── failure.created ───────────────────────────────────────────────────────────

describe("createLiveBridge: failure.created", () => {
  it("broadcasts failure_event.start and calls apnsPushFailure", async () => {
    const failureCalls = [];
    const { bridge, broadcasts } = makeBridge({
      apnsPushFailure: (r) => failureCalls.push(r),
    });
    const row = {
      id: "fail-001", flow_id: "12345678-0000-0000-0000-000000000001",
      pipeline_id: "team_test-pipe", branch_item_id: null,
      failed_at: Date.now(), failure_node: "EXECUTE",
      error_type: null, failure_summary: "Something went wrong",
    };
    failureEmitter.emit("failure.created", row);
    await Promise.resolve(); // apnsPushFailure is fire-and-forget
    assert.equal(broadcasts.length, 1);
    assert.equal(broadcasts[0].type, "failure_event.start");
    assert.equal(broadcasts[0].failureId, "fail-001");
    assert.equal(failureCalls.length, 1, "apnsPushFailure must be called");
    await bridge.stop();
  });

  it("does not call apnsPushFailure when not configured", async () => {
    const { bridge, broadcasts } = makeBridge();
    failureEmitter.emit("failure.created", {
      id: "fail-002", flow_id: "12345678-0000-0000-0000-000000000001",
      pipeline_id: "team_test-pipe", branch_item_id: null,
      failed_at: Date.now(), failure_node: "X", error_type: null, failure_summary: "Y",
    });
    assert.equal(broadcasts.length, 1, "broadcast still fires without APNs configured");
    await bridge.stop();
  });
});

// ── start() seed ──────────────────────────────────────────────────────────────

describe("createLiveBridge: start() seed", () => {
  it("opens gate items for blocked flows — suppressApns so no APNs push fires", async () => {
    const apnsCalls = [];
    const flow = makeGateFlow({ flowId: "12345678-0000-0000-0000-000000000001" });
    const { bridge, broadcasts } = makeBridge({
      runStore: makeRunStore([flow]),
      apnsPushApproval: (a) => apnsCalls.push(a),
    });
    await bridge.start();
    await flush();
    await Promise.resolve(); // APNs fire-and-forget
    const starts = broadcasts.filter((b) => b.activityType === "AgentApproval");
    assert.ok(starts.length >= 1, "gate item must be opened during seed");
    assert.equal(apnsCalls.length, 0, "no APNs push during seed");
    await bridge.stop();
  });

  // start()'s seed no longer does anything with running flows at all (that
  // branch was removed — see the comment in live-bridge.js's start()).
  // A freshly-connected client learns about already-running flows via
  // snapshot()'s own listActive() query instead, covered by
  // "returns projected PipelineProgress for each open progress entry"
  // below.
});

// ── stop() ────────────────────────────────────────────────────────────────────

describe("createLiveBridge: stop()", () => {
  it("removes all listeners — subsequent events are no-ops", async () => {
    const { bridge, broadcasts } = makeBridge();
    await bridge.stop();
    // Emit after stop; nothing should land
    await emitGateOpened({
      flowId: "12345678-0000-0000-0000-000000000001",
      nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "X" } }],
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main" },
      stateJson: { pipelineId: "team_test-pipe" },
    });
    flowEmitter.emit("flow.step.changed", {
      flowId: "12345678-0000-0000-0000-000000000001",
      lastStep: "NODE_A", stateJson: {}, status: "running",
    });
    await flush();
    assert.equal(broadcasts.length, 0, "no broadcasts after stop");
  });
});

// ── snapshot() ────────────────────────────────────────────────────────────────

describe("createLiveBridge: snapshot()", () => {
  it("returns empty array when nothing is open", async () => {
    const { bridge } = makeBridge();
    const snap = await bridge.snapshot();
    assert.deepEqual(snap, []);
    await bridge.stop();
  });

  it("returns projected AgentApproval for each open gate key", async () => {
    const flowId = "12345678-0000-0000-0000-000000000001";
    const flow = makeGateFlow({ flowId });
    const { bridge, broadcasts } = makeBridge({
      runStore: makeRunStore([flow]),
    });
    await emitGateOpened({
      flowId, nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Approve?" } }],
      waitJson: flow.waitJson,
      stateJson: flow.stateJson,
    });
    const snap = await bridge.snapshot();
    assert.ok(snap.length >= 1, "snapshot must include the open gate item");
    assert.ok(snap.some((m) => m.activityId === `agent-approval:${flowId}:item-1`));
    await bridge.stop();
  });

  it("emittedAtMs in snapshot reflects flow_node_invocations.invoked_at for the gate node, ignoring flow.createdAt/updatedAt", async () => {
    // Sole source of truth — see gate-projector.js's blockedAtMs comment.
    // updatedAt is a generic "last write to this row" column, bumped by
    // ANY write path — including a field/draft edit on the gate item
    // that has nothing to do with when it started waiting; createdAt is
    // the whole FLOW's creation time, which can predate the gate node's
    // actual dispatch by however long earlier steps took to run. Both are
    // set here specifically to prove neither leaks through.
    const flowId = "12345678-0000-0000-0000-000000000001";
    const invokedAt = Date.now() - 1_800_000; // 30m ago — gate node actually dispatched
    const flow = makeGateFlow({ flowId, createdAt: Date.now() - 3_600_000, updatedAt: Date.now() });
    const nodeExecutionsStore = makeNodeExecutionsStore({
      getLatestInvocationInvokedAt: async () => invokedAt,
    });
    const { bridge } = makeBridge({ runStore: makeRunStore([flow]), nodeExecutionsStore });
    await emitGateOpened({
      flowId, nodeId: "PROPOSE",
      items: [{ itemId: "item-1", requestPayload: { title: "Approve?" } }],
      waitJson: flow.waitJson,
      stateJson: flow.stateJson,
    });
    const snap = await bridge.snapshot();
    const msg = snap.find((m) => m.activityId === `agent-approval:${flowId}:item-1`);
    assert.ok(msg, "snapshot must include the gate item");
    assert.equal(msg.emittedAtMs, invokedAt, "snapshot emittedAtMs must equal flow_node_invocations.invoked_at for the gate node");
    await bridge.stop();
  });

  // "returns a pipeclaw.run.updated-shaped patch for each running
  // top-level flow" / "excludes child flows ... from the reconcile
  // snapshot" removed — snapshot() briefly emitted these, but that broke
  // InboxAPI.swift's InboxClient.fetch(), which strictly decodes
  // /pipeclaw/ios/state's `open` array (this function's return value) as
  // [InboxActivity] — every element must have activityId/activityType/
  // attributes/contentState, which a RunSummaryPatch-shaped entry doesn't,
  // so the whole array decode threw. Running-pipeline reconcile data now
  // comes from a separate `runningPipelines` JSON key that ios/index.js's
  // STATE_PATH handler builds straight from its own _runningFlows map —
  // snapshot() no longer touches this at all. See the fix commit and
  // ios/index.js's STATE_PATH handler for the corrected shape.
});
