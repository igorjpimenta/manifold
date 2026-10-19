import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createBudgetGate } from "../../../engine/executor-budget.js";

const logger = { info: () => {}, warn: () => {} };

// Fake nodeCallStats — no history by default, matching the real function's
// null-on-no-samples contract, so existing tests keep exercising the
// $0.001-$0.05 fallback branch of estimateNodeCost exactly as before.
const noStats = async () => null;

function makeBudgetGate(aqOpts = {}, nodeCallStats = noStats) {
  const defaults = {
    isGateLocked: async () => false,
    lookupNodeDecision: async () => null,
    enqueueNodeApproval: async () => "item-123",
    ...aqOpts,
  };
  return createBudgetGate({ getApprovalQueue: () => defaults, logger, nodeCallStats });
}

// ── no approval queue ─────────────────────────────────────────────────────────

describe("createBudgetGate — no approval queue", () => {
  it("proceeds when getApprovalQueue returns null", async () => {
    const { maybeBudgetGate } = createBudgetGate({ getApprovalQueue: () => null, logger });
    const result = await maybeBudgetGate({ flowId: "flow-1", nodeId: "NODE", pipelineId: "p", pipelineName: "p", node: {}, itemCount: 1 });
    assert.deepEqual(result, { proceed: true });
  });

  it("proceeds when approvalQueue lacks isGateLocked", async () => {
    const { maybeBudgetGate } = createBudgetGate({ getApprovalQueue: () => ({}), logger });
    const result = await maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} });
    assert.deepEqual(result, { proceed: true });
  });
});

// ── gate not locked ───────────────────────────────────────────────────────────

describe("createBudgetGate — gate not locked", () => {
  it("proceeds when isGateLocked returns false", async () => {
    const { maybeBudgetGate } = makeBudgetGate({ isGateLocked: async () => false });
    const result = await maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} });
    assert.deepEqual(result, { proceed: true });
  });
});

// ── prior decision ────────────────────────────────────────────────────────────

describe("createBudgetGate — prior decision", () => {
  it("proceeds when decision is approve", async () => {
    const { maybeBudgetGate } = makeBudgetGate({
      isGateLocked: async () => true,
      lookupNodeDecision: async () => "approve",
    });
    const result = await maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} });
    assert.deepEqual(result, { proceed: true });
  });

  it("skips when decision is reject", async () => {
    const { maybeBudgetGate } = makeBudgetGate({
      isGateLocked: async () => true,
      lookupNodeDecision: async () => "reject",
    });
    const result = await maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} });
    assert.deepEqual(result, { skip: true });
  });

  it("pauses without enqueueing when decision is pending", async () => {
    let enqueued = false;
    const { maybeBudgetGate } = makeBudgetGate({
      isGateLocked: async () => true,
      lookupNodeDecision: async () => "pending",
      enqueueNodeApproval: async () => { enqueued = true; return "item-x"; },
    });
    const result = await maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} });
    assert.equal(result.pause, true);
    assert.ok(result.blockedInfo._kind === "budget-gate-approval");
    assert.equal(enqueued, false);
  });
});

// ── no prior decision — enqueue ───────────────────────────────────────────────

describe("createBudgetGate — gate locked, no decision", () => {
  it("enqueues item and returns pause + blockedInfo", async () => {
    let enqueuedArgs = null;
    const { maybeBudgetGate } = makeBudgetGate({
      isGateLocked: async () => true,
      lookupNodeDecision: async () => null,
      enqueueNodeApproval: async (args) => { enqueuedArgs = args; return "item-new"; },
    });
    const result = await maybeBudgetGate({
      flowId: "flow-abc", nodeId: "DRAFT", pipelineId: "team_test", pipelineName: "team_test",
      node: { agent: "cyrano" }, itemCount: 2,
    });
    assert.equal(result.pause, true);
    assert.equal(result.blockedInfo._kind, "budget-gate-approval");
    assert.ok(enqueuedArgs !== null);
    assert.equal(enqueuedArgs.nodeId, "DRAFT");
    assert.equal(enqueuedArgs.pipelineId, "team_test");
  });

  it("still pauses when enqueueNodeApproval throws", async () => {
    const { maybeBudgetGate } = makeBudgetGate({
      isGateLocked: async () => true,
      lookupNodeDecision: async () => null,
      enqueueNodeApproval: async () => { throw new Error("quota exceeded"); },
    });
    const result = await maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} });
    assert.equal(result.pause, true);
  });

  it("enqueues a finite cost estimate derived from real nodeCallStats (regression: the missing-await bug made this NaN)", async () => {
    let enqueuedArgs = null;
    const { maybeBudgetGate } = makeBudgetGate(
      {
        isGateLocked: async () => true,
        lookupNodeDecision: async () => null,
        enqueueNodeApproval: async (args) => { enqueuedArgs = args; return "item-new"; },
      },
      async () => ({ p25: 0.01, p50: 0.02, p75: 0.03, sampleSize: 5 }),
    );
    const result = await maybeBudgetGate({
      flowId: "flow-abc", nodeId: "SCORE", pipelineId: "team_test", pipelineName: "team_test",
      node: {}, itemCount: 4,
    });
    assert.equal(result.pause, true);
    assert.ok(enqueuedArgs !== null);
    assert.ok(Number.isFinite(enqueuedArgs.costLow), `costLow must be finite, got ${enqueuedArgs.costLow}`);
    assert.ok(Number.isFinite(enqueuedArgs.costHigh), `costHigh must be finite, got ${enqueuedArgs.costHigh}`);
    assert.equal(enqueuedArgs.costLow, 0.01 * 4);
    assert.equal(enqueuedArgs.costHigh, 0.03 * 4);
  });

  it("falls back to the default cost band when nodeCallStats has no history", async () => {
    let enqueuedArgs = null;
    const { maybeBudgetGate } = makeBudgetGate(
      {
        isGateLocked: async () => true,
        lookupNodeDecision: async () => null,
        enqueueNodeApproval: async (args) => { enqueuedArgs = args; return "item-new"; },
      },
      async () => null,
    );
    const result = await maybeBudgetGate({
      flowId: "flow-abc", nodeId: "SCORE", pipelineId: "team_test", pipelineName: "team_test",
      node: {}, itemCount: 3,
    });
    assert.equal(result.pause, true);
    assert.ok(Number.isFinite(enqueuedArgs.costLow));
    assert.ok(Number.isFinite(enqueuedArgs.costHigh));
    assert.equal(enqueuedArgs.costLow, 0.001 * 3);
    assert.equal(enqueuedArgs.costHigh, 0.05 * 3);
  });
});

// ── isGateLocked errors propagate ───────────────────────────────────────────

describe("createBudgetGate — isGateLocked throws", () => {
  it("propagates a thrown error from isGateLocked instead of silently proceeding", async () => {
    const { maybeBudgetGate } = makeBudgetGate({
      isGateLocked: async () => { throw new Error("disk full"); },
    });
    await assert.rejects(
      () => maybeBudgetGate({ flowId: "f", nodeId: "N", pipelineId: "p", pipelineName: "p", node: {} }),
      /disk full/,
    );
  });
});
