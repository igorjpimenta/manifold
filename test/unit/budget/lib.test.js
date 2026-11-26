import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { deserialize, isGateLocked, resolveNodeDecision, classifyDecisionStatus, todayLocal } from "../../../budget/lib.js";


describe("deserialize", () => {
  const base = {
    id: "abc",
    label: "SCORE × 5",
    agent: "scorer",
    agent_emoji: "⚖️",
    trigger: "cron",
    cost_low: 0.001,
    cost_high: 0.003,
    brief: null,
    model_chain: null,
    pipeline: "workana-proposal",
    node: "score",
    token_estimate: null,
    status: "pending",
    created_at: 1700000000000,
    expires_at: null,
    decided_at: null,
    decision: null,
    pipeline_id: null,
    pipeline_args: null,
    flow_id: null,
  };

  it("maps snake_case columns to camelCase fields", () => {
    const r = deserialize(base);
    assert.equal(r.id, "abc");
    assert.equal(r.agentEmoji, "⚖️");
    assert.equal(r.costLow, 0.001);
    assert.equal(r.costHigh, 0.003);
    assert.equal(r.createdAt, 1700000000000);
    assert.equal(r.tokenEstimate, null);
    assert.equal(r.flowId, null);
  });

  it("parses model_chain JSON string", () => {
    const r = deserialize({ ...base, model_chain: '["zai/glm-4.5-flash"]' });
    assert.deepEqual(r.modelChain, ["zai/glm-4.5-flash"]);
  });

  it("returns empty array when model_chain is null", () => {
    const r = deserialize({ ...base, model_chain: null });
    assert.deepEqual(r.modelChain, []);
  });

  it("parses pipeline_args JSON string", () => {
    const r = deserialize({ ...base, pipeline_args: '{"foo":"bar"}' });
    assert.deepEqual(r.pipelineArgs, { foo: "bar" });
  });

  it("returns null for pipeline_args when absent", () => {
    const r = deserialize({ ...base, pipeline_args: null });
    assert.equal(r.pipelineArgs, null);
  });

  it("carries through optional fields", () => {
    const r = deserialize({
      ...base,
      flow_id: "flow-uuid",
      decided_at: 1700001000000,
      decision: "approve",
      expires_at: 1700099000000,
    });
    assert.equal(r.flowId, "flow-uuid");
    assert.equal(r.decidedAt, 1700001000000);
    assert.equal(r.decision, "approve");
    assert.equal(r.expiresAt, 1700099000000);
  });
});


describe("isGateLocked", () => {
  it("returns false when row is null", () => {
    assert.equal(isGateLocked(null, "2026-06-24"), false);
  });

  it("returns false when locked=0", () => {
    assert.equal(isGateLocked({ locked: 0, day: "2026-06-24" }, "2026-06-24"), false);
  });

  it("returns true when locked=1 and day equals today", () => {
    assert.equal(isGateLocked({ locked: 1, day: "2026-06-24" }, "2026-06-24"), true);
  });

  it("returns true when locked=1 and day is in the future (edge case)", () => {
    assert.equal(isGateLocked({ locked: 1, day: "2026-06-30" }, "2026-06-24"), true);
  });

  it("returns false when locked=1 but day is yesterday (stale lock)", () => {
    assert.equal(isGateLocked({ locked: 1, day: "2026-06-23" }, "2026-06-24"), false);
  });

  it("returns false when locked=1 but day is null", () => {
    assert.equal(isGateLocked({ locked: 1, day: null }, "2026-06-24"), false);
  });
});


describe("todayLocal", () => {
  it("stamps the local calendar day, not the UTC one", () => {
    const justAfterUtcMidnight = new Date("2026-09-29T00:30:00Z");
    const utc = justAfterUtcMidnight.toISOString().slice(0, 10);
    const local = todayLocal(justAfterUtcMidnight);

    if (justAfterUtcMidnight.getTimezoneOffset() > 0) {
      assert.equal(utc, "2026-09-29");
      assert.equal(local, "2026-09-28");
    } else {
      assert.equal(local, utc);
    }
  });

  it("zero-pads single-digit months and days", () => {
    const d = new Date(2026, 0, 5, 12, 0, 0);
    assert.equal(todayLocal(d), "2026-01-05");
  });

  it("is the day isGateLocked() defaults to, so a same-day lock always reads back locked", () => {
    const row = { locked: 1, day: todayLocal() };
    assert.equal(isGateLocked(row), true);
    assert.equal(isGateLocked(row), isGateLocked(row, todayLocal()));
  });

  it("keeps a lock stamped late in the local day valid while UTC has already rolled over", () => {
    const lateEvening = new Date("2026-09-29T00:30:00Z");
    const stamped = { locked: 1, day: todayLocal(lateEvening) };
    assert.equal(isGateLocked(stamped, todayLocal(lateEvening)), true);
    if (lateEvening.getTimezoneOffset() > 0) {
      assert.equal(todayLocal(lateEvening), "2026-09-28");
    }
  });
});


describe("resolveNodeDecision", () => {
  it("returns null when row is null", () => {
    assert.equal(resolveNodeDecision(null), null);
  });

  it("returns 'pending' when status is pending", () => {
    assert.equal(resolveNodeDecision({ status: "pending", decision: null }), "pending");
  });

  it("returns 'approve' when decision is approve", () => {
    assert.equal(resolveNodeDecision({ status: "approved", decision: "approve" }), "approve");
  });

  it("returns 'reject' when decision is reject", () => {
    assert.equal(resolveNodeDecision({ status: "rejected", decision: "reject" }), "reject");
  });

  it("returns null for an unknown status/decision pair", () => {
    assert.equal(resolveNodeDecision({ status: "expired", decision: null }), null);
  });

  it("pending status takes priority over a stale decision field", () => {
    assert.equal(resolveNodeDecision({ status: "pending", decision: "approve" }), "pending");
  });
});


describe("classifyDecisionStatus", () => {
  it("maps 'approve' to 'approved'", () => {
    assert.equal(classifyDecisionStatus("approve"), "approved");
  });

  it("maps 'reject' to 'rejected'", () => {
    assert.equal(classifyDecisionStatus("reject"), "rejected");
  });

  it("returns null for unknown values", () => {
    assert.equal(classifyDecisionStatus("skip"), null);
    assert.equal(classifyDecisionStatus(null), null);
    assert.equal(classifyDecisionStatus(undefined), null);
  });
});
