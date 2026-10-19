import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  startAgentApproval, updateAgentApproval, endAgentApproval,
  startAgentRunning, updateAgentRunning, endAgentRunning,
  startFailureEvent, synthAgentApproval,
} from "../../../ios/projectors.js";

const UUID = "11111111-2222-3333-4444-555555555555";

// ── startAgentApproval ────────────────────────────────────────────────────────

describe("startAgentApproval", () => {
  const flow = {
    flowId: UUID,
    nodeId: "GATE",
    reason: "review needed",
    needed: ["approval"],
    hint: "please check",
    blockedAtMs: 1_000_000,
    layout: [],
  };
  const agentMeta = { emoji: "🤝", name: "Herculano" };
  const pipelineMeta = { id: "team_pipe", displayName: "Team Pipe", emoji: "📋", sfSymbol: "briefcase.fill" };

  it("emits live_activity.start with type=AgentApproval", () => {
    const msg = startAgentApproval({ flow, agentMeta, pipelineMeta });
    assert.equal(msg.type, "live_activity.start");
    assert.equal(msg.activityType, "AgentApproval");
  });

  it("activityId uses needs-info shape when itemId is absent", () => {
    const msg = startAgentApproval({ flow: { ...flow, itemId: "" }, agentMeta, pipelineMeta });
    assert.equal(msg.activityId, `agent-approval:${UUID}`);
  });

  it("activityId includes itemId when present", () => {
    const msg = startAgentApproval({ flow: { ...flow, itemId: "item-99" }, agentMeta, pipelineMeta });
    assert.equal(msg.activityId, `agent-approval:${UUID}:item-99`);
  });

  it("uses blockedAtMs as emittedAtMs", () => {
    const msg = startAgentApproval({ flow, agentMeta, pipelineMeta });
    assert.equal(msg.emittedAtMs, 1_000_000);
  });

  it("fills in defaults for missing agentMeta fields", () => {
    const msg = startAgentApproval({ flow, agentMeta: {}, pipelineMeta });
    assert.equal(msg.attributes.agentEmoji, "🤖");
    assert.equal(msg.attributes.agentName, "Agent");
  });

  it("fills in defaults for missing pipelineMeta fields", () => {
    const msg = startAgentApproval({ flow, agentMeta, pipelineMeta: null });
    assert.equal(msg.attributes.pipelineId, "unknown");
    assert.equal(msg.attributes.pipelineEmoji, "📊");
    assert.equal(msg.attributes.pipelineSfSymbol, "tray.fill");
  });

  it("passes reason, needed, hint into contentState", () => {
    const msg = startAgentApproval({ flow, agentMeta, pipelineMeta });
    assert.equal(msg.contentState.reason, "review needed");
    assert.deepEqual(msg.contentState.needed, ["approval"]);
    assert.equal(msg.contentState.hint, "please check");
  });

  it("discussSupported defaults to false", () => {
    const msg = startAgentApproval({ flow, agentMeta, pipelineMeta });
    assert.equal(msg.contentState.discussSupported, false);
  });

  it("discussSupported true when flow sets it", () => {
    const msg = startAgentApproval({ flow: { ...flow, discussSupported: true }, agentMeta, pipelineMeta });
    assert.equal(msg.contentState.discussSupported, true);
  });

  it("gateKind forwarded", () => {
    const msg = startAgentApproval({ flow: { ...flow, gateKind: "verify-required" }, agentMeta, pipelineMeta });
    assert.equal(msg.contentState.gateKind, "verify-required");
  });

  it("staleAfterMs is 300000", () => {
    const msg = startAgentApproval({ flow, agentMeta, pipelineMeta });
    assert.equal(msg.staleAfterMs, 300_000);
  });
});

// ── updateAgentApproval ───────────────────────────────────────────────────────

describe("updateAgentApproval", () => {
  it("emits live_activity.update with correct activityId (no itemId)", () => {
    const msg = updateAgentApproval({ flowId: UUID, itemId: "", content: { reason: "updated" } });
    assert.equal(msg.type, "live_activity.update");
    assert.equal(msg.activityId, `agent-approval:${UUID}`);
    assert.equal(msg.contentState.reason, "updated");
  });

  it("activityId includes itemId when present", () => {
    const msg = updateAgentApproval({ flowId: UUID, itemId: "item-2", content: {} });
    assert.equal(msg.activityId, `agent-approval:${UUID}:item-2`);
  });
});

// ── endAgentApproval ─────────────────────────────────────────────────────────

describe("endAgentApproval", () => {
  it("emits live_activity.end with default dismissalPolicy", () => {
    const msg = endAgentApproval({ flowId: UUID, itemId: "" });
    assert.equal(msg.type, "live_activity.end");
    assert.equal(msg.dismissalPolicy, "after:30s");
  });

  it("respects custom dismissalPolicy", () => {
    const msg = endAgentApproval({ flowId: UUID, itemId: "", dismissalPolicy: "immediate" });
    assert.equal(msg.dismissalPolicy, "immediate");
  });

  it("includes finalState when provided", () => {
    const msg = endAgentApproval({ flowId: UUID, itemId: "", finalState: { outcome: "approved" } });
    assert.deepEqual(msg.finalState, { outcome: "approved" });
  });

  it("omits finalState key when not provided", () => {
    const msg = endAgentApproval({ flowId: UUID, itemId: "" });
    assert.ok(!("finalState" in msg));
  });
});

// startPipelineProgress/updatePipelineProgress/endPipelineProgress removed —
// PipelineProgress is now built by gate-projector.js's buildRunPatch (see
// its own test file) rather than a dedicated live_activity.* projector.

// ── startAgentRunning ─────────────────────────────────────────────────────────

describe("startAgentRunning", () => {
  const flow = { flowId: UUID, nodeId: "DRAFT", nodeLabel: "Write draft", startedAtMs: 3_000_000 };
  const agentMeta = { emoji: "✍️", name: "Cyrano" };
  const pipelineMeta = { id: "team_pipe", displayName: "Team Pipe", emoji: "📋" };
  const nodeMeta = { kind: "single", skill: "write-proposal" };

  it("emits live_activity.start with type=AgentRunning", () => {
    const msg = startAgentRunning({ flow, agentMeta, pipelineMeta, nodeMeta });
    assert.equal(msg.type, "live_activity.start");
    assert.equal(msg.activityType, "AgentRunning");
    assert.equal(msg.activityId, `agent-running:${UUID}:DRAFT`);
  });

  it("contentState carries nodeLabel, skill, kind", () => {
    const msg = startAgentRunning({ flow, agentMeta, pipelineMeta, nodeMeta });
    assert.equal(msg.contentState.nodeLabel, "Write draft");
    assert.equal(msg.contentState.skill, "write-proposal");
    assert.equal(msg.contentState.kind, "single");
  });

  it("subflows null when not provided", () => {
    const msg = startAgentRunning({ flow, agentMeta, pipelineMeta, nodeMeta });
    assert.equal(msg.contentState.subflows, null);
  });

  it("subflows forwarded when provided", () => {
    const subflows = [{ itemId: "a", status: "running" }];
    const msg = startAgentRunning({ flow, agentMeta, pipelineMeta, nodeMeta, subflows });
    assert.deepEqual(msg.contentState.subflows, subflows);
  });

  it("staleAfterMs is 600000", () => {
    const msg = startAgentRunning({ flow, agentMeta, pipelineMeta, nodeMeta });
    assert.equal(msg.staleAfterMs, 600_000);
  });
});

// ── updateAgentRunning ────────────────────────────────────────────────────────

describe("updateAgentRunning", () => {
  it("emits live_activity.update with flowId+nodeId key", () => {
    const msg = updateAgentRunning({ flowId: UUID, nodeId: "DRAFT", content: { nodeLabel: "x" } });
    assert.equal(msg.type, "live_activity.update");
    assert.equal(msg.activityId, `agent-running:${UUID}:DRAFT`);
  });
});

// ── endAgentRunning ───────────────────────────────────────────────────────────

describe("endAgentRunning", () => {
  it("emits live_activity.end with 5s dismissal", () => {
    const msg = endAgentRunning({ flowId: UUID, nodeId: "DRAFT" });
    assert.equal(msg.type, "live_activity.end");
    assert.equal(msg.dismissalPolicy, "after:5s");
  });
});

// ── startFailureEvent ─────────────────────────────────────────────────────────

describe("startFailureEvent", () => {
  const row = {
    id: "fail-1", flow_id: UUID, pipeline_id: "team_pipe",
    branch_item_id: "item-7", failure_node: "SEND",
    error_type: null, failure_summary: "exit 1",
    failed_at: 4_000_000,
  };

  it("emits failure_event.start", () => {
    const msg = startFailureEvent({ row });
    assert.equal(msg.type, "failure_event.start");
    assert.equal(msg.failureId, "fail-1");
    assert.equal(msg.flowId, UUID);
    assert.equal(msg.emittedAtMs, 4_000_000);
  });

  it("isRetryable true when branchItemId + failureNode present", () => {
    const msg = startFailureEvent({ row });
    assert.equal(msg.isRetryable, true);
  });

  it("isRetryable false when branch_item_id missing", () => {
    const msg = startFailureEvent({ row: { ...row, branch_item_id: null } });
    assert.equal(msg.isRetryable, false);
  });
});

// ── synthAgentApproval ────────────────────────────────────────────────────────

describe("synthAgentApproval", () => {
  it("returns start message by default", () => {
    const msg = synthAgentApproval({ seed: "abc" });
    assert.equal(msg.type, "live_activity.start");
    assert.equal(msg.activityType, "AgentApproval");
  });

  it("uses provided flowId in activityId", () => {
    const msg = synthAgentApproval({ flowId: UUID });
    assert.ok(msg.activityId.includes(UUID));
  });

  it("falls back to synthetic-<seed> when flowId absent", () => {
    const msg = synthAgentApproval({ seed: "xyz" });
    assert.ok(msg.activityId.includes("synthetic-xyz"));
  });

  it("returns update message for phase=update", () => {
    const msg = synthAgentApproval({ phase: "update" });
    assert.equal(msg.type, "live_activity.update");
  });

  it("returns end message for phase=end", () => {
    const msg = synthAgentApproval({ phase: "end" });
    assert.equal(msg.type, "live_activity.end");
    assert.equal(msg.dismissalPolicy, "immediate");
  });

  it("works with no arguments", () => {
    const msg = synthAgentApproval();
    assert.equal(msg.type, "live_activity.start");
  });
});
