import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  projectGateItem, buildRunPatch,
  buildSubflowAggregateSummary, pipelineMetaFor,
} from "../../../ios/gate-projector.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeFlow(overrides = {}) {
  return {
    flowId: "12345678-0000-0000-0000-000000000001",
    lastStep: "NODE_A",
    stateJson: { pipelineId: "team_test-pipe" },
    waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main", items: [] },
    ...overrides,
  };
}

// ── projectGateItem ───────────────────────────────────────────────────────────

describe("projectGateItem: basic shape", () => {
  it("returns a live_activity.start AgentApproval message with correct activityId", () => {
    const flow = makeFlow();
    const item = { itemId: "item-1", requestPayload: { title: "Approve?" } };
    const msg = projectGateItem(flow, item, {}, {});
    assert.equal(msg.type, "live_activity.start");
    assert.equal(msg.activityType, "AgentApproval");
    assert.equal(msg.activityId, "agent-approval:12345678-0000-0000-0000-000000000001:item-1");
    assert.equal(msg.contentState.reason, "Approve?");
  });

  it("passes requestPayload.layout straight through to contentState.layout", () => {
    const flow = makeFlow();
    const layout = [{ kind: "deck", id: "q", questions: [{ question: "Q1?", hint: null }] }];
    const item = { itemId: "item-1", requestPayload: { title: "T", layout } };
    const msg = projectGateItem(flow, item, {}, {});
    assert.deepEqual(msg.contentState.layout, layout);
  });

  it("defaults contentState.layout to empty array when requestPayload has none", () => {
    const flow = makeFlow();
    const item = { itemId: "item-1", requestPayload: { title: "T" } };
    const msg = projectGateItem(flow, item, {}, {});
    assert.deepEqual(msg.contentState.layout, []);
  });

  it("sets attributes.groupId to null when nodeAgentLookup is absent", () => {
    const flow = makeFlow();
    const item = { itemId: "item-1", requestPayload: { title: "A" } };
    const msg = projectGateItem(flow, item, {}, { nodeAgentLookup: null });
    assert.equal(msg.attributes.groupId, null);
  });
});

describe("projectGateItem: deckPresentation", () => {
  it("stamps groupId keyed by nodeId when gate node opts in via deckPresentation", () => {
    const flow = makeFlow({
      waitJson: { kind: "human-gate", nodeId: "FINAL_REVIEW", responder: "main", items: [] },
    });
    const item = { itemId: "check-1", requestPayload: { title: "Check?" } };
    const nodeAgentLookup = (pipelineId, nodeId) =>
      nodeId === "FINAL_REVIEW" ? { kind: "human-gate", deckPresentation: true } : null;
    const msg = projectGateItem(flow, item, {}, { nodeAgentLookup });
    assert.equal(msg.attributes.groupId, "FINAL_REVIEW");
  });

  it("leaves groupId null when gate node does NOT opt in", () => {
    const flow = makeFlow({
      waitJson: { kind: "human-gate", nodeId: "FINAL_REVIEW", responder: "main", items: [] },
    });
    const item = { itemId: "check-1", requestPayload: { title: "Check?" } };
    const nodeAgentLookup = (pipelineId, nodeId) =>
      nodeId === "FINAL_REVIEW" ? { kind: "human-gate" } : null;
    const msg = projectGateItem(flow, item, {}, { nodeAgentLookup });
    assert.equal(msg.attributes.groupId, null);
  });

  it("stamps groupId for a subflow node that opts in via deckPresentation", () => {
    const flow = makeFlow({
      waitJson: { kind: "human-gate", nodeId: "PRIME_FILL", responder: "main", items: [] },
    });
    const item = { itemId: "item-1", requestPayload: { title: "Fill?" } };
    const nodeAgentLookup = (pipelineId, nodeId) =>
      nodeId === "PRIME_FILL" ? { kind: "subflow", deckPresentation: true } : null;
    const msg = projectGateItem(flow, item, {}, { nodeAgentLookup });
    assert.equal(msg.attributes.groupId, "PRIME_FILL");
  });
});

describe("projectGateItem: allSubflowGroups (migrated childFlows model)", () => {
  // Build an allSubflowGroups shape that looks like what getAllSubflowGroups returns
  // after a childFlows-migrated subflow node has settled its loop items.
  //
  // allSubflowGroups = { PRIME_FILL: { "item-1": { flowId, groupId, subflowKey, status, ... } } }
  it("stamps groupId AND childFlowId when the item lives in a subflow group", () => {
    const flow = makeFlow({
      waitJson: { kind: "human-gate", nodeId: "PRIME_FILL", responder: "main", items: [] },
    });
    const item = { itemId: "item-1", requestPayload: { title: "Fill?" } };
    const allSubflowGroups = {
      PRIME_FILL: {
        "item-1": {
          groupId: "PRIME_FILL", subflowKey: "item-1",
          flowId: "child-flow-xyz",
          status: "succeeded", step: "FILL",
          startedAt: 1, updatedAt: 2, doneAt: 3,
          awaiting: null, errors: [],
        },
      },
    };
    const nodeAgentLookup = (pipelineId, nodeId) =>
      nodeId === "PRIME_FILL" ? { kind: "subflow", deckPresentation: true } : null;
    const msg = projectGateItem(flow, item, allSubflowGroups, { nodeAgentLookup });
    assert.equal(msg.attributes.groupId, "PRIME_FILL");
    assert.equal(msg.attributes.childFlowId, "child-flow-xyz");
  });

  it("leaves attributes.groupId null for a plain gate with no subflow groups", () => {
    const flow = makeFlow({
      waitJson: { kind: "human-gate", nodeId: "PROPOSE", responder: "main", items: [] },
    });
    const item = { itemId: "item-1", requestPayload: { title: "Approve?" } };
    const msg = projectGateItem(flow, item, {}, {});
    assert.equal(msg.attributes.groupId, null);
  });
});

describe("projectGateItem: blockedAtMs (surfaced as emittedAtMs)", () => {
  // flow_node_invocations.invoked_at (threaded in by live-bridge.js as
  // flow.gateInvokedAtMs) is the SOLE source — deliberately no fallback
  // chain. flow.createdAt/updatedAt were tried and discarded: createdAt
  // reflects the whole FLOW's creation and can predate the gate node's
  // actual dispatch by however long earlier steps took to run; updatedAt
  // is bumped by ANY write to the flow row (including an unrelated field/
  // draft edit via persistRevisedMeta/persistRevisedDraft), silently
  // resetting an item's "waiting since" clock to "now" on every edit.
  // projectors.js's startAgentApproval surfaces blockedAtMs as the
  // message's top-level emittedAtMs (falling back to Date.now() there,
  // one layer up, if gateInvokedAtMs is unavailable).
  it("uses flow.gateInvokedAtMs as blockedAtMs", () => {
    const flow = makeFlow({ gateInvokedAtMs: 1500 });
    const item = { itemId: "item-1", requestPayload: { title: "T" } };
    const msg = projectGateItem(flow, item, {}, {});
    assert.equal(msg.emittedAtMs, 1500);
  });

  it("ignores flow.createdAt/updatedAt entirely, even when present", () => {
    const flow = makeFlow({ gateInvokedAtMs: 1500, createdAt: 1000, updatedAt: 5000 });
    const item = { itemId: "item-1", requestPayload: { title: "T" } };
    const msg = projectGateItem(flow, item, {}, {});
    assert.equal(msg.emittedAtMs, 1500);
  });
});

// ── buildRunPatch ────────────────────────────────────────────────────────────
// Absorbed PipelineProgress's fields (see PIPECLAW_EVENT_STANDARDIZATION.
// design.md) — this replaces the old projectProgress/progressUpdateContent
// coverage above.

describe("buildRunPatch", () => {
  it("carries presentation fields resolved via pipelineLookup", () => {
    const flow = {
      flowId: "12345678-0000-0000-0000-000000000001",
      status: "running",
      lastStep: "NODE_A",
      stateJson: { pipelineId: "team_test-pipe" },
      waitJson: null,
      createdAt: 1000,
      updatedAt: 2000,
      endedAt: null,
    };
    const pipelineLookup = (id) =>
      id === "team_test-pipe" ? { displayName: "Test Pipe", emoji: "🧪" } : undefined;
    const patch = buildRunPatch(flow, 3000, { pipelineLookup });
    assert.equal(patch.flowId, flow.flowId);
    assert.equal(patch.pipelineId, "team_test-pipe");
    assert.equal(patch.status, "running");
    assert.equal(patch.currentNodeId, "NODE_A");
    assert.equal(patch.pipelineDisplayName, "Test Pipe");
    assert.equal(patch.pipelineEmoji, "🧪");
    assert.equal(patch.createdAtMs, 1000);
    assert.equal(patch.updatedAtMs, 2000);
  });

  it("falls back to prettified id/default emoji when pipelineLookup has no entry", () => {
    const flow = {
      flowId: "12345678-0000-0000-0000-000000000002",
      status: "running",
      lastStep: "NODE_B",
      stateJson: { pipelineId: "team_unknown-pipe" },
      waitJson: null,
      createdAt: 1000,
      updatedAt: 2000,
      endedAt: null,
    };
    const patch = buildRunPatch(flow, 3000, {});
    assert.ok(patch.pipelineDisplayName, "must fall back to a prettified name, not null");
    assert.equal(patch.pipelineEmoji, "📊");
  });

  it("pipelineId/pipelineDisplayName/pipelineEmoji are null when stateJson has no pipelineId", () => {
    const flow = {
      flowId: "12345678-0000-0000-0000-000000000003",
      status: "running",
      lastStep: null,
      stateJson: {},
      waitJson: null,
      createdAt: 1000,
      updatedAt: 2000,
      endedAt: null,
    };
    const patch = buildRunPatch(flow, 3000, {});
    assert.equal(patch.pipelineId, null);
    assert.equal(patch.pipelineDisplayName, null);
    assert.equal(patch.pipelineEmoji, null);
  });
});

// ── buildSubflowAggregateSummary ──────────────────────────────────────────────

describe("buildSubflowAggregateSummary", () => {
  it("returns null for empty or missing subflow rows", () => {
    assert.equal(buildSubflowAggregateSummary(null), null);
    assert.equal(buildSubflowAggregateSummary({}), null);
  });

  it("counts total and awaiting rows", () => {
    const rows = {
      "item-1": { step: "EXECUTE", awaiting: null },
      "item-2": { step: null, awaiting: { requestPayload: {} } },
    };
    const result = buildSubflowAggregateSummary(rows, {});
    assert.equal(result.total, 2);
    assert.equal(result.awaiting, 1);
  });

  it("groups subflow rows by agent and sorts by count descending", () => {
    const rows = {
      "item-1": { step: "WRITE", awaiting: null },
      "item-2": { step: "WRITE", awaiting: null },
      "item-3": { step: "REVIEW", awaiting: null },
    };
    const nodeAgentLookup = (pipelineId, nodeId) => {
      if (nodeId === "WRITE") return { agent: "writer", kind: "agent" };
      if (nodeId === "REVIEW") return { agent: "reviewer", kind: "agent" };
      return null;
    };
    const agentLookup = (id) => ({ name: id, emoji: "🤖" });
    const parentNode = {
      flowNodes: {
        WRITE: { kind: "agent", agent: "writer" },
        REVIEW: { kind: "agent", agent: "reviewer" },
      },
    };
    // buildSubflowAggregateSummary uses nodeAgentLookup(pipelineId, parentNodeId) to get flowNodes
    const result = buildSubflowAggregateSummary(rows, {
      nodeAgentLookup: (pId, nId) => nId === "PARENT" ? parentNode : null,
      agentLookup,
      pipelineId: "team_test-pipe",
      parentNodeId: "PARENT",
    });
    assert.equal(result.total, 3);
    assert.equal(result.byAgent[0].agentId, "writer");
    assert.equal(result.byAgent[0].count, 2);
    assert.equal(result.byAgent[1].agentId, "reviewer");
    assert.equal(result.byAgent[1].count, 1);
  });
});

// ── pipelineMetaFor ───────────────────────────────────────────────────────────

describe("pipelineMetaFor", () => {
  it("falls back to prettified pipeline id when lookup returns nothing", () => {
    const meta = pipelineMetaFor("team_my-pipeline", {});
    assert.equal(meta.id, "team_my-pipeline");
    assert.ok(meta.displayName, "should produce a displayName");
  });

  it("uses declared displayName and emoji when lookup succeeds", () => {
    const pipelineLookup = () => ({ displayName: "My Pipeline", emoji: "🚀", sfSymbol: "star.fill" });
    const meta = pipelineMetaFor("team_my-pipeline", { pipelineLookup });
    assert.equal(meta.displayName, "My Pipeline");
    assert.equal(meta.emoji, "🚀");
    assert.equal(meta.sfSymbol, "star.fill");
  });
});
