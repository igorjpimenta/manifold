import { describe, it } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { mkdirSync, writeFileSync, mkdtempSync } from "node:fs";
import {
  prettifyPipelineId,
  parallelGroupContaining,
  readStampedDiscussSession,
  discussSessionExists,
  discussSessionMtime,
  applyPendingExists,
} from "../../../ios/poller-helpers.js";

// ── prettifyPipelineId ────────────────────────────────────────────────────────

describe("prettifyPipelineId", () => {
  it("returns Pipeline for empty string", () => {
    assert.equal(prettifyPipelineId(""), "Pipeline");
  });

  it("returns Pipeline for non-string", () => {
    assert.equal(prettifyPipelineId(null), "Pipeline");
    assert.equal(prettifyPipelineId(undefined), "Pipeline");
  });

  it("prettifies team_name pattern", () => {
    assert.equal(prettifyPipelineId("workana_propose-draft"), "workana: propose draft");
  });

  it("leaves plain strings unchanged except hyphens", () => {
    assert.equal(prettifyPipelineId("no-prefix"), "no prefix");
  });

  it("handles single-word id", () => {
    assert.equal(prettifyPipelineId("simple"), "simple");
  });
});

// ── parallelGroupContaining ───────────────────────────────────────────────────

describe("parallelGroupContaining", () => {
  it("returns null for no groups", () => {
    assert.equal(parallelGroupContaining({}, "item-1"), null);
  });

  it("returns null when itemId is absent", () => {
    const groups = { PROPOSE: { "item-1": { groupId: "PROPOSE" } } };
    assert.equal(parallelGroupContaining(groups, null), null);
  });

  it("finds the group containing the itemId", () => {
    const groups = {
      PROPOSE: {
        "item-1": { groupId: "PROPOSE" },
        "item-2": { groupId: "PROPOSE" },
      },
      DRAFT: {
        "item-3": { groupId: "DRAFT" },
      },
    };
    assert.equal(parallelGroupContaining(groups, "item-2"), "PROPOSE");
    assert.equal(parallelGroupContaining(groups, "item-3"), "DRAFT");
  });

  it("returns null when itemId not in any group", () => {
    const groups = { PROPOSE: { "item-1": { groupId: "PROPOSE" } } };
    assert.equal(parallelGroupContaining(groups, "item-99"), null);
  });
});

// ── readStampedDiscussSession ─────────────────────────────────────────────────

describe("readStampedDiscussSession", () => {
  it("returns null for missing args", () => {
    assert.equal(readStampedDiscussSession(null, "node", "item"), null);
    assert.equal(readStampedDiscussSession("flow", null, "item"), null);
    assert.equal(readStampedDiscussSession("flow", "node", null), null);
  });

  it("returns null when sidecar file does not exist", () => {
    assert.equal(readStampedDiscussSession("nonexistent-flow-id", "nodeA", "item1"), null);
  });

  it("reads and returns valid sidecar", () => {
    const dir = path.join(os.homedir(), ".openclaw", "discuss-sessions");
    mkdirSync(dir, { recursive: true });
    const flowId = "12345678-aaaa-bbbb-cccc-dddddddddddd";
    const sidecarPath = path.join(dir, `${flowId.slice(0, 8)}__nodeA__item1.json`);
    writeFileSync(sidecarPath, JSON.stringify({ agent: "cyrano", sessionId: "gf-discuss-test" }));
    try {
      const result = readStampedDiscussSession(flowId, "nodeA", "item1");
      assert.equal(result?.agent, "cyrano");
      assert.equal(result?.sessionId, "gf-discuss-test");
    } finally {
      try { require("node:fs").unlinkSync(sidecarPath); } catch {}
    }
  });
});

// ── discussSessionExists / discussSessionMtime ────────────────────────────────

describe("discussSessionExists + discussSessionMtime", () => {
  it("returns false / null when agent is null", () => {
    const flow = { flowId: "12345678-0000-0000-0000-000000000000" };
    assert.equal(discussSessionExists(flow, "item1", null, "nodeA"), false);
    assert.equal(discussSessionMtime(flow, "item1", null, "nodeA"), null);
  });

  it("returns false / null for missing gateNodeId", () => {
    const flow = { flowId: "12345678-0000-0000-0000-000000000001" };
    assert.equal(discussSessionExists(flow, "item1", "cyrano", null), false);
    assert.equal(discussSessionMtime(flow, "item1", "cyrano", null), null);
  });

  it("returns false when session file does not exist", () => {
    const flow = { flowId: "12345678-0000-0000-0000-000000000002" };
    assert.equal(discussSessionExists(flow, "item99", "no-such-agent", "nodeA"), false);
  });

  it("returns true + non-null mtime when session file exists", () => {
    const flowId = "12345678-0000-0000-0000-000000000003";
    const agent  = "cyrano";
    const nodeId = "PROPOSE";
    const itemId = "item-x";
    const sessionId = `gf-discuss-${flowId.slice(0, 8)}-${nodeId}-${itemId}`;
    const sessDir = path.join(os.homedir(), ".openclaw", "agents", agent, "sessions");
    mkdirSync(sessDir, { recursive: true });
    const sessFile = path.join(sessDir, `${sessionId}.jsonl`);
    writeFileSync(sessFile, '{"role":"user","content":"hi"}\n');
    try {
      const flow = { flowId };
      assert.equal(discussSessionExists(flow, itemId, agent, nodeId), true);
      const mtime = discussSessionMtime(flow, itemId, agent, nodeId);
      assert.ok(typeof mtime === "number" && mtime > 0);
    } finally {
      try { require("node:fs").unlinkSync(sessFile); } catch {}
    }
  });
});

// ── applyPendingExists ────────────────────────────────────────────────────────

describe("applyPendingExists", () => {
  it("returns false for null args", () => {
    assert.equal(applyPendingExists(null, "item1"), false);
    assert.equal(applyPendingExists("flow1", null), false);
  });

  it("returns false when sidecar is absent", () => {
    assert.equal(applyPendingExists("nonexistent-flow-id", "nonexistent-item"), false);
  });

  it("returns true when sidecar exists", () => {
    const flowId = "pending-flow-id";
    const itemId = "pending-item";
    const dir = path.join(os.homedir(), ".openclaw", "discuss-pending");
    mkdirSync(dir, { recursive: true });
    const safe = (s) => s.replace(/[^A-Za-z0-9._-]+/g, "_");
    const sidecar = path.join(dir, `${safe(flowId)}__${safe(itemId)}.json`);
    writeFileSync(sidecar, '{"draft":"hello"}');
    try {
      assert.equal(applyPendingExists(flowId, itemId), true);
    } finally {
      try { require("node:fs").unlinkSync(sidecar); } catch {}
    }
  });
});
