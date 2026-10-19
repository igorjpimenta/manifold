import { describe, it } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { existsSync, unlinkSync, writeFileSync, mkdirSync } from "node:fs";
import {
  _discussSidecarPath,
  stampDiscussSession,
  readDiscussSession,
  DISCUSS_SESSIONS_DIR,
} from "../../../routes/discuss-session.js";

// ── sidecar path ──────────────────────────────────────────────────────────────

describe("_discussSidecarPath", () => {
  it("builds path from flowId8 + gateNodeId + itemId", () => {
    const p = _discussSidecarPath("12345678-aabb-ccdd-eeff-000000000000", "PROPOSE", "item-1");
    assert.ok(p.includes("12345678__PROPOSE__item-1.json"), `unexpected path: ${p}`);
    assert.ok(p.startsWith(DISCUSS_SESSIONS_DIR));
  });

  it("sanitizes special chars in gateNodeId and itemId", () => {
    const p = _discussSidecarPath("12345678-0000-0000-0000-000000000000", "gate/node", "item:1");
    assert.ok(!p.includes("/gate/"), "slashes should be sanitized");
    assert.ok(!p.includes(":"), "colons should be sanitized");
  });
});

// ── stamp + read round-trip ───────────────────────────────────────────────────

describe("stampDiscussSession / readDiscussSession", () => {
  const flowId  = "abcdef01-1234-5678-abcd-ef0123456789";
  const nodeId  = "PROPOSE";
  const itemId  = "item-roundtrip";
  const agent   = "cyrano";
  const session = "gf-discuss-abcdef01-propose-item-roundtrip";

  it("returns null for a sidecar that doesn't exist", () => {
    const result = readDiscussSession("nonexistent-flow-aaa", "NODE", "item");
    assert.equal(result, null);
  });

  it("round-trips agent + sessionId through stamp + read", () => {
    const sidecarPath = _discussSidecarPath(flowId, nodeId, itemId);
    try {
      stampDiscussSession(flowId, nodeId, itemId, agent, session);
      const result = readDiscussSession(flowId, nodeId, itemId);
      assert.equal(result?.agent, agent);
      assert.equal(result?.sessionId, session);
    } finally {
      try { unlinkSync(sidecarPath); } catch {}
    }
  });

  it("returns null when sidecar has wrong shape", () => {
    const sidecarPath = _discussSidecarPath(flowId, nodeId, itemId + "-bad");
    mkdirSync(DISCUSS_SESSIONS_DIR, { recursive: true });
    writeFileSync(sidecarPath, JSON.stringify({ wrong: "shape" }), "utf8");
    try {
      const result = readDiscussSession(flowId, nodeId, itemId + "-bad");
      assert.equal(result, null);
    } finally {
      try { unlinkSync(sidecarPath); } catch {}
    }
  });

  it("is idempotent — overwriting a sidecar returns the new value", () => {
    const sidecarPath = _discussSidecarPath(flowId, nodeId, itemId + "-idem");
    try {
      stampDiscussSession(flowId, nodeId, itemId + "-idem", "agent-v1", "session-v1");
      stampDiscussSession(flowId, nodeId, itemId + "-idem", "agent-v2", "session-v2");
      const result = readDiscussSession(flowId, nodeId, itemId + "-idem");
      assert.equal(result?.agent, "agent-v2");
    } finally {
      try { unlinkSync(sidecarPath); } catch {}
    }
  });
});
