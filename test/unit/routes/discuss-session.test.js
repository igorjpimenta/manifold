import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { tmpdir } from "node:os";
import { unlinkSync, writeFileSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import {
  _discussSidecarPath,
  stampDiscussSession,
  readDiscussSession,
  discussSessionsDir,
} from "../../../routes/discuss-session.js";

// Every test runs against an isolated store home so the real ~/.manifold
// is never touched. Resolution is per call, so setting the env here (after
// import) still relocates the store.
let prevDataDir;
let dataDir;
beforeEach(() => {
  prevDataDir = process.env.MANIFOLD_DATA_DIR;
  dataDir = mkdtempSync(path.join(tmpdir(), "manifold-discuss-session-"));
  process.env.MANIFOLD_DATA_DIR = dataDir;
});
afterEach(() => {
  if (prevDataDir === undefined) delete process.env.MANIFOLD_DATA_DIR;
  else process.env.MANIFOLD_DATA_DIR = prevDataDir;
  rmSync(dataDir, { recursive: true, force: true });
});

// ── sidecar path ──────────────────────────────────────────────────────────────

describe("_discussSidecarPath", () => {
  it("builds path from flowId8 + gateNodeId + itemId under the store home", () => {
    const p = _discussSidecarPath("12345678-aabb-ccdd-eeff-000000000000", "PROPOSE", "item-1");
    assert.ok(p.includes("12345678__PROPOSE__item-1.json"), `unexpected path: ${p}`);
    assert.ok(p.startsWith(discussSessionsDir()));
    assert.equal(discussSessionsDir(), path.join(dataDir, "discuss-sessions"));
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

  it("writes under the store home, never the harness home", () => {
    stampDiscussSession(flowId, nodeId, itemId, agent, session);
    try {
      assert.ok(_discussSidecarPath(flowId, nodeId, itemId).startsWith(dataDir));
      assert.ok(!_discussSidecarPath(flowId, nodeId, itemId).includes(".openclaw"));
    } finally {
      try { unlinkSync(_discussSidecarPath(flowId, nodeId, itemId)); } catch {}
    }
  });

  it("returns null when sidecar has wrong shape", () => {
    const sidecarPath = _discussSidecarPath(flowId, nodeId, itemId + "-bad");
    mkdirSync(discussSessionsDir(), { recursive: true });
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

  it("falls back to the harness-home sidecar when the store-home file is absent", () => {
    const prevHome = process.env.HOME;
    const tmpHome = mkdtempSync(path.join(tmpdir(), "manifold-discuss-session-home-"));
    try {
      process.env.HOME = tmpHome;
      const fileName = path.basename(_discussSidecarPath(flowId, nodeId, itemId + "-fb"));
      const fallbackDir = path.join(tmpHome, ".openclaw", "discuss-sessions");
      mkdirSync(fallbackDir, { recursive: true });
      writeFileSync(path.join(fallbackDir, fileName), JSON.stringify({ agent, sessionId: session }), "utf8");
      const result = readDiscussSession(flowId, nodeId, itemId + "-fb");
      assert.equal(result?.agent, agent);
      assert.equal(result?.sessionId, session);
    } finally {
      process.env.HOME = prevHome;
      rmSync(tmpHome, { recursive: true, force: true });
    }
  });

  it("prefers the store-home sidecar when both homes have one", () => {
    const prevHome = process.env.HOME;
    const tmpHome = mkdtempSync(path.join(tmpdir(), "manifold-discuss-session-home-"));
    try {
      process.env.HOME = tmpHome;
      stampDiscussSession(flowId, nodeId, itemId + "-both", "agent-new", "session-new");
      const fileName = path.basename(_discussSidecarPath(flowId, nodeId, itemId + "-both"));
      const fallbackDir = path.join(tmpHome, ".openclaw", "discuss-sessions");
      mkdirSync(fallbackDir, { recursive: true });
      writeFileSync(path.join(fallbackDir, fileName), JSON.stringify({ agent: "agent-old", sessionId: "session-old" }), "utf8");
      try {
        const result = readDiscussSession(flowId, nodeId, itemId + "-both");
        assert.equal(result?.agent, "agent-new");
      } finally {
        try { unlinkSync(_discussSidecarPath(flowId, nodeId, itemId + "-both")); } catch {}
      }
    } finally {
      process.env.HOME = prevHome;
      rmSync(tmpHome, { recursive: true, force: true });
    }
  });
});
