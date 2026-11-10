import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  newDiscussSessionId,
} from "../../../../../plugins/openclaw/manifold/sessions.js";

let prevHome;
let home;

beforeEach(() => {
  prevHome = process.env.HOME;
  home = mkdtempSync(path.join(tmpdir(), "manifold-sessions-test-"));
  process.env.HOME = home;
});

afterEach(() => {
  process.env.HOME = prevHome;
  rmSync(home, { recursive: true, force: true });
});

function sessionsDir(agentId) {
  const dir = path.join(home, ".openclaw", "agents", agentId, "sessions");
  mkdirSync(dir, { recursive: true });
  return dir;
}

describe("newDiscussSessionId", () => {
  it("mints in the harness namespace, deterministic in (flow, gate, item)", () => {
    const a = newDiscussSessionId({
      agentId: "scout", flowId: "abcdef01-0000-0000-0000-000000000000",
      gateNodeId: "PROPOSE", itemId: "round-1",
    });
    assert.equal(a, "agent:scout:discuss:abcdef01-PROPOSE-round-1");
    const b = newDiscussSessionId({
      agentId: "scout", flowId: "abcdef01-0000-0000-0000-000000000000",
      gateNodeId: "PROPOSE", itemId: "round-1",
    });
    assert.equal(b, a, "same thread coordinates must converge on one id");
  });

  it("sanitizes segments so the id stays a safe <sessionId>.jsonl filename", () => {
    const id = newDiscussSessionId({
      agentId: "scout", flowId: "abcdef01-0000-0000-0000-000000000000",
      gateNodeId: "a/b", itemId: "x:y z",
    });
    assert.ok(!id.includes("/"), "no path separators");
    assert.ok(!id.includes(" "), "no spaces");
    assert.ok(id.startsWith("agent:scout:discuss:"), id);
  });
});
