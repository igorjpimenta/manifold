import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  newDiscussSessionId,
  newNodeSessionId,
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

describe("newNodeSessionId", () => {
  it("mints in the harness namespace, deterministic in (agent, flow, node)", () => {
    const args = { agentId: "scorer", flowId: "abcdef01-0000-0000-0000-000000000000", nodeId: "SCORE_LLM" };
    assert.equal(newNodeSessionId(args), "agent:scorer:manifold:abcdef01-SCORE_LLM");
    assert.equal(newNodeSessionId({ ...args }), newNodeSessionId(args), "a retry must reuse the same session");
  });

  it("distinguishes nodes inside one flow and flows sharing a node", () => {
    const base = { agentId: "scorer", flowId: "abcdef01-0000-0000-0000-000000000000" };
    assert.notEqual(newNodeSessionId({ ...base, nodeId: "A" }), newNodeSessionId({ ...base, nodeId: "B" }));
    assert.notEqual(
      newNodeSessionId({ ...base, nodeId: "A" }),
      newNodeSessionId({ ...base, flowId: "abcdef02-0000-0000-0000-000000000000", nodeId: "A" }),
    );
  });

  it("sanitizes segments so the id stays a safe <sessionId>.jsonl filename", () => {
    const id = newNodeSessionId({ agentId: "a/b", flowId: "abcdef01-x", nodeId: "n:ode z" });
    assert.ok(!id.includes("/") && !id.includes(" "), id);
    assert.ok(id.startsWith("agent:a_b:manifold:"), id);
  });
});
