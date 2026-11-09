// openclaw-sessions.test.js — harness-owned discuss session ids: the
// harness mints in its own `agent:<agentId>:…` namespace, core passes the
// value through verbatim and never invents one.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { newDiscussSessionId } from "../../../../../plugins/openclaw/manifold/sessions.js";

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
