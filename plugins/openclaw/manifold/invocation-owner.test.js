// Unit tests for the harness invocation owner (plugins/openclaw/manifold/invocation-owner.js) — createInvocationOwnerResolver: isResolved, describe.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createInvocationOwnerResolver } from "./invocation-owner.js";

// ─── helpers ─────────────────────────────────────────────────────────────────

function resolverWith({ invocation = null, run = null } = {}) {
  return createInvocationOwnerResolver({
    nodeExecutionsStore: { getInvocation: async () => invocation },
    runStore: { get: async () => run },
  });
}

function holder(id = "inv-1") {
  return { pid: 1, since: 0, token: "t", owner: { kind: "invocation", id, flowId: "flow-abcdef12", nodeId: "SUBMIT" } };
}

// ─── isResolved ──────────────────────────────────────────────────────────────

describe("isResolved — unresolved while the invocation's work is open", () => {
  test("no result written and the flow is still open", async () => {
    const r = resolverWith({ invocation: { flowId: "f", resultWrittenAt: null }, run: { status: "running", endedAt: null } });
    assert.equal(await r.isResolved(holder()), false);
  });

  test("a missing or unreadable invocation counts as unresolved", async () => {
    assert.equal(await resolverWith({ invocation: null }).isResolved(holder()), false);
    assert.equal(await resolverWith({ invocation: { flowId: "f", resultWrittenAt: null }, run: null }).isResolved(holder()), false);
    assert.equal(await resolverWith().isResolved({ owner: { kind: "invocation" } }), false);
  });
});

describe("isResolved — resolved once the invocation is done", () => {
  test("the invocation wrote its result", async () => {
    const r = resolverWith({ invocation: { flowId: "f", resultWrittenAt: 123 }, run: { status: "running", endedAt: null } });
    assert.equal(await r.isResolved(holder()), true);
  });

  test("the flow ended without a result (failed / cancelled / skipped)", async () => {
    const r = resolverWith({ invocation: { flowId: "f", resultWrittenAt: null }, run: { status: "failed", endedAt: 456 } });
    assert.equal(await r.isResolved(holder()), true);
  });
});

// ─── describe ────────────────────────────────────────────────────────────────

describe("describe — names the holder for busy messages and timeout errors", () => {
  test("node, flow and invocation", () => {
    assert.equal(resolverWith().describe(holder("3f9e1234-aaaa").owner), "SUBMIT (flow flow-abc, invocation 3f9e1234)");
  });
});
