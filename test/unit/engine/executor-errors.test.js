import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  REVISION_RETRY_LIMIT,
  fetchSessionError,
  NodeBlocked,
  GatewayHandoffNeeded,
  isBreakerTrippingFailure,
  NodeFailure,
  isRequestScopedSubagentError,
} from "../../../engine/executor-errors.js";

// ── constants ─────────────────────────────────────────────────────────────────

describe("REVISION_RETRY_LIMIT", () => {
  it("is 5", () => assert.equal(REVISION_RETRY_LIMIT, 5));
});

// ── fetchSessionError ─────────────────────────────────────────────────────────

// The openclaw tasks DB is dropped legacy (core never reads harness
// homes) — fetchSessionError always resolves null and callers fall back to
// their default detail.
describe("fetchSessionError", () => {
  it("returns null for null session", async () => {
    assert.equal(await fetchSessionError(null), null);
  });
  it("returns null for undefined session", async () => {
    assert.equal(await fetchSessionError(undefined), null);
  });
  it("returns null for any session — the tasks-DB fallback is gone", async () => {
    assert.equal(await fetchSessionError("no-such-session-key-" + Math.random()), null);
  });
});

// ── NodeBlocked ───────────────────────────────────────────────────────────────

describe("NodeBlocked", () => {
  it("is an instance of Error", () => {
    assert.ok(new NodeBlocked({}) instanceof Error);
  });

  it("has name NodeBlocked", () => {
    assert.equal(new NodeBlocked({}).name, "NodeBlocked");
  });

  it("sets _blocked with reason/needed/hint", () => {
    const e = new NodeBlocked({ reason: "need url", needed: ["url"], hint: "run scout" });
    assert.equal(e._blocked.reason, "need url");
    assert.deepEqual(e._blocked.needed, ["url"]);
    assert.equal(e._blocked.hint, "run scout");
  });

  it("uses default message when reason is omitted", () => {
    const e = new NodeBlocked({});
    assert.equal(e.message, "node blocked");
  });

  it("uses reason as message", () => {
    const e = new NodeBlocked({ reason: "custom reason" });
    assert.equal(e.message, "custom reason");
  });
});

// ── GatewayHandoffNeeded ──────────────────────────────────────────────────────

describe("GatewayHandoffNeeded", () => {
  it("is an instance of Error", () => {
    assert.ok(new GatewayHandoffNeeded("ctx missing") instanceof Error);
  });

  it("has name GatewayHandoffNeeded", () => {
    assert.equal(new GatewayHandoffNeeded("x").name, "GatewayHandoffNeeded");
  });

  it("includes the original message in the error message", () => {
    const e = new GatewayHandoffNeeded("api not ready");
    assert.ok(e.message.includes("api not ready"), e.message);
  });
});

// ── NodeFailure ───────────────────────────────────────────────────────────────
// Thrown by a cascade leg instead of soft-returning {ok:false, error_type,
// _error}.

describe("NodeFailure", () => {
  it("is an instance of Error", () => {
    assert.ok(new NodeFailure("boom") instanceof Error);
  });

  it("has name NodeFailure", () => {
    assert.equal(new NodeFailure("boom").name, "NodeFailure");
  });

  it("uses the given message as e.message", () => {
    assert.equal(new NodeFailure("landing page unresolved").message, "landing page unresolved");
  });

  it("sets errorType from the options bag", () => {
    const e = new NodeFailure("infra hiccup", { errorType: "infra_unavailable" });
    assert.equal(e.errorType, "infra_unavailable");
  });

  it("defaults errorType to null when omitted", () => {
    assert.equal(new NodeFailure("boom").errorType, null);
  });

  it("defaults errorType to null when options bag itself is omitted", () => {
    assert.equal(new NodeFailure("boom", undefined).errorType, null);
  });
});

// ── isRequestScopedSubagentError ──────────────────────────────────────────────

describe("isRequestScopedSubagentError", () => {
  it("returns false for null", () => {
    assert.equal(isRequestScopedSubagentError(null), false);
  });

  it("returns true for RequestScopedSubagentRuntimeError by name", () => {
    const err = new Error("whatever");
    err.name = "RequestScopedSubagentRuntimeError";
    assert.equal(isRequestScopedSubagentError(err), true);
  });

  it("returns true for 'only available during a gateway request' message", () => {
    assert.equal(isRequestScopedSubagentError(new Error("only available during a gateway request")), true);
  });

  it("returns true for 'requires a gateway request scope' message", () => {
    assert.equal(isRequestScopedSubagentError(new Error("requires a gateway request scope")), true);
  });

  it("returns false for the gateway-startup refusal — transient, not a scope problem", () => {
    assert.equal(isRequestScopedSubagentError(new Error("agent.wait unavailable during gateway startup")), false);
    assert.equal(isRequestScopedSubagentError(new Error("unavailable during gateway startup")), false);
  });

  it("returns true for 'missing scope: operator.write' — a detached-continuation subagent dispatch", () => {
    assert.equal(isRequestScopedSubagentError(new Error("missing scope: operator.write")), true);
  });

  it("returns false for unrelated 'missing scope:' errors — not a blanket match", () => {
    assert.equal(isRequestScopedSubagentError(new Error("missing scope: operator.pairing")), false);
    assert.equal(isRequestScopedSubagentError(new Error("missing scope: operator.admin")), false);
  });

  it("returns false for unrelated errors", () => {
    assert.equal(isRequestScopedSubagentError(new Error("network timeout")), false);
  });
});

// A block has no TTL — it persists until an operator resets it — so which
// errors are allowed to create one is load-bearing. A standing configuration
// fault parked behind a manual-only remedy is worse than a retry.
describe("isBreakerTrippingFailure — what is allowed to park a node", () => {
  it("trips on a genuine symptom: the agent misbehaving", () => {
    assert.equal(isBreakerTrippingFailure(new Error("subagent timeout: no error message")), true);
    assert.equal(isBreakerTrippingFailure(new Error("API rate limit reached")), true);
    assert.equal(isBreakerTrippingFailure(new Error("boom")), true);
    assert.equal(isBreakerTrippingFailure(new Error()), true, "an unrecognised error trips rather than being swallowed");
    assert.equal(isBreakerTrippingFailure(undefined), true);
  });

  it("does NOT trip on a missing tool scope — an operator grants that, not a retry", () => {
    assert.equal(isBreakerTrippingFailure(new Error("missing scope: operator.write")), false);
    assert.equal(isBreakerTrippingFailure(new Error("Missing Scope: something.else")), false);
  });

  it("does NOT trip on a gateway-context mismatch — structural, not transient", () => {
    assert.equal(isBreakerTrippingFailure(new Error("subagent call requires gateway context: missing api")), false);
    assert.equal(isBreakerTrippingFailure(new Error("node needs-gateway-handoff but no api")), false);
  });

  it("does NOT trip on the two structural error classes", () => {
    assert.equal(isBreakerTrippingFailure(new NodeBlocked({ reason: "budget" })), false);
    assert.equal(isBreakerTrippingFailure(new GatewayHandoffNeeded({})), false);
  });

  it("matches on text only where no shared class exists — never over-matches a symptom", () => {
    // "scope" appears inside a legitimate failure too; the pattern must
    // require the phrase that only the permission error uses.
    assert.equal(isBreakerTrippingFailure(new Error("telescope mount scope drifted")), true);
    assert.equal(isBreakerTrippingFailure(new Error("context window exceeded")), true);
  });
});
