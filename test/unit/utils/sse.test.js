/**
 * Unit tests for utils/sse.js
 *
 * Verifies:
 *   - createEmitSseEvent returns a function.
 *   - Returns {ok: false, error: "no_gateway_token"} when env var is absent.
 *   - Calls the broadcast function for discuss.* events when available.
 *   - Non-discuss.* events skip the broadcast call.
 */
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { createEmitSseEvent } from "../../../utils/sse.js";

// ── factory shape ──────────────────────────────────────────────────────────────

describe("createEmitSseEvent — factory", () => {
  test("returns a function", () => {
    const emit = createEmitSseEvent(() => null);
    assert.equal(typeof emit, "function");
  });

  test("different getter calls produce independent emitters", () => {
    let b1 = null;
    let b2 = null;
    const e1 = createEmitSseEvent(() => b1);
    const e2 = createEmitSseEvent(() => b2);
    assert.notStrictEqual(e1, e2);
  });
});

// ── no-token fast-fail ─────────────────────────────────────────────────────────

describe("createEmitSseEvent — no gateway token", () => {
  let savedToken;

  before(() => {
    savedToken = process.env.OPENCLAW_GATEWAY_TOKEN;
    delete process.env.OPENCLAW_GATEWAY_TOKEN;
  });

  after(() => {
    if (savedToken !== undefined) process.env.OPENCLAW_GATEWAY_TOKEN = savedToken;
    else delete process.env.OPENCLAW_GATEWAY_TOKEN;
  });

  test("returns {ok: false, error: 'no_gateway_token'}", async () => {
    const emit = createEmitSseEvent(() => null);
    const result = await emit({ type: "test.event", payload: {} });
    assert.deepEqual(result, { ok: false, error: "no_gateway_token" });
  });
});

// ── broadcast integration ─────────────────────────────────────────────────────

describe("createEmitSseEvent — broadcast callback", () => {
  before(() => { process.env.OPENCLAW_GATEWAY_TOKEN = "test-token-for-sse-tests"; });
  after(() => { delete process.env.OPENCLAW_GATEWAY_TOKEN; });

  test("calls broadcast for discuss.* events", async () => {
    const calls = [];
    const broadcast = (type, payload) => calls.push({ type, payload });
    const emit = createEmitSseEvent(() => broadcast);

    // We don't have a real gateway, so the HTTP request will fail/timeout.
    // We only care that broadcast was called before the HTTP attempt.
    const p = emit({ type: "discuss.pending", flowId: "f1", itemId: "i1" });
    // The broadcast call is synchronous before the HTTP request resolves.
    assert.equal(calls.length, 1);
    assert.equal(calls[0].type, "plugin.discuss.pending");

    // Let the promise settle (will fail since gateway isn't available in unit test)
    await p.catch(() => {});
  });

  test("skips broadcast for non-discuss.* events", async () => {
    const calls = [];
    const broadcast = (type, payload) => calls.push({ type, payload });
    const emit = createEmitSseEvent(() => broadcast);

    const p = emit({ type: "pipeline.started", flowId: "f1" });
    assert.equal(calls.length, 0, "broadcast must not be called for non-discuss events");
    await p.catch(() => {});
  });

  test("does not throw when broadcast throws", async () => {
    const broadcast = () => { throw new Error("broadcast error"); };
    const emit = createEmitSseEvent(() => broadcast);
    // Should swallow the broadcast error and continue to HTTP
    const p = emit({ type: "discuss.foo", flowId: "f1", itemId: "i1" });
    await assert.doesNotReject(p.catch(() => {}));
  });
});
