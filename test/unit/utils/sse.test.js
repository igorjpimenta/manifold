/**
 * Unit tests for utils/sse.js
 *
 * Verifies (iOS push transport; WS delivery lives in events/emit.js):
 *   - createEmitSseEvent returns a function.
 *   - Returns {ok: false, error: "no_gateway_token"} when env var is absent.
 *   - Pushes the envelope untouched (canonical kind flows through opaquely).
 */
import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { createEmitSseEvent } from "../../../utils/sse.js";

// ── factory shape ──────────────────────────────────────────────────────────────

describe("createEmitSseEvent — factory", () => {
  test("returns a function", () => {
    const emit = createEmitSseEvent();
    assert.equal(typeof emit, "function");
  });

  test("takes no gateway wiring — push only", () => {
    assert.equal(createEmitSseEvent.length, 0);
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
    const emit = createEmitSseEvent();
    const result = await emit({ kind: "test.event", payload: {} });
    assert.deepEqual(result, { ok: false, error: "no_gateway_token" });
  });
});

// ── push passthrough ───────────────────────────────────────────────────────────

describe("createEmitSseEvent — push", () => {
  let server;
  let received;
  let port;

  before(async () => {
    received = [];
    server = http.createServer((req, res) => {
      let raw = "";
      req.on("data", (c) => { raw += c; });
      req.on("end", () => {
        received.push({ path: req.url, auth: req.headers.authorization, body: JSON.parse(raw) });
        res.statusCode = 200;
        res.end("ok");
      });
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    port = server.address().port;
    process.env.OPENCLAW_GATEWAY_TOKEN = "test-token-for-sse-tests";
    process.env.OPENCLAW_GATEWAY_PORT = String(port);
  });

  after(async () => {
    delete process.env.OPENCLAW_GATEWAY_TOKEN;
    delete process.env.OPENCLAW_GATEWAY_PORT;
    await new Promise((resolve) => server.close(resolve));
  });

  test("POSTs the envelope untouched to /pipeclaw/ios/push", async () => {
    const emit = createEmitSseEvent();
    const event = { kind: "session.transcript.completed", scope: "discuss", text: "hi" };
    const result = await emit(event);

    assert.equal(result.ok, true);
    assert.equal(received.length, 1);
    assert.equal(received[0].path, "/pipeclaw/ios/push");
    assert.equal(received[0].auth, "Bearer test-token-for-sse-tests");
    assert.deepEqual(received[0].body, event);
  });
});
