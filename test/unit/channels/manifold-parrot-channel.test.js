// Unit tests for channels/manifold-parrot-channel.js

import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createMockApi, createMockRes } from "../../helpers/mock-api.js";

// ─── module-level mocks ──────────────────────────────────────────────────────
// All external SDK deps are mocked before the module under test is imported.
// If the openclaw package is unavailable (outside the gateway environment),
// all tests are skipped with a clear reason — matching the pattern used by
// operator.test.js and runs.test.js.

let createManifoldParrotChannelEntry = null;
let importError = null;

// Capture the most recently registered sendText handler so tests can call it.
let capturedSendText = null;
// Track dispatchInboundDirectDmWithRuntime calls.
let dispatchCalls = [];
let dispatchResult = { route: { sessionKey: "session-key-1" } };
let dispatchShouldThrow = null;
let capturedDispatchArgs = null;

// Auth mock implementation — mutated by individual tests when needed.
const authMockImpl = {
  authorizeBridgeRequest: async () => ({ ok: true }),
  sendUnauthorized: (res, reason) => { res.statusCode = 401; res.end(reason ?? "unauthorized"); },
};

try {
  await mock.module("openclaw/plugin-sdk/channel-core", {
    exports: {
      createChatChannelPlugin: (opts) => {
        capturedSendText = opts?.outbound?.sendText ?? null;
        return { _channelCore: true, opts };
      },
      defineChannelPluginEntry: (entryOpts) => entryOpts,
    },
  });

  await mock.module("openclaw/plugin-sdk/channel-inbound", {
    exports: {
      dispatchInboundDirectDmWithRuntime: async (args) => {
        dispatchCalls.push(args);
        capturedDispatchArgs = args;
        if (dispatchShouldThrow) throw dispatchShouldThrow;
        return dispatchResult;
      },
    },
  });

  // live-activity-bridge/auth.js is dynamically imported inside the handler.
  // Resolve its URL relative to the test file — matching operator.test.js / runs.test.js.
  const authUrl = new URL("../../../plugins/openclaw/manifold/auth/bridge.js", import.meta.url).href;
  await mock.module(authUrl, {
    exports: {
      authorizeBridgeRequest: async (...a) => authMockImpl.authorizeBridgeRequest(...a),
      sendUnauthorized: (...a) => authMockImpl.sendUnauthorized(...a),
    },
  });

  // node:fs/promises — mock mkdir/writeFile/unlink so tests don't touch the FS.
  await mock.module("node:fs/promises", {
    exports: {
      mkdir:     async () => {},
      writeFile: async () => {},
      unlink:    async () => {},
    },
  });

  ({ createManifoldParrotChannelEntry } = await import("../../../plugins/openclaw/manifold/channels/manifold-parrot-channel.js"));
} catch (err) {
  importError = err?.message ?? String(err);
}

const SKIP = !createManifoldParrotChannelEntry;
const SKIP_MSG = importError
  ? `manifold-parrot-channel.js could not be imported: ${importError}`
  : undefined;

// ─── helpers ─────────────────────────────────────────────────────────────────

function makeFlow(overrides = {}) {
  const flowId = randomUUID();
  return {
    flowId,
    stateJson: { pipelineId: "test_pipe" },
    waitJson: { nodeId: "GATE" },
    ...overrides,
  };
}

function makeTarget(overrides = {}) {
  return {
    ok: true,
    itemId: "item-1",
    agent: "test-agent",
    needsStamp: false,
    promptBuilder: null,
    ...overrides,
  };
}

function makeDiscussStore(overrides = {}) {
  return {
    appendMessage: () => {},
    setTurnStatus: () => {},
    clearPending:  () => {},
    stampMessageError: () => {},
    getComponents: () => [],
    ...overrides,
  };
}

function makeReadStore(flow = null) {
  return { get: (id) => (id === flow?.flowId ? flow : null) };
}

function makeEmit(result = { ok: true }) {
  const calls = [];
  const fn = async (kind, payload) => { calls.push({ kind, payload }); return { ...result, kind }; };
  fn.calls = calls;
  return fn;
}

function makeStampDiscussSession() {
  const calls = [];
  const fn = (...args) => { calls.push(args); };
  fn.calls = calls;
  return fn;
}

/**
 * Build a full entry with default stubs and return the registered HTTP handler.
 */
function buildEntry(overrides = {}) {
  const flow = overrides.flow ?? makeFlow();
  const target = overrides.target ?? makeTarget();
  const discussStore = overrides.discussStore ?? makeDiscussStore();
  const emit = overrides.emit ?? makeEmit();
  const stampDiscussSession = overrides.stampDiscussSession ?? makeStampDiscussSession();
  const readStore = overrides.readStore ?? makeReadStore(flow);
  const resolveDiscussTarget = overrides.resolveDiscussTarget ?? (() => target);
  const readPending = overrides.readPending ?? (() => null);

  const entry = createManifoldParrotChannelEntry({
    readStore,
    discussStore,
    resolveDiscussTarget,
    stampDiscussSession,
    emit,
    readPending,
  });

  const { api, routes } = createMockApi();
  api.runtime = { lineage: { recordSpawn: () => {} } };
  entry.registerFull(api);

  const route = routes.find((r) => r.path === "/channels/manifold-parrot/message");

  return {
    entry, route, api, routes,
    flow, target, discussStore, emit, stampDiscussSession, readStore,
  };
}

/**
 * Build an entry whose channel runtime is initialised (startAccount called).
 * Returns the abort controller so tests can clean up the hanging promise.
 */
function buildReadyEntry(overrides = {}) {
  const built = buildEntry(overrides);
  const { entry } = built;

  const abortCtrl = new AbortController();
  const startPromise = entry.plugin.gateway.startAccount({
    channelRuntime: { routing: {} },
    cfg: { accountId: "default" },
    abortSignal: abortCtrl.signal,
    log: { info: () => {} },
  });
  // Do not await — it only resolves on abort. Suppress the unhandled rejection.
  startPromise.catch(() => {});

  return { ...built, abortCtrl, startPromise };
}

/** POST request with an async-iterable body. */
function makePostReq(body, extraHeaders = {}) {
  const raw = typeof body === "string" ? body : JSON.stringify(body);
  return {
    method: "POST",
    url: "/channels/manifold-parrot/message",
    headers: { host: "localhost", ...extraHeaders },
    [Symbol.asyncIterator]: async function* () { yield raw; },
  };
}

// ─── defineChannelPluginEntry — shape ────────────────────────────────────────

describe("createManifoldParrotChannelEntry — entry shape", () => {
  test("returns an object with id 'manifold-parrot'", { skip: SKIP ? SKIP_MSG : false }, () => {
    const entry = createManifoldParrotChannelEntry({
      readStore: makeReadStore(),
      discussStore: makeDiscussStore(),
      resolveDiscussTarget: () => makeTarget(),
      stampDiscussSession: () => {},
      readPending: () => null,
    });
    assert.equal(entry.id, "manifold-parrot");
  });

  test("exposes plugin.gateway.startAccount as a function", { skip: SKIP ? SKIP_MSG : false }, () => {
    const entry = createManifoldParrotChannelEntry({
      readStore: makeReadStore(),
      discussStore: makeDiscussStore(),
      resolveDiscussTarget: () => makeTarget(),
      stampDiscussSession: () => {},
      readPending: () => null,
    });
    assert.equal(typeof entry.plugin?.gateway?.startAccount, "function");
  });

  test("registerFull registers message route with auth 'plugin' and match 'exact'", { skip: SKIP ? SKIP_MSG : false }, () => {
    const { routes } = buildEntry();
    const r = routes.find((r) => r.path === "/channels/manifold-parrot/message");
    assert.ok(r, "message route registered");
    assert.equal(r.auth, "plugin");
    assert.equal(r.match, "exact");
    assert.equal(typeof r.handler, "function");
  });
});

// ─── sendText outbound loopback ───────────────────────────────────────────────

describe("outbound.sendText — loopback no-op", () => {
  test("returns { ok: true, messageId: <uuid-string> }", { skip: SKIP ? SKIP_MSG : false }, async () => {
    assert.ok(capturedSendText, "sendText was captured during mock");
    const result = await capturedSendText({});
    assert.equal(result.ok, true);
    assert.equal(typeof result.messageId, "string");
    assert.match(result.messageId, /^[0-9a-f-]{36}$/);
  });

  test("each call returns a distinct messageId", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const r1 = await capturedSendText({});
    const r2 = await capturedSendText({});
    assert.notEqual(r1.messageId, r2.messageId);
  });
});

// ─── startAccount / gateway lifecycle ────────────────────────────────────────

describe("gateway.startAccount — lifecycle", () => {
  test("rejects with 'aborted' when abortSignal fires", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const entry = createManifoldParrotChannelEntry({
      readStore: makeReadStore(),
      discussStore: makeDiscussStore(),
      resolveDiscussTarget: () => makeTarget(),
      stampDiscussSession: () => {},
      readPending: () => null,
    });
    const abortCtrl = new AbortController();
    const p = entry.plugin.gateway.startAccount({
      channelRuntime: {},
      cfg: {},
      abortSignal: abortCtrl.signal,
      log: { info: () => {} },
    });
    abortCtrl.abort();
    await assert.rejects(p, /aborted/);
  });

  test("rejects immediately when signal is already aborted", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const entry = createManifoldParrotChannelEntry({
      readStore: makeReadStore(),
      discussStore: makeDiscussStore(),
      resolveDiscussTarget: () => makeTarget(),
      stampDiscussSession: () => {},
      readPending: () => null,
    });
    const abortCtrl = new AbortController();
    abortCtrl.abort();
    await assert.rejects(
      entry.plugin.gateway.startAccount({
        channelRuntime: {},
        cfg: {},
        abortSignal: abortCtrl.signal,
        log: { info: () => {} },
      }),
      /aborted/
    );
  });
});

// ─── HTTP handler — method guard ─────────────────────────────────────────────

describe("HTTP handler — method guard", () => {
  test("rejects non-POST with 405 and Allow: POST header", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry();
    for (const method of ["GET", "PUT", "DELETE", "PATCH"]) {
      const req = {
        method,
        url: "/channels/manifold-parrot/message",
        headers: { host: "localhost" },
        [Symbol.asyncIterator]: async function* () {},
      };
      const res = createMockRes();
      const result = await route.handler(req, res);
      assert.equal(result, true, `${method}: handler must return true`);
      assert.equal(res.statusCode, 405, `${method}: status must be 405`);
      assert.equal(res.headers["Allow"], "POST", `${method}: Allow header must be POST`);
    }
  });
});

// ─── HTTP handler — auth ─────────────────────────────────────────────────────

describe("HTTP handler — auth", () => {
  test("returns 401 when authorizeBridgeRequest fails", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const originalAuth = authMockImpl.authorizeBridgeRequest;
    authMockImpl.authorizeBridgeRequest = async () => ({ ok: false, reason: "bad token" });
    try {
      const { route } = buildReadyEntry();
      const req = makePostReq({ flowId: randomUUID(), text: "hello" });
      const res = createMockRes();
      await route.handler(req, res);
      assert.equal(res.statusCode, 401);
    } finally {
      authMockImpl.authorizeBridgeRequest = originalAuth;
    }
  });
});

// ─── HTTP handler — body validation ─────────────────────────────────────────

describe("HTTP handler — body validation", () => {
  test("returns 400 for invalid JSON body", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry();
    const req = {
      method: "POST",
      url: "/channels/manifold-parrot/message",
      headers: { host: "localhost" },
      [Symbol.asyncIterator]: async function* () { yield "not-json{{{"; },
    };
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 400);
  });

  test("returns 400 with invalid_body when flowId is missing", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry();
    const req = makePostReq({ text: "hello" });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 400);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.ok, false);
    assert.equal(parsed.code, "invalid_body");
  });

  test("returns 400 with invalid_body when text is missing", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry();
    const req = makePostReq({ flowId: randomUUID() });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 400);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.code, "invalid_body");
  });

  test("returns 400 when flowId trims to empty string", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry();
    const req = makePostReq({ flowId: "   ", text: "hello" });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 400);
  });

  test("returns 400 for empty body (no bytes emitted)", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry();
    const req = {
      method: "POST",
      url: "/channels/manifold-parrot/message",
      headers: { host: "localhost" },
      [Symbol.asyncIterator]: async function* () {},
    };
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 400);
  });
});

// ─── HTTP handler — 503 when channel not ready ───────────────────────────────

describe("HTTP handler — channel not ready (503)", () => {
  test("returns 503 with channel_not_ready before startAccount runs", { skip: SKIP ? SKIP_MSG : false }, async () => {
    // buildEntry() does NOT call startAccount, so channelRuntimeRef/cfgRef are null.
    const { route } = buildEntry();
    const req = makePostReq({ flowId: randomUUID(), text: "hello" });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 503);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.ok, false);
    assert.equal(parsed.code, "channel_not_ready");
  });
});

// ─── HTTP handler — flow not found ───────────────────────────────────────────

describe("HTTP handler — flow not found (404)", () => {
  test("returns 404 with flow_not_found when readStore.get returns null", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { route } = buildReadyEntry({ readStore: { get: () => null } });
    const req = makePostReq({ flowId: randomUUID(), text: "hello" });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 404);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.ok, false);
    assert.equal(parsed.code, "flow_not_found");
  });
});

// ─── HTTP handler — resolveDiscussTarget failure ─────────────────────────────

describe("HTTP handler — resolveDiscussTarget failure", () => {
  test("returns target status/code/error when resolveDiscussTarget returns ok:false", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const { route } = buildReadyEntry({
      flow,
      resolveDiscussTarget: () => ({
        ok: false,
        status: 422,
        code: "no_gate_context",
        error: "gate context missing",
      }),
    });
    const req = makePostReq({ flowId: flow.flowId, text: "help" });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 422);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.ok, false);
    assert.equal(parsed.code, "no_gate_context");
    assert.equal(parsed.error, "gate context missing");
  });
});

// ─── HTTP handler — 202 response ─────────────────────────────────────────────

describe("HTTP handler — 202 response with turnId", () => {
  test("responds 202 with { ok: true, turnId } immediately", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const { route } = buildReadyEntry({ flow });
    const req = makePostReq({ flowId: flow.flowId, text: "what should I do?" });
    const res = createMockRes();
    await route.handler(req, res);
    assert.equal(res.statusCode, 202);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.ok, true);
    assert.match(parsed.turnId, /^[0-9a-f-]{36}$/);
  });

  test("turnId in response is a UUID v4", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const { route } = buildReadyEntry({ flow });
    const req = makePostReq({ flowId: flow.flowId, text: "hello" });
    const res = createMockRes();
    await route.handler(req, res);
    const { turnId } = JSON.parse(res.body);
    assert.match(turnId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});

// ─── HTTP handler — discussStore operator message side effects ────────────────

describe("HTTP handler — discussStore operator message", () => {
  test("appends operator message with role 'operator' and correct text", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const appendCalls = [];
    const discussStore = makeDiscussStore({
      appendMessage: (...args) => appendCalls.push(args),
    });
    const { route } = buildReadyEntry({ flow, discussStore });
    const req = makePostReq({ flowId: flow.flowId, text: "operator message text" });
    const res = createMockRes();
    await route.handler(req, res);

    assert.ok(appendCalls.length >= 1, "appendMessage called");
    const [calledFlowId, , msg] = appendCalls[0];
    assert.equal(calledFlowId, flow.flowId);
    assert.equal(msg.role, "operator");
    assert.equal(msg.text, "operator message text");
    assert.equal(typeof msg.turnId, "string");
    assert.equal(typeof msg.ts, "number");
  });

  test("sets turn status to 'running' immediately after appending operator message", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const statusCalls = [];
    const discussStore = makeDiscussStore({
      setTurnStatus: (...args) => statusCalls.push(args),
    });
    const { route } = buildReadyEntry({ flow, discussStore });
    const req = makePostReq({ flowId: flow.flowId, text: "check running status" });
    const res = createMockRes();
    await route.handler(req, res);

    const runningCall = statusCalls.find((c) => c[2] === "running");
    assert.ok(runningCall, "setTurnStatus('running') must be called");
    assert.equal(runningCall[0], flow.flowId);
  });
});

// ─── peerId construction ──────────────────────────────────────────────────────

describe("peerId construction", () => {
  test("peerId = '{flowIdShort}-{nodeId.lower()}-{itemId}' from waitJson.nodeId", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flowId = randomUUID();
    const flow = makeFlow({ flowId, waitJson: { nodeId: "REVIEW_GATE" } });
    const target = makeTarget({ itemId: "item-abc", agent: "test-agent", needsStamp: true });
    const stamp = makeStampDiscussSession();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target, stampDiscussSession: stamp });
    const req = makePostReq({ flowId, text: "peer id test" });
    const res = createMockRes();
    await route.handler(req, res);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(stamp.calls.length > 0, "stampDiscussSession called");
    const peerId = stamp.calls[0][4];
    assert.equal(peerId, `${flowId.slice(0, 8)}-review_gate-item-abc`);
  });

  test("peerId falls back to 'gate' when waitJson.nodeId is absent", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flowId = randomUUID();
    const flow = makeFlow({ flowId, waitJson: null });
    const target = makeTarget({ itemId: "item-xyz", needsStamp: true });
    const stamp = makeStampDiscussSession();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target, stampDiscussSession: stamp });
    const req = makePostReq({ flowId, text: "no nodeId test" });
    const res = createMockRes();
    await route.handler(req, res);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(stamp.calls.length > 0, "stampDiscussSession called");
    const peerId = stamp.calls[0][4];
    assert.equal(peerId, `${flowId.slice(0, 8)}-gate-item-xyz`);
  });
});

// ─── extraContext / promptBuilder injection ───────────────────────────────────

describe("HTTP handler — extraContext from promptBuilder", () => {
  test("extraContext.GroupSystemPrompt is set when promptBuilder resolves a string", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const target = makeTarget({ promptBuilder: async () => "You are a helpful assistant." });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target });
    const req = makePostReq({ flowId: flow.flowId, text: "help me" });
    const res = createMockRes();
    await route.handler(req, res);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(capturedDispatchArgs, "dispatch was called");
    assert.ok(capturedDispatchArgs.extraContext, "extraContext must be present");
    assert.equal(capturedDispatchArgs.extraContext.GroupSystemPrompt, "You are a helpful assistant.");
  });

  test("extraContext is omitted when promptBuilder is null", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const target = makeTarget({ promptBuilder: null });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target });
    const req = makePostReq({ flowId: flow.flowId, text: "no prompt builder" });
    const res = createMockRes();
    await route.handler(req, res);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(capturedDispatchArgs, "dispatch was called");
    assert.equal(capturedDispatchArgs.extraContext, undefined, "extraContext must be absent");
  });

  test("extraContext is omitted when promptBuilder throws", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const target = makeTarget({ promptBuilder: async () => { throw new Error("prompt failed"); } });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target });
    const req = makePostReq({ flowId: flow.flowId, text: "builder throws" });
    const res = createMockRes();
    await route.handler(req, res);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(capturedDispatchArgs, "dispatch still called after promptBuilder throw");
    assert.equal(capturedDispatchArgs.extraContext, undefined, "extraContext absent after throw");
  });
});

// ─── deliver callback — replyText extraction ──────────────────────────────────

describe("deliver callback — replyText extraction", () => {
  async function captureDeliver(payloadOverride) {
    const flow = makeFlow();
    const appendCalls = [];
    const discussStore = makeDiscussStore({
      appendMessage: (...a) => appendCalls.push(a),
    });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, discussStore });
    const req = makePostReq({ flowId: flow.flowId, text: "trigger deliver" });
    const res = createMockRes();
    await route.handler(req, res);
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(capturedDispatchArgs?.deliver, "deliver callback present");
    await capturedDispatchArgs.deliver(payloadOverride);
    return appendCalls;
  }

  test("string payload is used directly as replyText", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const calls = await captureDeliver("plain string reply");
    const agentMsg = calls.find((c) => c[2]?.role === "agent");
    assert.ok(agentMsg, "agent message appended");
    assert.equal(agentMsg[2].text, "plain string reply");
  });

  test("payload.text field used when payload is an object", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const calls = await captureDeliver({ text: "from .text field" });
    const agentMsg = calls.find((c) => c[2]?.role === "agent");
    assert.equal(agentMsg[2].text, "from .text field");
  });

  test("payload.body used when .text is absent", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const calls = await captureDeliver({ body: "from .body field" });
    const agentMsg = calls.find((c) => c[2]?.role === "agent");
    assert.equal(agentMsg[2].text, "from .body field");
  });

  test("payload.content used when .text and .body are absent", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const calls = await captureDeliver({ content: "from .content field" });
    const agentMsg = calls.find((c) => c[2]?.role === "agent");
    assert.equal(agentMsg[2].text, "from .content field");
  });

  test("replyText is empty string when payload has no known text field", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const calls = await captureDeliver({ unknownField: "xyz" });
    const agentMsg = calls.find((c) => c[2]?.role === "agent");
    assert.equal(agentMsg[2].text, "");
  });
});

// ─── deliver callback — discussStore side effects ─────────────────────────────

describe("deliver callback — discussStore updates", () => {
  async function invokeDeliver(discussStoreOverrides = {}) {
    const flow = makeFlow();
    const appendCalls = [];
    const statusCalls = [];
    const clearCalls = [];
    const discussStore = makeDiscussStore({
      appendMessage: (...a) => appendCalls.push(a),
      setTurnStatus: (...a) => statusCalls.push(a),
      clearPending:  (...a) => clearCalls.push(a),
      ...discussStoreOverrides,
    });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, discussStore });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "trigger" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));
    await capturedDispatchArgs.deliver("agent reply");
    return { appendCalls, statusCalls, clearCalls, flow };
  }

  test("appends agent reply with role 'agent'", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { appendCalls } = await invokeDeliver();
    const agentMsg = appendCalls.find((c) => c[2]?.role === "agent");
    assert.ok(agentMsg, "agent reply appended");
    assert.equal(agentMsg[2].text, "agent reply");
  });

  test("calls clearPending after agent reply", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { clearCalls, flow } = await invokeDeliver();
    assert.ok(clearCalls.length > 0, "clearPending called");
    assert.equal(clearCalls[0][0], flow.flowId);
  });

  test("sets turn status to 'idle' with null", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { statusCalls } = await invokeDeliver();
    const idleCall = statusCalls.find((c) => c[2] === "idle");
    assert.ok(idleCall, "setTurnStatus('idle') called");
    assert.equal(idleCall[3], null);
  });
});

// ─── deliver callback — proposed components ───────────────────────────────────

describe("deliver callback — proposed_draft and proposed_meta components", () => {
  test("reads proposed_draft text and proposed_meta fields from discussStore.getComponents", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const appendCalls = [];
    const discussStore = makeDiscussStore({
      appendMessage: (...a) => appendCalls.push(a),
      getComponents: () => [
        { kind: "proposed_draft", text: "Draft text here" },
        { kind: "proposed_meta",  fields: { title: "My Title" } },
      ],
    });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, discussStore });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "with components" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));
    await capturedDispatchArgs.deliver("reply");

    const agentMsg = appendCalls.find((c) => c[2]?.role === "agent");
    assert.ok(agentMsg, "agent message appended");
    assert.equal(agentMsg[2].proposedDraft, "Draft text here");
    assert.deepEqual(agentMsg[2].proposedMeta, { title: "My Title" });
  });

  test("proposedDraft and proposedMeta are null when no components returned", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const appendCalls = [];
    const discussStore = makeDiscussStore({
      appendMessage: (...a) => appendCalls.push(a),
      getComponents: () => [],
    });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, discussStore });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "no components" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));
    await capturedDispatchArgs.deliver("reply");

    const agentMsg = appendCalls.find((c) => c[2]?.role === "agent");
    assert.equal(agentMsg[2].proposedDraft, null);
    assert.equal(agentMsg[2].proposedMeta, null);
  });
});

// ─── deliver callback — SSE emission ─────────────────────────────────────────

describe("deliver callback — SSE emission", () => {
  async function runAndDeliver() {
    const flow = makeFlow();
    const emit = makeEmit();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, emit });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "sse test" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));
    await capturedDispatchArgs.deliver("the reply");
    return { emit, flow };
  }

  function completed(emit) {
    const call = emit.calls.find((c) => c.kind === "session.transcript.completed");
    assert.ok(call, "session.transcript.completed emitted");
    return call.payload;
  }

  test("emits session.transcript.completed scope=discuss", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { emit } = await runAndDeliver();
    const payload = completed(emit);
    assert.equal(payload.scope, "discuss");
    assert.equal(payload.text, "the reply");
  });

  test("event contains flowId and itemId", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { emit, flow } = await runAndDeliver();
    const payload = completed(emit);
    assert.equal(payload.flowId, flow.flowId);
    assert.equal(typeof payload.itemId, "string");
  });

  test("event contains numeric durationMs", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { emit } = await runAndDeliver();
    const payload = completed(emit);
    assert.equal(typeof payload.durationMs, "number");
  });

  test("event carries no legacy dupes", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { emit } = await runAndDeliver();
    const payload = completed(emit);
    assert.equal(payload.reply, undefined);
    assert.equal(payload.status, undefined);
    assert.equal(payload.type, undefined);
  });
});

// ─── onDispatchError ──────────────────────────────────────────────────────────

describe("onDispatchError — error handling", () => {
  async function triggerDispatchError(err, discussStoreOverrides = {}) {
    const flow = makeFlow();
    const emit = makeEmit();
    const errorStampCalls = [];
    const statusCalls = [];
    const discussStore = makeDiscussStore({
      stampMessageError: (...a) => errorStampCalls.push(a),
      setTurnStatus: (...a) => statusCalls.push(a),
      ...discussStoreOverrides,
    });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, emit, discussStore });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "dispatch error test" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(capturedDispatchArgs?.onDispatchError, "onDispatchError present");
    await capturedDispatchArgs.onDispatchError(err);
    return { emit, errorStampCalls, statusCalls, flow };
  }

  test("stamps message error in discussStore with error message", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { errorStampCalls } = await triggerDispatchError(new Error("dispatch failed"));
    assert.ok(errorStampCalls.length > 0, "stampMessageError called");
    assert.match(errorStampCalls[0][3], /dispatch failed/);
  });

  test("sets turn status to 'error' with null", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { statusCalls } = await triggerDispatchError(new Error("err"));
    const errCall = statusCalls.find((c) => c[2] === "error");
    assert.ok(errCall, "setTurnStatus('error') called");
    assert.equal(errCall[3], null);
  });

  test("emits session.transcript.failed with error message", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const { emit } = await triggerDispatchError(new Error("boom"));
    const call = emit.calls.find((c) => c.kind === "session.transcript.failed");
    assert.ok(call, "failed session.transcript emitted");
    assert.match(call.payload.error, /boom/);
    assert.equal(call.payload.scope, "discuss");
  });
});

// ─── runner error (dispatchInbound throws) ───────────────────────────────────

describe("runner error — dispatchInboundDirectDmWithRuntime throws", () => {
  test("stamps error when dispatch throws", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const errorStampCalls = [];
    const discussStore = makeDiscussStore({
      stampMessageError: (...a) => errorStampCalls.push(a),
    });

    dispatchShouldThrow = new Error("runner crash");
    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, discussStore });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "crash test" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 30));

    dispatchShouldThrow = null;

    assert.ok(errorStampCalls.length > 0, "stampMessageError called on runner error");
  });
});

// ─── stampDiscussSession — needsStamp guard ───────────────────────────────────

describe("stampDiscussSession — needsStamp conditional", () => {
  test("calls stampDiscussSession with peerId (not sessionKey) when needsStamp is true", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flowId = randomUUID();
    const flow = makeFlow({ flowId, waitJson: { nodeId: "MY_GATE" } });
    const target = makeTarget({ itemId: "item-stamp", agent: "my-agent", needsStamp: true });
    const stamp = makeStampDiscussSession();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target, stampDiscussSession: stamp });
    await route.handler(makePostReq({ flowId, text: "stamp test" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 30));

    assert.ok(stamp.calls.length > 0, "stampDiscussSession called");
    const [calledFlowId, calledNodeId, calledItemId, calledAgent, calledPeerId] = stamp.calls[0];
    assert.equal(calledFlowId, flowId);
    assert.equal(calledNodeId, "my_gate");
    assert.equal(calledItemId, "item-stamp");
    assert.equal(calledAgent, "my-agent");
    // peerId must not be the full session key — it's short-flowId-nodeId-itemId.
    assert.match(calledPeerId, /^[0-9a-f]{8}-my_gate-item-stamp$/);
    // Confirm it does NOT start with "agent:" (which would be the full sessionKey).
    assert.ok(!calledPeerId.startsWith("agent:"), "peerId must not be the full sessionKey");
  });

  test("does NOT call stampDiscussSession when needsStamp is false", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const target = makeTarget({ needsStamp: false });
    const stamp = makeStampDiscussSession();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target, stampDiscussSession: stamp });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "no stamp" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 30));

    assert.equal(stamp.calls.length, 0, "stampDiscussSession must NOT be called when needsStamp is false");
  });
});

// ─── dispatch args — routing contract ────────────────────────────────────────

describe("dispatchInboundDirectDmWithRuntime — call contract", () => {
  test("passes channel='manifold-parrot', accountId='default', senderId='operator'", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "dispatch shape" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.ok(capturedDispatchArgs, "dispatch was called");
    assert.equal(capturedDispatchArgs.channel, "manifold-parrot");
    assert.equal(capturedDispatchArgs.accountId, "default");
    assert.equal(capturedDispatchArgs.senderId, "operator");
    assert.equal(capturedDispatchArgs.senderAddress, "operator");
  });

  test("resolveAgentRoute returns correct agentId and accountId", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flowId = randomUUID();
    const flow = makeFlow({ flowId, waitJson: { nodeId: "GATE_A" } });
    const target = makeTarget({ itemId: "item-q", agent: "answer-agent" });

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow, target });
    await route.handler(makePostReq({ flowId, text: "agent route" }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    const resolved = capturedDispatchArgs.runtime.channel.routing.resolveAgentRoute();
    assert.equal(resolved.agentId, "answer-agent");
    assert.equal(resolved.accountId, "default");
    assert.match(resolved.sessionKey, /answer-agent/);
    assert.match(resolved.sessionKey, /manifold-parrot/);
  });

  test("messageId in dispatch matches turnId returned in HTTP response", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow });
    const res = createMockRes();
    await route.handler(makePostReq({ flowId: flow.flowId, text: "turnId match" }), res);
    const { turnId } = JSON.parse(res.body);

    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.equal(capturedDispatchArgs.messageId, turnId);
  });

  test("bodyForAgent matches the trimmed text from the request body", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();

    dispatchCalls = [];
    capturedDispatchArgs = null;

    const { route } = buildReadyEntry({ flow });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "  hello world  " }), createMockRes());
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setTimeout(r, 20));

    assert.equal(capturedDispatchArgs.bodyForAgent, "hello world");
  });
});

// ─── itemId forwarding to resolveDiscussTarget ────────────────────────────────

describe("HTTP handler — itemId forwarding", () => {
  test("passes body.itemId to resolveDiscussTarget as wantItemId", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const wantItemIdCalls = [];
    const resolveDiscussTarget = (_f, itemId) => {
      wantItemIdCalls.push(itemId);
      return makeTarget({ itemId: itemId ?? "default-item" });
    };

    const { route } = buildReadyEntry({ flow, resolveDiscussTarget });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "with itemId", itemId: "explicit-item" }), createMockRes());

    assert.ok(wantItemIdCalls.length > 0, "resolveDiscussTarget called");
    assert.equal(wantItemIdCalls[0], "explicit-item");
  });

  test("passes null when itemId is absent from body", { skip: SKIP ? SKIP_MSG : false }, async () => {
    const flow = makeFlow();
    const wantItemIdCalls = [];
    const resolveDiscussTarget = (_f, itemId) => {
      wantItemIdCalls.push(itemId);
      return makeTarget();
    };

    const { route } = buildReadyEntry({ flow, resolveDiscussTarget });
    await route.handler(makePostReq({ flowId: flow.flowId, text: "no itemId" }), createMockRes());

    assert.equal(wantItemIdCalls[0], null);
  });
});
