/**
 * Unit tests for routes/operator.js
 *
 * Verifies:
 *   - registerOperatorRoutes registers 9 routes at the right paths/auth/match.
 *   - Key plugin-auth routes (gate/resolve, cancel, start) are present.
 *   - Key gateway-auth elevation routes (_elev/*) are present.
 *   - Method guards on plugin-auth POST-only routes.
 *
 * operator.js has a static import of `openclaw/plugin-sdk/agent-runtime`.
 * We mock that before importing the module under test. If the mock itself
 * fails to resolve (e.g. in a fresh CI environment), all tests are skipped
 * with a clear reason — they still run in any environment where the gateway
 * is normally installed.
 */
import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

function createMockReqWithBody(method, path, body) {
  const chunks = body != null ? [Buffer.from(typeof body === "string" ? body : JSON.stringify(body))] : [];
  return {
    ...createMockReq(method, path),
    [Symbol.asyncIterator]: async function* () { for (const c of chunks) yield c; },
  };
}

// Mock the external deps that operator.js statically imports:
//   1. openclaw/plugin-sdk/agent-runtime  (resolveAgentEffectiveModelPrimary)
//   2. live-activity-bridge/auth.js       (authorizeBridgeRequest)
//   3. state/state-store.js               (createStateStore) — spied, not
//      stubbed inert, so the ownerKey-mismatch cancel fallback's call
//      arguments can be asserted directly (see "cancel handler ownerKey
//      mismatch fallback" below).
let registerOperatorRoutes = null;
let importError = null;

const createStateStoreCalls = [];
let mockCancelStore = { setRunning: async () => ({ applied: true }), cancel: async () => ({ cancelled: true }) };

try {
  await mock.module("openclaw/plugin-sdk/agent-runtime", {
    exports: { resolveAgentEffectiveModelPrimary: async () => null },
  });

  const authUrl = new URL("../../../ios/auth.js", import.meta.url).href;
  await mock.module(authUrl, {
    exports: {
      authorizeBridgeRequest: async () => ({ ok: true }),
      sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
    },
  });

  const stateStoreUrl = new URL("../../../state/state-store.js", import.meta.url).href;
  await mock.module(stateStoreUrl, {
    exports: {
      createStateStore: (api, ownerKey, opts) => {
        createStateStoreCalls.push({ ownerKey, opts });
        return mockCancelStore;
      },
    },
  });

  ({ registerOperatorRoutes } = await import("../../../routes/operator.js"));
} catch (err) {
  importError = err?.message ?? String(err);
}

const SKIP = !registerOperatorRoutes;
const SKIP_MSG = importError ? `operator.js could not be imported: ${importError}` : undefined;

// ── fixtures ──────────────────────────────────────────────────────────────────

function makeDeps(api, overrides = {}) {
  return {
    api,
    store: {
      get: () => null,
      list: () => [],
      setRunning: () => ({ applied: true }),
    },
    readStore: { list: () => [], get: () => null },
    executor: { runFlow: async () => {}, resume: async () => {} },
    gateResolver: { recordItemDecision: async () => {} },
    failureStore: { get: () => null, list: () => [], recordRetry: () => {} },
    pipelinesRef: { value: new Map() },
    ensurePipelinesLoaded: async () => {},
    invokeElevated: async () => ({}),
    stampDiscussSession: async () => {},
    normalizePendingFields: async () => ({}),
    readPending: () => null,
    mergePendingFields: () => {},
    setPendingText: () => {},
    componentEmitter: async () => {},
    ...overrides,
  };
}

// ── registration ──────────────────────────────────────────────────────────────

describe("registerOperatorRoutes — registration", { skip: SKIP_MSG }, () => {
  test("registers 10 routes total", () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    assert.equal(routes.length, 10, `expected 10 routes, got ${routes.length}: ${routes.map((r) => r.path).join(", ")}`);
  });

  // plugin-auth routes (mobile can hit these directly)
  for (const { path, auth } of [
    { path: "/manifold/gate/resolve", auth: "plugin" },
    { path: "/manifold/cancel",        auth: "plugin" },
    { path: "/manifold/start",         auth: "plugin" },
  ]) {
    test(`${path}: auth=${auth}, match=exact`, () => {
      const { api, routes } = createMockApi();
      registerOperatorRoutes(makeDeps(api));
      const r = routes.find((r) => r.path === path);
      assert.ok(r, `route ${path} registered`);
      assert.equal(r.auth, auth);
      assert.equal(r.match, "exact");
    });
  }

  // gateway-auth elevation routes (internal cross-route hops)
  for (const path of [
    "/manifold/_elev/start",
    "/manifold/_elev/gate-resolve",
    "/manifold/resume-from-gate",
    "/manifold/_elev/rerun",
    "/manifold/_elev/retry-failure",
    "/manifold/_elev/discuss",
    "/manifold/_elev/discuss_history",
  ]) {
    test(`${path}: auth=gateway, match=exact`, () => {
      const { api, routes } = createMockApi();
      registerOperatorRoutes(makeDeps(api));
      const r = routes.find((r) => r.path === path);
      assert.ok(r, `route ${path} registered`);
      assert.equal(r.auth, "gateway");
      assert.equal(r.match, "exact");
    });
  }

  test("all handlers are functions", () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    for (const r of routes) assert.equal(typeof r.handler, "function");
  });
});

// ── method guards ─────────────────────────────────────────────────────────────

describe("registerOperatorRoutes — method guards", { skip: SKIP_MSG }, () => {
  for (const path of ["/manifold/gate/resolve", "/manifold/cancel", "/manifold/start"]) {
    test(`${path}: GET → 405`, async () => {
      const { api, routes } = createMockApi();
      registerOperatorRoutes(makeDeps(api));
      const { handler } = routes.find((r) => r.path === path);
      const res = createMockRes();
      await handler(createMockReq("GET", path), res);
      assert.equal(res.statusCode, 405);
    });
  }
});

// ── gate/resolve handler behaviour ────────────────────────────────────────────
//
// /manifold/gate/resolve is auth:"plugin" — it must never call
// gateResolver.recordItemDecision() directly (that path can trigger
// executor.resume(), which may dispatch a kind:"agent" node needing
// operator.write scope this route doesn't have ambient). It must loop back
// through invokeElevated to /manifold/_elev/gate-resolve instead.

describe("registerOperatorRoutes — gate/resolve handler", { skip: SKIP_MSG }, () => {
  test("loops back via invokeElevated instead of calling recordItemDecision directly", async () => {
    const { api, routes } = createMockApi();
    const recordItemDecision = mock.fn(async () => { throw new Error("should not be called directly"); });
    const invokeElevated = mock.fn(async (path, body) => ({
      ok: true, status: 200, body: JSON.stringify({ ok: true, status: "applied", echo: { path, body } }),
    }));
    registerOperatorRoutes(makeDeps(api, {
      gateResolver: { recordItemDecision },
      invokeElevated,
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/gate/resolve");
    const req = createMockReqWithBody("POST", "/manifold/gate/resolve", {
      flowId: "f1", itemId: "i1", decision: "approve", source: "app",
    });
    const res = createMockRes();
    await handler(req, res);
    assert.equal(recordItemDecision.mock.calls.length, 0);
    assert.equal(invokeElevated.mock.calls.length, 1);
    const [path, body] = invokeElevated.mock.calls[0].arguments;
    assert.equal(path, "/manifold/_elev/gate-resolve");
    assert.equal(body.flowId, "f1");
    assert.equal(body.itemId, "i1");
    assert.equal(body.decision, "approve");
    assert.equal(res.statusCode, 200);
    assert.deepEqual(JSON.parse(res.body).echo.path, "/manifold/_elev/gate-resolve");
  });

  test("propagates the elevated route's status/body verbatim", async () => {
    const { api, routes } = createMockApi();
    const invokeElevated = mock.fn(async () => ({
      ok: false, status: 409, body: JSON.stringify({ ok: false, code: "already-decided" }),
    }));
    registerOperatorRoutes(makeDeps(api, { invokeElevated }));
    const { handler } = routes.find((r) => r.path === "/manifold/gate/resolve");
    const req = createMockReqWithBody("POST", "/manifold/gate/resolve", {
      flowId: "f1", itemId: "i1", decision: "approve", source: "app",
    });
    const res = createMockRes();
    await handler(req, res);
    assert.equal(res.statusCode, 409);
    assert.deepEqual(JSON.parse(res.body), { ok: false, code: "already-decided" });
  });
});

// ── _elev/gate-resolve handler behaviour ──────────────────────────────────────

describe("registerOperatorRoutes — _elev/gate-resolve handler", { skip: SKIP_MSG }, () => {
  test("calls gateResolver.recordItemDecision with the request body", async () => {
    const { api, routes } = createMockApi();
    const recordItemDecision = mock.fn(async () => ({ ok: true, status: "applied" }));
    registerOperatorRoutes(makeDeps(api, { gateResolver: { recordItemDecision } }));
    const { handler } = routes.find((r) => r.path === "/manifold/_elev/gate-resolve");
    const req = createMockReqWithBody("POST", "/manifold/_elev/gate-resolve", {
      flowId: "f1", itemId: "i1", decision: "approve", source: "app", editedText: "revised",
    });
    const res = createMockRes();
    await handler(req, res);
    assert.equal(recordItemDecision.mock.calls.length, 1);
    const call = recordItemDecision.mock.calls[0].arguments[0];
    assert.equal(call.flowId, "f1");
    assert.equal(call.itemId, "i1");
    assert.equal(call.decision, "approve");
    assert.equal(call.editedText, "revised");
    assert.equal(res.statusCode, 200);
    assert.deepEqual(JSON.parse(res.body), { ok: true, status: "applied" });
  });

  test("flow_not_found → 404", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api, {
      gateResolver: { recordItemDecision: async () => ({ ok: false, code: "flow_not_found" }) },
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/_elev/gate-resolve");
    const req = createMockReqWithBody("POST", "/manifold/_elev/gate-resolve", { flowId: "missing", itemId: "i1", decision: "approve", source: "app" });
    const res = createMockRes();
    await handler(req, res);
    assert.equal(res.statusCode, 404);
  });

  test("GET → 405", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/_elev/gate-resolve");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/_elev/gate-resolve"), res);
    assert.equal(res.statusCode, 405);
  });
});

// ── cancel handler behaviour ──────────────────────────────────────────────────

describe("registerOperatorRoutes — cancel handler", { skip: SKIP_MSG }, () => {
  test("missing flowId → 400", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/cancel");
    const req = { ...createMockReq("POST", "/manifold/cancel"), body: "{}" };
    const res = createMockRes();
    // Simulate the gateway providing a parsed body (empty object)
    req.json = async () => ({});
    await handler(req, res);
    assert.ok(res.statusCode === 400 || res.statusCode === 422, `expected 4xx, got ${res.statusCode}`);
  });

  test("body.reason is threaded through to executor.cancel(flowId, reason)", async () => {
    const { api, routes } = createMockApi();
    let cancelArgs = null;
    registerOperatorRoutes(makeDeps(api, {
      executor: {
        runFlow: async () => {}, resume: async () => {},
        cancel: async (flowId, reason) => { cancelArgs = [flowId, reason]; return { cancelled: true }; },
      },
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/cancel");
    const req = createMockReqWithBody("POST", "/manifold/cancel", { flowId: "flow-1", reason: "cancelled by operator" });
    const res = createMockRes();
    await handler(req, res);
    assert.deepEqual(cancelArgs, ["flow-1", "cancelled by operator"]);
  });

  test("no reason in body → executor.cancel(flowId, null)", async () => {
    const { api, routes } = createMockApi();
    let cancelArgs = null;
    registerOperatorRoutes(makeDeps(api, {
      executor: {
        runFlow: async () => {}, resume: async () => {},
        cancel: async (flowId, reason) => { cancelArgs = [flowId, reason]; return { cancelled: true }; },
      },
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/cancel");
    const req = createMockReqWithBody("POST", "/manifold/cancel", { flowId: "flow-1" });
    const res = createMockRes();
    await handler(req, res);
    assert.deepEqual(cancelArgs, ["flow-1", null]);
  });
});

// ── cancel handler: ownerKey-mismatch fallback ──────────────────────────────
//
// When the primary executor.cancel() reports "Flow not found." (the flow
// belongs to a different ownerKey — legacy graph-flow-owner flows, or any
// flow this process's default store doesn't recognize), the handler builds a
// second, ad-hoc state-store bound to the flow's real ownerKey and retries
// through it. That store MUST carry admissionQueueStore and resumeParentRef:
// state-store.js internals (triggerAdmissionRelease, triggerParentResume)
// no-op without them, permanently leaking the flow-start/child-dispatch
// admission slot and never waking a waiting parent.
describe("registerOperatorRoutes — cancel handler ownerKey mismatch fallback", { skip: SKIP_MSG }, () => {
  test("builds the fallback cancelStore with the full dependency set, not just runStore", async () => {
    createStateStoreCalls.length = 0;
    const { api, routes } = createMockApi();
    const sentinelAdmissionQueueStore = { admit: async () => ({}) };
    const sentinelResumeParentRef = { resume: async () => {} };
    const sentinelRunStore = { list: () => [] };
    const sentinelPipelinesRef = { value: new Map() };
    const sentinelFailureStore = { get: () => null };
    registerOperatorRoutes(makeDeps(api, {
      executor: {
        runFlow: async () => {}, resume: async () => {},
        cancel: async () => ({ cancelled: false, reason: "Flow not found." }),
      },
      readStore: {
        list: () => [],
        get: async () => ({
          flowId: "flow-legacy-1", status: "blocked", revision: 3,
          lastStep: "SOME_NODE", stateJson: {}, ownerKey: "graph-flow-owner",
        }),
      },
      ownerSessionKey: "manifold-owner",
      runStore: sentinelRunStore,
      admissionQueueStore: sentinelAdmissionQueueStore,
      resumeParentRef: sentinelResumeParentRef,
      pipelinesRef: sentinelPipelinesRef,
      failureStore: sentinelFailureStore,
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/cancel");
    const req = createMockReqWithBody("POST", "/manifold/cancel", { flowId: "flow-legacy-1" });
    const res = createMockRes();
    await handler(req, res);

    assert.equal(createStateStoreCalls.length, 1, "expected the ownerKey-mismatch fallback to construct exactly one cancelStore");
    const { ownerKey, opts } = createStateStoreCalls[0];
    assert.equal(ownerKey, "graph-flow-owner");
    assert.equal(opts.runStore, sentinelRunStore);
    assert.equal(opts.admissionQueueStore, sentinelAdmissionQueueStore, "admissionQueueStore must be threaded through — its absence silently no-ops triggerAdmissionRelease, leaking the flow's admission slot forever");
    assert.equal(opts.resumeParentRef, sentinelResumeParentRef, "resumeParentRef must be threaded through — its absence silently no-ops triggerParentResume, leaving a waiting parent stuck forever");
    assert.equal(opts.pipelinesRef, sentinelPipelinesRef);
    assert.equal(opts.failureStore, sentinelFailureStore);
    assert.ok(opts.nodeExecutionsStore, "nodeExecutionsStore must be present so cascade-cancel/parent-resume can inspect subflow children");
  });
});
