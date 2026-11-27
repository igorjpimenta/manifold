import { test, mock, describe, after } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const TEST_BEARER = "test-bearer-operator";
const SAVED_BEARER = process.env.MANIFOLD_BEARER;
process.env.MANIFOLD_BEARER = TEST_BEARER;
after(() => {
  if (SAVED_BEARER === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = SAVED_BEARER;
});

function createMockReqWithBody(method, path, body, { bearer = true } = {}) {
  const chunks = body != null ? [Buffer.from(typeof body === "string" ? body : JSON.stringify(body))] : [];
  return {
    ...createMockReq(method, path, bearer ? { authorization: `Bearer ${TEST_BEARER}` } : {}),
    [Symbol.asyncIterator]: async function* () { for (const c of chunks) yield c; },
  };
}

const STAMP = { session: "agent:main:k1", agentId: "main" };

let registerOperatorRoutes = null;
let importError = null;

const createStateStoreCalls = [];
let mockCancelStore = { setRunning: async () => ({ applied: true }), cancel: async () => ({ cancelled: true }) };

try {
  await mock.module("openclaw/plugin-sdk/agent-runtime", {
    exports: { resolveAgentEffectiveModelPrimary: async () => null },
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
    ...overrides,
  };
}

function route(routes, path) {
  const r = routes.find((r) => r.path === path);
  assert.ok(r, `route ${path} registered`);
  return r;
}

describe("registerOperatorRoutes — registration", { skip: SKIP_MSG }, () => {
  test("registers 4 routes total", () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    assert.equal(routes.length, 4, `expected 4 routes, got ${routes.length}: ${routes.map((r) => r.path).join(", ")}`);
  });

  for (const path of [
    "/manifold/gate/resolve",
    "/manifold/cancel",
    "/manifold/start",
    "/manifold/resume-from-gate",
  ]) {
    test(`${path}: auth=plugin, match=exact`, () => {
      const { api, routes } = createMockApi();
      registerOperatorRoutes(makeDeps(api));
      const r = route(routes, path);
      assert.equal(r.auth, "plugin");
      assert.equal(r.match, "exact");
    });
  }

  test("no /manifold/_elev/* route is registered — the work runs in the public handler", () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    assert.deepEqual(routes.filter((r) => r.path.includes("/_elev/")), []);
  });

  test("all handlers are functions", () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    for (const r of routes) assert.equal(typeof r.handler, "function");
  });
});

describe("registerOperatorRoutes — method guards", { skip: SKIP_MSG }, () => {
  for (const path of ["/manifold/gate/resolve", "/manifold/cancel", "/manifold/start"]) {
    test(`${path}: GET → 405`, async () => {
      const { api, routes } = createMockApi();
      registerOperatorRoutes(makeDeps(api));
      const res = createMockRes();
      await route(routes, path).handler(createMockReq("GET", path), res);
      assert.equal(res.statusCode, 405);
    });
  }
});

describe("registerOperatorRoutes — gate/resolve handler", { skip: SKIP_MSG }, () => {
  const body = { flowId: "f1", itemId: "i1", decision: "approve", source: "ws", ...STAMP };

  test("calls gateResolver.recordItemDecision with the request body", async () => {
    const { api, routes } = createMockApi();
    const recordItemDecision = mock.fn(async () => ({ ok: true, status: "applied" }));
    registerOperatorRoutes(makeDeps(api, { gateResolver: { recordItemDecision } }));
    const res = createMockRes();
    await route(routes, "/manifold/gate/resolve").handler(createMockReqWithBody("POST", "/manifold/gate/resolve", body), res);
    assert.equal(recordItemDecision.mock.calls.length, 1);
    const call = recordItemDecision.mock.calls[0].arguments[0];
    assert.equal(call.flowId, "f1");
    assert.equal(call.itemId, "i1");
    assert.equal(call.decision, "approve");
    assert.equal(res.statusCode, 200);
    assert.deepEqual(JSON.parse(res.body), { ok: true, status: "applied" });
  });

  test("propagates the decision result's status", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api, {
      gateResolver: { recordItemDecision: async () => ({ ok: false, code: "already-decided" }) },
    }));
    const res = createMockRes();
    await route(routes, "/manifold/gate/resolve").handler(createMockReqWithBody("POST", "/manifold/gate/resolve", body), res);
    assert.equal(res.statusCode, 409);
    assert.deepEqual(JSON.parse(res.body), { ok: false, code: "already-decided" });
  });

  test("flow_not_found → 404", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api, {
      gateResolver: { recordItemDecision: async () => ({ ok: false, code: "flow_not_found" }) },
    }));
    const res = createMockRes();
    await route(routes, "/manifold/gate/resolve").handler(createMockReqWithBody("POST", "/manifold/gate/resolve", { flowId: "missing", itemId: "i1", decision: "approve", source: "ws", ...STAMP }), res);
    assert.equal(res.statusCode, 404);
  });

  test("editedText is threaded through", async () => {
    const { api, routes } = createMockApi();
    const recordItemDecision = mock.fn(async () => ({ ok: true, status: "applied" }));
    registerOperatorRoutes(makeDeps(api, { gateResolver: { recordItemDecision } }));
    const res = createMockRes();
    await route(routes, "/manifold/gate/resolve").handler(
      createMockReqWithBody("POST", "/manifold/gate/resolve", { ...body, editedText: "revised" }), res);
    assert.equal(recordItemDecision.mock.calls[0].arguments[0].editedText, "revised");
  });
});

describe("registerOperatorRoutes — identity guards", { skip: SKIP_MSG }, () => {
  async function gateRes(body) {
    const { api, routes } = createMockApi();
    const recordItemDecision = mock.fn(async () => { throw new Error("must not run unstamped"); });
    registerOperatorRoutes(makeDeps(api, { gateResolver: { recordItemDecision } }));
    const res = createMockRes();
    await route(routes, "/manifold/gate/resolve").handler(createMockReqWithBody("POST", "/manifold/gate/resolve", body), res);
    return { res, recordItemDecision };
  }

  test("gate/resolve: missing identity → 400 missing_identity, no side effects", async () => {
    const { res, recordItemDecision } = await gateRes({ flowId: "f1", itemId: "i1", decision: "approve" });
    assert.equal(res.statusCode, 400);
    assert.deepEqual(JSON.parse(res.body), {
      ok: false, code: "missing_identity",
      error: "session, agentId required",
    });
    assert.equal(recordItemDecision.mock.calls.length, 0);
  });

  test("gate/resolve: empty identity fields are missing too", async () => {
    const { res } = await gateRes({ flowId: "f1", session: "  ", agentId: "main" });
    assert.equal(res.statusCode, 400);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.code, "missing_identity");
    assert.ok(parsed.error.includes("session") && parsed.error.includes("agentId"), parsed.error);
  });

  test("gate/resolve: missing flowId is reported alongside the identity fields", async () => {
    const { res } = await gateRes({ decision: "approve" });
    assert.equal(res.statusCode, 400);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.code, "missing_identity");
    assert.ok(parsed.error.includes("flowId"), parsed.error);
  });

  test("start: missing identity → 400 missing_identity", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api, { pipelinesRef: { value: new Map([["p1", {}]]) } }));
    const res = createMockRes();
    await route(routes, "/manifold/start").handler(createMockReqWithBody("POST", "/manifold/start", { pipelineId: "p1" }), res);
    assert.equal(res.statusCode, 400);
    assert.equal(JSON.parse(res.body).code, "missing_identity");
  });

  test("start: an unknown pipeline still 404s before the identity guard", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    const res = createMockRes();
    await route(routes, "/manifold/start").handler(createMockReqWithBody("POST", "/manifold/start", { pipelineId: "nope", ...STAMP }), res);
    assert.equal(res.statusCode, 404);
    assert.match(JSON.parse(res.body).error, /unknown pipeline/);
  });
});

describe("registerOperatorRoutes — start handler", { skip: SKIP_MSG }, () => {
  test("stamped start runs the pipeline and answers with the new flowId", async () => {
    const { api, routes } = createMockApi();
    const start = mock.fn(async () => ({ flowId: "flow-new", stateJson: { runId: "r1" } }));
    registerOperatorRoutes(makeDeps(api, {
      executor: { start, runFlow: async () => {}, resume: async () => {} },
      pipelinesRef: { value: new Map([["p1", {}]]) },
    }));
    const res = createMockRes();
    await route(routes, "/manifold/start").handler(createMockReqWithBody("POST", "/manifold/start", { pipelineId: "p1", args: { a: 1 }, ...STAMP }), res);
    assert.equal(start.mock.calls.length, 1);
    const [pipelineId, args, triggeredBy] = start.mock.calls[0].arguments;
    assert.equal(pipelineId, "p1");
    assert.deepEqual(args, { a: 1 });
    assert.equal(triggeredBy.kind, "operator_http");
    assert.equal(res.statusCode, 200);
    assert.deepEqual(JSON.parse(res.body), { ok: true, pipelineId: "p1", flowId: "flow-new" });
  });

  test("parentSessionKey in the body reaches the lineage trigger", async () => {
    const { api, routes } = createMockApi();
    const recorded = [];
    api.runtime = {
      lineage: {
        recordTrigger: async (edge) => { recorded.push(edge); },
      },
    };
    registerOperatorRoutes(makeDeps(api, {
      executor: { start: async () => ({ flowId: "flow-new", stateJson: {} }), runFlow: async () => {}, resume: async () => {} },
      pipelinesRef: { value: new Map([["p1", {}]]) },
    }));
    const res = createMockRes();
    await route(routes, "/manifold/start").handler(
      createMockReqWithBody("POST", "/manifold/start", { pipelineId: "p1", parentSessionKey: "agent:parent:k9", ...STAMP }), res);
    assert.equal(res.statusCode, 200);
    assert.equal(recorded.length, 1);
    assert.equal(recorded[0].parentSessionKey, "agent:parent:k9");
  });
});

describe("registerOperatorRoutes — cancel handler", { skip: SKIP_MSG }, () => {
  test("missing flowId → 400", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    const res = createMockRes();
    await route(routes, "/manifold/cancel").handler(createMockReqWithBody("POST", "/manifold/cancel", {}), res);
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
    const res = createMockRes();
    await route(routes, "/manifold/cancel").handler(createMockReqWithBody("POST", "/manifold/cancel", { flowId: "flow-1", reason: "cancelled by operator" }), res);
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
    const res = createMockRes();
    await route(routes, "/manifold/cancel").handler(createMockReqWithBody("POST", "/manifold/cancel", { flowId: "flow-1" }), res);
    assert.deepEqual(cancelArgs, ["flow-1", null]);
  });
});

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
    const res = createMockRes();
    await route(routes, "/manifold/cancel").handler(createMockReqWithBody("POST", "/manifold/cancel", { flowId: "flow-legacy-1" }), res);

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

describe("registerOperatorRoutes — resume-from-gate handler", { skip: SKIP_MSG }, () => {
  test("rejects a request without the bearer", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    const res = createMockRes();
    await route(routes, "/manifold/resume-from-gate").handler(
      createMockReqWithBody("POST", "/manifold/resume-from-gate", { flowId: "f1" }, { bearer: false }), res);
    assert.equal(res.statusCode, 401);
  });

  test("missing flowId → 400", async () => {
    const { api, routes } = createMockApi();
    registerOperatorRoutes(makeDeps(api));
    const res = createMockRes();
    await route(routes, "/manifold/resume-from-gate").handler(
      createMockReqWithBody("POST", "/manifold/resume-from-gate", {}), res);
    assert.equal(res.statusCode, 400);
  });
});
