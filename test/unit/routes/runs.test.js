/**
 * Unit tests for routes/runs.js
 *
 * Verifies:
 *   - registerRunRoutes registers 4 routes at the right paths/auth/match.
 *   - Method guards on cost + list + queue routes.
 *   - Cost, list, and queue handlers return expected shapes.
 */
import { test, mock, describe } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../ios/auth.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
  },
});

// sendFullRun now calls failureStore.queryErrorsByFlow(...) (Phase 1.5,
// moved from node-executions.js) and getNodeExecutionsStore().getInvocation(...)
// (parent-link resolution), and the runs list handler calls
// .summarizeChildrenByFlowIds(...) (Phase 2 dual-mode branch summary) — all
// mocked so tests are deterministic regardless of the real (or
// singleton-cached) DB.
let queryErrorsByFlowImpl = () => [];
let queryErrorsByFlowIdsImpl = () => new Map();
let summarizeChildrenByFlowIdsImpl = () => new Map();
let getInvocationImpl = () => null;
// B2.5 — sendFullRun now enriches via assembleState(), which reads
// listResultsForFlow/listSubflowNodeIdsForFlow through optional chaining
// (safe when absent from this mock — resolves to []), and separately
// overrides .gates via listGateRecordsForFlow (called directly, not
// optional-chained — must be mocked or every sendFullRun test throws).
let listGateRecordsForFlowImpl = () => ({});
let listResultsForFlowImpl = () => [];
const nodeExecutionsUrl = new URL("../../../state/node-executions.js", import.meta.url).href;
await mock.module(nodeExecutionsUrl, {
  exports: {
    getNodeExecutionsStore: () => ({
      summarizeChildrenByFlowIds: (flowIds) => summarizeChildrenByFlowIdsImpl(flowIds),
      getInvocation: (id) => getInvocationImpl(id),
      listGateRecordsForFlow: (flowId) => listGateRecordsForFlowImpl(flowId),
      listResultsForFlow: (flowId) => listResultsForFlowImpl(flowId),
    }),
  },
});

const { registerRunRoutes } = await import("../../../routes/runs.js");

function authReq(method, path) {
  return createMockReq(method, path, { authorization: "Bearer x" });
}

function prefixHandler(routes) {
  return routes.find((r) => r.path === "/manifold/runs/" && r.match === "prefix").handler;
}

function makeDeps(api, overrides = {}) {
  return {
    api,
    readStore: { listStatusCounts: () => ({}), listRunsPage: () => [], listCostSnapshot: () => [], get: () => null, listQueued: () => [] },
    costForAllPipelines: async () => ({ pipelines: [], sinceMs: null }),
    pipelinesRef: { value: new Map() },
    invokeElevated: async () => ({}),
    failureStore: {
      queryErrorsByFlow: async (flowId) => queryErrorsByFlowImpl(flowId),
      queryErrorsByFlowIds: async (flowIds) => queryErrorsByFlowIdsImpl(flowIds),
    },
    discussStore: { getComponent: () => null, listComponents: () => [], getBulkSummaries: () => ({}) },
    handleDiscuss:           async () => {},
    handleDiscussHistory:    async () => {},
    handleDiscussApplyDraft: async () => {},
    handleDiscussAudio:      async () => {},
    handleDiscussAudioGet:   async () => {},
    ...overrides,
  };
}

// ── registration ──────────────────────────────────────────────────────────────

describe("registerRunRoutes — registration", () => {
  test("registers exactly 6 routes", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    assert.equal(routes.length, 6);
  });

  test("cost route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const r = routes.find((r) => r.path === "/manifold/cost");
    assert.ok(r, "/manifold/cost route present");
    assert.equal(r.auth, "plugin");
    assert.equal(r.match, "exact");
  });

  test("runs list route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const r = routes.find((r) => r.path === "/manifold/runs" && r.match === "exact");
    assert.ok(r, "/manifold/runs exact route present");
    assert.equal(r.auth, "plugin");
  });

  test("runs prefix route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const r = routes.find((r) => r.path === "/manifold/runs/" && r.match === "prefix");
    assert.ok(r, "/manifold/runs/ prefix route present");
    assert.equal(r.auth, "plugin");
  });

  test("queue route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const r = routes.find((r) => r.path === "/manifold/queue");
    assert.ok(r, "/manifold/queue route present");
    assert.equal(r.auth, "plugin");
    assert.equal(r.match, "exact");
  });

  test("errors route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const r = routes.find((r) => r.path === "/manifold/errors");
    assert.ok(r, "/manifold/errors route present");
    assert.equal(r.auth, "plugin");
    assert.equal(r.match, "exact");
  });

  test("tree-summary route: path, auth, match", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const r = routes.find((r) => r.path === "/manifold/tree-summary");
    assert.ok(r, "/manifold/tree-summary route present");
    assert.equal(r.auth, "plugin");
    assert.equal(r.match, "exact");
  });

  test("all handlers are functions", () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    for (const r of routes) assert.equal(typeof r.handler, "function");
  });
});

// ── method guards ─────────────────────────────────────────────────────────────

describe("registerRunRoutes — method guards", () => {
  test("cost route: POST → 405", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/cost");
    const res = createMockRes();
    await handler(createMockReq("POST", "/manifold/cost"), res);
    assert.equal(res.statusCode, 405);
  });

  test("runs list route: POST → 405", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/runs" && r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("POST", "/manifold/runs"), res);
    assert.equal(res.statusCode, 405);
  });

  test("queue route: POST → 405", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/queue");
    const res = createMockRes();
    await handler(createMockReq("POST", "/manifold/queue"), res);
    assert.equal(res.statusCode, 405);
  });
});

// ── handler behaviour ─────────────────────────────────────────────────────────

describe("registerRunRoutes — cost handler", () => {
  test("returns {sinceMs, untilMs, total, pipelines}", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api, {
      costForAllPipelines: async () => ({ pipelines: [{ id: "team_pipe", total: 1.23 }], sinceMs: null }),
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/cost");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/cost"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(Array.isArray(body.pipelines), "pipelines is array");
    assert.equal(typeof body.total, "number");
    assert.equal(typeof body.untilMs, "number");
  });
});

describe("registerRunRoutes — queue handler", () => {
  test("empty store returns {items: []}", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/queue");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/queue"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.deepEqual(body.items, []);
  });

  test("populated store: field shape matches the lean mapper exactly", async () => {
    const { api, routes } = createMockApi();
    const fakeQueuedRow = {
      flowId: "child-abc", pipelineId: "nyxen_linkedin-prospect",
      parentFlowId: "parent-xyz", nodeId: "FETCH_PROFILES", subflowKey: "jane-doe",
      currentStep: "SCRAPE_PROFILE", lastStep: "__start__",
      statusReason: "queued: waiting for cross-process lock browser:linkedin (node SCRAPE_PROFILE)",
      createdAt: 1000, updatedAt: 2000,
    };
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: () => null, listQueued: () => [fakeQueuedRow] },
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/queue");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/queue"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.equal(body.items.length, 1);
    assert.deepEqual(body.items[0], {
      flowId: "child-abc", pipelineId: "nyxen_linkedin-prospect",
      parentFlowId: "parent-xyz", nodeId: "FETCH_PROFILES", subflowKey: "jane-doe",
      status: "queued", currentStep: "SCRAPE_PROFILE", lastStep: "__start__",
      statusReason: "queued: waiting for cross-process lock browser:linkedin (node SCRAPE_PROFILE)",
      createdAtMs: 1000, updatedAtMs: 2000,
    });
  });

  test("a top-level queued flow (no parent) has parentFlowId/nodeId/subflowKey null", async () => {
    const { api, routes } = createMockApi();
    const fakeQueuedRow = {
      flowId: "top-level-abc", pipelineId: "freelance_workana-reply",
      parentFlowId: null, nodeId: null, subflowKey: null,
      currentStep: "SEND_REPLY", lastStep: "ENSURE_INFRA",
      statusReason: "queued: waiting for an agent-dispatch slot (node SEND_REPLY)",
      createdAt: 500, updatedAt: 600,
    };
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: () => null, listQueued: () => [fakeQueuedRow] },
    }));
    const { handler } = routes.find((r) => r.path === "/manifold/queue");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/queue"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.items[0].parentFlowId, null);
    assert.equal(body.items[0].nodeId, null);
    assert.equal(body.items[0].subflowKey, null);
  });
});

describe("registerRunRoutes — errors handler", () => {
  test("no flowIds param → 400", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/errors");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/errors"), res);
    assert.equal(res.statusCode, 400);
  });

  test("returns errors keyed by flowId", async () => {
    queryErrorsByFlowIdsImpl = () => new Map([
      ["flow-a", [{ nodeId: "SEND", message: "boom", at: 1 }]],
    ]);
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/errors");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/errors?flowIds=flow-a"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(body.errors["flow-a"], "flow-a key present");
    assert.equal(body.errors["flow-a"][0].message, "boom");
  });

  test("unknown flowId → empty array in response", async () => {
    queryErrorsByFlowIdsImpl = () => new Map();
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/errors");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/errors?flowIds=flow-z"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.deepEqual(body.errors, {});
  });
});

describe("registerRunRoutes — runs list handler", () => {
  test("empty store returns {runs: []}", async () => {
    const { api, routes } = createMockApi();
    registerRunRoutes(makeDeps(api));
    const { handler } = routes.find((r) => r.path === "/manifold/runs" && r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/runs"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(Array.isArray(body.runs), "runs is an array");
  });

  test("run records include expected fields", async () => {
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "abc123", status: "succeeded",
      pipelineId: "team_pipe",
      currentStep: null, lastStep: "DONE",
      createdAtMs: 1000, updatedAtMs: 2000,
    };
    registerRunRoutes(makeDeps(api, { readStore: { listStatusCounts: () => ({}), listRunsPage: () => [fakeRun], get: () => null } }));
    const { handler } = routes.find((r) => r.path === "/manifold/runs" && r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/runs"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.runs.length, 1);
    assert.equal(body.runs[0].flowId, "abc123");
    assert.equal(body.runs[0].status, "succeeded");
    assert.equal(body.runs[0].lastStep, "DONE");
    assert.equal(body.runs[0].currentStep, null);
  });

  test("currentStep is passed through when set", async () => {
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "def456", status: "running",
      pipelineId: "team_pipe",
      currentStep: "FETCH", lastStep: "INIT",
      createdAtMs: 1000, updatedAtMs: 2000,
    };
    registerRunRoutes(makeDeps(api, { readStore: { listStatusCounts: () => ({}), listRunsPage: () => [fakeRun], get: () => null } }));
    const { handler } = routes.find((r) => r.path === "/manifold/runs" && r.match === "exact");
    const res = createMockRes();
    await handler(createMockReq("GET", "/manifold/runs"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.runs[0].currentStep, "FETCH");
    assert.equal(body.runs[0].lastStep, "INIT");
  });
});

// ── sendFullRun (GET /manifold/runs/:flowId) — errorEntries (Phase 1.5) ───────

describe("registerRunRoutes — sendFullRun errorEntries", () => {
  test("includes a top-level errorEntries array sourced from failureStore.queryErrorsByFlow", async () => {
    queryErrorsByFlowImpl = (flowId) => {
      assert.equal(flowId, "flow-full");
      return [{ nodeId: "SUBMIT", subflowKey: null, message: "boom", at: 1 }];
    };
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "flow-full", status: "failed",
      stateJson: { pipelineId: "team_pipe" },
      lastStep: "SUBMIT",
      createdAt: 1000, updatedAt: 2000,
    };
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-full" ? fakeRun : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-full"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.deepEqual(body.errorEntries, [{ nodeId: "SUBMIT", subflowKey: null, message: "boom", at: 1 }]);
    // stateJson otherwise ships unchanged — only `loop`/`errors` are
    // stripped, neither present on this fixture. `gates` is always present
    // now (sourced from listGateRecordsForFlow, not the raw stateJson blob)
    // — {} here since this fixture's flow never opened a gate.
    assert.deepEqual(body.stateJson, { pipelineId: "team_pipe", gates: {} });
  });

  test("errorEntries is an empty array when the flow has no recorded errors", async () => {
    queryErrorsByFlowImpl = () => [];
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "flow-clean", status: "succeeded",
      stateJson: { pipelineId: "team_pipe" },
      lastStep: "DONE",
      createdAt: 1000, updatedAt: 2000,
    };
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-clean" ? fakeRun : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-clean"), res);
    const body = JSON.parse(res.body);
    assert.deepEqual(body.errorEntries, []);
  });

  test("strips the legacy errors[] key from stateJson", async () => {
    queryErrorsByFlowImpl = () => [{ nodeId: "SUBMIT", subflowKey: null, message: "boom", at: 1 }];
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "flow-legacy", status: "failed",
      // Historical row persisted before the redirect — still carries a
      // baked-in errors[] in its stored stateJson.
      stateJson: { pipelineId: "team_pipe", errors: [{ node: "SUBMIT", message: "boom", at: 1 }] },
      lastStep: "SUBMIT",
      createdAt: 1000, updatedAt: 2000,
    };
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-legacy" ? fakeRun : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-legacy"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.stateJson.errors, undefined);
    assert.equal(body.stateJson.pipelineId, "team_pipe", "other stateJson fields survive the strip");
    assert.deepEqual(body.errorEntries, [{ nodeId: "SUBMIT", subflowKey: null, message: "boom", at: 1 }]);
  });
});

// ── sendFullRun — parent-run link resolution ───────────────────────────────

describe("registerRunRoutes — sendFullRun parent-link resolution", () => {
  test("top-level flow (no parentInvocationId): parentFlowId/nodeId/subflowKey are null, pipelineId unchanged", async () => {
    queryErrorsByFlowImpl = () => [];
    getInvocationImpl = () => { throw new Error("must not be called for a top-level flow"); };
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "flow-top", status: "running",
      stateJson: { pipelineId: "team_pipe" },
      lastStep: "SCAN",
      createdAt: 1000, updatedAt: 2000,
      parentInvocationId: null,
    };
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-top" ? fakeRun : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-top"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.pipelineId, "team_pipe");
    assert.equal(body.parentFlowId, null);
    assert.equal(body.nodeId, null);
    assert.equal(body.subflowKey, null);
  });

  test("child flow: resolves parentFlowId/nodeId/subflowKey and the REAL top-level pipelineId, not the synthetic one", async () => {
    queryErrorsByFlowImpl = () => [];
    getInvocationImpl = (id) => {
      assert.equal(id, "inv-1");
      return { id: "inv-1", flowId: "flow-parent", pipelineId: "nyxen_linkedin-prospect", nodeId: "FETCH_PROFILES", subflowKey: "jane-doe" };
    };
    const { api, routes } = createMockApi();
    const fakeChild = {
      flowId: "flow-child", status: "queued",
      stateJson: { pipelineId: "nyxen_linkedin-prospect::FETCH_PROFILES" }, // synthetic — must NOT be what's returned
      lastStep: "__start__",
      createdAt: 1000, updatedAt: 2000,
      parentInvocationId: "inv-1",
    };
    const fakeParent = { flowId: "flow-parent", pipelineId: "nyxen_linkedin-prospect" };
    registerRunRoutes(makeDeps(api, {
      readStore: {
        list: () => [],
        get: (id) => {
          if (id === "flow-child") return fakeChild;
          if (id === "flow-parent") return fakeParent;
          return null;
        },
      },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-child"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.pipelineId, "nyxen_linkedin-prospect", "resolved via the parent's own row, not the synthetic pipeline_id");
    assert.equal(body.parentFlowId, "flow-parent");
    assert.equal(body.nodeId, "FETCH_PROFILES");
    assert.equal(body.subflowKey, "jane-doe");
  });

  test("child flow whose parent invocation can't be resolved: falls back to the child's own (synthetic) pipelineId, parent fields stay null", async () => {
    queryErrorsByFlowImpl = () => [];
    getInvocationImpl = () => null;
    const { api, routes } = createMockApi();
    const fakeChild = {
      flowId: "flow-orphan-child", status: "running",
      stateJson: { pipelineId: "team_pipe::LOOP" },
      lastStep: "__start__",
      createdAt: 1000, updatedAt: 2000,
      parentInvocationId: "inv-missing",
    };
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-orphan-child" ? fakeChild : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-orphan-child"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.pipelineId, "team_pipe::LOOP");
    assert.equal(body.parentFlowId, null);
    assert.equal(body.nodeId, null);
    assert.equal(body.subflowKey, null);
  });
});

// ── sendFullRun — B2.5: assembleState() + gate_records ─────────────────────
// stateJson now comes from assembleState() (baseState + each node's own
// resultJson replayed on top) instead of the raw stateJson blob, and .gates
// is always overridden from listGateRecordsForFlow — never assembleState()'s
// own generic per-key merge, which is unsafe specifically for .gates (see
// routes/runs.js's own comment on the staleness class this avoids).

describe("registerRunRoutes — sendFullRun assembleState()/gate_records", () => {
  test("stateJson.gates comes from listGateRecordsForFlow, not the raw stateJson blob's own .gates", async () => {
    queryErrorsByFlowImpl = () => [];
    // A deliberately stale/wrong value baked into the raw stateJson blob —
    // proves the response never trusts it.
    listGateRecordsForFlowImpl = (flowId) => {
      assert.equal(flowId, "flow-gated");
      return { GATE_A: { decision: "approved", at: 5 } };
    };
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "flow-gated", status: "blocked",
      stateJson: { pipelineId: "team_pipe", gates: { GATE_A: { decision: "STALE", at: 1 } } },
      lastStep: "GATE_A",
      createdAt: 1000, updatedAt: 2000,
    };
    registerRunRoutes(makeDeps(api, { readStore: { list: () => [], get: (id) => id === "flow-gated" ? fakeRun : null } }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-gated"), res);
    const body = JSON.parse(res.body);
    assert.deepEqual(body.stateJson.gates, { GATE_A: { decision: "approved", at: 5 } });
  });

  test("stateJson merges a node's own resultJson (via assembleState) on top of the raw baseState", async () => {
    queryErrorsByFlowImpl = () => [];
    listGateRecordsForFlowImpl = () => ({});
    listResultsForFlowImpl = (flowId) => {
      assert.equal(flowId, "flow-resultjson");
      return [{ nodeId: "SCAN", resultJson: JSON.stringify({ SCAN: { listings: ["a", "b"] } }), resultWrittenAt: 12345, resultTooLargeBytes: null }];
    };
    const { api, routes } = createMockApi();
    const fakeRun = {
      flowId: "flow-resultjson", status: "succeeded",
      // SCAN's own key is absent from the raw stateJson blob — only
      // assembleState()'s resultJson replay produces it.
      stateJson: { pipelineId: "team_pipe" },
      lastStep: "SCAN",
      createdAt: 1000, updatedAt: 2000,
    };
    registerRunRoutes(makeDeps(api, {
      readStore: { list: () => [], get: (id) => id === "flow-resultjson" ? fakeRun : null },
    }));
    const res = createMockRes();
    await prefixHandler(routes)(authReq("GET", "/manifold/runs/flow-resultjson"), res);
    const body = JSON.parse(res.body);
    assert.equal(body.stateJson.pipelineId, "team_pipe", "baseState fields survive");
    assert.deepEqual(body.stateJson.SCAN, { listings: ["a", "b"] }, "resultJson replay merges the node's own output in");
  });
});
