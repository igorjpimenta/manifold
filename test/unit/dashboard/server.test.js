// Dashboard reads come straight from manifold's own stores — no gateway hop,
// no pairing token, no gatewayFetch mock. Each converted read path is hit
// over HTTP against a real dashboard server wired to stub stores.

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";

delete process.env.OPENCLAW_GATEWAY_TOKEN;

const { createDashboardServer } = await import("../../../dashboard/server.js");

const FLOW_ID = "11111111-1111-4111-8111-111111111111";
const MISSING_ID = "99999999-9999-4999-8999-999999999999";

const LIST_ROW = {
  flowId: FLOW_ID,
  pipelineId: "pipe1",
  status: "running",
  lastStep: "N1",
  statusReason: null,
  createdAtMs: 1_700_000_000_000,
  updatedAtMs: 1_700_000_001_000,
  endedAtMs: null,
  displayTitle: null,
  displayMetrics: null,
};

const STORED_FLOW = {
  flowId: FLOW_ID,
  ownerKey: "owner",
  pipelineId: "pipe1",
  revision: 3,
  status: "running",
  lastStep: "N1",
  statusReason: null,
  stateJson: { pipelineId: "pipe1", runId: "run1" },
  waitJson: null,
  cancelRequestedAt: null,
  createdAt: 1_700_000_000_000,
  updatedAt: 1_700_000_001_000,
  endedAt: null,
  parentInvocationId: null,
};

const ERROR_ENTRY = {
  nodeId: "N1",
  subflowKey: null,
  message: "boom",
  errorType: null,
  at: 1_700_000_000_500,
  executionId: "e1",
  invocationId: "i1",
};

const readStore = {
  get: async (id) => (id === FLOW_ID ? { ...STORED_FLOW, stateJson: { ...STORED_FLOW.stateJson } } : null),
  listRunsPage: async () => [{ ...LIST_ROW }],
  listForParentInvocations: async () => [],
};

const failureStore = {
  queryErrorsByFlow: async (id) => (id === FLOW_ID ? [{ ...ERROR_ENTRY }] : []),
  queryErrorsByFlowIds: async (ids) => {
    const m = new Map();
    for (const id of ids) if (id === FLOW_ID) m.set(id, [{ ...ERROR_ENTRY }]);
    return m;
  },
};

const discussStore = { getBulkSummaries: async () => ({}) };

const nodeExecutionsStore = {
  summarizeChildrenByFlowIds: async () => new Map(),
  listVisitedNodeIdsByFlow: async (ids) => new Map(ids.map((id) => [id, new Set(["N1"])])),
  listInvocationsForFlow: async (id) => (id === FLOW_ID ? [{
    invocationId: "inv1", nodeId: "N1", subflowKey: null,
    invokedAt: 1_700_000_000_000, enteredAt: 1_700_000_000_000, exitedAt: 1_700_000_000_100,
    kind: "dispatch", executionCount: 1, everRetriedOrRevived: false, latestExecutionId: null,
  }] : []),
  listGateRecordsForFlow: async () => ({}),
  listInvocationIdsForNodes: async () => new Map(),
  listSubflowNodeIdsForFlow: async () => [],
  listResultsForFlow: async () => [],
  getInvocation: async () => null,
};

const pipelineRegistryStore = {
  getNodeLineageBatch: async () => ({}),
  getNodeVersionsBatch: async (reqs) => reqs.map(() => ({ kind: "single", agent: "test-agent" })),
};

let liveSubscriber = null;
const eventBus = {
  subscribe: (cb) => { liveSubscriber = cb; return () => { liveSubscriber = null; }; },
  snapshot: () => ({ ok: true }),
  getEventsFor: async () => [{ kind: "run.created" }],
};

import net from "node:net";

async function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.once("error", reject);
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

let BASE;
let server;

before(async () => {
  const PORT = await freePort();
  BASE = `http://127.0.0.1:${PORT}`;
  server = createDashboardServer({
    writeStore: {},
    readStore,
    failureStore,
    discussStore,
    nodeExecutionsStore,
    pipelinesRef: { value: new Map() },
    pipelineRegistryStore,
    ensurePipelinesLoaded: async () => {},
    executor: {},
    eventBus,
    port: PORT,
    host: "127.0.0.1",
    logger: {},
    recordItemDecision: async () => ({ ok: false, code: "flow_not_found" }),
  });
  await server.start();
});

after(async () => { await server?.stop(); });

async function get(path) {
  const r = await fetch(`${BASE}${path}`);
  return { status: r.status, body: await r.json() };
}

describe("dashboard/server.js — direct store reads (no gateway)", () => {
  test("GET /api/runs lists runs with error counts, no token required", async () => {
    const { status, body } = await get("/api/runs");
    assert.equal(status, 200);
    assert.equal(body.length, 1);
    assert.equal(body[0].flowId, FLOW_ID);
    assert.equal(body[0].errorsCount, 1);
    assert.equal(body[0].lastStep, "N1");
  });

  test("GET /api/runs/:id returns full run with errorEntries", async () => {
    const { status, body } = await get(`/api/runs/${FLOW_ID}`);
    assert.equal(status, 200);
    assert.equal(body.flowId, FLOW_ID);
    assert.equal(body.pipelineId, "pipe1");
    assert.equal(body.errorEntries.length, 1);
    assert.equal(body.errorEntries[0].message, "boom");
  });

  test("GET /api/runs/:id 404s for unknown flow", async () => {
    const { status } = await get(`/api/runs/${MISSING_ID}`);
    assert.equal(status, 404);
  });

  test("GET /api/runs/:id/subflows returns grouped envelope", async () => {
    const { status, body } = await get(`/api/runs/${FLOW_ID}/subflows`);
    assert.equal(status, 200);
    assert.ok(Array.isArray(body.subflows));
  });

  test("GET /api/runs/:id/tree returns node rows", async () => {
    const { status, body } = await get(`/api/runs/${FLOW_ID}/tree`);
    assert.equal(status, 200);
    assert.equal(body.items.length, 1);
    assert.equal(body.items[0].nodeId, "N1");
    assert.equal(body.items[0].type, "node");
  });

  test("GET /api/runs/:id/compare compares against peers", async () => {
    const { status } = await get(`/api/runs/${FLOW_ID}/compare`);
    assert.equal(status, 200);
  });

  test("GET /api/runs/:id/summary derives system summary", async () => {
    const { status, body } = await get(`/api/runs/${FLOW_ID}/summary`);
    assert.equal(status, 200);
    assert.equal(body.source, "system");
    assert.equal(body.summary.errorCount, 1);
  });

  test("GET /api/stats aggregates from direct runs+errors", async () => {
    const { status } = await get("/api/stats");
    assert.equal(status, 200);
  });

  test("GET /api/stats/pipelines/:id uses direct tree-summary", async () => {
    const { status } = await get("/api/stats/pipelines/pipe1");
    assert.equal(status, 200);
  });

  test("GET /api/search searches direct runs", async () => {
    const { status, body } = await get("/api/search?q=pipe1");
    assert.equal(status, 200);
    assert.ok(Array.isArray(body.hits));
  });

  test("GET /api/pipelines surfaces history-only ids from direct runs", async () => {
    const { status, body } = await get("/api/pipelines");
    assert.equal(status, 200);
    assert.equal(body.length, 1);
    assert.equal(body[0].id, "pipe1");
    assert.equal(body[0].historical, true);
  });

  test("GET /api/runs/:id/events still serves the WS-backed bus", async () => {
    const { status, body } = await get(`/api/runs/${FLOW_ID}/events`);
    assert.equal(status, 200);
    assert.equal(body.length, 1);
  });

  test("WS /api/events/ws receives live bus events", async () => {
    assert.ok(liveSubscriber, "server subscribes to the event bus on start");
    const ws = new WebSocket(`${BASE.replace(/^http/, "ws")}/api/events/ws`);
    const received = [];
    ws.onmessage = (e) => { received.push(JSON.parse(String(e.data))); };
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("ws open timed out")), 5000);
      ws.onopen = () => { clearTimeout(timer); resolve(); };
      ws.onerror = (e) => { clearTimeout(timer); reject(new Error(`ws error: ${e?.message ?? "unknown"}`)); };
    });
    try {
      const evt = { kind: "run.updated", flowId: FLOW_ID, at: Date.now() };
      liveSubscriber(evt);
      const started = Date.now();
      while (received.length === 0 && Date.now() - started < 5000) {
        await new Promise((r) => setTimeout(r, 10));
      }
      assert.deepEqual(received[0], evt);
    } finally {
      try { ws.close(); } catch {}
    }
  });
});
