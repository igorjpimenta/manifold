/**
 * Unit tests for the transcripts endpoint in routes/runs.js.
 *
 * Verifies transcripts come from manifold's own stores only:
 *   - discuss threads carry full turns from discuss-store (operator/agent
 *     roles mapped to user/assistant), attributed via stored (flowId, itemId)
 *     keys and flow_sessions identity rows — never parsed from key shape.
 *   - dispatch entries carry a single assistant turn taken from the node's
 *     stored resultJson when it holds reply text, and list metadata-only
 *     otherwise (content is never invented).
 *   - lite=1 returns turnCount without turn text; sessionKey=X drills into
 *     the exact stored transcript.
 */
import { test, mock, describe, after } from "node:test";
import assert from "node:assert/strict";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const SAVED_BEARER = process.env.MANIFOLD_BEARER;
process.env.MANIFOLD_BEARER = "x";
after(() => {
  if (SAVED_BEARER === undefined) delete process.env.MANIFOLD_BEARER;
  else process.env.MANIFOLD_BEARER = SAVED_BEARER;
});

let listResultsForFlowImpl = () => [];
const nodeExecutionsUrl = new URL("../../../state/node-executions.js", import.meta.url).href;
await mock.module(nodeExecutionsUrl, {
  exports: {
    getNodeExecutionsStore: () => ({
      listResultsForFlow: (flowId) => listResultsForFlowImpl(flowId),
      listInvocationsForFlow: () => [],
      listExecutionsForInvocation: () => [],
      summarizeChildrenByFlowIds: () => new Map(),
      getInvocation: () => null,
      listGateRecordsForFlow: () => ({}),
    }),
  },
});

const { registerRunRoutes } = await import("../../../routes/runs.js");

const FLOW_ID = "flow-abc-123";
const FLOW = { flowId: FLOW_ID, stateJson: { pipelineId: "pipe-1", runId: "run-1" } };

function makeDeps(api, overrides = {}) {
  return {
    api,
    readStore: { get: async (id) => (id === FLOW_ID ? FLOW : null) },
    costForAllPipelines: async () => ({ pipelines: [], sinceMs: null }),
    runStore: {},
    pipelinesRef: { value: new Map() },
    pipelineRegistryStore: {},
    invokeElevated: async () => ({}),
    discussStore: { listForFlow: async () => [] },
    failureStore: { queryErrorsByFlow: async () => [] },
    sessionIndex: { listForFlow: async () => [] },
    handleDiscuss:           async () => {},
    handleDiscussHistory:    async () => {},
    handleDiscussApplyDraft: async () => {},
    handleDiscussAudio:      async () => {},
    handleDiscussAudioGet:   async () => {},
    ...overrides,
  };
}

function prefixHandler(api, routes) {
  void api;
  return routes.find((r) => r.path === "/manifold/runs/" && r.match === "prefix").handler;
}

async function getTranscripts(t, query, depsOverrides) {
  const { api, routes } = createMockApi();
  const deps = makeDeps(api, depsOverrides);
  registerRunRoutes(deps);
  const handler = prefixHandler(api, routes);
  const req = createMockReq("GET", `/manifold/runs/${FLOW_ID}/transcripts${query}`, { authorization: "Bearer x" });
  const res = createMockRes();
  await handler(req, res);
  assert.equal(res.statusCode, 200);
  return JSON.parse(res.body);
}

// ── discuss threads ──────────────────────────────────────────────────────────

describe("transcripts — discuss threads from discuss-store", () => {
  test("lists full turns with stored identity, roles mapped to user/assistant", async (t) => {
    const body = await getTranscripts(t, "", {
      discussStore: { listForFlow: async () => [{
        itemId: "item-1",
        messages: [
          { id: "m1", role: "operator", text: "should we proceed?", ts: 1000 },
          { id: "m2", role: "agent", text: "yes, looks good", ts: 2000 },
        ],
        turnStatus: "idle",
        updatedAt: 2000,
      }] },
      sessionIndex: { listForFlow: async () => [
        { sessionKey: "sk-discuss-1", nodeId: "PROPOSE.GATE", branchItemId: "item-1", spawnedAt: 500 },
      ] },
    });
    assert.equal(body.transcripts.length, 1);
    const entry = body.transcripts[0];
    assert.equal(entry.sessionKey, "sk-discuss-1");
    assert.equal(entry.sessionId, "sk-discuss-1");
    assert.equal(entry.nodeId, "PROPOSE.GATE");
    assert.equal(entry.itemKey, "item-1");
    assert.deepEqual(entry.turns.map((x) => x.role), ["user", "assistant"]);
    assert.deepEqual(entry.turns.map((x) => x.text), ["should we proceed?", "yes, looks good"]);
  });

  test("thread without a stored identity row still lists with null identity", async (t) => {
    const body = await getTranscripts(t, "", {
      discussStore: { listForFlow: async () => [{
        itemId: "item-9",
        messages: [{ id: "m1", role: "operator", text: "hello?", ts: 100 }],
        turnStatus: "running",
        updatedAt: 100,
      }] },
    });
    assert.equal(body.transcripts.length, 1);
    assert.equal(body.transcripts[0].sessionKey, null);
    assert.equal(body.transcripts[0].nodeId, "discuss");
    assert.equal(body.transcripts[0].itemKey, "item-9");
    assert.equal(body.transcripts[0].turns.length, 1);
  });

  test("lite=1 returns turnCount without turn text", async (t) => {
    const body = await getTranscripts(t, "?lite=1", {
      discussStore: { listForFlow: async () => [{
        itemId: "item-1",
        messages: [
          { id: "m1", role: "operator", text: "a", ts: 1 },
          { id: "m2", role: "agent", text: "b", ts: 2 },
        ],
        turnStatus: "idle",
        updatedAt: 2,
      }] },
      sessionIndex: { listForFlow: async () => [
        { sessionKey: "sk-d", nodeId: "G", branchItemId: "item-1", spawnedAt: 1 },
      ] },
    });
    assert.equal(body.transcripts.length, 1);
    assert.equal(body.transcripts[0].turnCount, 2);
    assert.ok(!("turns" in body.transcripts[0]));
  });
});

// ── dispatch turns ───────────────────────────────────────────────────────────

describe("transcripts — dispatch turns from resultJson", () => {
  const sessionRows = [
    { sessionKey: "sk-m1", nodeId: "SUM", branchItemId: null, spawnedAt: 700 },
  ];

  test("stored reply text becomes a single assistant turn", async (t) => {
    listResultsForFlowImpl = () => [
      { nodeId: "SUM", visitSeq: 1, resultWrittenAt: 800, resultJson: JSON.stringify({ text: "summary says yes" }) },
    ];
    try {
      const body = await getTranscripts(t, "", {
        sessionIndex: { listForFlow: async () => sessionRows },
        pipelinesRef: { value: new Map([["pipe-1", { nodes: { SUM: { agent: "scout" } } }]]) },
      });
      assert.equal(body.transcripts.length, 1);
      const entry = body.transcripts[0];
      assert.equal(entry.sessionKey, "sk-m1");
      assert.equal(entry.agent, "scout");
      assert.equal(entry.nodeId, "SUM");
      assert.equal(entry.itemKey, null);
      assert.equal(entry.turns.length, 1);
      assert.equal(entry.turns[0].role, "assistant");
      assert.equal(entry.turns[0].text, "summary says yes");
    } finally {
      listResultsForFlowImpl = () => [];
    }
  });

  test("result without reply text lists metadata-only", async (t) => {
    listResultsForFlowImpl = () => [
      { nodeId: "SUM", visitSeq: 1, resultWrittenAt: 800, resultJson: JSON.stringify({ score: 42 }) },
    ];
    try {
      const body = await getTranscripts(t, "", {
        sessionIndex: { listForFlow: async () => sessionRows },
      });
      assert.equal(body.transcripts.length, 1);
      assert.deepEqual(body.transcripts[0].turns, []);
      assert.equal(body.transcripts[0].sessionKey, "sk-m1");
    } finally {
      listResultsForFlowImpl = () => [];
    }
  });

  test("dispatch lite=1 reports turnCount 1/0", async (t) => {
    listResultsForFlowImpl = () => [
      { nodeId: "SUM", visitSeq: 1, resultWrittenAt: 800, resultJson: JSON.stringify("bare reply") },
    ];
    try {
      const body = await getTranscripts(t, "?lite=1", {
        sessionIndex: { listForFlow: async () => sessionRows },
      });
      assert.equal(body.transcripts[0].turnCount, 1);
    } finally {
      listResultsForFlowImpl = () => [];
    }
  });
});

// ── drill-in ─────────────────────────────────────────────────────────────────

describe("transcripts — sessionKey drill-in", () => {
  test("sessionKey=X returns only the matching stored transcript", async (t) => {
    const body = await getTranscripts(t, "?sessionKey=sk-m1", {
      discussStore: { listForFlow: async () => [{
        itemId: "item-1",
        messages: [{ id: "m1", role: "operator", text: "q", ts: 1 }],
        turnStatus: "idle",
        updatedAt: 1,
      }] },
      sessionIndex: { listForFlow: async () => [
        { sessionKey: "sk-m1", nodeId: "SUM", branchItemId: null, spawnedAt: 5 },
        { sessionKey: "sk-d1", nodeId: "G", branchItemId: "item-1", spawnedAt: 6 },
      ] },
    });
    assert.equal(body.transcripts.length, 1);
    assert.equal(body.transcripts[0].sessionKey, "sk-m1");
    assert.ok(Array.isArray(body.transcripts[0].turns));
  });

  test("unknown sessionKey returns an empty list", async (t) => {
    const body = await getTranscripts(t, "?sessionKey=nope", {
      sessionIndex: { listForFlow: async () => [
        { sessionKey: "sk-m1", nodeId: "SUM", branchItemId: null, spawnedAt: 5 },
      ] },
    });
    assert.deepEqual(body.transcripts, []);
  });
});
