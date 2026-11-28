import { test, mock, describe, after } from "node:test";
import assert from "node:assert/strict";
import { rm } from "node:fs/promises";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { tmpdir } from "node:os";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const ISOLATED_DATA_DIR = mkdtempSync(path.join(tmpdir(), "manifold-discuss-test-"));
process.env.MANIFOLD_DATA_DIR = ISOLATED_DATA_DIR;
after(async () => {
  rmSync(ISOLATED_DATA_DIR, { recursive: true, force: true });
});

let assembleStateCalls = [];
let assembleStateReturn = { enriched: true };
const executorHelpersUrl = new URL("../../../engine/executor-helpers.js", import.meta.url).href;
await mock.module(executorHelpersUrl, {
  exports: {
    assembleState: async (flowId, opts) => {
      assembleStateCalls.push({ flowId, opts });
      return assembleStateReturn;
    },
  },
});

const { registerDiscussHandlers } = await import("../../../routes/discuss.js");
const sessions = await import("../../../plugins/openclaw/manifold/sessions.js");

function createMockReqWithBody(method, path_, body, headers = {}) {
  const chunks = body != null ? [Buffer.from(typeof body === "string" ? body : JSON.stringify(body))] : [];
  return {
    ...createMockReq(method, path_, headers),
    [Symbol.asyncIterator]: async function* () { for (const c of chunks) yield c; },
  };
}

function makeDeps(api, overrides = {}) {
  const threads = new Map();
  const key = (flowId, gateNodeId, itemId) => `${flowId} ${gateNodeId} ${itemId}`;
  return {
    api,
    store: { get: () => null, list: () => [] },
    pipelinesRef: { value: new Map() },
    emit: async () => ({ ok: true }),
    discussDefaultAgent: null,
    runDiscussTurn: async () => ({ status: 500, body: { ok: false, code: "not_bound" } }),
    sessions,
    discussStore: {
      getComponent: () => null,
      listComponents: () => [],
      setComponent: () => {},
      readPending: async () => null,
      clearPending: async () => {},
    },
    sessionIndex: {
      async recordThread({ flowId, gateNodeId, itemId, agentId, session }) {
        threads.set(key(flowId, gateNodeId, itemId), { session, agentId: agentId ?? null });
      },
      async findThread(flowId, gateNodeId, itemId) {
        return threads.get(key(flowId, gateNodeId, itemId)) ?? null;
      },
    },
    ...overrides,
  };
}

describe("registerDiscussHandlers — return shape", () => {
  test("does NOT call api.registerHttpRoute", () => {
    const { api, routes } = createMockApi();
    registerDiscussHandlers(makeDeps(api));
    assert.equal(routes.length, 0, "discuss.js must not register routes directly");
  });

  const REQUIRED_HANDLERS = [
    "handleDiscuss",
    "handleDiscussAudio",
    "handleDiscussAudioGet",
    "handleDiscussAudioRetry",
    "handleDiscussHistory",
    "handleDiscussApplyDraft",
  ];

  const REQUIRED_HELPERS = [
    "resolveDiscussTarget",
  ];

  for (const name of [...REQUIRED_HANDLERS, ...REQUIRED_HELPERS]) {
    test(`returns function: ${name}`, () => {
      const { api } = createMockApi();
      const result = registerDiscussHandlers(makeDeps(api));
      assert.equal(typeof result[name], "function", `${name} must be a function`);
    });
  }
});

describe("registerDiscussHandlers — resolveDiscussTarget mid-batch (running)", () => {
  test("always 503s with gate_transition when flow is running and waitJson is null", async () => {
    const { api } = createMockApi();
    const deps = makeDeps(api);
    const { resolveDiscussTarget } = registerDiscussHandlers(deps);
    const target = await resolveDiscussTarget(
      { flowId: "flow-1", status: "running", waitJson: null, stateJson: { pipelineId: "test-pipeline" } },
      "alpha",
    );
    assert.equal(target.ok, false);
    assert.equal(target.code, "gate_transition");
    assert.equal(target.status, 503);
  });
});

describe("registerDiscussHandlers — resolveDiscussTarget roster-backed identity", () => {
  const testFlowId = `unit-test-discuss-roster-${Date.now()}`;

  function gateDeps(api) {
    return makeDeps(api, {
      pipelinesRef: {
        value: new Map([["test-pipeline", { nodes: { GATE: { kind: "human-gate", discuss: { agent: "config-agent" } } } }]]),
      },
    });
  }

  function gateFlow() {
    return {
      flowId: testFlowId,
      status: "blocked",
      waitJson: { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "item-1" }] },
      stateJson: { pipelineId: "test-pipeline" },
    };
  }

  test("unstamped thread resolves with needsStamp and the config agent", async () => {
    const { api } = createMockApi();
    const { resolveDiscussTarget } = registerDiscussHandlers(gateDeps(api));
    const target = await resolveDiscussTarget(gateFlow(), "item-1");
    assert.equal(target.ok, true);
    assert.equal(target.needsStamp, true);
    assert.equal(target.agent, "config-agent");
    assert.equal(typeof target.session, "string");
  });

  test("stamped thread resolves verbatim, roster agent wins over config", async () => {
    const { api } = createMockApi();
    const deps = gateDeps(api);
    const { resolveDiscussTarget } = registerDiscussHandlers(deps);
    await deps.sessionIndex.recordThread({
      flowId: testFlowId, gateNodeId: "GATE", itemId: "item-1",
      agentId: "roster-agent", session: "agent:test:thread-1",
    });
    const target = await resolveDiscussTarget(gateFlow(), "item-1");
    assert.equal(target.ok, true);
    assert.equal(target.needsStamp, false);
    assert.equal(target.session, "agent:test:thread-1");
    assert.equal(target.agent, "roster-agent");
  });
});

describe("registerDiscussHandlers — handleDiscuss builds ctx.state via assembleState()", () => {
  const testFlowId = `unit-test-discuss-${Date.now()}`;

  after(async () => {
    await rm(path.join(os.homedir(), ".openclaw", "discuss-turns", testFlowId), { recursive: true, force: true }).catch(() => {});
  });

  test("promptBuilder receives ctx.state from assembleState(), called with this flow's runStore/nodeExecutionsStore/baseState", async () => {
    assembleStateCalls = [];
    assembleStateReturn = { enriched: true, DRAFT: { text: "hi" } };
    let capturedCtx = null;
    const promptBuilder = (ctx) => { capturedCtx = ctx; return "system prompt"; };

    const { api } = createMockApi();
    const sourceFlow = {
      flowId: testFlowId,
      status: "blocked",
      waitJson: { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "item-1" }] },
      stateJson: { pipelineId: "test-pipeline", runId: "run-1" },
    };
    const pipelinesRef = {
      value: new Map([["test-pipeline", { nodes: { GATE: { kind: "human-gate", discuss: { agent: "test-agent", promptBuilder } } } }]]),
    };
    const runStore = { sentinel: "runStore" };
    const nodeExecutionsStore = { sentinel: "nodeExecutionsStore" };
    const deps = makeDeps(api, { pipelinesRef, runStore, nodeExecutionsStore });
    const { handleDiscuss } = registerDiscussHandlers(deps);

    const req = createMockReqWithBody("POST", `/manifold/runs/${testFlowId}/discuss`, { text: "what's going on?" });
    const res = createMockRes();
    await handleDiscuss(req, res, sourceFlow);

    assert.equal(assembleStateCalls.length, 1);
    assert.equal(assembleStateCalls[0].flowId, testFlowId);
    assert.equal(assembleStateCalls[0].opts.runStore, runStore);
    assert.equal(assembleStateCalls[0].opts.nodeExecutionsStore, nodeExecutionsStore);
    assert.deepEqual(assembleStateCalls[0].opts.baseState, sourceFlow.stateJson);

    assert.ok(capturedCtx, "promptBuilder must have been called");
    assert.deepEqual(capturedCtx.state, assembleStateReturn, "ctx.state must be assembleState()'s return value, not the raw stateJson blob");
    assert.equal(res.statusCode, 202);
  });
});

describe("registerDiscussHandlers — handleDiscussHistory awaits discussStore.getState", () => {
  test("returns 200 with messages when the store resolves state asynchronously", async () => {
    const { api } = createMockApi();
    const sourceFlow = {
      flowId: "flow-history-1",
      status: "blocked",
      waitJson: { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "item-1" }] },
      stateJson: { pipelineId: "test-pipeline" },
    };
    const storedMessages = [
      { role: "operator", text: "hi", ts: 1 },
      { role: "agent", text: "hello back", ts: 2 },
    ];
    const pipelinesRef = {
      value: new Map([["test-pipeline", { nodes: { GATE: { kind: "human-gate", discuss: { agent: "test-agent" } } } }]]),
    };
    const deps = makeDeps(api, {
      pipelinesRef,
      discussStore: {
        getState: async () => ({
          messages: storedMessages,
          turnStatus: "idle",
          activeTurnId: null,
          pending: null,
        }),
      },
    });
    const { handleDiscussHistory } = registerDiscussHandlers(deps);

    const req = createMockReq("GET", `/manifold/runs/${sourceFlow.flowId}/discuss/history`, { "x-manifold-harness": "test-harness" });
    const res = createMockRes();
    await handleDiscussHistory(req, res, sourceFlow);

    assert.equal(res.statusCode, 200);
    const parsed = JSON.parse(res.body);
    assert.equal(parsed.ok, true);
    assert.equal(parsed.messages.length, 2);
    assert.equal(parsed.messages[0].text, "hi");
    assert.equal(parsed.messages[1].text, "hello back");
  });
});

describe("registerDiscussHandlers — handleDiscussAudioRetry", () => {
  const testFlowId = `unit-test-discuss-audio-retry-${Date.now()}`;
  const itemId = "item-1";

  after(async () => {
    await rm(path.join(ISOLATED_DATA_DIR, "media", "audio", "test-harness", testFlowId), { recursive: true, force: true }).catch(() => {});
  });

  const sourceFlow = {
    flowId: testFlowId,
    status: "blocked",
    waitJson: { kind: "human-gate", nodeId: "GATE", items: [{ itemId }] },
    stateJson: { pipelineId: "test-pipeline" },
  };
  const pipelinesRef = {
    value: new Map([["test-pipeline", { nodes: { GATE: { kind: "human-gate", discuss: { agent: "test-agent" } } } }]]),
  };

  test("re-transcribes an existing audio file and updates its meta sidecar in place", async () => {
    const { audioDirForFlow } = await import("../../../routes/discuss-audio.js");
    const audioDir = audioDirForFlow(testFlowId, itemId, "test-harness");
    const { mkdir, writeFile, readFile } = await import("node:fs/promises");
    await mkdir(audioDir, { recursive: true });
    const turnId = "11111111-1111-1111-1111-111111111111";
    const audioUrl = `/manifold/runs/${testFlowId}/discuss/audio/${turnId}.m4a`;
    await writeFile(path.join(audioDir, `${turnId}.m4a`), Buffer.from("fake audio bytes"));
    await writeFile(path.join(audioDir, `${turnId}.meta.json`), JSON.stringify({
      turnId, itemId, audioUrl, transcript: null, status: "failed",
      error: "Command failed: whisper --model turbo ...", createdAtMs: 1000, transcribedAtMs: 2000,
    }));

    const emitted = [];
    let resolveTranscriptEvent;
    let resolveTurnEvent;
    const transcriptEventSeen = new Promise((r) => { resolveTranscriptEvent = r; });
    const turnEventSeen = new Promise((r) => { resolveTurnEvent = r; });
    const emit = async (kind, payload) => {
      emitted.push({ kind, payload });
      if (kind === "session.transcript.updated") resolveTranscriptEvent();
      if (kind === "session.transcript.completed") resolveTurnEvent();
      return { ok: true, kind, deliveries: {} };
    };

    const { api } = createMockApi();
    api.runtime = { mediaUnderstanding: { transcribeAudioFile: async () => ({ text: "hello from retry" }) } };
    const deps = makeDeps(api, {
      pipelinesRef,
      emit,
      runDiscussTurn: async () => ({ status: 200, body: { ok: true, reply: "agent reply" } }),
    });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);

    const req = createMockReqWithBody("POST", `/manifold/runs/${testFlowId}/discuss/audio/retry`, { turnId, itemId }, { "x-manifold-harness": "test-harness" });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);

    assert.equal(res.statusCode, 202);
    const body = JSON.parse(res.body);
    assert.equal(body.ok, true);
    assert.equal(body.turnId, turnId);
    assert.equal(body.audioUrl, audioUrl);

    const metaPath = path.join(audioDir, `${turnId}.meta.json`);
    const deadline = Date.now() + 5000;
    let updatedMeta = null;
    for (;;) {
      try {
        updatedMeta = JSON.parse(await readFile(metaPath, "utf8"));
      } catch {
        updatedMeta = null;
      }
      if (updatedMeta?.status === "completed") break;
      if (Date.now() > deadline) {
        assert.fail(`meta sidecar never flipped to completed (stuck at ${updatedMeta?.status ?? "unreadable"})`);
      }
      await new Promise((r) => setTimeout(r, 25));
    }
    assert.equal(updatedMeta.transcript, "hello from retry");
    assert.equal(updatedMeta.error, null);
    assert.equal(updatedMeta.createdAtMs, 1000, "original createdAtMs must survive a retry");

    await transcriptEventSeen;
    await turnEventSeen;
    await new Promise((r) => setTimeout(r, 20));

    const transcriptEvent = emitted.find((c) => c.kind === "session.transcript.updated");
    assert.ok(transcriptEvent, "must emit session.transcript.updated");
    assert.equal(transcriptEvent.payload.scope, "discuss");
    assert.equal(transcriptEvent.payload.text, "hello from retry");
    assert.equal(transcriptEvent.payload.turnId, turnId);

    const turnEvent = emitted.find((c) => c.kind === "session.transcript.completed");
    assert.ok(turnEvent, "a successful retry must dispatch an agent turn, same as a fresh upload would");
    assert.equal(turnEvent.payload.scope, "discuss");
    assert.equal(turnEvent.payload.text, "agent reply");
  });

  test("canonical path: retry emits session.transcript.* with scope=discuss", async () => {    const { audioDirForFlow } = await import("../../../routes/discuss-audio.js");
    const audioDir = audioDirForFlow(testFlowId, itemId, "test-harness");
    const { mkdir, writeFile } = await import("node:fs/promises");
    await mkdir(audioDir, { recursive: true });
    const turnId = "22222222-2222-2222-2222-222222222222";
    const audioUrl = `/manifold/runs/${testFlowId}/discuss/audio/${turnId}.m4a`;
    await writeFile(path.join(audioDir, `${turnId}.m4a`), Buffer.from("fake audio bytes"));
    await writeFile(path.join(audioDir, `${turnId}.meta.json`), JSON.stringify({
      turnId, itemId, audioUrl, transcript: null, status: "failed",
      error: "boom", createdAtMs: 1000, transcribedAtMs: 2000,
    }));

    const emitted = [];
    let resolveTurnEvent;
    const turnEventSeen = new Promise((r) => { resolveTurnEvent = r; });
    const emit = async (kind, payload) => {
      emitted.push({ kind, payload });
      if (kind === "session.transcript.completed") resolveTurnEvent();
      return { ok: true, kind, deliveries: {} };
    };

    const { api } = createMockApi();
    api.runtime = { mediaUnderstanding: { transcribeAudioFile: async () => ({ text: "hello canonical" }) } };
    const deps = makeDeps(api, {
      pipelinesRef,
      emit,
      runDiscussTurn: async () => ({ status: 200, body: { ok: true, reply: "agent reply" } }),
    });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);

    const req = createMockReqWithBody("POST", `/manifold/runs/${testFlowId}/discuss/audio/retry`, { turnId, itemId }, { "x-manifold-harness": "test-harness" });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);
    assert.equal(res.statusCode, 202);

    await turnEventSeen;
    await new Promise((r) => setTimeout(r, 20));

    const transcriptCall = emitted.find((c) => c.kind === "session.transcript.updated");
    assert.ok(transcriptCall, "must emit session.transcript.updated");
    assert.equal(transcriptCall.payload.scope, "discuss");
    assert.equal(transcriptCall.payload.text, "hello canonical");
    assert.equal(transcriptCall.payload.transcript, undefined, "transcript must not be duplicated — text is canonical");

    const turnCall = emitted.find((c) => c.kind === "session.transcript.completed");
    assert.ok(turnCall, "must emit session.transcript.completed for the agent turn");
    assert.equal(turnCall.payload.scope, "discuss");
    assert.equal(turnCall.payload.turnId, turnId);
    assert.equal(turnCall.payload.text, "agent reply");
    assert.equal(turnCall.payload.reply, undefined, "reply must not be duplicated — text is canonical");
    assert.equal(turnCall.payload.status, undefined, "no status dupe — the kind carries it");
  });

  test("returns 404 when there's no meta sidecar for the turnId", async () => {
    const { api } = createMockApi();
    api.runtime = { mediaUnderstanding: { transcribeAudioFile: async () => ({ text: "x" }) } };
    const deps = makeDeps(api, { pipelinesRef });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);
    const req = createMockReqWithBody("POST", `/manifold/runs/${testFlowId}/discuss/audio/retry`, { turnId: "does-not-exist", itemId }, { "x-manifold-harness": "test-harness" });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);
    assert.equal(res.statusCode, 404);
    assert.equal(JSON.parse(res.body).code, "turn_not_found");
  });

  test("returns 400 when turnId is missing from the body", async () => {
    const { api } = createMockApi();
    const deps = makeDeps(api, { pipelinesRef });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);
    const req = createMockReqWithBody("POST", `/manifold/runs/${testFlowId}/discuss/audio/retry`, { itemId }, { "x-manifold-harness": "test-harness" });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);
    assert.equal(res.statusCode, 400);
    assert.equal(JSON.parse(res.body).code, "missing_turn_id");
  });

  test("refuses audio without a declared harness, before any store touch", async () => {
    const { api } = createMockApi();
    const deps = makeDeps(api, { pipelinesRef });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);
    const req = createMockReqWithBody("POST", `/manifold/runs/${testFlowId}/discuss/audio/retry`, { turnId: "t", itemId });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);
    assert.equal(res.statusCode, 400);
    assert.equal(JSON.parse(res.body).code, "missing_harness");
  });
});

describe("registerDiscussHandlers — apply-draft invariant", () => {  test("handleDiscussApplyDraft does not expose gateResolver in its closure", () => {
    const { api } = createMockApi();
    const deps = makeDeps(api);
    let resolveCallCount = 0;
    deps.gateResolver = {
      recordItemDecision: () => { resolveCallCount++; },
    };
    const result = registerDiscussHandlers(deps);
    assert.equal(typeof result.handleDiscussApplyDraft, "function");
    assert.equal(resolveCallCount, 0, "gateResolver must not be called during factory setup");
  });
});

describe("registerDiscussHandlers — resolveDiscussTarget harness-owned ids", () => {
  const testFlowId = `unit-test-discuss-harness-id-${Date.now()}`;

  after(async () => {
    await rm(path.join(os.homedir(), ".openclaw", "discuss-sessions"), { recursive: true, force: true }).catch(() => {});
  });

  function gateFlow() {
    return {
      flowId: testFlowId,
      status: "blocked",
      waitJson: { kind: "human-gate", nodeId: "GATE", items: [{ itemId: "item-1" }] },
      stateJson: { pipelineId: "test-pipeline" },
    };
  }

  function gatePipelines() {
    return {
      value: new Map([["test-pipeline", { nodes: { GATE: { kind: "human-gate", discuss: { agent: "test-agent" } } } }]]),
    };
  }

  test("new thread takes the harness adapter id verbatim — never gf-discuss", async () => {
    const { api } = createMockApi();
    const deps = makeDeps(api, { pipelinesRef: gatePipelines() });
    const { resolveDiscussTarget } = registerDiscussHandlers(deps);
    const target = await resolveDiscussTarget(gateFlow(), "item-1");
    assert.equal(target.ok, true);
    assert.equal(target.session, `agent:test-agent:discuss:${testFlowId.slice(0, 8)}-gate-item-1`);
    assert.ok(!("sessionKey" in target) && !("sessionId" in target), "core exposes one session term");
  });

  test("refuses loudly when the harness provides no session id", async () => {
    const { api } = createMockApi();
    const deps = makeDeps(api, { pipelinesRef: gatePipelines(), sessions: {} });
    const { resolveDiscussTarget } = registerDiscussHandlers(deps);
    const target = await resolveDiscussTarget(gateFlow(), "item-1");
    assert.equal(target.ok, false);
    assert.equal(target.code, "discuss_session_unavailable");
    assert.equal(target.status, 503);
  });
});
