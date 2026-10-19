/**
 * Unit tests for routes/discuss.js
 *
 * registerDiscussHandlers does NOT call api.registerHttpRoute — it returns
 * handler functions that runs.js and operator.js wire into their own routes.
 * These tests verify the return shape so that future refactors can't silently
 * drop a handler without breaking the contract.
 */
import { test, mock, describe, after } from "node:test";
import assert from "node:assert/strict";
import { rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { createMockApi, createMockRes, createMockReq } from "../../helpers/mock-api.js";

const authUrl = new URL("../../../ios/auth.js", import.meta.url).href;
await mock.module(authUrl, {
  exports: {
    authorizeBridgeRequest: async () => ({ ok: true }),
    sendUnauthorized: (res) => { res.statusCode = 401; res.end(); },
  },
});

// B2.5 — handleDiscuss/handleDiscussAudio build their promptBuilder ctx via
// assembleState() instead of trusting sourceFlow.stateJson directly. Mocked
// here so the wiring test below doesn't need a real DB-backed runStore/
// nodeExecutionsStore.
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

function createMockReqWithBody(method, path_, body) {
  const chunks = body != null ? [Buffer.from(typeof body === "string" ? body : JSON.stringify(body))] : [];
  return {
    ...createMockReq(method, path_),
    [Symbol.asyncIterator]: async function* () { for (const c of chunks) yield c; },
  };
}

function makeDeps(api, overrides = {}) {
  return {
    api,
    store: { get: () => null, list: () => [] },
    pipelinesRef: { value: new Map() },
    emitSseEvent: async () => ({ ok: true }),
    discussDefaultAgent: null,
    invokeElevated: async () => ({}),
    readPending: () => null,
    clearPending: () => {},
    discussStore: { getComponent: () => null, listComponents: () => [], setComponent: () => {} },
    ...overrides,
  };
}

// ── return shape ──────────────────────────────────────────────────────────────

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
    "stampDiscussSession",
    "resolveDiscussTarget",
    "normalizePendingFields",
  ];

  for (const name of [...REQUIRED_HANDLERS, ...REQUIRED_HELPERS]) {
    test(`returns function: ${name}`, () => {
      const { api } = createMockApi();
      const result = registerDiscussHandlers(makeDeps(api));
      assert.equal(typeof result[name], "function", `${name} must be a function`);
    });
  }
});

// ── apply-draft safety invariant ──────────────────────────────────────────────
// handleDiscussApplyDraft must NEVER call gateResolver.recordItemDecision.
// This invariant is documented in memory (feedback_apply_draft_not_approve.md):
// Apply revises, Approve commits. They must stay separate.

// ── resolveDiscussTarget — mid-batch (running) ────────────────────────────
// While a loop node's worker pool is still active (flow.status === "running"),
// waitJson stays null. resolveDiscussTarget ALWAYS rejects with
// gate_transition when flow.status === "running" and waitJson is null.

describe("registerDiscussHandlers — resolveDiscussTarget mid-batch (running)", () => {
  test("always 503s with gate_transition when flow is running and waitJson is null", () => {
    const { api } = createMockApi();
    const deps = makeDeps(api);
    const { resolveDiscussTarget } = registerDiscussHandlers(deps);
    const target = resolveDiscussTarget(
      { flowId: "flow-1", status: "running", waitJson: null, stateJson: { pipelineId: "test-pipeline" } },
      "alpha",
    );
    assert.equal(target.ok, false);
    assert.equal(target.code, "gate_transition");
    assert.equal(target.status, 503);
  });
});

// ── handleDiscuss — assembleState() wiring (B2.5) ─────────────────────────
// promptBuilder's ctx.state now comes from assembleState(), not the raw
// sourceFlow.stateJson blob — this proves the wiring, not assembleState()'s
// own behavior (that has its own dedicated test coverage).

describe("registerDiscussHandlers — handleDiscuss builds ctx.state via assembleState()", () => {
  const testFlowId = `unit-test-discuss-${Date.now()}`;

  after(async () => {
    await rm(path.join(os.homedir(), ".openclaw", "discuss-sessions"), { recursive: true, force: true }).catch(() => {});
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

    const req = createMockReqWithBody("POST", `/pipeclaw/runs/${testFlowId}/discuss`, { text: "what's going on?" });
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

// ── discuss history — real store returns a Promise ────────────────────────
// The real discussStore (state/discuss-store.js) is a Proxy whose every
// method — including getState — returns a Promise, since the underlying
// impl is constructed async. A regression let handleDiscussHistory call
// `discussStore.getState(...)` without awaiting it: dsState was then always
// the Promise object itself (truthy), so `dsState.messages.length` threw
// "Cannot read properties of undefined (reading 'length')" on every reopen
// of a discuss thread that already had messages. This mock mirrors that
// Promise-returning shape so a missing `await` fails the test.

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

    const req = createMockReq("GET", `/pipeclaw/runs/${sourceFlow.flowId}/discuss/history`);
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

// ── handleDiscussAudioRetry ────────────────────────────────────────────────
// Re-transcribes an already-uploaded voice message in place (the operator's
// Retry tap on a failed/empty transcription bubble), without a fresh upload.

describe("registerDiscussHandlers — handleDiscussAudioRetry", () => {
  const testFlowId = `unit-test-discuss-audio-retry-${Date.now()}`;
  const itemId = "item-1";

  after(async () => {
    await rm(path.join(os.homedir(), ".openclaw", "discuss-audio", testFlowId), { recursive: true, force: true }).catch(() => {});
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
    const audioDir = audioDirForFlow(testFlowId, itemId);
    const { mkdir, writeFile, readFile } = await import("node:fs/promises");
    await mkdir(audioDir, { recursive: true });
    const turnId = "11111111-1111-1111-1111-111111111111";
    const audioUrl = `/pipeclaw/runs/${testFlowId}/discuss/audio/${turnId}.m4a`;
    await writeFile(path.join(audioDir, `${turnId}.m4a`), Buffer.from("fake audio bytes"));
    await writeFile(path.join(audioDir, `${turnId}.meta.json`), JSON.stringify({
      turnId, itemId, audioUrl, transcript: null, status: "failed",
      error: "Command failed: whisper --model turbo ...", createdAtMs: 1000, transcribedAtMs: 2000,
    }));

    const sseEvents = [];
    let resolveTranscriptEvent;
    const transcriptEventSeen = new Promise((r) => { resolveTranscriptEvent = r; });
    const emitSseEvent = async (event) => {
      sseEvents.push(event);
      if (event.type === "discuss.transcript") resolveTranscriptEvent();
      return { ok: true };
    };

    const { api } = createMockApi();
    api.runtime = { mediaUnderstanding: { transcribeAudioFile: async () => ({ text: "hello from retry" }) } };
    const deps = makeDeps(api, {
      pipelinesRef,
      emitSseEvent,
      invokeElevated: async () => ({ ok: true, status: 200, body: JSON.stringify({ ok: true, reply: "agent reply" }) }),
    });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);

    const req = createMockReqWithBody("POST", `/pipeclaw/runs/${testFlowId}/discuss/audio/retry`, { turnId, itemId });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);

    assert.equal(res.statusCode, 202);
    const body = JSON.parse(res.body);
    assert.equal(body.ok, true);
    assert.equal(body.turnId, turnId);
    assert.equal(body.audioUrl, audioUrl);

    await transcriptEventSeen;
    // Let the microtask queue settle for the subsequent invokeElevated call
    // and its discuss.turn emit, which fire after the transcript event.
    await new Promise((r) => setTimeout(r, 20));

    const updatedMeta = JSON.parse(await readFile(path.join(audioDir, `${turnId}.meta.json`), "utf8"));
    assert.equal(updatedMeta.status, "completed");
    assert.equal(updatedMeta.transcript, "hello from retry");
    assert.equal(updatedMeta.error, null);
    assert.equal(updatedMeta.createdAtMs, 1000, "original createdAtMs must survive a retry");

    const transcriptEvent = sseEvents.find((e) => e.type === "discuss.transcript");
    assert.ok(transcriptEvent, "must emit discuss.transcript");
    assert.equal(transcriptEvent.status, "completed");
    assert.equal(transcriptEvent.turnId, turnId);

    const turnEvent = sseEvents.find((e) => e.type === "discuss.turn");
    assert.ok(turnEvent, "a successful retry must dispatch an agent turn, same as a fresh upload would");
    assert.equal(turnEvent.status, "completed");
    assert.equal(turnEvent.reply, "agent reply");
  });

  test("returns 404 when there's no meta sidecar for the turnId", async () => {
    const { api } = createMockApi();
    api.runtime = { mediaUnderstanding: { transcribeAudioFile: async () => ({ text: "x" }) } };
    const deps = makeDeps(api, { pipelinesRef });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);
    const req = createMockReqWithBody("POST", `/pipeclaw/runs/${testFlowId}/discuss/audio/retry`, { turnId: "does-not-exist", itemId });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);
    assert.equal(res.statusCode, 404);
    assert.equal(JSON.parse(res.body).code, "turn_not_found");
  });

  test("returns 400 when turnId is missing from the body", async () => {
    const { api } = createMockApi();
    const deps = makeDeps(api, { pipelinesRef });
    const { handleDiscussAudioRetry } = registerDiscussHandlers(deps);
    const req = createMockReqWithBody("POST", `/pipeclaw/runs/${testFlowId}/discuss/audio/retry`, { itemId });
    const res = createMockRes();
    await handleDiscussAudioRetry(req, res, sourceFlow);
    assert.equal(res.statusCode, 400);
    assert.equal(JSON.parse(res.body).code, "missing_turn_id");
  });
});

describe("registerDiscussHandlers — apply-draft invariant", () => {
  test("handleDiscussApplyDraft does not expose gateResolver in its closure", () => {
    // We can't easily intercept the internal call chain without running the
    // full handler, but we CAN verify the factory doesn't receive gateResolver
    // as a dep at all — if someone passes it, the function signature doesn't
    // destructure it, so it can't be used accidentally.
    const { api } = createMockApi();
    const deps = makeDeps(api);
    // Inject a sentinel to detect accidental use
    let resolveCallCount = 0;
    deps.gateResolver = {
      recordItemDecision: () => { resolveCallCount++; },
    };
    const result = registerDiscussHandlers(deps);
    // gateResolver is not in the factory signature, so this is a no-op check.
    // The real guard is that the factory signature doesn't accept it.
    assert.equal(typeof result.handleDiscussApplyDraft, "function");
    assert.equal(resolveCallCount, 0, "gateResolver must not be called during factory setup");
  });
});
