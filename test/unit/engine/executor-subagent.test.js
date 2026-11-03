import { describe, it, before } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { mkdtempSync } from "node:fs";
import { createSubagentDispatch } from "../../../engine/executor-subagent.js";
import { GatewayHandoffNeeded } from "../../../engine/executor-errors.js";
import { writePendingResult, clearPendingResult, readPendingResult } from "../../../state/tool-handoff.js";

// Isolated store home: the poll-based waiter reads the real sqlite store,
// so point it at a tmp dir — never the developer's ~/.manifold.
before(() => {
  process.env.MANIFOLD_DATA_DIR = mkdtempSync(path.join(os.tmpdir(), "manifold-executor-subagent-"));
});

const logger = { info: () => {}, warn: () => {} };

function makeSubagent(opts = {}) {
  return {
    run: opts.run ?? (async () => ({ runId: "run-abc" })),
    waitForRun: opts.waitForRun ?? (async () => ({ status: "ok" })),
    getSessionMessages: opts.getSessionMessages ?? (async () => ({
      messages: [{ role: "assistant", content: "reply text" }],
    })),
  };
}

// ── runSubagentTurn ────────────────────────────────────────────────────────────

describe("runSubagentTurn — no subagent", () => {
  it("throws GatewayHandoffNeeded when subagent is null", async () => {
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => null, sessionIndex: null, logger });
    await assert.rejects(
      () => runSubagentTurn({ sessionKey: "k", message: "hi" }),
      (err) => err instanceof GatewayHandoffNeeded
    );
  });
});

describe("runSubagentTurn — successful turn", () => {
  it("returns last assistant text", async () => {
    const sub = makeSubagent();
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const result = await runSubagentTurn({ sessionKey: "k", message: "hello" });
    assert.equal(result.text, "reply text");
    assert.equal(result.runId, "run-abc");
  });

  it("concatenates array content blocks", async () => {
    const sub = makeSubagent({
      getSessionMessages: async () => ({
        messages: [{
          role: "assistant",
          content: [{ type: "text", text: "part 1" }, { type: "text", text: "part 2" }],
        }],
      }),
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const result = await runSubagentTurn({ sessionKey: "k", message: "hi" });
    assert.ok(result.text.includes("part 1") && result.text.includes("part 2"), result.text);
  });

  it("skips non-assistant messages and finds last assistant", async () => {
    const sub = makeSubagent({
      getSessionMessages: async () => ({
        messages: [
          { role: "user", content: "user msg" },
          { role: "assistant", content: "first reply" },
          { role: "user", content: "follow up" },
          { role: "assistant", content: "final reply" },
        ],
      }),
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const result = await runSubagentTurn({ sessionKey: "k", message: "x" });
    assert.equal(result.text, "final reply");
  });

  it("throws when waitForRun returns non-ok status", async () => {
    const sub = makeSubagent({ waitForRun: async () => ({ status: "timeout", error: "agent timed out" }) });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    await assert.rejects(
      () => runSubagentTurn({ sessionKey: "k", message: "x" }),
      (err) => err.message.includes("timeout")
    );
  });
});

// ── runSubagentTurn — tool-result race (tool-handoff.js sqlite row) ─────
//
// A deliver_result write for this session resolves the race before
// waitForRun settles: same early-exit as the old in-process wake-up, but
// the rendezvous is a store row, so the writer may live in another
// process (MCP server) than this waiter.

describe("runSubagentTurn — tool-result race", () => {
  it("returns early with empty text + toolResult when a row lands before waitForRun settles", async () => {
    const sessionKey = `sk-race-${randomUUID()}`;
    let getSessionMessagesCalled = false;
    const sub = makeSubagent({
      run: async () => ({ runId: "run-race-1" }),
      // Never settles on its own within the test — only the tool-result
      // row should resolve this race.
      waitForRun: () => new Promise(() => {}),
      getSessionMessages: async () => { getSessionMessagesCalled = true; return { messages: [] }; },
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const pending = runSubagentTurn({ sessionKey, message: "x" });
    // runSubagentTurn doesn't start polling until AFTER `await
    // subagent.run(...)` resolves (at least one microtask later). The
    // waiter checks the store once on entry, so a pre-existing row still
    // resolves; the setImmediate below just mirrors the realistic
    // write-after-start ordering.
    await new Promise((resolve) => setImmediate(resolve));
    await writePendingResult({ sessionKey, runId: "run-race-1", kind: "score", payload: { score: 9 } });
    const result = await pending;
    assert.deepEqual(result, {
      text: "",
      runId: "run-race-1",
      toolResult: { kind: "score", payload: { score: 9 } },
    });
    assert.equal(getSessionMessagesCalled, false, "tool-result path must skip fetching the model's own wrap-up reply");
    await clearPendingResult(sessionKey);
  });

  it("a row for a DIFFERENT session does not resolve this dispatch's race", async () => {
    const otherKey = `sk-other-${randomUUID()}`;
    const sub = makeSubagent({
      run: async () => ({ runId: "run-race-2" }),
      waitForRun: async () => ({ status: "ok" }), // settles quickly, normal path
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    await writePendingResult({ sessionKey: otherKey, payload: { v: 1 } });
    const result = await runSubagentTurn({ sessionKey: `sk-race-${randomUUID()}`, message: "x" });
    assert.equal(result.text, "reply text", "must take the normal waitForRun path, unaffected by an unrelated row");
    await clearPendingResult(otherKey);
  });

  it("when waitForRun settles first (ok), the normal message-fetch path runs and the poller is left cancelled", async () => {
    const sub = makeSubagent({ run: async () => ({ runId: "run-race-3" }) });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const result = await runSubagentTurn({ sessionKey: `sk-race-${randomUUID()}`, message: "x" });
    assert.equal(result.text, "reply text");
    assert.equal(result.toolResult ?? null, null, "normal path carries no tool result");
  });

  it("when waitForRun rejects, the poller is cancelled and the rejection still propagates", async () => {
    const sub = makeSubagent({
      run: async () => ({ runId: "run-race-4" }),
      waitForRun: async () => { throw new Error("transport error"); },
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    await assert.rejects(
      () => runSubagentTurn({ sessionKey: `sk-race-${randomUUID()}`, message: "x" }),
      (err) => err.message === "transport error",
    );
  });
});

// ── runSubagentTurn — no manifold-side deadline ──────────────────────────────

describe("runSubagentTurn — no manifold-side deadline", () => {
  it("waits for the run's own end: a silent run is awaited even if the node still sets timeoutMs/idleTimeoutMs, and agent.wait gets the maximum timer delay, never a node value or the gateway's 30s default", async () => {
    let waitParams = null;
    const sub = makeSubagent({
      run: async () => ({ runId: "run-silent-1" }),
      waitForRun: (params) => {
        waitParams = params;
        return new Promise((resolve) => setTimeout(() => resolve({ status: "ok" }), 60));
      },
    });
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const node = {
      agent: "scorer",
      timeoutMs: 10,
      idleTimeoutMs: 10,
      apply: (state, output) => ({ text: output.text }),
    };
    const result = await dispatchAgentWithRetry({
      node, nodeId: "SCORE_LLM", sessionKey: "sk-silent-1", initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-silent",
    });
    assert.deepEqual(result, { text: "reply text" });
    assert.deepEqual(waitParams, { runId: "run-silent-1", timeoutMs: 2_147_483_647 });
  });

  it("a run that the runtime itself ends with a timeout still surfaces as a failure", async () => {
    const sub = makeSubagent({
      run: async () => ({ runId: "run-silent-2" }),
      waitForRun: async () => ({ status: "timeout" }),
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    await assert.rejects(
      () => runSubagentTurn({ sessionKey: "sk-silent-2", message: "x" }),
      /subagent timeout: no error message/,
    );
  });
});

// ── dispatchAgentWithRetry ───────────────────────────────────────────────────

describe("dispatchAgentWithRetry — no retry config", () => {
  it("dispatches once and calls apply", async () => {
    const sub = makeSubagent();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    let applyCalledWith = null;
    const node = {
      agent: "cyrano",
      apply: (state, output) => { applyCalledWith = output; return { text: output.text }; },
    };
    const result = await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey: "sk", initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-x",
    });
    assert.ok(applyCalledWith !== null);
    assert.deepEqual(result, { text: "reply text" });
  });

  it("threads needsCtx through to apply() as the 5th positional arg — found missing while migrating a real pipeline's apply() off full-clone inheritance", async () => {
    const sub = makeSubagent();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    let receivedNeeds = "not called";
    const node = {
      agent: "cyrano",
      apply: (state, output, item, meta, needs) => { receivedNeeds = needs; return {}; },
    };
    const fakeNeedsCtx = async (varName) => `resolved-${varName}`;
    await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x", needsCtx: fakeNeedsCtx,
    });
    assert.equal(receivedNeeds, fakeNeedsCtx);
  });

  it("threads needsCtx through to a named-params apply() as ctx.needs", async () => {
    const sub = makeSubagent();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    let receivedNeeds = "not called";
    const node = {
      agent: "cyrano",
      apply: ({ state, output, needs }) => { receivedNeeds = needs; return {}; },
    };
    const fakeNeedsCtx = async (varName) => `resolved-${varName}`;
    await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x", needsCtx: fakeNeedsCtx,
    });
    assert.equal(receivedNeeds, fakeNeedsCtx);
  });

  it("an apply() that doesn't ask for needsCtx is unaffected — additive, not a breaking signature change", async () => {
    const sub = makeSubagent();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const node = {
      agent: "cyrano",
      apply: (state, output) => ({ text: output.text }),
    };
    const result = await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x", needsCtx: async () => "x",
    });
    assert.deepEqual(result, { text: "reply text" });
  });
});

describe("dispatchAgentWithRetry — retry config is inert (single dispatch, no retry loop)", () => {
  it("no retry config — apply() throwing on the only attempt propagates immediately", async () => {
    const sub = makeSubagent();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const node = { agent: "a", apply: () => { throw new Error("no retry configured"); } };
    await assert.rejects(
      () => dispatchAgentWithRetry({ node, nodeId: "N", sessionKey: "sk", initialMessage: "go", applyCtx: {}, flowId: "f" }),
      (err) => err.message === "no retry configured",
    );
  });

  it("a stale retry config on the node is fully inert — throws after exactly one dispatch attempt, never retries", async () => {
    let callCount = 0;
    const sub = makeSubagent({
      run: async () => { callCount++; throw new Error("first attempt fails"); },
    });
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const node = {
      agent: "a",
      retry: { attempts: 3, delayMs: 100 },
      apply: () => ({ ok: true }),
    };
    await assert.rejects(
      () => dispatchAgentWithRetry({ node, nodeId: "N", sessionKey: "sk", initialMessage: "go", applyCtx: {}, flowId: "f" }),
      (err) => err.message === "first attempt fails",
    );
    assert.equal(callCount, 1);
  });
});

// ── dispatchAgentWithRetry — session transcript event ─────────────────────────
// The dispatch reply is otherwise visible only inside apply(). Nullable like
// sessionIndex — dispatch without an emitter emits nothing and is otherwise
// identical.

describe("dispatchAgentWithRetry — session.transcript.completed", () => {
  function makeEmit() {
    const calls = [];
    const emit = async (kind, payload) => {
      calls.push({ kind, payload });
      return { ok: true, kind, deliveries: {} };
    };
    emit.calls = calls;
    return emit;
  }

  it("emits scope=node with nodeId, sessionKey and reply text on success", async () => {
    const sub = makeSubagent();
    const emit = makeEmit();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger, emit });
    const node = {
      agent: "cyrano",
      apply: (state, output) => ({ text: output.text }),
    };
    const result = await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey: "sk-emit-1", initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-emit",
    });
    assert.equal(result.text, "reply text");
    assert.equal(emit.calls.length, 1);
    assert.equal(emit.calls[0].kind, "session.transcript.completed");
    assert.deepEqual(emit.calls[0].payload, {
      scope: "node",
      nodeId: "DRAFT",
      sessionKey: "sk-emit-1",
      text: "reply text",
    });
  });

  it("skips emission on the tool-fired path (no reply text), and apply() receives the tool result", async () => {
    const sessionKey = `sk-emit-skip-${randomUUID()}`;
    const sub = makeSubagent({
      run: async () => ({ runId: "run-emit-skip-1" }),
      waitForRun: () => new Promise(() => {}),
    });
    const emit = makeEmit();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger, emit });
    let applySaw = null;
    const node = {
      agent: "cyrano",
      apply: (state, output) => { applySaw = output?.toolResult ?? null; return { text: output.text }; },
    };
    const pending = dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey, initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-emit-skip",
    });
    await new Promise((resolve) => setImmediate(resolve));
    await writePendingResult({ sessionKey, runId: "run-emit-skip-1", kind: "draft", payload: { text: "hello" } });
    const result = await pending;
    assert.deepEqual(result, { text: "" });
    assert.deepEqual(applySaw, { kind: "draft", payload: { text: "hello" } }, "apply() receives the delivered result");
    assert.equal(emit.calls.length, 0, "tool-fired path carries no transcript — nothing emitted");
    assert.equal(await readPendingResult(sessionKey), null, "consumed rows do not linger for the next turn");
  });

  it("dispatch without an emitter emits nothing and is otherwise identical", async () => {
    const sub = makeSubagent();
    const { dispatchAgentWithRetry } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const node = {
      agent: "cyrano",
      apply: (state, output) => ({ text: output.text }),
    };
    const result = await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", sessionKey: "sk", initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-x",
    });
    assert.deepEqual(result, { text: "reply text" });
  });
});
