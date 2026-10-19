import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createSubagentDispatch } from "../../../engine/executor-subagent.js";
import { GatewayHandoffNeeded } from "../../../engine/executor-errors.js";
import { notifyToolFired } from "../../../state/tool-handoff.js";

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

// ── runSubagentTurn — tool-fired race (tool-handoff.js notifyToolFired) ────
//
// Fixes the 788-occurrence "parse error: no valid score" failure class:
// waitForRun only settles when the WHOLE run reaches its own natural end,
// even when the one tool call a node cares about already landed. A hook (e.g. hooks/score-tool-wake.js) calls notifyToolFired(runId)
// the instant that tool fires; runSubagentTurn races waitForRun against
// waitForToolSignal(runId) so it can return early instead of waiting for —
// or timing out against — the rest of the turn.

describe("runSubagentTurn — tool-fired race", () => {
  it("returns early with empty text when notifyToolFired fires before waitForRun settles", async () => {
    let getSessionMessagesCalled = false;
    const sub = makeSubagent({
      run: async () => ({ runId: "run-race-1" }),
      // Never settles on its own within the test — only the tool-fired
      // signal should resolve this race.
      waitForRun: () => new Promise(() => {}),
      getSessionMessages: async () => { getSessionMessagesCalled = true; return { messages: [] }; },
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const pending = runSubagentTurn({ sessionKey: "k", message: "x" });
    // runSubagentTurn doesn't call waitForToolSignal until AFTER `await
    // subagent.run(...)` resolves (at least one microtask later) — calling
    // notifyToolFired synchronously here would fire before anything is
    // listening (found this the hard way: passed locally on timing, hung
    // for the full 60s default test timeout in CI). setImmediate flushes
    // the whole microtask queue first, guaranteeing the waiter is
    // registered before we notify it.
    await new Promise((resolve) => setImmediate(resolve));
    notifyToolFired("run-race-1");
    const result = await pending;
    assert.deepEqual(result, { text: "", runId: "run-race-1" });
    assert.equal(getSessionMessagesCalled, false, "tool-fired path must skip fetching the model's own wrap-up reply");
  });

  it("a notifyToolFired call for a DIFFERENT runId does not resolve this dispatch's race", async () => {
    const sub = makeSubagent({
      run: async () => ({ runId: "run-race-2" }),
      waitForRun: async () => ({ status: "ok" }), // settles quickly, normal path
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    notifyToolFired("some-other-run-id");
    const result = await runSubagentTurn({ sessionKey: "k", message: "x" });
    assert.equal(result.text, "reply text", "must take the normal waitForRun path, unaffected by an unrelated notify");
  });

  it("when waitForRun settles first (ok), the normal message-fetch path runs and the tool signal is left cancelled", async () => {
    const sub = makeSubagent({ run: async () => ({ runId: "run-race-3" }) });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    const result = await runSubagentTurn({ sessionKey: "k", message: "x" });
    assert.equal(result.text, "reply text");
    // A late notify for this now-finished dispatch must not throw or affect
    // anything — the waiter was already cancelled once the race was decided.
    assert.doesNotThrow(() => notifyToolFired("run-race-3"));
  });

  it("when waitForRun rejects, the tool signal is cancelled and the rejection still propagates", async () => {
    const sub = makeSubagent({
      run: async () => ({ runId: "run-race-4" }),
      waitForRun: async () => { throw new Error("transport error"); },
    });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => sub, sessionIndex: null, logger });
    await assert.rejects(
      () => runSubagentTurn({ sessionKey: "k", message: "x" }),
      (err) => err.message === "transport error",
    );
    assert.doesNotThrow(() => notifyToolFired("run-race-4"));
  });
});

// ── runSubagentTurn — no pipeclaw-side deadline ──────────────────────────────

describe("runSubagentTurn — no pipeclaw-side deadline", () => {
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
