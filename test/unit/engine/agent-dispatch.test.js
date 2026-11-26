import { test, it, describe, before } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { mkdtempSync } from "node:fs";
import { createAgentDispatch } from "../../../engine/agent-dispatch.js";
import { writePendingResult, readPendingResult } from "../../../state/tool-handoff.js";

before(() => {
  process.env.MANIFOLD_DATA_DIR = mkdtempSync(path.join(os.tmpdir(), "manifold-agent-dispatch-"));
});

const logger = { info: () => {}, warn: () => {} };

function makeTurn(overrides = {}) {
  const calls = [];
  const turn = async (args) => {
    calls.push(args);
    if (overrides.throw) throw overrides.throw;
    return overrides.result ?? { text: "reply text", runId: "run-abc" };
  };
  turn.calls = calls;
  return turn;
}

function makeNode(overrides = {}) {
  return { agent: "cyrano", apply: (state, output) => ({ text: output.text }), ...overrides };
}

describe("dispatchAgentWithRetry — the injected turn", () => {
  it("dispatches once and calls apply with the turn's output", async () => {
    const turn = makeTurn();
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: turn, sessionIndex: null, logger });
    let applyCalledWith = null;
    const node = makeNode({ apply: (state, output) => { applyCalledWith = output; return { text: output.text }; } });
    const result = await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", session: "sk", initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-x",
    });
    assert.ok(applyCalledWith !== null);
    assert.deepEqual(result, { text: "reply text" });
    assert.equal(turn.calls.length, 1);
    assert.equal(turn.calls[0].session, "sk");
    assert.equal(turn.calls[0].message, "go");
    assert.equal(turn.calls[0].agentId, "cyrano");
  });

  it("passes the node's skill through to the turn", async () => {
    const turn = makeTurn();
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: turn, sessionIndex: null, logger });
    await dispatchAgentWithRetry({
      node: makeNode({ skill: "writing" }), nodeId: "DRAFT", session: "sk", initialMessage: "go",
      applyCtx: {}, flowId: "flow-x",
    });
    assert.equal(turn.calls[0].skill, "writing");
  });

  it("refuses loudly when no turn function is bound", async () => {
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: null, sessionIndex: null, logger });
    await assert.rejects(
      () => dispatchAgentWithRetry({ node: makeNode(), nodeId: "N", session: "sk", initialMessage: "go", applyCtx: {}, flowId: "f" }),
      /no agent runtime bound/,
    );
  });
});

describe("dispatchAgentWithRetry — apply() contract", () => {
  it("threads needsCtx through to apply() as the 5th positional arg", async () => {
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: makeTurn(), sessionIndex: null, logger });
    let receivedNeeds = "not called";
    const node = makeNode({ apply: (state, output, item, meta, needs) => { receivedNeeds = needs; return {}; } });
    const fakeNeedsCtx = async (varName) => `resolved-${varName}`;
    await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", session: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x", needsCtx: fakeNeedsCtx,
    });
    assert.equal(receivedNeeds, fakeNeedsCtx);
  });

  it("threads needsCtx through to a named-params apply() as ctx.needs", async () => {
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: makeTurn(), sessionIndex: null, logger });
    let receivedNeeds = "not called";
    const node = makeNode({ apply: ({ state, output, needs }) => { receivedNeeds = needs; return {}; } });
    const fakeNeedsCtx = async (varName) => `resolved-${varName}`;
    await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", session: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x", needsCtx: fakeNeedsCtx,
    });
    assert.equal(receivedNeeds, fakeNeedsCtx);
  });

  it("an apply() that doesn't ask for needsCtx is unaffected", async () => {
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: makeTurn(), sessionIndex: null, logger });
    const result = await dispatchAgentWithRetry({
      node: makeNode(), nodeId: "DRAFT", session: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x", needsCtx: async () => "x",
    });
    assert.deepEqual(result, { text: "reply text" });
  });

  it("no retry config — apply() throwing propagates immediately", async () => {
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: makeTurn(), sessionIndex: null, logger });
    const node = makeNode({ apply: () => { throw new Error("no retry configured"); } });
    await assert.rejects(
      () => dispatchAgentWithRetry({ node, nodeId: "N", session: "sk", initialMessage: "go", applyCtx: {}, flowId: "f" }),
      (err) => err.message === "no retry configured",
    );
  });

  it("a stale retry config on the node is fully inert — one attempt, never a retry loop", async () => {
    let callCount = 0;
    const turn = makeTurn({ throw: new Error("first attempt fails") });
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: turn, sessionIndex: null, logger });
    const node = makeNode({ retry: { attempts: 3, delayMs: 100 }, apply: () => ({ ok: true }) });
    await assert.rejects(
      () => dispatchAgentWithRetry({ node, nodeId: "N", session: "sk", initialMessage: "go", applyCtx: {}, flowId: "f" }),
      (err) => err.message === "first attempt fails",
    );
    assert.equal(turn.calls.length, 1);
    assert.equal(callCount, 0);
  });
});

describe("dispatchAgentWithRetry — the delivered tool result", () => {
  it("passes output.toolResult to apply() as the harness returned it", async () => {
    const session = `sk-dispatch-${randomUUID()}`;
    const turn = makeTurn({ result: { text: "", runId: "run-tool", toolResult: { kind: "draft", payload: { text: "hi" } } } });
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: turn, sessionIndex: null, logger });
    let saw = null;
    const node = makeNode({ apply: (state, output) => { saw = output.toolResult; return {}; } });
    await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", session, initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x",
    });
    assert.deepEqual(saw, { kind: "draft", payload: { text: "hi" } });
  });

  it("leaves the hand-off store alone when no tool result was delivered", async () => {
    const session = `sk-dispatch-${randomUUID()}`;
    await writePendingResult({ session, payload: { stale: true } });
    const turn = makeTurn();
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: turn, sessionIndex: null, logger });
    await dispatchAgentWithRetry({
      node: makeNode(), nodeId: "DRAFT", session, initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x",
    });
    assert.ok(await readPendingResult(session), "a row this dispatch did not consume stays put");
  });
});

describe("dispatchAgentWithRetry — session transcript event", () => {
  function makeEmit() {
    const calls = [];
    const emit = async (kind, payload) => { calls.push({ kind, payload }); return { ok: true }; };
    emit.calls = calls;
    return emit;
  }

  it("emits scope=node with nodeId, session and reply text on success", async () => {
    const emit = makeEmit();
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: makeTurn(), sessionIndex: null, logger, emit });
    const result = await dispatchAgentWithRetry({
      node: makeNode(), nodeId: "DRAFT", session: "sk-emit-1", initialMessage: "go",
      applyCtx: { pipelineId: "p" }, applyExtraArg: undefined, flowId: "flow-emit",
    });
    assert.equal(result.text, "reply text");
    assert.equal(emit.calls.length, 1);
    assert.equal(emit.calls[0].kind, "session.transcript.completed");
    assert.deepEqual(emit.calls[0].payload, {
      scope: "node", nodeId: "DRAFT", session: "sk-emit-1", text: "reply text",
    });
  });

  it("skips emission on the tool-fired path (no reply text), and apply() receives the tool result", async () => {
    const session = `sk-emit-skip-${randomUUID()}`;
    const emit = makeEmit();
    const turn = makeTurn({ result: { text: "", runId: "run-emit-skip-1", toolResult: { kind: "draft", payload: { text: "hello" } } } });
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: turn, sessionIndex: null, logger, emit });
    let applySaw = null;
    const node = makeNode({ apply: (state, output) => { applySaw = output?.toolResult ?? null; return { text: output.text }; } });
    const result = await dispatchAgentWithRetry({
      node, nodeId: "DRAFT", session, initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-emit-skip",
    });
    assert.deepEqual(result, { text: "" });
    assert.deepEqual(applySaw, { kind: "draft", payload: { text: "hello" } });
    assert.equal(emit.calls.length, 0, "tool-fired path carries no transcript — nothing emitted");
  });

  it("dispatch without an emitter emits nothing and is otherwise identical", async () => {
    const { dispatchAgentWithRetry } = createAgentDispatch({ runTurn: makeTurn(), sessionIndex: null, logger });
    const result = await dispatchAgentWithRetry({
      node: makeNode(), nodeId: "DRAFT", session: "sk", initialMessage: "go",
      applyCtx: {}, applyExtraArg: undefined, flowId: "flow-x",
    });
    assert.deepEqual(result, { text: "reply text" });
  });
});
