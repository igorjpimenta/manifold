import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createRunTurn, createGatewaySubagentGate } from "../../../../../plugins/openclaw/manifold/run-turn.js";
import { createToolResults } from "../../../../../plugins/openclaw/manifold/tool-results.js";
import { GatewayHandoffNeeded } from "../../../../../engine/executor-errors.js";

const logger = { info: () => {}, warn: () => {} };
// One buffer per test, exactly like one turn per plugin registration.
const toolResults = createToolResults();
const STARTUP = () => new Error("agent.wait unavailable during gateway startup");

function deferred() {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
}

const tick = () => new Promise((r) => setTimeout(r, 20));

function makeRuntime(overrides = {}) {
  return {
    run: overrides.run ?? (async () => ({ runId: "run-abc" })),
    waitForRun: overrides.waitForRun ?? (async () => ({ status: "ok" })),
    getSessionMessages: overrides.getSessionMessages ?? (async () => ({
      messages: [{ role: "assistant", content: "reply text" }],
    })),
  };
}


describe("createGatewaySubagentGate", () => {
  test("no RPC reaches the runtime until whenGatewayReady settles", async () => {
    const ready = deferred();
    const calls = [];
    const raw = { run: async (p) => { calls.push(p); return { runId: "r1" }; } };
    const gate = createGatewaySubagentGate({ whenGatewayReady: () => ready.promise });
    const pending = gate(raw).run({ sessionKey: "k" });
    await tick();
    assert.deepEqual(calls, [], "no call inside the startup window");
    ready.resolve();
    await pending;
    assert.equal(calls.length, 1);
  });

  test("a missing runtime stays missing (null passes through)", () => {
    const gate = createGatewaySubagentGate({});
    assert.equal(gate(null), null);
    assert.equal(gate(undefined), undefined);
  });

  test("gates the same raw runtime only once", () => {
    const gate = createGatewaySubagentGate({});
    const raw = makeRuntime();
    assert.equal(gate(raw), gate(raw));
  });
});


describe("createRunTurn — the turn", () => {
  test("returns the last assistant text with the run id", async () => {
    const seen = [];
    const raw = makeRuntime({
      run: async (args) => { seen.push(["run", args]); return { runId: "run-abc" }; },
      getSessionMessages: async (args) => { seen.push(["msgs", args]); return { messages: [{ role: "assistant", content: "reply text" }] }; },
    });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    const out = await runTurn({ session: "k", message: "hi" });
    assert.equal(seen[0][1].sessionKey, "k", "core session maps to the harness sessionKey at run");
    assert.equal(seen[1][1].sessionKey, "k", "core session maps to the harness sessionKey at getSessionMessages");
    assert.equal(out.text, "reply text");
    assert.equal(out.runId, "run-abc");
    assert.equal(out.toolResult ?? null, null);
  });

  test("addresses the runtime and reports back the gateway's folded session", async () => {
    const seen = [];
    const raw = makeRuntime({
      run: async (args) => { seen.push(["run", args]); return { runId: "run-case" }; },
      getSessionMessages: async (args) => { seen.push(["msgs", args]); return { messages: [{ role: "assistant", content: "ok" }] }; },
    });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    const out = await runTurn({ session: "agent:SCOUT:manifold:abc-SCORE_LLM", message: "hi" });
    assert.equal(seen[0][1].sessionKey, "agent:scout:manifold:abc-score_llm");
    assert.equal(seen[1][1].sessionKey, "agent:scout:manifold:abc-score_llm");
    assert.equal(out.text, "ok");
  });

  test("refuses loudly when no runtime is bound", async () => {
    const { runTurn } = createRunTurn({ getRuntime: () => null, logger, toolResults });
    await assert.rejects(
      () => runTurn({ session: "k", message: "x" }),
      (err) => err instanceof GatewayHandoffNeeded,
    );
  });

  test("a startup refusal is no longer a request-scope handoff", async () => {
    const raw = makeRuntime({ waitForRun: async () => { throw STARTUP(); } });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    await assert.rejects(
      () => runTurn({ session: "k", message: "x" }),
      (err) => !(err instanceof GatewayHandoffNeeded) && /unavailable during gateway startup/.test(err.message),
    );
  });

  test("a non-ok wait status fails the turn with the status in the message", async () => {
    const raw = makeRuntime({ waitForRun: async () => ({ status: "timeout", error: "agent timed out" }) });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    await assert.rejects(
      () => runTurn({ session: "k", message: "x" }),
      /agent turn timeout: agent timed out/,
    );
  });

  test("joins array content blocks into the reply text", async () => {
    const raw = makeRuntime({
      getSessionMessages: async () => ({
        messages: [{ role: "assistant", content: [{ type: "text", text: "part 1" }, { type: "text", text: "part 2" }] }],
      }),
    });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    const out = await runTurn({ session: "k", message: "x" });
    assert.ok(out.text.includes("part 1") && out.text.includes("part 2"), out.text);
  });

  test("skips non-assistant messages and finds the last assistant one", async () => {
    const raw = makeRuntime({
      getSessionMessages: async () => ({
        messages: [
          { role: "user", content: "user msg" },
          { role: "assistant", content: "first reply" },
          { role: "user", content: "follow up" },
          { role: "assistant", content: "final reply" },
        ],
      }),
    });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    const out = await runTurn({ session: "k", message: "x" });
    assert.equal(out.text, "final reply");
  });

  test("resolves early with the delivered tool result, skipping the wrap-up fetch", async () => {
    const session = `sk-turn-${randomUUID()}`;
    let fetched = false;
    const raw = makeRuntime({
      waitForRun: () => new Promise(() => {}),
      getSessionMessages: async () => { fetched = true; return { messages: [] }; },
    });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    const pending = runTurn({ session, message: "x" });
    await new Promise((r) => setImmediate(r));
    toolResults.write({ session, runId: "run-tool", kind: "score", payload: { score: 7 } });
    const out = await pending;
    assert.equal(out.text, "");
    assert.deepEqual(out.toolResult, { kind: "score", payload: { score: 7 } });
    assert.equal(fetched, false, "the tool-fired path must not fetch the model's wrap-up reply");
    assert.equal(toolResults.read(session), null, "the delivered row is consumed by the turn");
  });

  test("finds a result the model delivered after its run ended, and consumes it", async () => {
    const session = `sk-late-${randomUUID()}`;
    const raw = makeRuntime({
      getSessionMessages: async () => {
        // A retried tool call landing after waitForRun already returned.
        toolResults.write({ session, kind: "score", payload: { score: 41 } });
        return { messages: [{ role: "assistant", content: "submitted" }] };
      },
    });
    const { runTurn } = createRunTurn({ getRuntime: () => raw, logger, toolResults });
    const out = await runTurn({ session, message: "x" });
    assert.equal(out.text, "", "the tool payload wins over the model's confirmation text");
    assert.deepEqual(out.toolResult, { kind: "score", payload: { score: 41 } });
    assert.equal(toolResults.read(session), null);
  });

  test("never resurrects a row stamped before the turn started", async () => {
    const session = `sk-stale-${randomUUID()}`;
    toolResults.write({ session, kind: "score", payload: { score: 1 } });
    await new Promise((r) => setTimeout(r, 5));
    const { runTurn } = createRunTurn({ getRuntime: () => makeRuntime(), logger, toolResults });
    const out = await runTurn({ session, message: "x" });
    assert.equal(out.toolResult ?? null, null);
    assert.ok(toolResults.read(session), "a previous attempt's row is left alone");
    toolResults.clear(session);
  });

  test("waits for gateway readiness before the first call", async () => {
    const ready = deferred();
    const calls = [];
    const raw = {
      run: async () => { calls.push("run"); return { runId: "run-boot" }; },
      waitForRun: async () => { calls.push("wait"); return { status: "ok" }; },
      getSessionMessages: async () => ({ messages: [{ role: "assistant", content: "drafted" }] }),
    };
    const { runTurn } = createRunTurn({ getRuntime: () => raw, whenGatewayReady: () => ready.promise, logger, toolResults });
    const turn = runTurn({ session: "k", message: "continue" });
    await tick();
    assert.deepEqual(calls, [], "no run started inside the startup window");
    ready.resolve();
    assert.equal((await turn).text, "drafted");
    assert.deepEqual(calls, ["run", "wait"]);
  });
});
