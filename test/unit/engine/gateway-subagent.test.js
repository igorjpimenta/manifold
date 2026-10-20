// Unit tests for engine/gateway-subagent.js — createGatewaySubagentGate: readiness wait before calls, pass-through.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createGatewaySubagentGate } from "../../../engine/gateway-subagent.js";
import { createSubagentDispatch } from "../../../engine/executor-subagent.js";
import { GatewayHandoffNeeded } from "../../../engine/executor-errors.js";

// ─── helpers ─────────────────────────────────────────────────────────────────

const logger = { info: () => {}, warn: () => {} };
const STARTUP = () => new Error("agent.wait unavailable during gateway startup");

function deferred() {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
}

const tick = () => new Promise((r) => setTimeout(r, 20));

// ─── readiness wait ──────────────────────────────────────────────────────────

describe("gate — waits for gateway readiness before calling", () => {
  test("no RPC reaches the gateway until whenGatewayReady settles", async () => {
    const ready = deferred();
    const calls = [];
    const raw = { run: async (p) => { calls.push(p); return { runId: "r1" }; } };
    const gate = createGatewaySubagentGate({ whenGatewayReady: () => ready.promise });
    const pending = gate(raw).run({ sessionKey: "k" });
    await tick();
    assert.equal(calls.length, 0);
    ready.resolve();
    assert.deepEqual(await pending, { runId: "r1" });
    assert.equal(calls.length, 1);
  });

  test("once ready, calls go straight through", async () => {
    const raw = { waitForRun: async () => ({ status: "ok" }) };
    const gate = createGatewaySubagentGate({ whenGatewayReady: async () => {} });
    assert.deepEqual(await gate(raw).waitForRun({ runId: "r" }), { status: "ok" });
  });

  test("a call refused anyway surfaces as-is — no retry", async () => {
    let n = 0;
    const raw = { waitForRun: async () => { n += 1; throw STARTUP(); } };
    const gate = createGatewaySubagentGate({ whenGatewayReady: async () => {} });
    await assert.rejects(() => gate(raw).waitForRun({ runId: "r" }), /unavailable during gateway startup/);
    assert.equal(n, 1);
  });
});

// ─── pass-through ────────────────────────────────────────────────────────────

describe("gate — pass-through", () => {
  test("a missing runtime stays missing", () => {
    const gate = createGatewaySubagentGate({});
    assert.equal(gate(null), null);
    assert.equal(gate(undefined), undefined);
  });

  test("members it doesn't gate reach the raw runtime unchanged", async () => {
    const raw = { deleteSession: async (p) => ({ deleted: p.sessionKey }), run: async () => ({}) };
    const gate = createGatewaySubagentGate({ whenGatewayReady: () => new Promise(() => {}) });
    assert.deepEqual(await gate(raw).deleteSession({ sessionKey: "s" }), { deleted: "s" });
  });

  test("the same runtime object always gets the same wrapper", () => {
    const raw = { run: async () => ({}) };
    const gate = createGatewaySubagentGate({});
    assert.equal(gate(raw), gate(raw));
  });
});

// ─── through the engine's dispatch ───────────────────────────────────────────

describe("dispatch through the gate — a revival landing in the startup window", () => {
  test("nothing is dispatched until the gateway is ready, then the turn completes", async () => {
    const ready = deferred();
    const calls = [];
    const raw = {
      run: async () => { calls.push("run"); return { runId: "run-boot" }; },
      waitForRun: async () => { calls.push("wait"); return { status: "ok" }; },
      getSessionMessages: async () => ({ messages: [{ role: "assistant", content: "drafted" }] }),
    };
    const gate = createGatewaySubagentGate({ whenGatewayReady: () => ready.promise });
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => gate(raw), sessionIndex: null, logger });
    const turn = runSubagentTurn({ sessionKey: "k", message: "continue" });
    await tick();
    assert.deepEqual(calls, [], "no run started inside the startup window");
    ready.resolve();
    assert.equal((await turn).text, "drafted");
    assert.deepEqual(calls, ["run", "wait"]);
  });

  test("a startup refusal is no longer a request-scope handoff", async () => {
    const raw = {
      run: async () => ({ runId: "run-boot" }),
      waitForRun: async () => { throw STARTUP(); },
    };
    const { runSubagentTurn } = createSubagentDispatch({ getSubagent: () => raw, sessionIndex: null, logger });
    await assert.rejects(
      () => runSubagentTurn({ sessionKey: "k", message: "x" }),
      (err) => !(err instanceof GatewayHandoffNeeded) && /unavailable during gateway startup/.test(err.message),
    );
  });
});
