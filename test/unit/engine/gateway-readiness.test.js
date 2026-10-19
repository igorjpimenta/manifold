// Unit tests for engine/gateway-readiness.js — registerGatewayReadiness: gateway process marking, gateway_start, shared state.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { registerGatewayReadiness, createReadinessState } from "../../../engine/gateway-readiness.js";

// ─── helpers ─────────────────────────────────────────────────────────────────

// A plugin api stand-in: records hook registrations so tests can fire
// gateway_start, and records any subagent call so tests can assert none is made.
function makeApi() {
  const hooks = {};
  const calls = [];
  const record = (name) => async (p) => { calls.push({ name, p }); };
  const api = {
    on: (name, fn) => { (hooks[name] ??= []).push(fn); },
    runtime: { subagent: { run: record("run"), waitForRun: record("waitForRun"), getSessionMessages: record("getSessionMessages") } },
  };
  return { api, calls, fire: (name) => (hooks[name] ?? []).forEach((fn) => fn({ port: 18789 }, {})) };
}

const tick = () => new Promise((r) => setTimeout(r, 10));

async function settled(promise) {
  let done = false;
  promise.then(() => { done = true; });
  await tick();
  return done;
}

// ─── not the gateway process ─────────────────────────────────────────────────

describe("a process where pipeclaw's services never start (the CLI)", () => {
  test("whenReady resolves at once", async () => {
    const { api } = makeApi();
    const r = registerGatewayReadiness(api, { state: createReadinessState() });
    assert.equal(await settled(r.whenReady()), true);
    assert.equal(r.isReady(), false);
  });
});

// ─── gateway startup ─────────────────────────────────────────────────────────

describe("gateway startup — services start, then gateway_start", () => {
  test("marking the process never awaits and makes no RPC — it runs inside a service start()", async () => {
    const { api, calls } = makeApi();
    const r = registerGatewayReadiness(api, { state: createReadinessState() });
    assert.equal(r.markGatewayProcess(), undefined);
    await tick();
    assert.equal(calls.length, 0);
  });

  test("whenReady stays pending until gateway_start fires", async () => {
    const { api, fire } = makeApi();
    const r = registerGatewayReadiness(api, { state: createReadinessState() });
    r.markGatewayProcess();
    assert.equal(await settled(r.whenReady()), false);
    fire("gateway_start");
    assert.equal(await settled(r.whenReady()), true);
    assert.equal(r.isReady(), true);
  });

  test("with no gateway_start, whenReady never settles — there is no fallback", async () => {
    const { api } = makeApi();
    const r = registerGatewayReadiness(api, { state: createReadinessState() });
    r.markGatewayProcess();
    await new Promise((res) => setTimeout(res, 50));
    assert.equal(await settled(r.whenReady()), false);
  });
});

// ─── shared state ────────────────────────────────────────────────────────────

describe("shared state — registrations after readiness (reloads, second module instance)", () => {
  test("are ready immediately, without registering for gateway_start", async () => {
    const state = createReadinessState();
    const boot = makeApi();
    registerGatewayReadiness(boot.api, { state }).markGatewayProcess();
    boot.fire("gateway_start");

    const hooks = [];
    const later = { on: (name) => hooks.push(name) };
    const r = registerGatewayReadiness(later, { state });
    r.markGatewayProcess();
    assert.equal(r.isReady(), true);
    assert.equal(await settled(r.whenReady()), true);
    assert.deepEqual(hooks, []);
  });

  test("a registration before readiness is settled by gateway_start fired on any registration", async () => {
    const state = createReadinessState();
    const a = makeApi();
    const b = makeApi();
    const ra = registerGatewayReadiness(a.api, { state });
    const rb = registerGatewayReadiness(b.api, { state });
    ra.markGatewayProcess();
    b.fire("gateway_start");
    assert.equal(await settled(ra.whenReady()), true);
    assert.equal(await settled(rb.whenReady()), true);
  });
});
