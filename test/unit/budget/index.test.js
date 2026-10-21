import { describe, it } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { registerBudgetRoutes, openStore } from "../../../budget/index.js";
import { todayLocal } from "../../../budget/lib.js";

// registerBudgetRoutes's real store (openStore(), hitting ~/.openclaw/ops.sqlite)
// is swapped for a fake via the `openStore` DI param — tests run without
// touching real sqlite or the gateway's service-registration lifecycle.

function makeFakeApi() {
  const services = [];
  const routes = [];
  return {
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    registerHttpRoute: (route) => { routes.push(route); },
    registerService: (svc) => { services.push(svc); },
    runtime: {},
    _services: services,
    _routes: routes,
  };
}

function makeFakeClient(overrides = {}) {
  const today = new Date().toISOString().slice(0, 10);
  return {
    execute: async () => ({ rows: [{ locked: 1, day: today, ...overrides }] }),
    close: () => {},
  };
}

describe("registerBudgetRoutes — isGateLocked awaits attachment without an external start()", () => {
  it("resolves on the first call by awaiting attachment, even though registerService's start() is never invoked", async () => {
    const api = makeFakeApi();
    let openStoreCalls = 0;
    const fakeClient = makeFakeClient();
    const openStore = async () => { openStoreCalls++; return fakeClient; };

    registerBudgetRoutes(api, { openStore });

    // A single call must just await attachment and return the real answer —
    // no throw, no special "not ready" state the caller has to handle, even
    // though registerService.start() is never invoked (matching the live
    // orphaned-generation bug: a closure whose own start() never runs must
    // still self-heal on demand).
    const locked = await api.runtime.approvalQueue.isGateLocked();
    assert.equal(locked, true);
    assert.equal(openStoreCalls, 1);
  });

  it("memoizes the in-flight attach — concurrent calls before it resolves share one openStore() call", async () => {
    const api = makeFakeApi();
    let openStoreCalls = 0;
    let resolveOpen;
    const openStore = () => {
      openStoreCalls++;
      return new Promise((resolve) => { resolveOpen = resolve; });
    };

    registerBudgetRoutes(api, { openStore });

    const p1 = api.runtime.approvalQueue.isGateLocked();
    const p2 = api.runtime.approvalQueue.isGateLocked();
    const p3 = api.runtime.approvalQueue.isGateLocked();

    resolveOpen(makeFakeClient());
    const [r1, r2, r3] = await Promise.all([p1, p2, p3]);

    assert.equal(openStoreCalls, 1, "three calls racing before attachment resolves must share one in-flight openStore()");
    assert.deepEqual([r1, r2, r3], [true, true, true]);
  });

  it("a genuine attach failure propagates rather than being treated as 'not locked'", async () => {
    const api = makeFakeApi();
    const openStore = async () => { throw new Error("disk full"); };

    registerBudgetRoutes(api, { openStore });

    // Must NOT silently resolve to false (which would let arbitrarily
    // expensive nodes dispatch with zero budget control just because the
    // store itself is broken) — a broken attach is a real failure, not a
    // "not locked" answer.
    await assert.rejects(
      () => api.runtime.approvalQueue.isGateLocked(),
      /disk full/,
    );
  });
});

// Real sqlite, real (unmocked) openStore() — the seam every test above
// deliberately bypasses via the openStore DI param. These exercise the
// actual change this store's default openStore() made: getSharedDb(dbPath)
// instead of a fresh openDb(dbPath) call, so a second registerBudgetRoutes
// generation against the SAME dbPath (simulating a gateway plugin reload)
// reuses the already-open, already-migrated connection instead of paying a
// fresh connection-open + migration cost.
describe("registerBudgetRoutes — real openStore(), connection reuse across generations", () => {
  function tmpDbPath() {
    return path.join(os.tmpdir(), `budget-index-test-${randomUUID()}.sqlite`);
  }

  // Real openStore(), pinned to a tmp dbPath via the openStore DI param —
  // still exercises the actual getSharedDb(dbPath) call inside openStore()
  // itself, just without touching the real ~/.openclaw/ops.sqlite.
  function realOpenStoreAt(dbPath) {
    return () => openStore({ dbPath, legacyDbPath: null });
  }

  it("a second generation against the same dbPath sees data written by the first, with no separate migration failure", async () => {
    const dbPath = tmpDbPath();
    const gen1Api = makeFakeApi();
    registerBudgetRoutes(gen1Api, { openStore: realOpenStoreAt(dbPath) });
    // Real callers always reach isGateLocked() first (executor-budget.js's
    // maybeBudgetGate), which is what attaches `client` — enqueueNodeApproval
    // has no readiness guard of its own, matching production call order.
    await gen1Api.runtime.approvalQueue.isGateLocked();

    const itemId = await gen1Api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: "flow-1", nodeId: "SCORE_LLM", pipelineId: "p", label: "SCORE_LLM",
    });
    assert.ok(itemId);

    // Simulate a gateway plugin reload: a brand-new registerBudgetRoutes
    // closure, same dbPath, no shared JS state with gen1 beyond db.js's
    // process-wide getSharedDb() cache.
    const gen2Api = makeFakeApi();
    registerBudgetRoutes(gen2Api, { openStore: realOpenStoreAt(dbPath) });
    await gen2Api.runtime.approvalQueue.isGateLocked();

    const decision = await gen2Api.runtime.approvalQueue.lookupNodeDecision({ flowId: "flow-1", nodeId: "SCORE_LLM" });
    assert.equal(decision, "pending", "gen2 must see the row gen1 wrote through the same underlying store");
  });

  it("isGateLocked() reflects a lock set by an earlier generation, proving they share live state not just the file at open time", async () => {
    const dbPath = tmpDbPath();
    const gen1Api = makeFakeApi();
    registerBudgetRoutes(gen1Api, { openStore: realOpenStoreAt(dbPath) });
    await gen1Api.runtime.approvalQueue.isGateLocked(); // attaches gen1's client

    const today = todayLocal();
    const gen1Route = gen1Api._routes.find((r) => r.path === "/manifold/budget/gate/lock");
    await gen1Route.handler({ method: "POST" }, fakeRes());

    const gen2Api = makeFakeApi();
    registerBudgetRoutes(gen2Api, { openStore: realOpenStoreAt(dbPath) });
    const locked = await gen2Api.runtime.approvalQueue.isGateLocked();
    assert.equal(locked, true, `gen2 must observe gen1's lock (day=${today})`);
  });
});

function fakeRes() {
  const res = { statusCode: null, headers: {}, body: null };
  res.setHeader = (k, v) => { res.headers[k] = v; };
  res.end = (b) => { res.body = b; };
  return res;
}
