import { describe, it } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { registerBudgetRoutes, openStore } from "../../../budget/index.js";
import { createRunStore } from "../../../state/run-store.js";
import { todayLocal } from "../../../budget/lib.js";


  async function seedFlow(dbPath, flowId, pipelineId = "p") {
    const store = createRunStore({ dbPath: `${dbPath}.runtime.sqlite` });
    await store.insert({ flowId, pipelineId, status: "running" });
  }

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

    await assert.rejects(
      () => api.runtime.approvalQueue.isGateLocked(),
      /disk full/,
    );
  });
});

describe("registerBudgetRoutes — real openStore(), connection reuse across generations", () => {
  function tmpDbPath() {
    return path.join(os.tmpdir(), `budget-index-test-${randomUUID()}.sqlite`);
  }

  function realOpenStoreAt(dbPath) {
    return () => openStore({ dbPath });
  }


  it("a second generation against the same dbPath sees data written by the first, with no separate migration failure", async () => {
    const dbPath = tmpDbPath();
    const gen1Api = makeFakeApi();
    registerBudgetRoutes(gen1Api, { openStore: realOpenStoreAt(dbPath) });
    await gen1Api.runtime.approvalQueue.isGateLocked();
    await seedFlow(dbPath, "flow-1");

    const itemId = await gen1Api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: "flow-1", nodeId: "SCORE_LLM", pipelineId: "p", label: "SCORE_LLM",
    });
    assert.ok(itemId);

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
    await gen1Api.runtime.approvalQueue.isGateLocked();

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

function postReq(body) {
  const chunks = [Buffer.from(JSON.stringify(body))];
  return {
    method: "POST",
    [Symbol.asyncIterator]: async function* () { for (const c of chunks) yield c; },
  };
}


describe("registerBudgetRoutes — canonical emissions", () => {
  function tmpDbPath() {
    return path.join(os.tmpdir(), `budget-emit-test-${randomUUID()}.sqlite`);
  }

  function makeApiWithBroadcast() {
    const api = makeFakeApi();
    const calls = [];
    const broadcast = (name, payload) => calls.push({ name, payload });
    return { api, calls, getBroadcast: () => broadcast };
  }

  it("decide emits budget.queue.updated with decision + count", async () => {
    const { api, calls, getBroadcast } = makeApiWithBroadcast();
    const dbPath = tmpDbPath();
    registerBudgetRoutes(api, { openStore: () => openStore({ dbPath }), getBroadcast });
    await api.runtime.approvalQueue.isGateLocked();
    await seedFlow(dbPath, "flow-1");

    const itemId = await api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: "flow-1", nodeId: "SCORE_LLM", pipelineId: "p", label: "SCORE_LLM",
    });
    const route = api._routes.find((r) => r.path === "/manifold/budget/decide");
    await route.handler(postReq({ ids: [itemId], decision: "approve" }), fakeRes());

    const call = calls.find((c) => c.name === "plugin.budget.queue.updated");
    assert.ok(call, "queue.changed must fire");
    assert.equal(call.payload.decision, "approve");
    assert.equal(call.payload.count, 1);
    assert.equal(call.payload.kind, "budget.queue.updated");
  });

  it("enqueue emits approval.requested with id", async () => {
    const { api, calls, getBroadcast } = makeApiWithBroadcast();
    const dbPath = tmpDbPath();
    registerBudgetRoutes(api, { openStore: () => openStore({ dbPath }), getBroadcast });
    await api.runtime.approvalQueue.isGateLocked();
    await seedFlow(dbPath, "flow-1");

    const itemId = await api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: "flow-1", nodeId: "SCORE_LLM", pipelineId: "p", label: "SCORE_LLM",
    });
    const call = calls.find((c) => c.name === "plugin.approval.requested");
    assert.ok(call, "approval.requested must fire on enqueue");
    assert.equal(call.payload.id, itemId);
  });

  it("decide resumes flow-bound approvals through injected onFlowResume (no HTTP loopback)", async () => {
    const { api, calls, getBroadcast } = makeApiWithBroadcast();
    const resumes = [];
    const onFlowResume = async (args) => { resumes.push(args); return { ok: true }; };
    const dbPath = tmpDbPath();
    registerBudgetRoutes(api, {
      openStore: () => openStore({ dbPath }),
      getBroadcast,
      onFlowResume,
    });
    await api.runtime.approvalQueue.isGateLocked();
    await seedFlow(dbPath, "flow-9");

    const itemId = await api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: "flow-9", nodeId: "SUBMIT", pipelineId: "p", label: "SUBMIT",
    });
    const route = api._routes.find((r) => r.path === "/manifold/budget/decide");
    await route.handler(postReq({ ids: [itemId], decision: "reject" }), fakeRes());

    assert.equal(resumes.length, 1);
    assert.deepEqual(resumes[0], { flowId: "flow-9", nodeId: "SUBMIT", decision: "reject" });
    assert.ok(calls.find((c) => c.name === "plugin.budget.queue.updated"), "decision still broadcasts");
  });

  it("queue items carry kind tags: node by default, run-start when passed", async () => {
    const dbPath = tmpDbPath();
    const { api } = makeApiWithBroadcast();
    const client = await openStore({ dbPath });
    registerBudgetRoutes(api, { openStore: async () => client });
    await api.runtime.approvalQueue.isGateLocked();
    await seedFlow(dbPath, "flow-1");

    const nodeItem = await api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: "flow-1", nodeId: "SCORE_LLM", pipelineId: "p", label: "SCORE_LLM",
    });
    const startItem = await api.runtime.approvalQueue.enqueueNodeApproval({
      flowId: null, nodeId: null, pipelineId: "p", label: "START p",
      kind: "run-start",
    });

    const { rows } = await client.runtime.execute({ sql: "SELECT id, kind FROM queue_items WHERE id IN (?, ?)", args: [nodeItem, startItem] });
    const byId = new Map(rows.map((r) => [r.id, r.kind]));
    assert.equal(byId.get(nodeItem), "node", "per-node approvals default to kind=node");
    assert.equal(byId.get(startItem), "run-start", "start-gate approvals carry kind=run-start");
  });

  it("lock/unlock emit budget.lock.changed with locked + day", async () => {
    const { api, calls, getBroadcast } = makeApiWithBroadcast();
    registerBudgetRoutes(api, { openStore: () => openStore({ dbPath: tmpDbPath() }), getBroadcast });
    await api.runtime.approvalQueue.isGateLocked();

    const lockRoute = api._routes.find((r) => r.path === "/manifold/budget/gate/lock");
    await lockRoute.handler({ method: "POST" }, fakeRes());
    const unlockRoute = api._routes.find((r) => r.path === "/manifold/budget/gate/unlock");
    await unlockRoute.handler({ method: "POST" }, fakeRes());

    const locked = calls.filter((c) => c.name === "plugin.budget.lock.changed");
    assert.equal(locked.length, 2);
    assert.equal(locked[0].payload.locked, true);
    assert.equal(locked[1].payload.locked, false);
    for (const c of locked) {
      assert.equal(c.payload.kind, "budget.lock.changed");
      assert.equal(typeof c.payload.day, "string");
    }
  });
});
