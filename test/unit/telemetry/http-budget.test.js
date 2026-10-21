import { describe, it } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { registerBudgetRoutes } from "../../../telemetry/http-budget.js";
import { initCapStore } from "../../../telemetry/budget-alert.js";

// setCapAndPersist/setLockThresholdAndPersist (called by the PUT handlers
// below) write to a real cap_versions SQLite table — point them at an
// isolated per-run temp file so this suite never touches the real
// ~/.openclaw/ops.sqlite.
await initCapStore({
  dbPath: path.join(os.tmpdir(), `manifold-http-budget-test-${randomUUID()}.sqlite`),
  legacyCapFile: path.join(os.tmpdir(), `manifold-http-budget-test-legacy-${randomUUID()}.json`), // doesn't exist — deterministic defaults
});

// ── helpers ───────────────────────────────────────────────────────────────────

function makeApi() {
  const routes = new Map();
  return {
    _routes: routes,
    registerHttpRoute: (def) => {
      const key = `${def.method ?? "ANY"} ${def.path}`;
      routes.set(def.path, def);
    },
  };
}

function makeJson() {
  const writes = [];
  const json = (res, body, status = 200) => {
    res.statusCode = status;
    res._body = JSON.stringify(body);
    writes.push({ status, body });
  };
  const bad = (res, msg, status = 503) => json(res, { error: msg }, status);
  return { json, bad, writes };
}

function makeRes() {
  return { statusCode: 200, _body: null, headers: {}, end: function(b) { this._body = b ?? ""; } };
}

function makeReq(method, path, body = null) {
  const chunks = body ? [Buffer.from(JSON.stringify(body))] : [];
  const req = {
    method,
    url: `http://x${path}`,
    [Symbol.asyncIterator]: async function*() { for (const c of chunks) yield c; },
  };
  return req;
}

// Mirrors budget/index.js's gate lock/unlock broadcast contract: getBroadcast()
// returns the actual broadcast(event, payload) function.
function makeBroadcast() {
  const calls = [];
  const broadcast = (event, payload) => calls.push({ event, payload });
  return { getBroadcast: () => broadcast, calls };
}

// ── /budget/cap ───────────────────────────────────────────────────────────────

describe("registerBudgetRoutes /budget/cap", () => {
  it("GET returns current capUSD", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    const store = { db: {} };
    registerBudgetRoutes({ api, getStore: () => store, logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    assert.ok(def, "route registered");

    const req = makeReq("GET", "/manifold/telemetry/budget/cap");
    const res = makeRes();
    await def.handler(req, res);
    const body = JSON.parse(res._body);
    assert.ok("capUSD" in body);
    assert.equal(typeof body.capUSD, "number");
  });

  it("PUT with invalid capUSD returns 400", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    const req = makeReq("PUT", "/manifold/telemetry/budget/cap", { capUSD: -5 });
    const res = makeRes();
    await def.handler(req, res);
    assert.equal(res.statusCode, 400);
    const body = JSON.parse(res._body);
    assert.ok(body.error.includes("positive"));
  });

  it("PUT with valid capUSD returns ok:true", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    const req = makeReq("PUT", "/manifold/telemetry/budget/cap", { capUSD: 5.0 });
    const res = makeRes();
    await def.handler(req, res);
    const body = JSON.parse(res._body);
    assert.equal(body.ok, true);
    assert.equal(body.capUSD, 5.0);
  });

  it("PUT with valid capUSD broadcasts plugin.budget.cap.changed so iOS can reload the chart", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    const { getBroadcast, calls } = makeBroadcast();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad, getBroadcast });

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    const req = makeReq("PUT", "/manifold/telemetry/budget/cap", { capUSD: 7.5 });
    await def.handler(req, makeRes());

    assert.equal(calls.length, 1);
    assert.equal(calls[0].event, "plugin.budget.cap.changed");
    assert.equal(calls[0].payload.capUSD, 7.5);
  });

  it("PUT with invalid capUSD does not broadcast", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    const { getBroadcast, calls } = makeBroadcast();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad, getBroadcast });

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    const req = makeReq("PUT", "/manifold/telemetry/budget/cap", { capUSD: -5 });
    await def.handler(req, makeRes());

    assert.equal(calls.length, 0);
  });

  it("no getBroadcast passed — PUT still succeeds (broadcast is optional)", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad }); // getBroadcast omitted

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    const req = makeReq("PUT", "/manifold/telemetry/budget/cap", { capUSD: 2.0 });
    const res = makeRes();
    await def.handler(req, res);
    assert.equal(JSON.parse(res._body).ok, true);
  });

  it("unsupported method sets 405", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/cap");
    const req = makeReq("DELETE", "/manifold/telemetry/budget/cap");
    const res = makeRes();
    await def.handler(req, res);
    assert.equal(res.statusCode, 405);
  });
});

// ── /budget/gate-threshold ────────────────────────────────────────────────────

describe("registerBudgetRoutes /budget/gate-threshold", () => {
  it("GET returns current lockThreshold", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/gate-threshold");
    assert.ok(def, "route registered");

    const req = makeReq("GET", "/manifold/telemetry/budget/gate-threshold");
    const res = makeRes();
    await def.handler(req, res);
    const body = JSON.parse(res._body);
    assert.ok("lockThreshold" in body);
    assert.equal(typeof body.lockThreshold, "number");
  });

  it("PUT with lockThreshold > 1 returns 400", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/gate-threshold");
    const req = makeReq("PUT", "/manifold/telemetry/budget/gate-threshold", { lockThreshold: 1.5 });
    const res = makeRes();
    await def.handler(req, res);
    assert.equal(res.statusCode, 400);
  });

  it("PUT with valid threshold 0.8 returns ok:true", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/gate-threshold");
    const req = makeReq("PUT", "/manifold/telemetry/budget/gate-threshold", { lockThreshold: 0.8 });
    const res = makeRes();
    await def.handler(req, res);
    const body = JSON.parse(res._body);
    assert.equal(body.ok, true);
    assert.equal(body.lockThreshold, 0.8);
  });

  it("PUT with valid threshold broadcasts plugin.budget.cap.changed", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    const { getBroadcast, calls } = makeBroadcast();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad, getBroadcast });

    const def = api._routes.get("/manifold/telemetry/budget/gate-threshold");
    const req = makeReq("PUT", "/manifold/telemetry/budget/gate-threshold", { lockThreshold: 0.6 });
    await def.handler(req, makeRes());

    assert.equal(calls.length, 1);
    assert.equal(calls[0].event, "plugin.budget.cap.changed");
    assert.equal(calls[0].payload.lockThreshold, 0.6);
  });
});

// ── /budget/cap-history ───────────────────────────────────────────────────────

describe("registerBudgetRoutes /budget/cap-history", () => {
  it("GET returns versions array with capUSD, lockThreshold, registeredAt, retiredAt", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/cap-history");
    assert.ok(def, "route registered");

    const req = makeReq("GET", "/manifold/telemetry/budget/cap-history");
    const res = makeRes();
    await def.handler(req, res);
    const body = JSON.parse(res._body);
    assert.ok(Array.isArray(body.versions));
    assert.ok(body.versions.length >= 1, "seeded store has ≥1 row");
    const v = body.versions[0];
    assert.equal(typeof v.capUSD, "number");
    assert.equal(typeof v.lockThreshold, "number");
    assert.equal(typeof v.registeredAt, "number");
    assert.ok("retiredAt" in v);
  });

  it("after PUT cap 9.5, last version has capUSD 9.5 and retiredAt null", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const capDef = api._routes.get("/manifold/telemetry/budget/cap");
    await capDef.handler(
      makeReq("PUT", "/manifold/telemetry/budget/cap", { capUSD: 9.5 }),
      makeRes(),
    );

    const histDef = api._routes.get("/manifold/telemetry/budget/cap-history");
    const res = makeRes();
    await histDef.handler(makeReq("GET", "/manifold/telemetry/budget/cap-history"), res);
    const body = JSON.parse(res._body);
    const last = body.versions.at(-1);
    assert.equal(last.capUSD, 9.5);
    assert.equal(last.retiredAt, null);
  });
});

// ── /budget/alert-history ─────────────────────────────────────────────────────
// Populated-data coverage (ordering, limit) lives in
// budget-alert-history.test.js, which can insert rows directly; this suite
// shares one store across all its tests (seeded at module load) so it only
// checks the route's registration and empty-state shape here.

describe("registerBudgetRoutes /budget/alert-history", () => {
  it("GET returns an events array (empty on a fresh store)", async () => {
    const api = makeApi();
    const { json, bad } = makeJson();
    registerBudgetRoutes({ api, getStore: () => ({}), logger: null, json, bad });

    const def = api._routes.get("/manifold/telemetry/budget/alert-history");
    assert.ok(def, "route registered");

    const req = makeReq("GET", "/manifold/telemetry/budget/alert-history");
    const res = makeRes();
    await def.handler(req, res);
    const body = JSON.parse(res._body);
    assert.ok(Array.isArray(body.events));
  });
});
