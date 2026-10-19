import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { registerCostRoutes } from "../../../telemetry/http-cost.js";
import { UNATTRIBUTED_LABEL, UNATTRIBUTED_FILTER } from "../../../telemetry/labels.js";

// ── helpers ───────────────────────────────────────────────────────────────────

async function makeClient(rows = []) {
  const dbPath = join(tmpdir(), `http-cost-test-${randomUUID()}.sqlite`);
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute(`
    CREATE TABLE cost_by_call (
      ts INTEGER, cost_total REAL, flow_id TEXT, pipeline_id TEXT,
      node_id TEXT, model TEXT, provider TEXT,
      input_tokens INTEGER, output_tokens INTEGER,
      cache_read_tokens INTEGER, cache_write_tokens INTEGER,
      total_tokens INTEGER,
      cost_input REAL, cost_output REAL,
      cost_cache_read REAL, cost_cache_write REAL,
      session_id TEXT, session_key TEXT, agent_id TEXT,
      item_key TEXT, msg_id TEXT,
      root_kind TEXT, root_label TEXT, root_session_key TEXT
    )
  `);
  await client.execute(`
    CREATE TABLE session_lineage (
      child_session_key TEXT, parent_session_key TEXT,
      root_session_key TEXT, root_kind TEXT, root_label TEXT,
      flow_id TEXT, agent_id TEXT, source TEXT, kind TEXT
    )
  `);
  for (const r of rows) {
    await client.execute({
      sql: `
        INSERT INTO cost_by_call (ts, cost_total, session_key, model, provider, root_kind, root_label, flow_id, pipeline_id, node_id)
        VALUES (?,?,?,?,?,?,?,?,?,?)
      `,
      args: [r.ts ?? Date.now(), r.cost_total ?? 0.001, r.session_key ?? "s1", r.model ?? "claude-3-5-sonnet", r.provider ?? "anthropic", r.root_kind ?? "explicit", r.root_label ?? "label-a", r.flow_id ?? null, r.pipeline_id ?? null, r.node_id ?? null],
    });
  }
  return client;
}

async function makeStore(rows = []) {
  return { _client: await makeClient(rows) };
}

function makeApi() {
  const routes = new Map();
  return {
    _routes: routes,
    registerHttpRoute: (def) => routes.set(def.path, def),
  };
}

function makeHelpers() {
  const json = (res, body, status = 200) => {
    res.statusCode = status;
    res._body = JSON.stringify(body);
  };
  const bad = (res, msg, status = 503) => json(res, { error: msg }, status);
  const parseRange = (urlParams) => {
    const since = urlParams.get("since");
    const until = urlParams.get("until");
    const args = [], where = [];
    if (since) { const ms = Date.parse(since); if (Number.isFinite(ms)) { where.push("ts >= ?"); args.push(ms); } }
    if (until) { const ms = Date.parse(until); if (Number.isFinite(ms)) { where.push("ts <= ?"); args.push(ms); } }
    return { where, args };
  };
  return { json, bad, parseRange };
}

function makeRes() {
  return { statusCode: 200, _body: null, headers: {}, setHeader: () => {} };
}

function makeReq(path) {
  return { method: "GET", url: `http://x${path}` };
}

// ── route registration ────────────────────────────────────────────────────────

describe("registerCostRoutes registration", () => {
  it("registers all expected route paths", () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    registerCostRoutes({ api, getStore: () => makeStore(), logger: null, json, bad, parseRange });

    const expected = [
      "/pipeclaw/telemetry/cost/today",
      "/pipeclaw/telemetry/cost/by-root",
      "/pipeclaw/telemetry/cost/by-trigger",
      "/pipeclaw/telemetry/cost/by-pipeline",
      "/pipeclaw/telemetry/cost/by-flow",
      "/pipeclaw/telemetry/cost/by-node",
      "/pipeclaw/telemetry/cost/by-model",
      "/pipeclaw/telemetry/cost/by-cron-job",
      "/pipeclaw/telemetry/cost/by-day",
      "/pipeclaw/telemetry/cost/by-hour",
      "/pipeclaw/telemetry/cost/by-session",
      "/pipeclaw/telemetry/cost/runs",
      "/pipeclaw/telemetry/cost/top-calls",
      "/pipeclaw/telemetry/cost/breakdown",
      "/pipeclaw/telemetry/cost/home",
    ];
    for (const path of expected) {
      assert.ok(api._routes.has(path), `missing route: ${path}`);
    }
  });
});

// ── /cost/today ───────────────────────────────────────────────────────────────

describe("/cost/today", () => {
  it("returns 503 when store is null", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    registerCostRoutes({ api, getStore: () => null, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/today");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/today"), res);
    assert.equal(res.statusCode, 503);
  });

  it("returns summary, byRoot, byModel, today fields", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore([{ cost_total: 0.01, model: "claude-3-5-sonnet", root_label: "label-a", root_kind: "explicit" }]);
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/today");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/today"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.ok("summary" in body);
    assert.ok("byRoot" in body);
    assert.ok("byModel" in body);
    assert.ok("today" in body);
  });
});

// ── /cost/by-session ─────────────────────────────────────────────────────────

describe("/cost/by-session", () => {
  it("returns 503 when flow_id or node_id missing", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/by-session");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/by-session?flow_id=abc"), res);
    assert.equal(res.statusCode, 503);
    const body = JSON.parse(res._body);
    assert.ok(body.error.includes("node_id"));
  });

  it("returns rows array for valid flow_id+node_id", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/by-session");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/by-session?flow_id=f1&node_id=n1"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.ok(Array.isArray(body.rows));
  });
});

// ── /cost/breakdown ───────────────────────────────────────────────────────────

describe("/cost/breakdown", () => {
  it("returns 400 for unknown dim", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/breakdown");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/breakdown?dim=bogus"), res);
    assert.equal(res.statusCode, 400);
  });

  it("returns dimension+total+rows for dim=trigger", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore([{ cost_total: 0.02, root_kind: "explicit", root_label: "op" }]);
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/breakdown");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/breakdown?dim=trigger"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.equal(body.dimension, "trigger");
    assert.ok(typeof body.total === "number");
    assert.ok(Array.isArray(body.rows));
  });

  // The breakdown must never emit a null label: the iOS client decodes
  // `label` as a non-optional String (CostAPI.swift), so a null label fails
  // the whole decode and silently blanks the Budget card. NULL pipeline_id
  // rows belong in a labeled sentinel bucket whose spend is counted — these
  // assert the sentinel is a real string and the spend is actually present.
  it("surfaces NULL pipeline_id rows as a labeled bucket for dim=pipeline", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore([
      { cost_total: 0.03, pipeline_id: "team_alpha-pipeline", flow_id: "f1" },
      { cost_total: 0.04, pipeline_id: null, flow_id: null },
    ]);
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/breakdown");
    const res = makeRes();
    await def.handler(makeReq("/pipeclaw/telemetry/cost/breakdown?dim=pipeline"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.equal(body.dimension, "pipeline");

    for (const row of body.rows) {
      assert.equal(typeof row.label, "string", "label must decode as a non-optional String");
    }
    const unattributed = body.rows.find((r) => r.label === UNATTRIBUTED_LABEL);
    assert.ok(unattributed, `expected an ${UNATTRIBUTED_LABEL} row, got ${JSON.stringify(body.rows.map((r) => r.label))}`);
    assert.equal(unattributed.calls, 1);
    assert.equal(unattributed.cost, 0.04);
    // Outcome counters are meaningless without a flow_id and would otherwise
    // read as a real ROI signal to the iOS ROI filter.
    assert.equal(unattributed.success_rate, undefined);
    assert.equal(unattributed.total_runs, undefined);
    // total must reconcile with the rows, so the operator can account for
    // every cent of the /cost/home top-line.
    assert.equal(body.total, 0.07);

    // A real pipeline is unaffected: team prefix stripped, filter round-trips
    // to the stored value.
    const named = body.rows.find((r) => r.label === "alpha-pipeline");
    assert.ok(named, "expected the attributed pipeline row to survive");
    assert.deepEqual(named.filter, { pipeline_id: "team_alpha-pipeline" });
  });

  it("round-trips the unattributed row's filter back to the NULL rows", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore([
      { cost_total: 0.03, pipeline_id: "team_alpha-pipeline", flow_id: "f1" },
      { cost_total: 0.04, pipeline_id: null, flow_id: null },
    ]);
    registerCostRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/pipeclaw/telemetry/cost/breakdown");

    const res = makeRes();
    // Exactly the filter the transform emits for the unattributed row. A
    // literal `pipeline_id = '(unattributed)'` would match zero rows, since
    // no row stores that string — the __null__ sentinel is what resolves.
    await def.handler(makeReq(`/pipeclaw/telemetry/cost/breakdown?dim=pipeline&pipeline_id=${encodeURIComponent(UNATTRIBUTED_FILTER.pipeline_id)}`), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.equal(body.rows.length, 1);
    assert.equal(body.rows[0].label, UNATTRIBUTED_LABEL);
    assert.equal(body.rows[0].cost, 0.04);
  });
});
