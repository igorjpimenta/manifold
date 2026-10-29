import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { registerTreeRoutes } from "../../../telemetry/http-tree.js";

// ── helpers ───────────────────────────────────────────────────────────────────

async function makeClient() {
  const dbPath = join(tmpdir(), `http-tree-test-${randomUUID()}.sqlite`);
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute(`
    CREATE TABLE cost_by_call (
      ts INTEGER, cost_total REAL, flow_id TEXT, pipeline_id TEXT,
      node_id TEXT, model TEXT, session_id TEXT, session_key TEXT,
      root_kind TEXT, root_label TEXT, root_session_key TEXT,
      item_key TEXT
    )
  `);
  await client.execute(`
    CREATE TABLE session_lineage (
      child_session_key TEXT, parent_session_key TEXT,
      root_session_key TEXT, root_kind TEXT, root_label TEXT,
      flow_id TEXT, agent_id TEXT, source TEXT, kind TEXT
    )
  `);
  return client;
}

async function makeStore() {
  return { _client: await makeClient() };
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
  return { statusCode: 200, _body: null };
}

function makeReq(path) {
  return { method: "GET", url: `http://x${path}` };
}

// ── route registration ────────────────────────────────────────────────────────

describe("registerTreeRoutes registration", () => {
  it("registers /tree/expand route", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerTreeRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    assert.ok(api._routes.has("/manifold/telemetry/tree/expand"));
  });
});

// ── /tree/expand __root__ ─────────────────────────────────────────────────────

describe("/tree/expand parent=__root__", () => {
  it("returns 503 when store is null", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    registerTreeRoutes({ api, getStore: () => null, logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=__root__"), res);
    assert.equal(res.statusCode, 503);
  });

  it("returns empty children array for empty db", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerTreeRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=__root__"), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res._body);
    assert.equal(body.parent, "__root__");
    assert.ok(Array.isArray(body.children));
    assert.equal(body.children.length, 0);
  });

  it("returns trigger-level children from cost_by_call", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const client = await makeClient();
    await client.execute({
      sql: `INSERT INTO cost_by_call (ts, cost_total, root_kind, root_label, session_key, root_session_key)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: [Date.now(), 0.05, "explicit", "op-label", "agent:main:explicit:s1", "agent:main:explicit:s1"],
    });
    await client.execute({
      sql: `INSERT INTO cost_by_call (ts, cost_total, root_kind, root_label, session_key, root_session_key)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: [Date.now(), 0.02, "cron-owner", "my-job", "agent:main:cron:my-job", "agent:main:cron:my-job"],
    });
    registerTreeRoutes({ api, getStore: () => ({ _client: client }), logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=__root__"), res);
    const body = JSON.parse(res._body);
    assert.ok(body.children.length >= 1);
    for (const c of body.children) {
      assert.ok(c.key.startsWith("trigger:"), `unexpected key: ${c.key}`);
      assert.equal(c.kind, "trigger");
      assert.ok(typeof c.cost === "number");
      assert.ok(typeof c.hasChildren === "boolean");
    }
  });
});

// ── /tree/expand trigger:<kind> parent (instance level) ─────────────────────

describe("/tree/expand parent=trigger:<kind>", () => {
  it("merges UUID-suffixed explicit labels into one non-expandable Ad-hoc sessions row", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const client = await makeClient();
    const rows = [
      ["explicit", "scorer-creation-c61af7cf-3cf1-43ff-873a-40ceb6b2cb17", 0.03],
      ["explicit", "test-positive-e3b48771-baf6-4815-8fc9-35b222701b4d", 0.02],
      ["explicit", "freelance_workana-proposal", 0.10], // a real pipeline id — must stay separate
    ];
    for (const [rootKind, rootLabel, cost] of rows) {
      await client.execute({
        sql: `INSERT INTO cost_by_call (ts, cost_total, root_kind, root_label, session_key, root_session_key)
              VALUES (?, ?, ?, ?, ?, ?)`,
        args: [Date.now(), cost, rootKind, rootLabel, `agent:main:explicit:${rootLabel}`, `agent:main:explicit:${rootLabel}`],
      });
    }
    registerTreeRoutes({ api, getStore: () => ({ _client: client }), logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=trigger%3Aexplicit"), res);
    const body = JSON.parse(res._body);

    const adHoc = body.children.find((c) => c.label === "Ad-hoc sessions");
    assert.ok(adHoc, `expected an Ad-hoc sessions row, got labels: ${body.children.map((c) => c.label)}`);
    assert.equal(adHoc.calls, 2, "should merge both UUID-suffixed rows");
    assert.equal(adHoc.cost, 0.05, "should sum the merged rows' cost");
    assert.equal(adHoc.hasChildren, false, "a merged bucket has no single raw label to expand into");

    const pipeline = body.children.find((c) => c.label === "workana-proposal");
    assert.ok(pipeline, `expected the real pipeline id to stay its own row, got labels: ${body.children.map((c) => c.label)}`);
    assert.equal(pipeline.calls, 1);
    assert.equal(pipeline.hasChildren, true);
  });
});

// ── /tree/expand invalid parents ─────────────────────────────────────────────

describe("/tree/expand invalid parent shapes", () => {
  it("returns 400 for unknown parent shape", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerTreeRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=bogus-shape"), res);
    assert.equal(res.statusCode, 400);
  });

  it("returns 400 for the retired pipeline| parent shape", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerTreeRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=pipeline%7Ckind%7Clabel%7Cid"), res);
    assert.equal(res.statusCode, 400);
  });

  it("returns 400 for malformed node| parent", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const store = await makeStore();
    registerTreeRoutes({ api, getStore: () => store, logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq("/manifold/telemetry/tree/expand?parent=node%7Conly-one-segment"), res);
    assert.equal(res.statusCode, 400);
  });
});

// ── /tree/expand flow: parent ─────────────────────────────────────────────────

describe("/tree/expand parent=flow:<id>", () => {
  it("returns node children for a known flow", async () => {
    const api = makeApi();
    const { json, bad, parseRange } = makeHelpers();
    const client = await makeClient();
    const flowId = "flow-abc-123";
    await client.execute({
      sql: `INSERT INTO cost_by_call (ts, cost_total, flow_id, node_id, session_key, root_session_key, root_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
      args: [Date.now(), 0.01, flowId, "NODE_A", "agent:w:sub:s1", "agent:w:sub:s1", "explicit"],
    });
    await client.execute({
      sql: `INSERT INTO cost_by_call (ts, cost_total, flow_id, node_id, session_key, root_session_key, root_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
      args: [Date.now(), 0.02, flowId, "NODE_B", "agent:w:sub:s2", "agent:w:sub:s2", "explicit"],
    });
    registerTreeRoutes({ api, getStore: () => ({ _client: client }), logger: null, json, bad, parseRange });
    const def = api._routes.get("/manifold/telemetry/tree/expand");
    const res = makeRes();
    await def.handler(makeReq(`/manifold/telemetry/tree/expand?parent=flow%3A${flowId}`), res);
    const body = JSON.parse(res._body);
    assert.equal(body.parent, `flow:${flowId}`);
    assert.equal(body.children.length, 2);
    for (const c of body.children) {
      assert.ok(c.key.startsWith("node|"), `unexpected key: ${c.key}`);
      assert.equal(c.kind, "flow-node");
    }
  });
});
