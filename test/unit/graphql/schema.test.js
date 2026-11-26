import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createRootValue, executeQuery, schema } from "../../../graphql/schema.js";

function tmpDb(name) {
  const dir = path.join(os.tmpdir(), `graphql-test-${randomUUID()}`);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${name}.sqlite`);
}

const stubFailureStore = {
  list: async ({ resolved, limit }) => {
    assert.equal(resolved, false);
    assert.ok(limit <= 200);
    return [
      { id: "f1", flow_id: "flow-1", pipeline_id: "pipe_a", failure_node: "SCORE", failure_summary: "boom", error_type: "infra", failed_at: 1000 },
    ];
  },
};

const stubRunStore = {
  listRunsPage: async ({ status, limit }) => {
    assert.ok(limit <= 200);
    if (status === "running") return [{ flowId: "flow-9", pipelineId: "pipe_a", status: "running", createdAtMs: 3000, updatedAtMs: 3100 }];
    return [{ flowId: "flow-1", pipelineId: "pipe_a", status: "failed", createdAtMs: 1000, updatedAtMs: 1500 }];
  },
  get: async (flowId) => (flowId === "flow-1"
    ? { flowId: "flow-1", pipelineId: "pipe_a", status: "failed", createdAtMs: 1000, updatedAtMs: 1500 }
    : null),
};

async function seedOps(dbPath) {
  const db = createClient({ url: `file:${dbPath}` });
  await db.execute(`CREATE TABLE gate_state (
    id TEXT NOT NULL PRIMARY KEY DEFAULT 'singleton', locked INTEGER NOT NULL DEFAULT 0, day TEXT
  )`);
  await db.execute("INSERT INTO gate_state (id, locked, day) VALUES ('singleton', 1, '2026-10-23')");
  db.close();
}

async function seedRuntime(dbPath) {
  const db = createClient({ url: `file:${dbPath}` });
  await db.execute(`CREATE TABLE queue_items (
    id TEXT PRIMARY KEY, label TEXT NOT NULL, agent TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL, flow_id TEXT, node TEXT
  )`);
  await db.execute({
    sql: "INSERT INTO queue_items (id, label, agent, status, created_at, flow_id, node) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
    args: ["q1", "SCORE", "scout", 2500, "flow-1", "SCORE"],
  });
  await db.execute(`CREATE TABLE gate_records (
    flow_id TEXT NOT NULL, node_id TEXT NOT NULL, record_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL, PRIMARY KEY (flow_id, node_id)
  )`);
  await db.execute(`CREATE TABLE flow_runs (
    flow_id TEXT PRIMARY KEY, pipeline_id TEXT, status TEXT NOT NULL,
    state_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
  )`);
  const open = JSON.stringify({
    nodeId: "GAP_GATE", kind: "human-gate",
    items: [{ itemId: "gap:1", requestPayload: { title: "Need input", layout: [] } }],
  });
  await db.execute({
    sql: "INSERT INTO gate_records (flow_id, node_id, record_json, updated_at) VALUES (?, ?, ?, ?)",
    args: ["flow-1", "GAP_GATE", open, 5000],
  });
  await db.execute({
    sql: "INSERT INTO gate_records (flow_id, node_id, record_json, updated_at) VALUES (?, ?, ?, ?)",
    args: ["flow-old", "GAP_GATE", JSON.stringify({ kind: "human-gate", consumedAt: 1, items: [] }), 1000],
  });
  await db.execute({
    sql: "INSERT INTO flow_runs (flow_id, pipeline_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
    args: ["flow-1", "pipe_a", "waiting", 4000, 4500],
  });
  db.close();
}

function makeRoot() {
  const runtimeDbPath = tmpDb("runtime");
  const opsDbPath = tmpDb("ops");
  const root = createRootValue({
    failureStore: stubFailureStore,
    runStore: stubRunStore,
    runtimeDbPath,
    opsDbPath,
  });
  return { root, runtimeDbPath, opsDbPath };
}

describe("graphql schema — inherits the drizzle defs", () => {
  function fieldNames(typeName) {
    return Object.keys(schema.getType(typeName).getFields()).sort();
  }

  test("leaf fields track the table columns (rename-proof)", () => {
    assert.deepEqual(fieldNames("Failure"), ["errorType", "failedAt", "flowId", "id", "nodeId", "pipelineId", "summary"]);
    assert.deepEqual(fieldNames("Approval"), ["agent", "createdAt", "flowId", "id", "label", "node"]);
    assert.deepEqual(fieldNames("Gate"), ["day", "locked"]);
    assert.deepEqual(fieldNames("FlowRun"), ["createdAt", "flowId", "pipelineId", "status", "updatedAt"]);
    assert.deepEqual(
      Object.keys(schema.getQueryType().getFields()).sort(),
      ["approvals", "failures", "gate", "openGates", "run", "runs", "snapshot"],
    );
  });

  test("unknown columns throw at build time instead of drifting", async () => {
    const { columnFields } = await import("../../../graphql/columns.js");
    const { failureEvents } = await import("../../../state/schema.js");
    assert.throws(() => columnFields(failureEvents, ["no_such_column"]), /not on table/);
  });
});

describe("graphql schema — failures surface as errors, not empty", () => {
  test("store throw becomes errors array with null data field", async () => {
    const failing = createRootValue({
      failureStore: { list: async () => { throw Object.assign(new Error("boom"), { code: "SOME_ERR" }); } },
      runStore: { listRunsPage: async () => [], get: async () => null },
      runtimeDbPath: tmpDb("runtime"),
      opsDbPath: tmpDb("ops"),
    });
    const out = await executeQuery({ root: failing, query: "{ failures { id } }" });
    assert.ok((out.errors ?? []).length > 0);
    assert.match(out.errors[0].message, /boom/);
  });
});

describe("graphql schema — store-backed fields", () => {
  test("failures maps store rows; runs filter by status; run resolves", async () => {
    const { root, runtimeDbPath, opsDbPath } = makeRoot();
    await seedOps(opsDbPath);
    await seedRuntime(runtimeDbPath);
    const f = await executeQuery({ root, query: "{ failures { id flowId pipelineId nodeId summary errorType failedAt } }" });
    assert.equal(f.errors, undefined);
    assert.equal(f.data.failures[0].flowId, "flow-1");
    assert.equal(f.data.failures[0].nodeId, "SCORE");
    const r = await executeQuery({ root, query: '{ runs(status: "running") { flowId status createdAt } }' });
    assert.deepEqual(r.data.runs.map((x) => x.flowId), ["flow-9"]);
    assert.equal(r.data.runs[0].createdAt, 3000);
    const one = await executeQuery({ root, query: '{ run(flowId: "flow-1") { status updatedAt } }' });
    assert.equal(one.data.run.status, "failed");
    const miss = await executeQuery({ root, query: '{ run(flowId: "nope") { flowId } }' });
    assert.equal(miss.data.run, null);
  });
});

describe("graphql schema — raw-sql fields", () => {
  test("approvals, gate, snapshot, openGates", async () => {
    const { root, runtimeDbPath, opsDbPath } = makeRoot();
    await seedOps(opsDbPath);
    await seedRuntime(runtimeDbPath);
    const a = await executeQuery({ root, query: "{ approvals { id flowId agent } gate { locked day } }" });
    assert.equal(a.errors, undefined);
    assert.equal(a.data.approvals[0].agent, "scout");
    assert.equal(a.data.gate.locked, 1);
    const s = await executeQuery({ root, query: "{ snapshot { failures { id } approvals { id } gate { locked } runningCount } }" });
    assert.equal(s.data.snapshot.runningCount, 1);
    assert.equal(s.data.snapshot.failures.length, 1);
    const i = await executeQuery({ root, query: "{ openGates { activityId flowId itemId title gateKind payload } }" });
    assert.equal(i.errors, undefined);
    assert.equal(i.data.openGates.length, 1);
    assert.equal(i.data.openGates[0].activityId, "flow-1:gap:1");
    assert.equal(JSON.parse(i.data.openGates[0].payload).title, "Need input");
  });

  test("invalid query errors; missing tables fail loudly, never fake-empty", async () => {
    const { root } = makeRoot();
    const bad = await executeQuery({ root, query: "{ nope }" });
    assert.ok((bad.errors ?? []).length > 0);
    const empty = createRootValue({
      failureStore: { list: async () => [] },
      runStore: { listRunsPage: async () => [], get: async () => null },
      runtimeDbPath: tmpDb("e1"),
      opsDbPath: tmpDb("e2"),
    });
    const out = await executeQuery({ root: empty, query: "{ approvals { id } gate { locked } openGates { activityId } }" });
    assert.ok((out.errors ?? []).length > 0, "DB failures must surface as errors, not empty lists");
  });
});
