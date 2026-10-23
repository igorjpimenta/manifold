/**
 * Unit tests for graphql/schema.js
 *
 * Store-backed fields (failures, runs) run against stub stores (the
 * stores own their tests); raw-sql fields (approvals, gate, inbox) run
 * against real tmp sqlite files.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createRootValue, executeQuery } from "../../../graphql/schema.js";

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
  await db.execute(`CREATE TABLE queue_items (
    id TEXT PRIMARY KEY, label TEXT NOT NULL, agent TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL, flow_id TEXT, node TEXT
  )`);
  await db.execute({
    sql: "INSERT INTO queue_items (id, label, agent, status, created_at, flow_id, node) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
    args: ["q1", "SCORE", "scout", 2500, "flow-1", "SCORE"],
  });
  await db.execute(`CREATE TABLE gate_state (
    id TEXT NOT NULL PRIMARY KEY DEFAULT 'singleton', locked INTEGER NOT NULL DEFAULT 0, day TEXT
  )`);
  await db.execute("INSERT INTO gate_state (id, locked, day) VALUES ('singleton', 1, '2026-10-23')");
  db.close();
}

async function seedRuntime(dbPath) {
  const db = createClient({ url: `file:${dbPath}` });
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
  test("approvals, gate, snapshot, inbox", async () => {
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
    const i = await executeQuery({ root, query: "{ inbox { activityId flowId itemId title gateKind payload } }" });
    assert.equal(i.errors, undefined);
    assert.equal(i.data.inbox.length, 1);
    assert.equal(i.data.inbox[0].activityId, "flow-1:gap:1");
    assert.equal(JSON.parse(i.data.inbox[0].payload).title, "Need input");
  });

  test("invalid query errors; missing tables degrade to empty", async () => {
    const { root } = makeRoot();
    const bad = await executeQuery({ root, query: "{ nope }" });
    assert.ok((bad.errors ?? []).length > 0);
    const empty = createRootValue({
      failureStore: { list: async () => [] },
      runStore: { listRunsPage: async () => [], get: async () => null },
      runtimeDbPath: tmpDb("e1"),
      opsDbPath: tmpDb("e2"),
    });
    const out = await executeQuery({ root: empty, query: "{ approvals { id } gate { locked } inbox { activityId } }" });
    assert.equal(out.errors, undefined);
    assert.deepEqual(out.data.approvals, []);
    assert.equal(out.data.gate, null);
    assert.deepEqual(out.data.inbox, []);
  });
});
