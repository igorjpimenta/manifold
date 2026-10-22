import { test, describe } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createBridge } from "../index.js";

function tmpDb(name) {
  const dir = path.join(os.tmpdir(), `ios-transport-test-${randomUUID()}`);
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${name}.sqlite`);
}

async function seedRuntime(dbPath) {
  const db = createClient({ url: `file:${dbPath}` });
  await db.execute(`CREATE TABLE failure_events (
    id TEXT PRIMARY KEY, flow_id TEXT NOT NULL, failed_at INTEGER NOT NULL,
    failure_summary TEXT NOT NULL, invocation_id TEXT, execution_id TEXT, error_type TEXT
  )`);
  await db.execute(`CREATE TABLE flow_node_invocations (
    id TEXT PRIMARY KEY, flow_id TEXT NOT NULL, pipeline_id TEXT, node_id TEXT NOT NULL,
    subflow_key TEXT, visit_seq INTEGER NOT NULL DEFAULT 1, invoked_at INTEGER NOT NULL
  )`);
  await db.execute(`CREATE TABLE flow_runs (
    flow_id TEXT PRIMARY KEY, pipeline_id TEXT, status TEXT NOT NULL,
    state_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
  )`);
  db.close();
}

async function seedOps(dbPath) {
  const db = createClient({ url: `file:${dbPath}` });
  await db.execute(`CREATE TABLE queue_items (
    id TEXT PRIMARY KEY, label TEXT NOT NULL, agent TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL, flow_id TEXT, node TEXT
  )`);
  await db.execute(`CREATE TABLE gate_state (
    id TEXT NOT NULL PRIMARY KEY DEFAULT 'singleton', locked INTEGER NOT NULL DEFAULT 0, day TEXT
  )`);
  await db.execute(`INSERT INTO gate_state (id, locked) VALUES ('singleton', 0)`);
  db.close();
}

function makeHarness(dbPaths) {
  const broadcasts = [];
  const pushes = [];
  const bridge = createBridge({
    ...dbPaths,
    pollIntervalMs: 60_000,
    getBroadcast: () => (name, payload) => broadcasts.push({ name, payload }),
    pushEvent: async (body) => { pushes.push(body); return { ok: true }; },
    logger: { info: () => {}, warn: () => {}, error: () => {} },
  });
  return { bridge, broadcasts, pushes };
}

describe("ios-transport bridge", () => {
  test("first cycle baselines silently, second cycle reports arrivals", async () => {
    const runtimeDbPath = tmpDb("runtime");
    const opsDbPath = tmpDb("ops");
    await seedRuntime(runtimeDbPath);
    await seedOps(opsDbPath);
    const { bridge, broadcasts, pushes } = makeHarness({ runtimeDbPath, opsDbPath });

    const first = await bridge.pollOnce();
    assert.deepEqual(first.failures, []);
    assert.deepEqual(first.approvals, []);
    assert.equal(broadcasts.length, 0, "baseline emits nothing");
    assert.equal(pushes.length, 0);

    const db = createClient({ url: `file:${runtimeDbPath}` });
    await db.execute({
      sql: "INSERT INTO failure_events (id, flow_id, failed_at, failure_summary, error_type) VALUES (?, ?, ?, ?, ?)",
      args: ["f1", "flow-1", Date.now(), "boom", "infra_unavailable"],
    });
    db.close();
    const ops = createClient({ url: `file:${opsDbPath}` });
    await ops.execute({
      sql: "INSERT INTO queue_items (id, label, agent, status, created_at, flow_id, node) VALUES (?, ?, ?, 'pending', ?, ?, ?)",
      args: ["q1", "SCORE", "scout", Date.now(), "flow-1", "SCORE"],
    });
    ops.close();

    const second = await bridge.pollOnce();
    assert.deepEqual(second.failures, ["f1"]);
    assert.deepEqual(second.approvals, ["q1"]);

    const failureWs = broadcasts.find((b) => b.name === "plugin.failure_event.start");
    assert.ok(failureWs, "failure WS name emitted");
    assert.equal(failureWs.payload.failureId, "f1");
    assert.equal(pushes.find((p) => p.kind === "failure")?.failureId, "f1");

    const approvalWs = broadcasts.find((b) => b.name === "plugin.approval.requested");
    assert.ok(approvalWs, "approval WS name emitted");
    assert.equal(pushes.find((p) => p.kind === "approval")?.flowId, "flow-1");
  });

  test("rerun without new rows emits nothing (idempotent)", async () => {
    const runtimeDbPath = tmpDb("runtime");
    const opsDbPath = tmpDb("ops");
    await seedRuntime(runtimeDbPath);
    await seedOps(opsDbPath);
    const { bridge, broadcasts, pushes } = makeHarness({ runtimeDbPath, opsDbPath });

    await bridge.pollOnce();
    const second = await bridge.pollOnce();
    assert.deepEqual(second.failures, []);
    assert.deepEqual(second.approvals, []);
    assert.equal(second.gateChanged, false);
    assert.equal(broadcasts.length, 0);
    assert.equal(pushes.length, 0);
  });

  test("gate lock transition pushes budget kind", async () => {
    const runtimeDbPath = tmpDb("runtime");
    const opsDbPath = tmpDb("ops");
    await seedRuntime(runtimeDbPath);
    await seedOps(opsDbPath);
    const { bridge, pushes } = makeHarness({ runtimeDbPath, opsDbPath });

    await bridge.pollOnce();
    const ops = createClient({ url: `file:${opsDbPath}` });
    await ops.execute("UPDATE gate_state SET locked = 1, day = '2026-10-22' WHERE id = 'singleton'");
    ops.close();

    const second = await bridge.pollOnce();
    assert.equal(second.gateChanged, true);
    assert.ok(pushes.find((p) => p.kind === "budget"), "budget push sent on lock transition");
  });

  test("missing tables fail a cycle without throwing", async () => {
    const runtimeDbPath = tmpDb("empty-runtime");
    const opsDbPath = tmpDb("empty-ops");
    const { bridge } = makeHarness({ runtimeDbPath, opsDbPath });
    const res = await bridge.pollOnce();
    assert.deepEqual(res.failures, []);
    assert.deepEqual(res.approvals, []);
  });
});
