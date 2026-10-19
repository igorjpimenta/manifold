/**
 * Unit tests for state/run-store.js
 *
 * Covers: schema creation + v2 migration, insert, CAS write methods
 * (casSetRunning, casSetWaiting, casFinish, casSkip, casFail, cancelFlow),
 * stampCancelRequested, stampStateJson, updateStateJsonCAS,
 * get, list, listFor, and mapRow JSON parsing.
 */
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createRunStore, TERMINAL_STATUSES, TERMINAL_STATUS_SET } from "../../../state/run-store.js";

const NO_LEGACY = path.join(os.tmpdir(), `run-store-no-legacy-${randomUUID()}.sqlite`);

function tmpDb() {
  return path.join(os.tmpdir(), `run-store-test-${randomUUID()}.sqlite`);
}

async function makeStore(overrides = {}) {
  return createRunStore({ dbPath: tmpDb(), legacyDbPath: NO_LEGACY, ...overrides });
}

function fakeFlow(overrides = {}) {
  return {
    flowId:       randomUUID(),
    ownerKey:     "pipeclaw-owner",
    pipelineId:   "team_pipe",
    revision:     0,
    status:       "running",
    lastStep:     null,
    stateJson:    { pipelineId: "team_pipe" },
    createdAt:    Date.now(),
    updatedAt:    Date.now(),
    ...overrides,
  };
}

// Raw seed helper for tests that need a pre-migration DB state.
async function seedLegacyDb(dbPath, rows = []) {
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute(`
    CREATE TABLE flow_runs (
      flow_id              TEXT    PRIMARY KEY,
      sync_mode            TEXT    NOT NULL DEFAULT 'managed',
      owner_key            TEXT    NOT NULL DEFAULT '',
      controller_id        TEXT,
      revision             INTEGER NOT NULL DEFAULT 0,
      status               TEXT    NOT NULL,
      notify_policy        TEXT    NOT NULL DEFAULT 'default',
      goal                 TEXT    NOT NULL DEFAULT '',
      current_step         TEXT,
      blocked_task_id      TEXT,
      blocked_summary      TEXT,
      state_json           TEXT    NOT NULL DEFAULT '{}',
      wait_json            TEXT,
      cancel_requested_at  INTEGER,
      created_at           INTEGER NOT NULL,
      updated_at           INTEGER NOT NULL,
      ended_at             INTEGER
    )
  `);
  for (const row of rows) {
    await client.execute({ sql: row.sql, args: row.args });
  }
  client.close();
}

// ── schema + migration ────────────────────────────────────────────────────────

describe("createRunStore — schema", () => {
  test("opens DB and creates flow_runs table without throwing", async () => {
    const store = await makeStore();
    const row = await store.get(randomUUID());
    assert.equal(row, null);
    store.close();
  });

  test("v2 migration: last_step column is queryable", async () => {
    const store = await makeStore();
    const f = fakeFlow({ lastStep: "INIT" });
    await store.insert(f);
    const got = await store.get(f.flowId);
    assert.equal(got.lastStep, "INIT");
    store.close();
  });

  test("idempotent: opening same dbPath twice does not throw", async () => {
    const dbPath = tmpDb();
    const s1 = createRunStore({ dbPath, legacyDbPath: NO_LEGACY });
    await s1.close();
    const s2 = createRunStore({ dbPath, legacyDbPath: NO_LEGACY });
    await s2.close();
  });
});

// ── insert ────────────────────────────────────────────────────────────────────

describe("createRunStore — insert", () => {
  test("stores a flow and retrieves it via get()", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", lastStep: null });
    await store.insert(f);
    const got = await store.get(f.flowId);
    assert.equal(got.flowId, f.flowId);
    assert.equal(got.status, "running");
    assert.equal(got.pipelineId, f.pipelineId);
    store.close();
  });

  test("is idempotent — second insert is ignored (OR IGNORE)", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running" });
    await store.insert(f);
    await store.insert({ ...f, status: "failed" });
    assert.equal((await store.get(f.flowId)).status, "running");
    store.close();
  });

  test("stateJson is parsed from JSON", async () => {
    const store = await makeStore();
    const f = fakeFlow({ stateJson: { pipelineId: "team_pipe", cost: 1.5 } });
    await store.insert(f);
    const got = await store.get(f.flowId);
    assert.equal(got.stateJson.pipelineId, "team_pipe");
    assert.equal(got.stateJson.cost, 1.5);
    store.close();
  });
});

// ── casSetRunning ─────────────────────────────────────────────────────────────

describe("createRunStore — casSetRunning", () => {
  test("updates status to running, advances lastStep, clears wait_json", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "blocked", revision: 0 });
    await store.insert(f);
    const r = await store.casSetRunning(f.flowId, { expectedRevision: 0, lastStep: "GATE", stateJson: {} });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "running");
    assert.equal(got.lastStep, "GATE");
    assert.equal(got.waitJson, null);
    assert.equal(got.revision, 1);
    store.close();
  });

  test("returns revision_conflict when revision does not match", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 3 });
    await store.insert(f);
    const r = await store.casSetRunning(f.flowId, { expectedRevision: 0, lastStep: "X", stateJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "revision_conflict");
    store.close();
  });

  test("returns terminal when flow is already terminal", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "succeeded", revision: 5 });
    await store.insert(f);
    const r = await store.casSetRunning(f.flowId, { expectedRevision: 5, lastStep: "X", stateJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "terminal");
    store.close();
  });

  test("returns not_found for unknown flowId", async () => {
    const store = await makeStore();
    const r = await store.casSetRunning(randomUUID(), { expectedRevision: 0, lastStep: "X", stateJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "not_found");
    store.close();
  });
});

// ── casSetWaiting ─────────────────────────────────────────────────────────────

describe("createRunStore — casSetWaiting", () => {
  test("sets status=blocked, lastStep=prior node, waitJson", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    const wj = { items: [{ itemId: "abc" }] };
    const r = await store.casSetWaiting(f.flowId, {
      expectedRevision: 0, lastStep: "FETCH", blockingNodeId: "GATE",
      stateJson: {}, waitJson: wj, statusReason: "waiting for gate",
    });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "blocked");
    assert.equal(got.lastStep, "FETCH");
    assert.deepEqual(got.waitJson, wj);
    assert.equal(got.statusReason, "waiting for gate");
    assert.equal(got.revision, 1);
    store.close();
  });

  test("returns revision_conflict when revision does not match", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 2 });
    await store.insert(f);
    const r = await store.casSetWaiting(f.flowId, { expectedRevision: 0, lastStep: "X", stateJson: {}, waitJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "revision_conflict");
    store.close();
  });
});

// ── casSetQueued ──────────────────────────────────────────────────────────────

describe("createRunStore — casSetQueued", () => {
  test("sets status=queued, lastStep=prior node, waitJson", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    const wj = { kind: "agent-semaphore" };
    const r = await store.casSetQueued(f.flowId, {
      expectedRevision: 0, lastStep: "FETCH", blockingNodeId: "DRAFT",
      stateJson: {}, waitJson: wj, statusReason: "waiting for a dispatch slot",
    });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "queued");
    assert.equal(got.lastStep, "FETCH");
    assert.deepEqual(got.waitJson, wj);
    assert.equal(got.statusReason, "waiting for a dispatch slot");
    assert.equal(got.revision, 1);
    store.close();
  });

  test("returns revision_conflict when revision does not match", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 2 });
    await store.insert(f);
    const r = await store.casSetQueued(f.flowId, { expectedRevision: 0, lastStep: "X", stateJson: {}, waitJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "revision_conflict");
    store.close();
  });

  test("returns terminal when flow is already terminal", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "succeeded", revision: 5 });
    await store.insert(f);
    const r = await store.casSetQueued(f.flowId, { expectedRevision: 5, lastStep: "X", stateJson: {}, waitJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "terminal");
    store.close();
  });
});

// ── casFinish / casFail / casSkip ─────────────────────────────────────────────

describe("createRunStore — casFinish / casFail / casSkip", () => {
  test("casFinish: status=succeeded, ended_at set", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    const r = await store.casFinish(f.flowId, { expectedRevision: 0, stateJson: { done: true } });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "succeeded");
    assert.ok(got.endedAt > 0, "endedAt should be set");
    store.close();
  });

  test("casFail: status=failed, statusReason preserved", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    const r = await store.casFail(f.flowId, { expectedRevision: 0, stateJson: {}, statusReason: "step threw" });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "failed");
    assert.equal(got.statusReason, "step threw");
    assert.ok(got.endedAt > 0);
    store.close();
  });

  test("casSkip: status=skipped, ended_at set", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    const r = await store.casSkip(f.flowId, { expectedRevision: 0, stateJson: { reason: "nothing to do" } });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "skipped");
    assert.ok(got.endedAt > 0);
    store.close();
  });

  // Regression test: casSkipStmt used to hardcode status_reason (formerly
  // blocked_summary) = NULL, silently dropping the node's own
  // _skipped.reason. Confirms the fix — the reason param now actually lands.
  test("casSkip: reason param persists to statusReason (regression: used to be silently dropped)", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    const r = await store.casSkip(f.flowId, {
      expectedRevision: 0, stateJson: {},
      reason: "Inbox up-to-date — 85 records checked, 0 needed action",
    });
    assert.equal(r.applied, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "skipped");
    assert.equal(got.statusReason, "Inbox up-to-date — 85 records checked, 0 needed action");
    store.close();
  });

  test("casFinish returns terminal when already succeeded", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "succeeded", revision: 4 });
    await store.insert(f);
    const r = await store.casFinish(f.flowId, { expectedRevision: 4, stateJson: {} });
    assert.equal(r.applied, false);
    assert.equal(r.code, "terminal");
    store.close();
  });
});

// ── cancelFlow ────────────────────────────────────────────────────────────────

describe("createRunStore — cancelFlow", () => {
  test("status=cancelled, ended_at set", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running" });
    await store.insert(f);
    const r = await store.cancelFlow(f.flowId);
    assert.equal(r.cancelled, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "cancelled");
    assert.ok(got.endedAt > 0);
    store.close();
  });

  test("returns cancelled=false if already terminal", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "succeeded" });
    await store.insert(f);
    const r = await store.cancelFlow(f.flowId);
    assert.equal(r.cancelled, false);
    assert.equal((await store.get(f.flowId)).status, "succeeded");
    store.close();
  });

  test("with no reason, clears any prior status_reason (regression guard: cancel used to leave stale blocked/queued/failed reasons behind)", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running" });
    await store.insert(f);
    await store.casSetWaiting(f.flowId, {
      expectedRevision: 0, lastStep: "GATE", blockingNodeId: "GATE",
      stateJson: {}, waitJson: { kind: "human-gate" }, statusReason: "awaiting 1 approval at GATE",
    });
    const r = await store.cancelFlow(f.flowId);
    assert.equal(r.cancelled, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "cancelled");
    assert.equal(got.statusReason, null);
    store.close();
  });

  test("with an explicit reason, persists it to status_reason", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running" });
    await store.insert(f);
    const r = await store.cancelFlow(f.flowId, "cancelled by operator");
    assert.equal(r.cancelled, true);
    assert.equal((await store.get(f.flowId)).statusReason, "cancelled by operator");
    store.close();
  });
});

// ── cancel path helpers ───────────────────────────────────────────────────────

describe("createRunStore — cancel path helpers", () => {
  test("stampCancelRequested sets cancel_requested_at", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running" });
    await store.insert(f);
    const before = Date.now();
    await store.stampCancelRequested(f.flowId);
    await store.flush(); // stampCancelRequested() defers through batch-writer
    const got = await store.get(f.flowId);
    assert.ok(got.cancelRequestedAt >= before);
    store.close();
  });

  test("stampStateJson updates only state_json", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", stateJson: { pipelineId: "x" } });
    await store.insert(f);
    await store.stampStateJson(f.flowId, { pipelineId: "x", cancelled: true });
    const got = await store.get(f.flowId);
    assert.equal(got.stateJson.cancelled, true);
    assert.equal(got.status, "running");
    store.close();
  });
});

// ── cancelWithStateJson (atomic combination) ───────────────────────────────────
// Replaces state-store.js's old sequential stampStateJson() + cancelFlow()
// pair — one db.batch() call so a crash between the two writes can no
// longer leave state_json updated with status still not 'cancelled'.

describe("createRunStore — cancelWithStateJson", () => {
  test("commits both the stateJson stamp and the status flip together", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", stateJson: { pipelineId: "x" } });
    await store.insert(f);
    const r = await store.cancelWithStateJson(f.flowId, { pipelineId: "x", cancelled: true }, "operator cancel");
    assert.equal(r.cancelled, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "cancelled");
    assert.equal(got.statusReason, "operator cancel");
    assert.equal(got.stateJson.cancelled, true);
    assert.ok(got.endedAt > 0);
    store.close();
  });

  test("stateJson=null skips the stateJson write, cancel still applies", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", stateJson: { pipelineId: "x" } });
    await store.insert(f);
    const r = await store.cancelWithStateJson(f.flowId, null);
    assert.equal(r.cancelled, true);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "cancelled");
    assert.equal(got.stateJson.pipelineId, "x", "original stateJson untouched when null is passed");
    store.close();
  });

  test("returns cancelled=false if already terminal, matching cancelFlow()'s own guard", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "succeeded", stateJson: { pipelineId: "x" } });
    await store.insert(f);
    const r = await store.cancelWithStateJson(f.flowId, { pipelineId: "x", cancelled: true }, "too late");
    assert.equal(r.cancelled, false);
    const got = await store.get(f.flowId);
    assert.equal(got.status, "succeeded", "terminal status must not be overwritten");
    store.close();
  });
});

// ── updateStateJsonCAS ────────────────────────────────────────────────────────

describe("createRunStore — updateStateJsonCAS", () => {
  test("applies when revision matches", async () => {
    const store = await makeStore();
    const f = fakeFlow({ revision: 5 });
    await store.insert(f);
    const r = await store.updateStateJsonCAS(f.flowId, 5, { updated: true });
    assert.equal(r.applied, true);
    assert.equal(r.via, "run-store-cas");
    assert.equal((await store.get(f.flowId)).stateJson.updated, true);
    store.close();
  });

  test("returns revision_conflict when revision doesn't match", async () => {
    const store = await makeStore();
    const f = fakeFlow({ revision: 5 });
    await store.insert(f);
    const r = await store.updateStateJsonCAS(f.flowId, 99, { updated: true });
    assert.equal(r.applied, false);
    assert.equal(r.code, "revision_conflict");
    store.close();
  });
});

// ── list / listFor ────────────────────────────────────────────────────────────

describe("createRunStore — list / listFor", () => {
  test("list returns every row (no per-tenant filter — sole pipeclaw store)", async () => {
    const store = await makeStore();
    await store.insert(fakeFlow({ pipelineId: "team_pipe" }));
    await store.insert(fakeFlow({ pipelineId: "other_pipe" }));
    const rows = await store.list();
    assert.equal(rows.length, 2);
    store.close();
  });

  test("list returns empty array when no rows", async () => {
    const store = await makeStore();
    assert.deepEqual(await store.list(), []);
    store.close();
  });

  // Regression test (Phase 2, childFlows): every consumer of list() means
  // "top-level pipeline runs" — a childFlows loop item (parentInvocationId
  // set) must never surface as its own row, or it pollutes every
  // list()-backed view (routes/pipelines.js's catalog, routes/runs.js's
  // list + compare-to-peers, CLI commands) with one fake entry per branch
  // item.
  test("list excludes childFlows children (parentInvocationId set), includes top-level rows only", async () => {
    const store = await makeStore();
    const invocationId = "inv-list-exclude-test";
    await store.insert(fakeFlow({ pipelineId: "team_pipe" }));
    await store.insert(fakeFlow({ pipelineId: "team_pipe::LOOP", parentInvocationId: invocationId }));
    const rows = await store.list();
    assert.equal(rows.length, 1, "only the top-level row should be returned");
    assert.equal(rows[0].pipelineId, "team_pipe");
    assert.equal(rows[0].parentInvocationId, null);
    store.close();
  });

  test("listFor returns only flows for the given pipelineId", async () => {
    const store = await makeStore();
    await store.insert(fakeFlow({ pipelineId: "team_a" }));
    await store.insert(fakeFlow({ pipelineId: "team_a" }));
    await store.insert(fakeFlow({ pipelineId: "team_b" }));
    const rows = await store.listFor("team_a");
    assert.equal(rows.length, 2);
    assert.ok(rows.every((r) => r.pipelineId === "team_a"));
    store.close();
  });

  test("listFor returns empty array for unknown pipelineId", async () => {
    const store = await makeStore();
    await store.insert(fakeFlow({ pipelineId: "team_a" }));
    assert.deepEqual(await store.listFor("nonexistent"), []);
    store.close();
  });
});

// ── get ───────────────────────────────────────────────────────────────────────

describe("createRunStore — get", () => {
  test("returns null for unknown flowId", async () => {
    const store = await makeStore();
    assert.equal(await store.get(randomUUID()), null);
    store.close();
  });
});

// ── v3 migration: pipeline_id, dropped columns ──────────────────────────────

describe("createRunStore — v3 migration", () => {
  test("dropped columns no longer appear in PRAGMA table_info", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const { rows } = await raw.execute("SELECT name FROM pragma_table_info('flow_runs')");
    const cols = rows.map((r) => r.name);
    for (const dead of ["sync_mode", "notify_policy", "blocked_task_id", "goal", "controller_id"]) {
      assert.ok(!cols.includes(dead), `${dead} should be dropped`);
    }
    raw.close();
  });

  test("pipeline_id backfilled from controller_id prefix on pre-existing rows", async () => {
    const dbPath = tmpDb();
    await seedLegacyDb(dbPath, [
      {
        sql: `INSERT INTO flow_runs (flow_id, controller_id, status, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
        args: ["f-pipeclaw", "pipeclaw:foo", "succeeded", "{}", 0, 0],
      },
      {
        sql: `INSERT INTO flow_runs (flow_id, controller_id, status, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
        args: ["f-legacy", "graph-flow:bar", "succeeded", "{}", 0, 0],
      },
    ]);

    const store = await createRunStore({ dbPath, legacyDbPath: NO_LEGACY });
    assert.equal((await store.get("f-pipeclaw")).pipelineId, "foo");
    assert.equal((await store.get("f-legacy")).pipelineId, "bar");
    store.close();
  });

  test("new rows via insert() write pipeline_id directly and listFor finds them", async () => {
    const store = await makeStore();
    await store.insert(fakeFlow({ pipelineId: "fresh_pipe" }));
    const rows = await store.listFor("fresh_pipe");
    assert.equal(rows.length, 1);
    assert.equal(rows[0].pipelineId, "fresh_pipe");
    store.close();
  });
});

// ── v4 migration: parent_invocation_id (renamed from parent_flow_node_execution_id) ──

describe("createRunStore — v4 migration", () => {
  test("parent_invocation_id column exists, old name gone", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const { rows } = await raw.execute("SELECT name FROM pragma_table_info('flow_runs')");
    const cols = rows.map((r) => r.name);
    assert.ok(cols.includes("parent_invocation_id"));
    assert.ok(!cols.includes("parent_flow_node_execution_id"));
    raw.close();
  });

  test("idx_pfr_parent_invocation unique index exists, old-named index gone", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const { rows: idx } = await raw.execute(`SELECT name, "unique" FROM pragma_index_list('flow_runs')`);
    const found = idx.find((i) => i.name === "idx_pfr_parent_invocation");
    assert.ok(found, "index should exist");
    assert.equal(Number(found["unique"]), 1);
    assert.ok(!idx.find((i) => i.name === "idx_pfr_parent_execution"));
    raw.close();
  });

  test("existing rows read NULL for parentInvocationId", async () => {
    const store = await makeStore();
    const f = fakeFlow();
    await store.insert(f);
    const got = await store.get(f.flowId);
    assert.equal(got.parentInvocationId, null);
    store.close();
  });

  test("insert() writes parentInvocationId and get() returns it", async () => {
    const store = await makeStore();
    const invocationId = randomUUID();
    const f = fakeFlow({ parentInvocationId: invocationId });
    await store.insert(f);
    const got = await store.get(f.flowId);
    assert.equal(got.parentInvocationId, invocationId);
  });

  test("two different non-NULL parent_invocation_id values both succeed", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const insertSql = `
      INSERT INTO flow_runs (flow_id, owner_key, pipeline_id, revision, status, last_step, state_json, created_at, updated_at, parent_invocation_id)
      VALUES (?, '', 'p', 0, 'blocked', NULL, '{}', 0, 0, ?)
    `;
    await raw.execute({ sql: insertSql, args: [randomUUID(), randomUUID()] });
    await raw.execute({ sql: insertSql, args: [randomUUID(), randomUUID()] });
    const { rows } = await raw.execute("SELECT flow_id FROM flow_runs WHERE parent_invocation_id IS NOT NULL");
    assert.equal(rows.length, 2);
    raw.close();
  });

  test("same parent_invocation_id twice throws SQLITE_CONSTRAINT", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const dup = randomUUID();
    const insertSql = `
      INSERT INTO flow_runs (flow_id, owner_key, pipeline_id, revision, status, last_step, state_json, created_at, updated_at, parent_invocation_id)
      VALUES (?, '', 'p', 0, 'blocked', NULL, '{}', 0, 0, ?)
    `;
    await raw.execute({ sql: insertSql, args: [randomUUID(), dup] });
    await assert.rejects(
      () => raw.execute({ sql: insertSql, args: [randomUUID(), dup] }),
      /UNIQUE constraint failed/,
    );
    raw.close();
  });
});

// ── v5 migration: status_reason (replaces blocked_summary) ─────────────────

describe("createRunStore — v5 migration", () => {
  test("status_reason column exists", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const { rows } = await raw.execute("SELECT name FROM pragma_table_info('flow_runs')");
    const cols = rows.map((r) => r.name);
    assert.ok(cols.includes("status_reason"));
    raw.close();
  });

  test("existing blocked_summary values are backfilled into status_reason", async () => {
    const dbPath = tmpDb();
    await seedLegacyDb(dbPath, [
      {
        sql: `INSERT INTO flow_runs (flow_id, controller_id, status, blocked_summary, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: ["f-with-reason", "pipeclaw:foo", "blocked", "waiting for gate", "{}", 0, 0],
      },
      {
        sql: `INSERT INTO flow_runs (flow_id, controller_id, status, blocked_summary, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: ["f-no-reason", "pipeclaw:foo", "running", null, "{}", 0, 0],
      },
    ]);

    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const check = createClient({ url: `file:${dbPath}` });
    const { rows: r1 } = await check.execute("SELECT status_reason FROM flow_runs WHERE flow_id = 'f-with-reason'");
    assert.equal(r1[0].status_reason, "waiting for gate");
    const { rows: r2 } = await check.execute("SELECT status_reason FROM flow_runs WHERE flow_id = 'f-no-reason'");
    assert.equal(r2[0].status_reason, null, "NULL blocked_summary must not backfill to a non-NULL status_reason");
    check.close();
  });

  test("casSetRunning clears status_reason (not just blocked_summary)", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "blocked", revision: 0 });
    await store.insert(f);
    await store.casSetWaiting(f.flowId, {
      expectedRevision: 0, lastStep: "A", blockingNodeId: "GATE",
      stateJson: {}, waitJson: {}, statusReason: "waiting for gate",
    });
    const blocked = await store.get(f.flowId);
    assert.equal(blocked.statusReason, "waiting for gate");
    await store.casSetRunning(blocked.flowId, { expectedRevision: blocked.revision, lastStep: "GATE", stateJson: {} });
    const running = await store.get(f.flowId);
    assert.equal(running.statusReason, null);
    store.close();
  });
});

// ── v6 migration: blocked_summary dropped ───────────────────────────────────

describe("createRunStore — v6 migration", () => {
  test("blocked_summary column no longer appears in PRAGMA table_info", async () => {
    const dbPath = tmpDb();
    await createRunStore({ dbPath, legacyDbPath: NO_LEGACY }).close();
    const raw = createClient({ url: `file:${dbPath}` });
    const { rows } = await raw.execute("SELECT name FROM pragma_table_info('flow_runs')");
    const cols = rows.map((r) => r.name);
    assert.ok(!cols.includes("blocked_summary"), "blocked_summary should be dropped");
    assert.ok(cols.includes("status_reason"), "status_reason must survive the drop");
    raw.close();
  });

  test("pre-existing blocked_summary data survives as status_reason after the drop", async () => {
    const dbPath = tmpDb();
    await seedLegacyDb(dbPath, [
      {
        sql: `INSERT INTO flow_runs (flow_id, controller_id, status, blocked_summary, state_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: ["f-survives", "pipeclaw:foo", "blocked", "waiting for gate", "{}", 0, 0],
      },
    ]);

    const store = await createRunStore({ dbPath, legacyDbPath: NO_LEGACY });
    assert.equal((await store.get("f-survives")).statusReason, "waiting for gate");
    store.close();
  });
});

// ── Phase 2: getByParentInvocationId / listForParentInvocations ────────────

describe("createRunStore — getByParentInvocationId", () => {
  test("returns null when no child exists for the invocation", async () => {
    const store = await makeStore();
    assert.equal(await store.getByParentInvocationId(randomUUID()), null);
    store.close();
  });

  test("returns the child row for a matching invocation", async () => {
    const store = await makeStore();
    const invocationId = randomUUID();
    await store.insert(fakeFlow({ parentInvocationId: invocationId }));
    const found = await store.getByParentInvocationId(invocationId);
    assert.ok(found);
    assert.equal(found.parentInvocationId, invocationId);
  });
});

describe("createRunStore — listForParentInvocations", () => {
  test("returns an empty array for an empty id list", async () => {
    const store = await makeStore();
    assert.deepEqual(await store.listForParentInvocations([]), []);
    store.close();
  });

  test("returns only rows matching the given invocation ids", async () => {
    const store = await makeStore();
    const inv1 = randomUUID();
    const inv2 = randomUUID();
    const inv3 = randomUUID();
    const f1 = fakeFlow({ parentInvocationId: inv1 });
    const f2 = fakeFlow({ parentInvocationId: inv2 });
    const f3 = fakeFlow({ parentInvocationId: inv3 });
    await store.insert(f1);
    await store.insert(f2);
    await store.insert(f3);

    const rows = await store.listForParentInvocations([inv1, inv2]);
    assert.equal(rows.length, 2);
    const flowIds = rows.map((r) => r.flowId).sort();
    assert.deepEqual(flowIds, [f1.flowId, f2.flowId].sort());
    store.close();
  });
});

// ── listActive() ──────────────────────────────────────────────────────────────

describe("createRunStore — listActive()", () => {
  test("returns only flows where ended_at IS NULL", async () => {
    const store = await makeStore();
    const active  = fakeFlow({ status: "running", revision: 0 });
    const blocked = fakeFlow({ status: "blocked",  revision: 0 });
    const terminal = fakeFlow({ status: "running", revision: 0 });
    await store.insert(active);
    await store.insert(blocked);
    await store.insert(terminal);
    // Drive terminal into succeeded so ended_at is stamped
    await store.casFinish(terminal.flowId, { expectedRevision: 0, stateJson: {} });

    const rows = await store.listActive();
    const ids = rows.map((r) => r.flowId);
    assert.ok(ids.includes(active.flowId),   "running flow must appear");
    assert.ok(ids.includes(blocked.flowId),  "blocked flow must appear");
    assert.ok(!ids.includes(terminal.flowId), "succeeded flow must be excluded");
    store.close();
  });

  test("returns empty array when all flows are terminal", async () => {
    const store = await makeStore();
    const f = fakeFlow({ status: "running", revision: 0 });
    await store.insert(f);
    await store.casFinish(f.flowId, { expectedRevision: 0, stateJson: {} });
    assert.deepEqual(await store.listActive(), []);
    store.close();
  });
});

// ─── TERMINAL_STATUSES / TERMINAL_STATUS_SET ───────────────────────────────────
//
// Exported specifically so callers deciding "is this flow cancellable /
// already done" (cli.js's and dashboard/server.js's cancel handlers) check
// against the real terminal set instead of hand-rolling their own allowlist
// of active statuses. Regression for a confirmed live bug (2026-09-25): both
// handlers used `status !== "running" && status !== "blocked"`, which
// misses "queued" (a real, non-terminal admission-pool wait) and wrongly
// refused to cancel a queued flow, reporting it as "already terminal".
describe("TERMINAL_STATUSES / TERMINAL_STATUS_SET", () => {
  test("contains exactly the four real terminal statuses", () => {
    assert.deepEqual([...TERMINAL_STATUSES].sort(), ["cancelled", "failed", "skipped", "succeeded"]);
  });

  test("does not include queued, running, or blocked — all genuinely non-terminal", () => {
    for (const status of ["queued", "running", "blocked"]) {
      assert.ok(!TERMINAL_STATUS_SET.has(status), `${status} must not be classified as terminal`);
    }
  });

  test("TERMINAL_STATUS_SET is a Set built from TERMINAL_STATUSES", () => {
    assert.ok(TERMINAL_STATUS_SET instanceof Set);
    for (const status of TERMINAL_STATUSES) {
      assert.ok(TERMINAL_STATUS_SET.has(status));
    }
    assert.equal(TERMINAL_STATUS_SET.size, TERMINAL_STATUSES.length);
  });
});
