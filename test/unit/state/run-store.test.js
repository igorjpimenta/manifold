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


function tmpDb() {
  return path.join(os.tmpdir(), `run-store-test-${randomUUID()}.sqlite`);
}

const dbPaths = new WeakMap();

async function makeStore(overrides = {}) {
  const dbPath = overrides.dbPath ?? tmpDb();
  const store = createRunStore({ ...overrides, dbPath });
  dbPaths.set(store, dbPath);
  return store;
}

// parent_invocation_id carries an FK; give the test invocation a real owning
// run rather than weakening the constraint the suite is checking.
async function seedInvocation(store, invocationId, ownerFlowId = null) {
  let flowId = ownerFlowId;
  if (flowId === null) {
    flowId = fakeFlow().flowId;
    await store.insert(fakeFlow({ flowId }));
  }
  const client = createClient({ url: `file:${dbPaths.get(store)}` });
  await client.execute({
    sql: "INSERT INTO flow_node_invocations (id, flow_id, node_id, invoked_at) VALUES (?, ?, ?, ?)",
    args: [invocationId, flowId, "SEED", Date.now()],
  });
  client.close();
}

function fakeFlow(overrides = {}) {
  return {
    flowId:       randomUUID(),
    ownerKey:     "manifold-owner",
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

// ── schema ────────────────────────────────────────────────────────────────────

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
    const s1 = createRunStore({ dbPath });
    await s1.close();
    const s2 = createRunStore({ dbPath });
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

  // The reason param must actually land in statusReason — a casSkipStmt
  // that writes NULL there silently drops the node's own _skipped.reason.
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
  test("list returns every row (no per-tenant filter — sole manifold store)", async () => {
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
    const topLevel = fakeFlow({ pipelineId: "team_pipe" });
    await store.insert(topLevel);
    await seedInvocation(store, invocationId, topLevel.flowId);
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

describe("createRunStore — getByParentInvocationId", () => {
  test("returns null when no child exists for the invocation", async () => {
    const store = await makeStore();
    assert.equal(await store.getByParentInvocationId(randomUUID()), null);
    store.close();
  });

  test("returns the child row for a matching invocation", async () => {
    const store = await makeStore();
    const invocationId = randomUUID();
    await seedInvocation(store, invocationId);
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
    await seedInvocation(store, inv1);
    await seedInvocation(store, inv2);
    await seedInvocation(store, inv3);
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
// Exported so callers deciding "is this flow cancellable / already done"
// check against the real terminal set instead of hand-rolling their own
// allowlist of active statuses. A `status !== "running" && status !==
// "blocked"` check misses "queued" (a real, non-terminal admission-pool
// wait) and wrongly refuses to cancel it, reporting it as "already
// terminal".
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
