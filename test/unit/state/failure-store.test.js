/**
 * Unit tests for state/failure-store.js
 *
 * Covers: createFailureStore — schema creation + idempotent open;
 * summarize (raw-stored, checked via SQL, reachable via
 * recordEngineFailure); classifyRecoveryHint (computed, checked via get()
 * against raw-seeded error_type/summary pairs); upsertFailureRow's two
 * identity paths (execution_id conflict, null-execution_id fallback
 * dedup); deriveFailureContext (pipeline_id via flow_runs,
 * branch_item_id/failure_node via invocation); computeResolvedAt's two
 * mechanisms (same-flow completion, cross-flow supersession); list()/get();
 * recordEngineFailure(); queryErrorsByFlow()/queryErrorsByFlowIds().
 */

import { test, describe, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createClient } from "@libsql/client";
import { createFailureStore } from "../../../state/failure-store.js";
import { failureEmitter } from "../../../events.js";

// ─── helpers ────────────────────────────────────────────────────────────────

function tmpDb() {
  return path.join(os.tmpdir(), `gf-test-${randomUUID()}.sqlite`);
}

async function makeStore(overrides = {}) {
  // Always inject a noop nodeExecutionsStore so the real
  // getNodeExecutionsStore() singleton — which opens and migrates the
  // default DB path — is never triggered in tests.
  const store = createFailureStore({
    dbPath: tmpDb(),
    nodeExecutionsStore: makeFakeNodeExecutions(),
    ...overrides,
  });
  const record = store.recordEngineFailure.bind(store);
  return new Proxy(store, {
    get(target, key) {
      if (key === "recordEngineFailure") {
        return async (args) => {
          if (args?.flowId) {
            await seedFlowRun(target, { flowId: args.flowId, pipelineId: args.pipelineId ?? "pipe" });
            await seedExecutionChain(target, {
              flowId: args.flowId, invocationId: args.invocationId, executionId: args.executionId,
            });
          }
          return record(args);
        };
      }
      const value = target[key];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

// failure-store.js's nodeExecutionsStore is injectable specifically so
// tests don't fall through to the real getNodeExecutionsStore() singleton.
// The fake exposes only the three accessors failure-store.js actually
// calls. getLatestExecutionOutcomeForItem's real shape is
// {executionId, exitedAt} (no `error`) — "did it succeed" is answered by
// computeResolvedAt asking failure_events directly for that execution_id.
function makeFakeNodeExecutions({
  invocations = {},
  latestExecutionForNode = () => null,
  latestExecutionOutcomeForItem = () => null,
} = {}) {
  return {
    getInvocation: (id) => invocations[id] ?? null,
    getLatestExecutionForNode: latestExecutionForNode,
    getLatestExecutionOutcomeForItem: latestExecutionOutcomeForItem,
  };
}

// flow_runs isn't created by failure-store.js's own migrator — it belongs
// to run-store.js. deriveFailureContext/computeResolvedAt read it directly
// via drizzle, so tests exercising pipeline_id derivation or same-flow
// resolution need to hand-create it against the same DB file.
async function seedFlowRun(store, { flowId, pipelineId = "pipe", status = "running", updatedAt = Date.now() }) {
  await store._db.execute(`
    CREATE TABLE IF NOT EXISTS flow_runs (
      flow_id TEXT PRIMARY KEY, owner_key TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL, last_step TEXT, status_reason TEXT, state_json TEXT NOT NULL DEFAULT '{}',
      wait_json TEXT, cancel_requested_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      ended_at INTEGER, parent_invocation_id TEXT, pipeline_id TEXT
    )`);
  await store._db.execute({
    sql: `INSERT INTO pipelines (id, name, created_at, updated_at)
          VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
    args: [pipelineId, pipelineId, updatedAt, updatedAt],
  });
  await store._db.execute({
    sql: `INSERT INTO flow_runs (flow_id, status, pipeline_id, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(flow_id) DO UPDATE SET status = excluded.status, pipeline_id = excluded.pipeline_id, updated_at = excluded.updated_at`,
    args: [flowId, status, pipelineId, updatedAt, updatedAt],
  });
}

// Raw insert matching the stored shape — used where a test needs to seed
// a row directly rather than go through recordEngineFailure.
async function seedExecutionChain(store, { flowId, invocationId = null, executionId = null, at = Date.now() } = {}) {
  let invId = invocationId;
  if (invId) {
    await store._db.execute({
      sql: `INSERT INTO flow_node_invocations (id, flow_id, node_id, visit_seq, invoked_at)
            VALUES (?, ?, 'NODE', 1, ?) ON CONFLICT(id) DO NOTHING`,
      args: [invId, flowId, at],
    });
  }
  if (executionId) {
    const existingExecution = await store._db.execute({
      sql: "SELECT invocation_id FROM flow_node_executions WHERE id = ?",
      args: [executionId],
    });
    if (existingExecution.rows.length) return;
    if (!invId) {
      const existing = await store._db.execute({
        sql: "SELECT id FROM flow_node_invocations WHERE flow_id = ? LIMIT 1",
        args: [flowId],
      });
      invId = existing.rows[0]?.id ?? randomUUID();
      if (!existing.rows.length) {
        await store._db.execute({
          sql: `INSERT INTO flow_node_invocations (id, flow_id, node_id, visit_seq, invoked_at)
                VALUES (?, ?, 'NODE', 1, ?) ON CONFLICT(id) DO NOTHING`,
          args: [invId, flowId, at],
        });
      }
    }
    await store._db.execute({
      sql: `INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, kind)
            VALUES (?, ?, ?, ?, 'legacy') ON CONFLICT(id) DO NOTHING`,
      args: [executionId, invId, at, at],
    });
  }
}

async function insertRawRow(store, { id = randomUUID(), flowId = "f1", failedAt = Date.now(), failureSummary = "err", invocationId = null, executionId = null, errorType = null, seedParent = true } = {}) {
  if (seedParent) {
    const { rows } = await store._db.execute({
      sql: "SELECT 1 FROM flow_runs WHERE flow_id = ?",
      args: [flowId],
    });
    if (!rows.length) await seedFlowRun(store, { flowId });
  }
  await seedExecutionChain(store, { flowId, invocationId, executionId, at: failedAt });
  await store._db.execute({
    sql: `INSERT INTO failure_events (id, flow_id, failed_at, failure_summary, invocation_id, execution_id, error_type)
          VALUES (?, ?, ?, ?, ?, ?, ?)`,
    args: [id, flowId, failedAt, failureSummary, invocationId, executionId, errorType],
  });
  return id;
}

// ─── createFailureStore — schema + migration ─────────────────────────────────

describe("createFailureStore — DB setup", () => {
  test("creates DB, table, and indexes without throwing", async () => {
    const store = await makeStore();
    assert.ok(store._db, "exposes _db");
    const { rows } = await store._db.execute("SELECT * FROM failure_events LIMIT 1");
    assert.ok(Array.isArray(rows));
    store.close();
  });

  test("schema is idempotent — opening twice on same file does not throw", async () => {
    const dbPath = tmpDb();
    const s1 = createFailureStore({ dbPath, nodeExecutionsStore: makeFakeNodeExecutions() });
    await s1.close();
    const s2 = createFailureStore({ dbPath, nodeExecutionsStore: makeFakeNodeExecutions() });
    await s2.close();
  });
});


// ─── summarize — still a raw-stored column ────────────────────────────────────
//
// summarize remains live — recordEngineFailure still calls it — and the
// coverage below exercises it through that path.

describe("summarize — via failure_summary (still a raw column)", () => {
  test("stores long string truncated to 280 chars with ellipsis", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const longMsg = "x".repeat(400);
    await store.recordEngineFailure({ flowId, pipelineId: "test_pipe", failureNode: "NODE", message: longMsg, state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    const summary = rows[0].failure_summary;
    assert.equal(summary.length, 280);
    assert.ok(summary.endsWith("…"));
    store.close();
  });

  test("collapses internal whitespace", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const msg = "   too   many   spaces   ";
    await store.recordEngineFailure({ flowId, pipelineId: "test_pipe", failureNode: "NODE", message: msg, state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows[0].failure_summary, "too many spaces");
    store.close();
  });

  test("handles non-string input via String() coercion (via engine failure)", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({ flowId, pipelineId: "test_pipe", failureNode: "N", message: 12345, state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows[0].failure_summary, "12345");
    store.close();
  });
});

// ─── classifyRecoveryHint — computed, checked via get() ───────────────────────
// Exercised directly against a raw-seeded (error_type, summary) pair —
// classifyRecoveryHint is a pure function of the STORED error_type/summary
// and doesn't care how they got there.

describe("classifyRecoveryHint — via get() (computed at read time)", () => {
  async function hintFor({ errorType = null, summary = "" } = {}) {
    const store = await makeStore();
    const flowId = randomUUID();
    const id = await insertRawRow(store, { flowId, errorType, failureSummary: summary });
    const row = await store.get(id);
    store.close();
    return row?.recovery_hint ?? null;
  }

  test("null error_type (ordinary engine-caught throw) → retry-step", async () => {
    assert.equal(await hintFor({ errorType: null }), "retry-step");
  });

  test("infra_unavailable → retry-step", async () => {
    assert.equal(await hintFor({ errorType: "infra_unavailable", summary: "browser down" }), "retry-step");
  });

  test("blocked + workana-forbidden → skip-only", async () => {
    assert.equal(await hintFor({ errorType: "blocked", summary: "workana-forbidden: HTTP 403" }), "skip-only");
  });

  test("blocked + unrecognized reason → investigate", async () => {
    assert.equal(await hintFor({ errorType: "blocked", summary: "something weird happened" }), "investigate");
  });

  test("confirmed_absent → investigate", async () => {
    assert.equal(await hintFor({ errorType: "confirmed_absent" }), "investigate");
  });

  test("inconclusive → investigate", async () => {
    assert.equal(await hintFor({ errorType: "inconclusive" }), "investigate");
  });

  test("never_attempted → investigate", async () => {
    assert.equal(await hintFor({ errorType: "never_attempted" }), "investigate");
  });

  test("subflow-mode-not-satisfied → investigate", async () => {
    assert.equal(await hintFor({ errorType: "subflow-mode-not-satisfied" }), "investigate");
  });

  test("unrecognized error_type → investigate (default)", async () => {
    assert.equal(await hintFor({ errorType: "some-unknown-type" }), "investigate");
  });

  test("recordEngineFailure with no errorType → retry-step", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({ flowId, pipelineId: "test_pipe", failureNode: "NODE", message: "executor threw", state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT id FROM failure_events WHERE flow_id = ?", args: [flowId] });
    const row = await store.get(rows[0].id);
    assert.equal(row.recovery_hint, "retry-step");
    store.close();
  });
});

// ─── deriveFailureContext — pipeline_id / branch_item_id / failure_node ──────

describe("deriveFailureContext — via get()", () => {
  test("pipeline_id derives from flow_runs via flow_id, even with no invocation_id", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await seedFlowRun(store, { flowId, pipelineId: "derived-pipe" });
    const id = await insertRawRow(store, { flowId });
    const row = await store.get(id);
    assert.equal(row.pipeline_id, "derived-pipe");
    store.close();
  });

  test("a failure row cannot reference a flow_runs row that does not exist", async () => {
    const store = await makeStore();
    await assert.rejects(
      () => insertRawRow(store, { flowId: "no-such-flow", seedParent: false }),
      /FOREIGN KEY constraint failed/,
    );
    store.close();
  });

  test("branch_item_id/failure_node derive from the linked invocation", async () => {
    const invocationId = randomUUID();
    const nodeExecutionsStore = makeFakeNodeExecutions({
      invocations: { [invocationId]: { pipelineId: "pipe", nodeId: "SUBMIT", subflowKey: "job-1" } },
    });
    const store = await makeStore({ nodeExecutionsStore });
    const id = await insertRawRow(store, { invocationId });
    const row = await store.get(id);
    assert.equal(row.branch_item_id, "job-1");
    assert.equal(row.failure_node, "SUBMIT");
    store.close();
  });

  test("branch_item_id/failure_node are null when there's no invocation_id at all", async () => {
    const store = await makeStore();
    const id = await insertRawRow(store, { invocationId: null });
    const row = await store.get(id);
    assert.equal(row.branch_item_id, null);
    assert.equal(row.failure_node, null);
    store.close();
  });
});

// ─── computeResolvedAt — the two resolution mechanisms ───────────────────────

describe("computeResolvedAt — via get()", () => {
  // Every failure_events row is guaranteed to trace to a real throw by
  // construction, so this mechanism is unconditional on flow status alone.
  test("same-flow completion resolves unconditionally once the flow reaches succeeded/skipped", async () => {
    const flowId = randomUUID();
    const store = await makeStore();
    await seedFlowRun(store, { flowId, status: "succeeded", updatedAt: 999999 });
    const id = await insertRawRow(store, { flowId, failedAt: 1000 });
    const row = await store.get(id);
    assert.equal(row.resolved_at, 999999, "resolved_at should be the flow's own updated_at");
    store.close();
  });

  test("same-flow completion resolves on flow status 'skipped' too", async () => {
    const flowId = randomUUID();
    const store = await makeStore();
    await seedFlowRun(store, { flowId, status: "skipped", updatedAt: 555555 });
    const id = await insertRawRow(store, { flowId, failedAt: 1000 });
    const row = await store.get(id);
    assert.equal(row.resolved_at, 555555);
    store.close();
  });

  test("same-flow completion does NOT resolve when the flow's own status is still 'failed'", async () => {
    const flowId = randomUUID();
    const store = await makeStore();
    await seedFlowRun(store, { flowId, status: "failed" });
    const id = await insertRawRow(store, { flowId, failedAt: 1000 });
    const row = await store.get(id);
    assert.equal(row.resolved_at, null);
    store.close();
  });

  // Cross-flow supersession — matching a LATER invocation of the same
  // (pipeline_id, node_id, subflow_key) tuple, in ANY flow, as resolving
  // THIS failure — must stay false: subflow_key isn't always a real
  // per-item identity (single-item retry-cascade nodes like SUBMIT
  // dispatch every item under the same constant literal key), so tuple
  // matching would falsely resolve one job's failure because an unrelated
  // job's later dispatch of the same node succeeded. These tests prove a
  // later, unrelated dispatch of the identical tuple does NOT resolve
  // this failure, root or subflow-child alike — resolution is
  // same-flow-completion only.
  async function seedChildOutcome(store, { pipelineId = "pipe", nodeId = "SUBMIT", subflowKey = "job-1", invokedAt = Date.now(), childStatus = "succeeded", childUpdatedAt = Date.now() }) {
    await ensureInvocationsTable(store);
    const parentFlowId = randomUUID();
    await seedFlowRun(store, { flowId: parentFlowId, pipelineId });
    await store._db.execute({
      sql: `INSERT INTO pipelines (id, name, created_at, updated_at)
            VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
      args: [`${pipelineId}::${nodeId}`, `${pipelineId}::${nodeId}`, childUpdatedAt, childUpdatedAt],
    });
    const parentInvocationId = randomUUID();
    await store._db.execute({
      sql: `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, subflow_key, invoked_at) VALUES (?, ?, ?, ?, ?, ?)`,
      args: [parentInvocationId, parentFlowId, pipelineId, nodeId, subflowKey, invokedAt],
    });
    const childFlowId = randomUUID();
    await store._db.execute(`
      CREATE TABLE IF NOT EXISTS flow_runs (
        flow_id TEXT PRIMARY KEY, owner_key TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL, last_step TEXT, status_reason TEXT, state_json TEXT NOT NULL DEFAULT '{}',
        wait_json TEXT, cancel_requested_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        ended_at INTEGER, parent_invocation_id TEXT, pipeline_id TEXT
      )`);
    await store._db.execute({
      sql: `INSERT INTO flow_runs (flow_id, status, pipeline_id, parent_invocation_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: [childFlowId, childStatus, `${pipelineId}::${nodeId}`, parentInvocationId, childUpdatedAt, childUpdatedAt],
    });
    return { parentInvocationId, childFlowId };
  }

  test("a later, unrelated dispatch of the same (pipeline_id, node_id, subflow_key) tuple succeeding does NOT resolve this failure", async () => {
    const invocationId = randomUUID();
    const nodeExecutionsStore = makeFakeNodeExecutions({
      invocations: { [invocationId]: { pipelineId: "pipe", nodeId: "SUBMIT", subflowKey: "job-1" } },
    });
    const store = await makeStore({ nodeExecutionsStore });
    const id = await insertRawRow(store, { invocationId, failedAt: 1000 });
    await seedChildOutcome(store, { invokedAt: 6000, childStatus: "succeeded", childUpdatedAt: 5000 });
    const row = await store.get(id);
    assert.equal(row.resolved_at, null);
    store.close();
  });

  test("still does not resolve when the later dispatch's child flow also failed", async () => {
    const invocationId = randomUUID();
    const nodeExecutionsStore = makeFakeNodeExecutions({
      invocations: { [invocationId]: { pipelineId: "pipe", nodeId: "SUBMIT", subflowKey: "job-1" } },
    });
    const store = await makeStore({ nodeExecutionsStore });
    const id = await insertRawRow(store, { invocationId, failedAt: 1000 });
    await seedChildOutcome(store, { invokedAt: 6000, childStatus: "failed", childUpdatedAt: 5000 });
    const row = await store.get(id);
    assert.equal(row.resolved_at, null);
    store.close();
  });

  test("still does not resolve when the later dispatch's child flow finished BEFORE this failure", async () => {
    const invocationId = randomUUID();
    const nodeExecutionsStore = makeFakeNodeExecutions({
      invocations: { [invocationId]: { pipelineId: "pipe", nodeId: "SUBMIT", subflowKey: "job-1" } },
    });
    const store = await makeStore({ nodeExecutionsStore });
    const id = await insertRawRow(store, { invocationId, failedAt: 1000 });
    await seedChildOutcome(store, { invokedAt: 200, childStatus: "succeeded", childUpdatedAt: 500 }); // before failedAt
    const row = await store.get(id);
    assert.equal(row.resolved_at, null);
    store.close();
  });

  test("branch_item_id still derives via the parent_invocation_id fallback, but resolved_at stays null regardless of a later cross-flow success", async () => {
    // Mirrors what actually happens in production: the failing node
    // (e.g. SUBMIT_CODE) is dispatched INSIDE a subflow's spawned child
    // flow, so its own invocation carries subflowKey:null — the tag only
    // lives on the PARENT's own invocation of the subflow node, reachable
    // via this failure's flow's own flow_runs.parent_invocation_id. This
    // display-only derivation (branch_item_id) is unaffected by removing
    // cross-flow supersession — only resolution semantics changed.
    const failingInvocationId = randomUUID();
    const failingFlowId = randomUUID();
    const parentInvocationId = randomUUID();
    // getInvocation() is a pure lookup against nodeExecutionsStore (the
    // mock here), NOT a raw-SQL join like the "later dispatch" query
    // below — so the parent invocation only needs to exist in the mock,
    // not as a real flow_node_invocations row.
    const nodeExecutionsStore = makeFakeNodeExecutions({
      invocations: {
        [failingInvocationId]: { pipelineId: "pipe::SUBMIT", nodeId: "SUBMIT_CODE", subflowKey: null },
        [parentInvocationId]: { pipelineId: "pipe", nodeId: "SUBMIT", subflowKey: "job-1" },
      },
    });
    const store = await makeStore({ nodeExecutionsStore });
    // This failure's own flow is itself a subflow child — seed its
    // flow_runs row with a parent_invocation_id pointing at the parent's
    // per-item dispatch of "SUBMIT" with subflowKey "job-1".
    await seedFlowRun(store, { flowId: failingFlowId, status: "failed" });
    const parentFlowId = randomUUID();
    await seedFlowRun(store, { flowId: parentFlowId, status: "running" });
    await store._db.execute({
      sql: `INSERT INTO flow_node_invocations (id, flow_id, node_id, visit_seq, invoked_at)
            VALUES (?, ?, 'SUBMIT', 1, ?)`,
      args: [parentInvocationId, parentFlowId, Date.now()],
    });
    await store._db.execute({
      sql: `UPDATE flow_runs SET parent_invocation_id = ? WHERE flow_id = ?`,
      args: [parentInvocationId, failingFlowId],
    });
    const id = await insertRawRow(store, { flowId: failingFlowId, invocationId: failingInvocationId, failedAt: 1000 });
    // A LATER dispatch of the same "pipe"/"SUBMIT"/"job-1" item spawned a
    // child that succeeded — this must not resolve the failure.
    await seedChildOutcome(store, { pipelineId: "pipe", nodeId: "SUBMIT", subflowKey: "job-1", invokedAt: 6000, childStatus: "succeeded", childUpdatedAt: 5000 });
    const row = await store.get(id);
    assert.equal(row.branch_item_id, "job-1", "branch_item_id should still derive via the parent_invocation_id fallback");
    assert.equal(row.resolved_at, null, "resolution does not depend on cross-flow item identity");
    store.close();
  });

  test("root-level failure (no invocation_id) stays open with no way to derive either mechanism", async () => {
    const store = await makeStore();
    const id = await insertRawRow(store, { invocationId: null, executionId: null });
    const row = await store.get(id);
    assert.equal(row.resolved_at, null);
    store.close();
  });
});

// ─── upsertFailureRow — identity ──────────────────────────────────────────────

describe("upsertFailureRow (via recordEngineFailure) — identity", () => {
  test("execution_id conflict updates the existing row instead of duplicating", async () => {
    const store = await makeStore();
    const executionId = randomUUID();
    await insertRawRow(store, { flowId: "f1", executionId, failedAt: 1000, failureSummary: "first" });
    // recordEngineFailure with the SAME executionId should update in place.
    await store.recordEngineFailure({ flowId: "f1", pipelineId: "pipe", failureNode: "N", message: "second", state: {}, executionId });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE execution_id = ?", args: [executionId] });
    assert.equal(rows.length, 1, "must not duplicate on execution_id conflict");
    assert.equal(rows[0].failure_summary, "second");
    store.close();
  });

  test("null execution_id falls back to manual dedup by flow_id", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({ flowId, pipelineId: "pipe", failureNode: "N", message: "first", state: {} });
    await store.recordEngineFailure({ flowId, pipelineId: "pipe", failureNode: "N", message: "second", state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows.length, 1, "a stuck retry loop with no execution_id must update in place, not duplicate");
    assert.equal(rows[0].failure_summary, "second");
    store.close();
  });

  test("distinct flow_ids with null execution_id never collide with each other", async () => {
    const store = await makeStore();
    await store.recordEngineFailure({ flowId: "flow-A", pipelineId: "pipe", failureNode: "N", message: "a", state: {} });
    await store.recordEngineFailure({ flowId: "flow-B", pipelineId: "pipe", failureNode: "N", message: "b", state: {} });
    const { rows } = await store._db.execute("SELECT * FROM failure_events");
    assert.equal(rows.length, 2);
    store.close();
  });
});

// ─── list() ──────────────────────────────────────────────────────────────────

describe("list()", () => {
  test("no args returns only unresolved rows", async () => {
    const resolvedFlowId = randomUUID();
    const store = await makeStore();
    const openId = await insertRawRow(store, { flowId: "f-open" });
    await seedFlowRun(store, { flowId: resolvedFlowId, status: "succeeded", updatedAt: 999 });
    await insertRawRow(store, { flowId: resolvedFlowId, failedAt: 500 });

    const rows = await store.list();
    const ids = rows.map((r) => r.id);
    assert.deepEqual(ids, [openId], "only the unresolved row should be returned by default");
    store.close();
  });

  test("resolved=true returns only resolved rows", async () => {
    const flowId = randomUUID();
    const store = await makeStore();
    await seedFlowRun(store, { flowId, status: "succeeded", updatedAt: 777 });
    const resolvedId = await insertRawRow(store, { flowId, failedAt: 100, failureSummary: "resolved-row" });
    await insertRawRow(store, { flowId: "other-open", failureSummary: "open-row" });

    const rows = await store.list({ resolved: true });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, resolvedId);
    store.close();
  });

  test("resolved=null returns all rows regardless of resolution status", async () => {
    const store = await makeStore();
    await insertRawRow(store, { flowId: "f1" });
    await insertRawRow(store, { flowId: "f2" });
    const rows = await store.list({ resolved: null });
    assert.equal(rows.length, 2);
    store.close();
  });

  test("since param filters by failed_at >= since", async () => {
    const store = await makeStore();
    const old = Date.now() - 100_000;
    const recent = Date.now();
    await insertRawRow(store, { flowId: "f1", failedAt: old, failureSummary: "old" });
    await insertRawRow(store, { flowId: "f2", failedAt: recent, failureSummary: "recent" });
    const rows = await store.list({ since: recent - 1, resolved: null });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].failure_summary, "recent");
    store.close();
  });

  test("limit param caps results (default 50)", async () => {
    const store = await makeStore();
    for (let i = 0; i < 10; i++) {
      await insertRawRow(store, { flowId: `f${i}` });
    }
    const rows3 = await store.list({ limit: 3, resolved: null });
    assert.equal(rows3.length, 3);
    const rowsAll = await store.list({ resolved: null });
    assert.equal(rowsAll.length, 10);
    store.close();
  });

  test("orders by failed_at DESC", async () => {
    const store = await makeStore();
    await insertRawRow(store, { flowId: "f1", failedAt: 1000, failureSummary: "first" });
    await insertRawRow(store, { flowId: "f2", failedAt: 2000, failureSummary: "second" });
    await insertRawRow(store, { flowId: "f3", failedAt: 3000, failureSummary: "third" });
    const rows = await store.list({ resolved: null });
    assert.equal(rows[0].failure_summary, "third");
    assert.equal(rows[1].failure_summary, "second");
    assert.equal(rows[2].failure_summary, "first");
    store.close();
  });
});

// ─── get() ───────────────────────────────────────────────────────────────────

describe("get()", () => {
  test("returns full derived row by id", async () => {
    const store = await makeStore();
    const id = await insertRawRow(store, { flowId: "f1", failureSummary: "err msg" });
    const row = await store.get(id);
    assert.ok(row, "row found");
    assert.equal(row.id, id);
    assert.equal(row.failure_summary, "err msg");
    store.close();
  });

  test("returns null for unknown id", async () => {
    const store = await makeStore();
    const row = await store.get(randomUUID());
    assert.equal(row, null);
    store.close();
  });
});

// failureEmitter coverage lives in recordEngineFailure()'s describe below
// — same emit call, same payload shape.

// ─── recordEngineFailure() ────────────────────────────────────────────────────

describe("recordEngineFailure()", () => {
  test("emits failure.recorded with failureId + flowId when emit is injected", async () => {
    const calls = [];
    const store = await makeStore({ emit: async (kind, payload) => { calls.push({ kind, payload }); } });
    const flowId = randomUUID();
    await store.recordEngineFailure({
      flowId, pipelineId: "pipe", failureNode: "NODE_X", message: "executor threw", state: {},
    });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].kind, "failure.recorded");
    assert.equal(typeof calls[0].payload.failureId, "string");
    assert.equal(calls[0].payload.flowId, flowId);
  });

  test("inserts a row with error_type=null and (derived) recovery_hint='retry-step'", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const result = await store.recordEngineFailure({
      flowId, pipelineId: "pipe", failureNode: "NODE_X", message: "executor threw", state: {},
    });
    assert.equal(result.recorded, 1);
    const { rows } = await store._db.execute({ sql: "SELECT id, error_type FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].error_type, null);
    const row = await store.get(rows[0].id);
    assert.equal(row.recovery_hint, "retry-step");
    store.close();
  });

  // child-resume-trigger.js's maybeResumeParent calls this with
  // errorType:"subflow-mode-not-satisfied" for a mode-not-satisfied
  // failure — not a real engine-caught throw, a policy verdict (see this
  // function's own doc comment). Default (previous test) stays null —
  // every other existing caller unaffected.
  test("explicit errorType param is stored as passed", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({
      flowId, pipelineId: "pipe", failureNode: "SUBMIT", message: "mode not satisfied", state: {},
      errorType: "subflow-mode-not-satisfied",
    });
    const { rows } = await store._db.execute({ sql: "SELECT error_type FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows[0].error_type, "subflow-mode-not-satisfied");
    store.close();
  });

  test("threads invocationId/executionId onto the row when passed", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({
      flowId, pipelineId: "pipe", failureNode: "NODE_X", message: "executor threw", state: {},
      invocationId: "inv-1", executionId: "exec-1",
    });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows[0].invocation_id, "inv-1");
    assert.equal(rows[0].execution_id, "exec-1");
    store.close();
  });

  test("invocation_id/execution_id stay NULL when neither is passed", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({ flowId, pipelineId: "pipe", failureNode: "NODE_X", message: "executor threw", state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows[0].invocation_id, null);
    assert.equal(rows[0].execution_id, null);
    store.close();
  });

  test("threads errorType onto the row when passed, null when omitted", async () => {
    const store = await makeStore();
    const flowIdA = randomUUID();
    const flowIdB = randomUUID();
    await store.recordEngineFailure({
      flowId: flowIdA, pipelineId: "pipe", failureNode: "NODE_X", message: "landing page unresolved", state: {},
      errorType: "infra_unavailable",
    });
    await store.recordEngineFailure({ flowId: flowIdB, pipelineId: "pipe", failureNode: "NODE_Y", message: "plain crash", state: {} });
    const rowA = (await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowIdA] })).rows[0];
    const rowB = (await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowIdB] })).rows[0];
    assert.equal(rowA.error_type, "infra_unavailable");
    assert.equal(rowB.error_type, null);
    store.close();
  });

  test("returns {recorded:0} when flowId missing", async () => {
    const store = await makeStore();
    const result = await store.recordEngineFailure({ flowId: null, pipelineId: "pipe", message: "x", state: {} });
    assert.deepEqual(result, { recorded: 0 });
    store.close();
  });

  test("returns {recorded:0} when pipelineId missing", async () => {
    const store = await makeStore();
    const result = await store.recordEngineFailure({ flowId: randomUUID(), pipelineId: null, message: "x", state: {} });
    assert.deepEqual(result, { recorded: 0 });
    store.close();
  });

  test("is idempotent — second call with no execution_id UPDATES the existing row instead of duplicating", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    await store.recordEngineFailure({ flowId, pipelineId: "pipe", failureNode: "N", message: "first", state: {} });
    await store.recordEngineFailure({ flowId, pipelineId: "pipe", failureNode: "N", message: "second", state: {} });
    const { rows } = await store._db.execute({ sql: "SELECT * FROM failure_events WHERE flow_id = ?", args: [flowId] });
    assert.equal(rows.length, 1, "must not duplicate — execution_id/flow_id dedup enforces this");
    assert.equal(rows[0].failure_summary, "second");
    store.close();
  });

  test("emits failure.created on failureEmitter with the fully-derived (snake_case) row shape", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    let emitted = null;
    const handler = (row) => { emitted = row; };
    failureEmitter.once("failure.created", handler);
    await store.recordEngineFailure({
      flowId, pipelineId: "test_pipe", failureNode: "NODE_X", message: "something broke", state: {},
    });
    assert.ok(emitted !== null, "failureEmitter fired");
    // Same shape as get()/list() — this is what a consumer's failure-event
    // projector actually reads.
    assert.equal(emitted.flow_id, flowId);
    assert.equal(emitted.error_type, null);
    assert.equal(emitted.failure_summary, "something broke");
    store.close();
  });

  test("does NOT emit failure.created when flowId is missing (early return)", async () => {
    const store = await makeStore();
    let fired = false;
    failureEmitter.once("failure.created", () => { fired = true; });
    await store.recordEngineFailure({ flowId: null, pipelineId: "pipe", message: "x", state: {} });
    assert.equal(fired, false);
    failureEmitter.removeAllListeners("failure.created");
    store.close();
  });
});

// ─── queryErrorsByFlow() / queryErrorsByFlowIds() ─────────────────────────────
// queryErrorsByFlow()/queryErrorsByFlowIds() run against failure_events LEFT
// JOIN flow_node_invocations, returning ErrorEntry-shaped rows.
// flow_node_invocations belongs to node-executions.js's own
// migrator, not failure-store.js's — these isolated test DBs need it
// hand-created, same pattern seedFlowRun already uses for flow_runs.

async function ensureInvocationsTable(store) {
  await store._db.execute(`
    CREATE TABLE IF NOT EXISTS flow_node_invocations (
      id TEXT PRIMARY KEY, flow_id TEXT NOT NULL, pipeline_id TEXT,
      node_id TEXT NOT NULL, subflow_key TEXT, invoked_at INTEGER NOT NULL
    )`);
}

async function seedInvocation(store, { id, flowId = "f1", nodeId = "NODE", subflowKey = null, invokedAt = Date.now() }) {
  await ensureInvocationsTable(store);
  await seedFlowRun(store, { flowId });
  await store._db.execute({
    sql: `INSERT INTO flow_node_invocations (id, flow_id, node_id, subflow_key, invoked_at) VALUES (?, ?, ?, ?, ?)`,
    args: [id, flowId, nodeId, subflowKey, invokedAt],
  });
}

describe("queryErrorsByFlow()", () => {
  test("returns [] for a flow with no rows at all", async () => {
    const store = await makeStore();
    await ensureInvocationsTable(store);
    assert.deepEqual(await store.queryErrorsByFlow(randomUUID()), []);
    store.close();
  });

  test("returns rows in ASC failed_at order, mapped to the ErrorEntry shape", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const invLate = randomUUID();
    const invEarly = randomUUID();
    await seedInvocation(store, { id: invEarly, flowId, nodeId: "EARLY", subflowKey: null });
    await seedInvocation(store, { id: invLate, flowId, nodeId: "LATE", subflowKey: "item-1" });
    await insertRawRow(store, {
      flowId, invocationId: invLate, executionId: "exec-late", failedAt: 300,
      failureSummary: "late fail", errorType: "blocked",
    });
    await insertRawRow(store, {
      flowId, invocationId: invEarly, executionId: "exec-early", failedAt: 100,
      failureSummary: "early fail", errorType: "infra_unavailable",
    });

    const rows = await store.queryErrorsByFlow(flowId);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].nodeId, "EARLY");
    assert.equal(rows[0].subflowKey, null);
    assert.equal(rows[0].message, "early fail");
    assert.equal(rows[0].errorType, "infra_unavailable");
    assert.equal(rows[0].at, 100);
    assert.equal(rows[0].executionId, "exec-early");
    assert.equal(rows[0].invocationId, invEarly);
    assert.equal(rows[1].nodeId, "LATE");
    assert.equal(rows[1].subflowKey, "item-1");
    assert.equal(rows[1].errorType, "blocked");
    store.close();
  });

  test("errorType is null when the row has none", async () => {
    const store = await makeStore();
    const flowId = randomUUID();
    const invId = randomUUID();
    await seedInvocation(store, { id: invId, flowId, nodeId: "PLAIN" });
    await insertRawRow(store, { flowId, invocationId: invId, executionId: "exec-1", failureSummary: "plain crash" });
    const rows = await store.queryErrorsByFlow(flowId);
    assert.equal(rows[0].errorType, null);
    store.close();
  });

  test("a row with no invocation_id at all still returns, with nodeId/subflowKey null (LEFT JOIN)", async () => {
    const store = await makeStore();
    await ensureInvocationsTable(store);
    const flowId = randomUUID();
    await insertRawRow(store, { flowId, invocationId: null, executionId: null, failureSummary: "no dispatch" });
    const rows = await store.queryErrorsByFlow(flowId);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].nodeId, null);
    assert.equal(rows[0].subflowKey, null);
    store.close();
  });

  test("does not return errors from a different flow", async () => {
    const store = await makeStore();
    await ensureInvocationsTable(store);
    const flowA = randomUUID();
    const flowB = randomUUID();
    await insertRawRow(store, { flowId: flowA, failureSummary: "a fail" });
    await insertRawRow(store, { flowId: flowB, failureSummary: "b fail" });
    const rows = await store.queryErrorsByFlow(flowA);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].message, "a fail");
    store.close();
  });
});

describe("queryErrorsByFlowIds()", () => {
  test("returns an empty Map for an empty or non-array input", async () => {
    const store = await makeStore();
    assert.deepEqual(await store.queryErrorsByFlowIds([]), new Map());
    assert.deepEqual(await store.queryErrorsByFlowIds(undefined), new Map());
    store.close();
  });

  test("groups errors by flowId, ASC within each group", async () => {
    const store = await makeStore();
    await ensureInvocationsTable(store);
    const flowA = randomUUID();
    const flowB = randomUUID();
    await insertRawRow(store, { flowId: flowA, failedAt: 200, failureSummary: "a1" });
    await insertRawRow(store, { flowId: flowA, failedAt: 100, failureSummary: "a2" });
    await insertRawRow(store, { flowId: flowB, failedAt: 1, failureSummary: "b1" });

    const result = await store.queryErrorsByFlowIds([flowA, flowB]);
    assert.equal(result.size, 2);
    assert.equal(result.get(flowA).length, 2);
    assert.equal(result.get(flowA)[0].message, "a2", "ASC by failed_at within the group");
    assert.equal(result.get(flowB).length, 1);
    store.close();
  });

  test("a flowId with zero matching rows is absent from the Map (not an empty-array entry)", async () => {
    const store = await makeStore();
    await ensureInvocationsTable(store);
    const flowWithErrors = randomUUID();
    const flowWithoutErrors = randomUUID();
    await insertRawRow(store, { flowId: flowWithErrors, failureSummary: "fail" });
    const result = await store.queryErrorsByFlowIds([flowWithErrors, flowWithoutErrors]);
    assert.equal(result.has(flowWithErrors), true);
    assert.equal(result.has(flowWithoutErrors), false);
    store.close();
  });
});
