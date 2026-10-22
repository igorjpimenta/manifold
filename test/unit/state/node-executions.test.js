// Unit tests for state/node-executions.js (renamed from step-timings.js) —
// getNodeExecutionsStore singleton, write(), listInvocationsForFlow(),
// listExecutionsForInvocation(), no-op fallback, v2 rebuild-and-backfill
// migration (id retype to UUID4, branch_item -> branch_key rename,
// flow_branches error backfill), lastErrorMessage(). As of Phase 1.5 this store
// carries no error content at all — see state/failure-store.js instead.

import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { createClient } from "@libsql/client";
import {
  getNodeExecutionsStore,
  createNodeExecutionsStore,
  lastErrorMessage,
} from "../../../state/node-executions.js";
import { createRunStore } from "../../../state/run-store.js";
import { resolveOrCreateChild } from "../../../engine/spawn-child.js";

// ─── helpers ─────────────────────────────────────────────────────────────────

function tmpDb() {
  return path.join(os.tmpdir(), `gf-ne-${randomUUID()}.sqlite`);
}

const UUID4_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// createNodeExecutionsStore opens its own isolated DB — bypasses the singleton.
// Exposes _db for raw SQL assertions.
async function makeIsolatedStore() {
  return createNodeExecutionsStore(tmpDb());
}

// Joins invocation+execution for raw-SQL test assertions that need columns
// (pipeline_id, the execution's own id) no public store method exposes —
// mirrors the store's own internal join shape (Phase 2's invocation/
// execution split, see node-executions.js's header).
async function rawExecutionRows(store, flowId) {
  const { rows } = await store._db.execute({
    sql: `SELECT e.id, i.flow_id, i.pipeline_id, i.node_id, i.subflow_key, e.entered_at, e.exited_at
            FROM flow_node_executions e JOIN flow_node_invocations i ON e.invocation_id = i.id
           WHERE i.flow_id = ? ORDER BY e.entered_at ASC`,
    args: [flowId],
  });
  return rows;
}

// ─── singleton behaviour ──────────────────────────────────────────────────────

// Initialize the singleton with a temp path before any test touches the module
// default, so the real ~/.openclaw dir is never written to.
const SINGLETON_TEMP_PATH = path.join(os.tmpdir(), `gf-ne-singleton-${randomUUID()}.sqlite`);
before(async () => { await getNodeExecutionsStore(SINGLETON_TEMP_PATH); });

describe("getNodeExecutionsStore — singleton", () => {
  test("returns the same object on repeated calls with no args", async () => {
    const s1 = await getNodeExecutionsStore();
    const s2 = await getNodeExecutionsStore();
    assert.strictEqual(s1, s2, "both calls must return the identical singleton object");
  });

  test("returns the same object when called with a different explicit path after init", async () => {
    const s1 = await getNodeExecutionsStore(tmpDb());
    const s2 = await getNodeExecutionsStore(tmpDb());
    assert.strictEqual(s1, s2, "singleton is returned regardless of subsequent dbPath args");
  });

  test("returned store exposes write, listInvocationsForFlow, and listExecutionsForInvocation", async () => {
    const store = await getNodeExecutionsStore();
    assert.equal(typeof store.write, "function");
    assert.equal(typeof store.listInvocationsForFlow, "function");
    assert.equal(typeof store.listExecutionsForInvocation, "function");
  });
});

// ─── singleton DB is writable ─────────────────────────────────────────────────

describe("singleton DB", () => {
  test("singleton is writable — write + listInvocationsForFlow round-trip succeeds", async () => {
    const store = await getNodeExecutionsStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "TEST_NODE", enteredAt: 1000, exitedAt: 2000, kind: "fresh" });
    const rows = await store.listInvocationsForFlow(flowId);
    assert.equal(rows.length, 1, "written row should be readable back from singleton store");
  });

  test("mkdirSync created the parent directory (store is functional, not a no-op)", async () => {
    const store = await getNodeExecutionsStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "DIR_CHECK", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const rows = await store.listInvocationsForFlow(flowId);
    assert.ok(rows.length > 0, "DB is writable, confirming mkdirSync succeeded");
  });
});

// ─── write() ─────────────────────────────────────────────────────────────────

describe("write()", () => {
  test("inserts a row with all fields populated", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const pipelineId = "test_pipeline";
    const nodeId = "SUBMIT_CODE";
    const subflowKey = "item-1";
    const enteredAt = 1_700_000_000_000;
    const exitedAt  = 1_700_000_005_000;

    await store.write({ flowId, pipelineId, nodeId, subflowKey, enteredAt, exitedAt, kind: "fresh" });
    await store.flush(); // write() defers through batch-writer — drain before reading back

    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows.length, 1);
    const row = rows[0];
    assert.equal(row.flow_id, flowId);
    assert.equal(row.pipeline_id, pipelineId);
    assert.equal(row.node_id, nodeId);
    assert.equal(row.subflow_key, subflowKey);
    assert.equal(Number(row.entered_at), enteredAt);
    assert.equal(Number(row.exited_at), exitedAt);
    store.close();
  });

  test("write() with pipelineId omitted stores SQL NULL (not the string 'null')", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "NODE_A", enteredAt: 100, exitedAt: 200, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows.length, 1);
    assert.strictEqual(rows[0].pipeline_id, null, "pipelineId should be SQL NULL when omitted");
    store.close();
  });

  test("write() with subflowKey omitted stores SQL NULL (not the string 'null')", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "NODE_B", enteredAt: 100, exitedAt: 200, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows.length, 1);
    assert.strictEqual(rows[0].subflow_key, null, "subflowKey should be SQL NULL when omitted");
    store.close();
  });

  test("write() with explicit pipelineId=null stores SQL NULL", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, pipelineId: null, nodeId: "NODE_C", enteredAt: 10, exitedAt: 20, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.strictEqual(rows[0].pipeline_id, null);
    store.close();
  });

  test("write() with explicit subflowKey=null stores SQL NULL", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "NODE_D", subflowKey: null, enteredAt: 10, exitedAt: 20, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.strictEqual(rows[0].subflow_key, null);
    store.close();
  });

  test("write() errors are swallowed and do not throw to callers", async () => {
    const store = await makeIsolatedStore();
    // Corrupt the store by closing the DB first; write() should still not throw.
    store.close();
    await assert.doesNotReject(
      () => store.write({ flowId: randomUUID(), nodeId: "X", enteredAt: 1, exitedAt: 2, kind: "fresh" }),
      "write() must not propagate internal errors"
    );
  });

  test("multiple writes accumulate rows", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "A", enteredAt: 100, exitedAt: 200, kind: "fresh" });
    await store.write({ flowId, nodeId: "B", enteredAt: 300, exitedAt: 400, kind: "fresh" });
    await store.write({ flowId, nodeId: "C", enteredAt: 500, exitedAt: 600, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows.length, 3);
    store.close();
  });

  test("write() generates a UUID4-shaped id, unique across writes", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "A", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.write({ flowId, nodeId: "B", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows.length, 2);
    for (const row of rows) assert.match(row.id, UUID4_RE, `id "${row.id}" should be UUID4-shaped`);
    assert.notEqual(rows[0].id, rows[1].id, "each row should have a unique id");
    store.close();
  });

  test("a row written with subflowKey populated round-trips (raw)", async () => {
    // listInvocationsForFlow deliberately excludes subflow-item invocations
    // (subflow_key set) — see its own describe block — so this round-trip
    // is checked via rawExecutionRows instead.
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "STAMP", subflowKey: "item-9", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.flush();

    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].subflow_key, "item-9");
    store.close();
  });

  test("returns { invocationId, executionId } on success, both UUID4-shaped", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const result = await store.write({ flowId, nodeId: "RET", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.flush();

    assert.match(result.invocationId, UUID4_RE);
    assert.match(result.executionId, UUID4_RE);
    const rows = await rawExecutionRows(store, flowId);
    assert.equal(rows[0].id, result.executionId, "returned executionId must match the row's own id");
    store.close();
  });

  test("returns undefined on a DB-write failure", async () => {
    const store = await makeIsolatedStore();
    store.close();
    const result = await store.write({ flowId: randomUUID(), nodeId: "X", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    assert.strictEqual(result, undefined);
  });
});

// queryByFlow()/queryByNode() were deleted in Phase 1.5 — flow_node_executions
// no longer stores error content, and nothing else called them (confirmed via
// repo-wide grep before deletion). queryByPipeline() survives unchanged
// (never selected error) and has its own coverage further down via
// summarizeChildrenByFlowIds()'s harness where relevant.

// ─── writeResult() ───────────────────────────────────────────────────────────
// Option B, Phase B0 — additive
// resultJson write. No size cap (see node-executions.js's
// own header comment): a result of any size is always stored in full,
// resultTooLargeBytes always written null.

describe("writeResult()", () => {
  test("writes resultJson + resultWrittenAt on a normal call", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const before = Date.now();
    const result = await store.writeResult(invocationId, { text: "hello", ok: true });
    const expectedBytes = Buffer.byteLength(JSON.stringify({ text: "hello", ok: true }), "utf8");
    assert.deepEqual(result, { ok: true, bytes: expectedBytes });
    const { rows } = await store._db.execute({
      sql: `SELECT result_json, result_written_at, result_too_large_bytes FROM flow_node_invocations WHERE id = ?`,
      args: [invocationId],
    });
    assert.deepEqual(JSON.parse(rows[0].result_json), { text: "hello", ok: true });
    assert.ok(Number(rows[0].result_written_at) >= before);
    assert.equal(rows[0].result_too_large_bytes, null);
  });

  test("a retry-overwrite (second call, same invocation) succeeds and replaces the value", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const first = await store.writeResult(invocationId, { attempt: 1 });
    assert.equal(first.ok, true);
    const second = await store.writeResult(invocationId, { attempt: 2 });
    assert.equal(second.ok, true, "a legitimate later write against its own freshly-read prior value must succeed, not race");
    const { rows } = await store._db.execute({
      sql: `SELECT result_json FROM flow_node_invocations WHERE id = ?`,
      args: [invocationId],
    });
    assert.deepEqual(JSON.parse(rows[0].result_json), { attempt: 2 });
  });

  test("a result well over the old 64KB cap is stored in full, byte-for-byte, resultTooLargeBytes stays null", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "SCAN", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    // Same shape as the real SCAN node: tiny scalar fields (accessToken,
    // bidderId) bundled flat alongside a large `listings` array — the
    // shape where a size cap once silently dropped the whole result,
    // including the tiny fields a later node (SUBMIT_CODE) depended on,
    // failing only when that unrelated node failed. No size cap remains,
    // so nothing is dropped.
    const big = {
      accessToken: "tok-abc123",
      bidderId: 987654,
      scanMode: "ok",
      listings: Array.from({ length: 2000 }, (_, i) => ({ id: i, text: "x".repeat(50) })),
    };
    const result = await store.writeResult(invocationId, big);
    assert.equal(result.ok, true);
    assert.ok(result.bytes > 64 * 1024, "sanity: this really is bigger than the old cap");
    const { rows } = await store._db.execute({
      sql: `SELECT result_json, result_too_large_bytes FROM flow_node_invocations WHERE id = ?`,
      args: [invocationId],
    });
    assert.equal(rows[0].result_too_large_bytes, null, "no size cap left to record a breach of");
    assert.deepEqual(JSON.parse(rows[0].result_json), big, "the full result, including `listings`, is stored — nothing dropped");
  });

  test("a bare array well over the old cap is also stored in full — no size-based special-casing left for any result shape", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "SCAN", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const bigArray = Array.from({ length: 5000 }, (_, i) => ({ id: i, text: "x".repeat(50) }));
    const result = await store.writeResult(invocationId, bigArray);
    assert.equal(result.ok, true);
    const { rows } = await store._db.execute({
      sql: `SELECT result_json, result_too_large_bytes FROM flow_node_invocations WHERE id = ?`,
      args: [invocationId],
    });
    assert.equal(rows[0].result_too_large_bytes, null);
    assert.deepEqual(JSON.parse(rows[0].result_json), bigArray);
  });

  test("unknown invocationId — no-such-invocation, no throw", async () => {
    const store = await makeIsolatedStore();
    const result = await store.writeResult(randomUUID(), { x: 1 });
    assert.deepEqual(result, { ok: false, reason: "no-such-invocation" });
  });

  test("falsy invocationId — no-invocation-id, no throw", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.writeResult(null, { x: 1 }), { ok: false, reason: "no-invocation-id" });
    assert.deepEqual(await store.writeResult(undefined, { x: 1 }), { ok: false, reason: "no-invocation-id" });
  });

  test("unserializable result (circular reference) — unserializable, no throw", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const circular = {};
    circular.self = circular;
    const result = await store.writeResult(invocationId, circular);
    assert.deepEqual(result, { ok: false, reason: "unserializable" });
  });

  test("null/undefined result is written as JSON null, not skipped", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "CACHE_SAVE", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const result = await store.writeResult(invocationId, null);
    assert.equal(result.ok, true);
    const { rows } = await store._db.execute({
      sql: `SELECT result_json, result_written_at FROM flow_node_invocations WHERE id = ?`,
      args: [invocationId],
    });
    assert.equal(rows[0].result_json, "null");
    assert.ok(rows[0].result_written_at != null, "resultWrittenAt must be set even for a genuinely-null result — that's the whole point of the column");
  });

  test("two concurrent writes to the SAME invocation never silently corrupt — exactly one write's data survives, and any rejection is a real detected race, not silent data loss", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "RACE", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const [r1, r2] = await Promise.all([
      store.writeResult(invocationId, { from: "A" }),
      store.writeResult(invocationId, { from: "B" }),
    ]);
    const wins = [r1, r2].filter((r) => r.ok);
    const losses = [r1, r2].filter((r) => !r.ok);
    assert.ok(wins.length >= 1, "at least one concurrent write must succeed");
    for (const loss of losses) {
      assert.equal(loss.reason, "race", "a rejected concurrent write must be explicitly detected as a race, never silently dropped");
    }
    const { rows } = await store._db.execute({
      sql: `SELECT result_json FROM flow_node_invocations WHERE id = ?`,
      args: [invocationId],
    });
    const finalFrom = JSON.parse(rows[0].result_json).from;
    assert.ok(["A", "B"].includes(finalFrom), "final stored value must be exactly one whole write, never a corrupted mix");
  });

  test("no-op store returns { ok: false, reason: 'db-error' } without throwing", async () => {
    const noopStore = { writeResult: async () => ({ ok: false, reason: "db-error" }) };
    assert.deepEqual(await noopStore.writeResult(randomUUID(), { x: 1 }), { ok: false, reason: "db-error" });
  });
});

// ─── writeGateRecord() / getGateRecord() ───────────────────────────────────
// gate_records (MIGRATIONS v11) — one row per (flowId, nodeId), a point
// identity no shared-key merge/replay can ever go stale against, unlike
// state.gates riding along inside a node's own resultJson row.

describe("writeGateRecord() / getGateRecord()", () => {
  test("getGateRecord returns null when the gate has never written a record", async () => {
    const store = await makeIsolatedStore();
    assert.equal(await store.getGateRecord(randomUUID(), "GATE_1"), null);
  });

  test("writes then reads back the exact record", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const record = { nodeId: "GATE_1", kind: "human-gate", pending: ["x"], items: [{ itemId: "x" }], timeoutMs: 60000, openedAt: 1000, lastOpenedAt: 1000 };
    const result = await store.writeGateRecord(flowId, "GATE_1", record);
    assert.equal(result.ok, true);
    assert.deepEqual(await store.getGateRecord(flowId, "GATE_1"), record);
  });

  test("a later write overwrites the same (flowId, nodeId) row in place — no duplicate rows", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.writeGateRecord(flowId, "GATE_1", { pending: ["x"] });
    await store.writeGateRecord(flowId, "GATE_1", { pending: [], consumedAt: 2000 });
    assert.deepEqual(await store.getGateRecord(flowId, "GATE_1"), { pending: [], consumedAt: 2000 });
    const { rows } = await store._db.execute({ sql: `SELECT COUNT(*) AS n FROM gate_records WHERE flow_id = ? AND node_id = ?`, args: [flowId, "GATE_1"] });
    assert.equal(Number(rows[0].n), 1);
  });

  test("two different gate nodes in the same flow write to independent rows — no cross-node interference", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.writeGateRecord(flowId, "GATE_1", { consumedAt: 1000 });
    await store.writeGateRecord(flowId, "GATE_2", { pending: ["y"] });
    assert.deepEqual(await store.getGateRecord(flowId, "GATE_1"), { consumedAt: 1000 });
    assert.deepEqual(await store.getGateRecord(flowId, "GATE_2"), { pending: ["y"] });
  });

  test("the same nodeId in two different flows is independent — keyed by (flowId, nodeId), not nodeId alone", async () => {
    const store = await makeIsolatedStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    await store.writeGateRecord(flowA, "GATE_1", { consumedAt: 1000 });
    assert.equal(await store.getGateRecord(flowB, "GATE_1"), null);
  });

  test("no-op store degrades without throwing", async () => {
    const noopStore = { writeGateRecord: async () => ({ ok: false, reason: "db-error" }), getGateRecord: async () => null };
    assert.deepEqual(await noopStore.writeGateRecord(randomUUID(), "GATE_1", {}), { ok: false, reason: "db-error" });
    assert.equal(await noopStore.getGateRecord(randomUUID(), "GATE_1"), null);
  });
});

// ─── listGateRecordsForFlow() ───────────────────────────────────────────────
// Batch sibling of getGateRecord — B2.5's eventual routes/runs.js cutover
// needs "every gate this flow has opened," not one specific node.

describe("listGateRecordsForFlow()", () => {
  test("empty flow — no gates opened at all", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listGateRecordsForFlow(randomUUID()), {});
  });

  test("returns every gate for the flow, keyed by nodeId", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.writeGateRecord(flowId, "GATE_1", { consumedAt: 1000 });
    await store.writeGateRecord(flowId, "GATE_2", { pending: ["y"] });
    assert.deepEqual(await store.listGateRecordsForFlow(flowId), {
      GATE_1: { consumedAt: 1000 },
      GATE_2: { pending: ["y"] },
    });
  });

  test("each row is independently correct — no merge, no staleness, regardless of write order", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    // GATE_1 opens and resolves BEFORE GATE_2 even exists — the exact
    // ordering that broke assembleState()'s own shallow-merge replay.
    await store.writeGateRecord(flowId, "GATE_1", { pending: [], consumedAt: 1000 });
    await store.writeGateRecord(flowId, "GATE_2", { pending: ["y"] });
    const all = await store.listGateRecordsForFlow(flowId);
    assert.ok(all.GATE_1?.consumedAt, "GATE_1's own record survives — it's its own row, not a snapshot embedded in someone else's");
    assert.ok(all.GATE_2, "GATE_2 is present even though its own write happened after GATE_1's already-consumed record existed");
  });

  test("scoped to the given flowId — another flow's gates are excluded", async () => {
    const store = await makeIsolatedStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    await store.writeGateRecord(flowA, "GATE_1", { consumedAt: 1000 });
    await store.writeGateRecord(flowB, "GATE_1", { pending: ["z"] });
    assert.deepEqual(await store.listGateRecordsForFlow(flowA), { GATE_1: { consumedAt: 1000 } });
    assert.deepEqual(await store.listGateRecordsForFlow(flowB), { GATE_1: { pending: ["z"] } });
  });

  test("no-op store returns {} without throwing", async () => {
    const noopStore = { listGateRecordsForFlow: async () => ({}) };
    assert.deepEqual(await noopStore.listGateRecordsForFlow(randomUUID()), {});
  });
});

// ─── listResultsForFlow() ──────────────────────────────────────────────────
// Option B, Phase B1 — feeds
// engine/executor-helpers.js's assembleState().

describe("listResultsForFlow()", () => {
  test("empty flow — no invocations at all", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listResultsForFlow(randomUUID()), []);
  });

  test("basic round trip: one node, resultWrittenAt/resultJson/resultTooLargeBytes map correctly", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.writeResult(invocationId, { DRAFT: { text: "hello" } });
    const results = await store.listResultsForFlow(flowId);
    assert.equal(results.length, 1);
    assert.equal(results[0].nodeId, "DRAFT");
    assert.equal(results[0].visitSeq, 1);
    assert.deepEqual(JSON.parse(results[0].resultJson), { DRAFT: { text: "hello" } });
    assert.ok(results[0].resultWrittenAt != null);
    assert.equal(results[0].resultTooLargeBytes, null);
  });

  // A dispatch with no resultJson written yet must be excluded from the SQL
  // result entirely, not merely filtered by assembleState() after the
  // fact — the per-node dedup below keeps only ONE row per node_id, so if
  // that slot were occupied by an in-flight (unwritten) row, an EARLIER
  // COMPLETED invocation for the same node (a real value a loop-back's own
  // next dispatch needs to read) would be silently hidden entirely, not
  // just marked not-ready. See listResultsForFlow's own comment.
  test("a dispatch with no resultJson written yet is excluded entirely — not present with resultWrittenAt null", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "SCAN", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const results = await store.listResultsForFlow(flowId);
    assert.deepEqual(results, []);
  });

  test("a re-entry's own in-flight (unwritten) invocation does not hide an EARLIER completed invocation for the same node", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const loop1 = await store.write({ flowId, nodeId: "LOOP", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.writeResult(loop1.invocationId, { LOOP: { count: 1 } });
    // A genuine re-entry creates its OWN new invocation before it has
    // anything to write — this is exactly the state a loop-back node's
    // own dispatch reads state IN THE MIDDLE OF.
    await store.write({ flowId, nodeId: "LOOP", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    const results = await store.listResultsForFlow(flowId);
    assert.equal(results.length, 1);
    assert.equal(results[0].nodeId, "LOOP");
    assert.deepEqual(JSON.parse(results[0].resultJson), { LOOP: { count: 1 } }, "must fall back to the earlier COMPLETED invocation, not disappear");
  });

  test("multiple distinct nodes, one row each, in dispatch order", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const a = await store.write({ flowId, nodeId: "PRIME", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.writeResult(a.invocationId, { PRIME: {} });
    const b = await store.write({ flowId, nodeId: "SCAN", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    await store.writeResult(b.invocationId, { SCAN: { listings: [] } });
    const results = await store.listResultsForFlow(flowId);
    assert.deepEqual(results.map((r) => r.nodeId), ["PRIME", "SCAN"]);
  });

  test("a retry (same invocation, resultJson overwritten in place) stays one row with the LATEST value", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.writeResult(invocationId, { DRAFT: { attempt: 1 } });
    await store.write({ flowId, nodeId: "DRAFT", enteredAt: 3, exitedAt: 4, kind: "retry", invocationId, isNewInvocation: false });
    await store.writeResult(invocationId, { DRAFT: { attempt: 2 } });
    const results = await store.listResultsForFlow(flowId);
    assert.equal(results.length, 1);
    assert.deepEqual(JSON.parse(results[0].resultJson), { DRAFT: { attempt: 2 } });
  });

  test("a genuine loop-back (new invocation, higher visit_seq) dedups to the LATEST row, repositioned to reflect when it actually happened", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    // DRAFT fires first (visit_seq 1)...
    const draft1 = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.writeResult(draft1.invocationId, { DRAFT: { pass: 1 } });
    // ...then GATE runs...
    const gate = await store.write({ flowId, nodeId: "GATE", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    await store.writeResult(gate.invocationId, { GATE: { decision: "retry" } });
    // ...then a real loop-back re-enters DRAFT (visit_seq 2, a NEW row, not a retry-overwrite).
    const draft2 = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 5, exitedAt: 6, kind: "fresh" });
    assert.equal(draft2.invocationId === draft1.invocationId, false, "a genuine loop-back must be a new invocation row, not the same one");
    await store.writeResult(draft2.invocationId, { DRAFT: { pass: 2 } });

    const results = await store.listResultsForFlow(flowId);
    assert.equal(results.length, 2, "DRAFT dedups to one row despite two invocations");
    // DRAFT's SECOND pass happened chronologically AFTER GATE — its position
    // in the replay order must reflect that, not its first occurrence.
    assert.deepEqual(results.map((r) => r.nodeId), ["GATE", "DRAFT"]);
    assert.deepEqual(JSON.parse(results.find((r) => r.nodeId === "DRAFT").resultJson), { DRAFT: { pass: 2 } });
  });

  test("excludes per-item subflow rows (subflow_key set) — only the flow's own top-level dispatches", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const top = await store.write({ flowId, nodeId: "PROPOSE", subflowKey: null, enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.writeResult(top.invocationId, {});
    await store.write({ flowId, nodeId: "PROPOSE", subflowKey: "listing-1", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    const results = await store.listResultsForFlow(flowId);
    assert.equal(results.length, 1);
    assert.equal(results[0].nodeId, "PROPOSE");
  });

  test("scoped to the given flowId — another flow's rows are excluded", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const other = randomUUID();
    await store.write({ flowId: other, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    assert.deepEqual(await store.listResultsForFlow(flowId), []);
  });

  test("no-op store returns []", async () => {
    const noopStore = { listResultsForFlow: async () => [] };
    assert.deepEqual(await noopStore.listResultsForFlow(randomUUID()), []);
  });
});

// ─── findCurrentStep() ─────────────────────────────────────────────────────

describe("findCurrentStep()", () => {
  test("returns null when the flow has no invocations yet", async () => {
    const store = await makeIsolatedStore();
    assert.equal(await store.findCurrentStep(randomUUID()), null);
    store.close();
  });

  test("returns the node_id of the latest subflow_key IS NULL row, ignoring later item rows for the same node", async () => {
    // Mirrors what a real subflow node dispatch produces: enter() creates
    // the identity row (subflowKey: null, exactly what executor.js's own
    // runFlow loop does), then per-item bookkeeping rows land under the
    // same flow_id + node_id with a real subflow_key — always later in
    // time, since item-row creation happens as part of processing that
    // same dispatch. This proves the subflow_key IS NULL filter is what
    // keeps findCurrentStep pointed at the node's own identity row instead
    // of one of its items, even though today they'd carry the same
    // node_id either way.
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, pipelineId: "p", nodeId: "PROPOSE", subflowKey: null, enteredAt: 100, exitedAt: 200, kind: "fresh" });
    await store.write({ flowId, pipelineId: "p", nodeId: "PROPOSE", subflowKey: "listing-1", enteredAt: 300, exitedAt: 400, kind: "fresh" });
    // Force the item row's invoked_at strictly later than the identity
    // row's, deterministically rather than relying on Date.now() ticking
    // between two sequential calls in the same test.
    await store._db.execute({
      sql: `UPDATE flow_node_invocations SET invoked_at = invoked_at + 1000 WHERE flow_id = ? AND subflow_key = 'listing-1'`,
      args: [flowId],
    });
    assert.equal(await store.findCurrentStep(flowId), "PROPOSE");
    store.close();
  });

  test("advances to a genuinely new top-level node once it's entered", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, pipelineId: "p", nodeId: "SCAN", subflowKey: null, enteredAt: 100, exitedAt: 200, kind: "fresh" });
    assert.equal(await store.findCurrentStep(flowId), "SCAN");
    await store.write({ flowId, pipelineId: "p", nodeId: "FILTER", subflowKey: null, enteredAt: 300, exitedAt: 400, kind: "fresh" });
    assert.equal(await store.findCurrentStep(flowId), "FILTER");
    store.close();
  });
});

// ─── no-op fallback when createClient fails to open ──────────────────────────

describe("no-op store — DatabaseSync fails to open", () => {
  test("write() on no-op store does not throw", () => {
    const noop = () => {};
    const noopStore = { _db: null, write: noop, close: noop };

    assert.doesNotThrow(() => {
      noopStore.write({ flowId: "f1", nodeId: "N", enteredAt: 1, exitedAt: 2 });
    });
  });

  test("opening a DB at an invalid path produces a no-op-like store via error swallow", async () => {
    // Write a regular file at what would be the parent dir path, so mkdirSync throws ENOTDIR.
    const blockingFile = path.join(os.tmpdir(), `gf-block-${randomUUID()}`);
    writeFileSync(blockingFile, "not a directory");
    const badPath = path.join(blockingFile, "node-executions.sqlite");

    const store = await createNodeExecutionsStore(badPath);
    // The store should be a no-op (not throw on open, not throw on write).
    assert.doesNotThrow(() => {
      store.write({ flowId: "f", nodeId: "n", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    });
    assert.deepEqual(await store.listInvocationsForFlow("f"), []);
  });
});

// ─── schema correctness ───────────────────────────────────────────────────────

describe("schema", () => {
  test("flow_node_invocations table is created with all expected columns", async () => {
    const store = await makeIsolatedStore();
    const { rows: info } = await store._db.execute({ sql: "PRAGMA table_info(flow_node_invocations)", args: [] });
    const cols = info.map((r) => r.name);

    for (const col of ["id", "flow_id", "pipeline_id", "node_id", "subflow_key", "invoked_at"]) {
      assert.ok(cols.includes(col), `column '${col}' must exist in flow_node_invocations`);
    }
    assert.ok(!cols.includes("group_id"), "group_id was dropped from flow_node_invocations (v4 migration)");
    store.close();
  });

  test("flow_node_executions table is narrowed to invocation_id + attempt timing, no error content (Phase 1.5)", async () => {
    const store = await makeIsolatedStore();
    const { rows: info } = await store._db.execute({ sql: "PRAGMA table_info(flow_node_executions)", args: [] });
    const cols = info.map((r) => r.name);

    for (const col of ["id", "invocation_id", "entered_at", "exited_at", "kind"]) {
      assert.ok(cols.includes(col), `column '${col}' must exist in flow_node_executions`);
    }
    for (const col of ["flow_id", "pipeline_id", "node_id", "subflow_key", "group_id", "error", "error_type"]) {
      assert.ok(!cols.includes(col), `column '${col}' must not remain on flow_node_executions`);
    }
    store.close();
  });

  // v13: freelance-filter-node.js's in-flight dedup query (workspace/.shared/
  // common/lib/freelance-filter-node.js) filters flow_node_invocations by
  // node_id and correlates on subflow_key, in both its own WHERE clause and
  // a NOT EXISTS subquery — neither column was covered by an existing
  // index. Without this index, SQLite falls back to a full scan of
  // flow_runs (driving the join) plus a second per-row scan inside the
  // correlated subquery; with it, the plan uses direct
  // (node_id, subflow_key) index searches throughout.
  test("idx_invocations_node_subflow index exists on flow_node_invocations (node_id, subflow_key)", async () => {
    const store = await makeIsolatedStore();
    const { rows } = await store._db.execute({
      sql: "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='flow_node_invocations'",
      args: [],
    });
    const names = rows.map((r) => r.name);
    assert.ok(names.includes("idx_invocations_node_subflow"), `expected idx_invocations_node_subflow, got: ${names.join(", ")}`);
    store.close();
  });

  test("schema is idempotent — createNodeExecutionsStore on same path twice does not throw", async () => {
    const dbPath = tmpDb();
    const store1 = createNodeExecutionsStore(dbPath);
    await store1.close();

    await assert.doesNotReject(async () => {
      const store2 = createNodeExecutionsStore(dbPath);
      await store2.close();
    }, "opening the same DB file a second time should not throw");
  });
});

// ─── lastErrorMessage() ────────────────────────────────────────────────────────

describe("lastErrorMessage()", () => {
  test("bare-string array entry returns the string", () => {
    assert.equal(lastErrorMessage('["subagent timeout: no error message"]'), "subagent timeout: no error message");
  });

  test("object entry returns .message", () => {
    const json = JSON.stringify([{ node: "cancel", itemId: "alpha", message: "session abort", at: 123 }]);
    assert.equal(lastErrorMessage(json), "session abort");
  });

  test("object entry with no .message falls back to JSON.stringify", () => {
    const json = JSON.stringify([{ node: "cancel", itemId: "alpha" }]);
    assert.equal(lastErrorMessage(json), JSON.stringify({ node: "cancel", itemId: "alpha" }));
  });

  test("malformed JSON returns null", () => {
    assert.equal(lastErrorMessage("not json"), null);
  });

  test("empty array returns null", () => {
    assert.equal(lastErrorMessage("[]"), null);
  });

  test("null input returns null", () => {
    assert.equal(lastErrorMessage(null), null);
  });

  test("multi-entry array returns only the last entry", () => {
    const json = JSON.stringify(["first error", "second error", "third error"]);
    assert.equal(lastErrorMessage(json), "third error");
  });

  test("mixed string/object array returns the last entry regardless of shape", () => {
    const json = JSON.stringify(["first (string)", { node: "n", message: "second (object)" }]);
    assert.equal(lastErrorMessage(json), "second (object)");
  });
});

// ─── v2 migration: rebuild-and-backfill (id retype, branch_item rename, flow_branches backfill) ──

describe("createNodeExecutionsStore — v2 + v3 migration (chained on first boot)", () => {
  test("old-shape row survives both rebuilds with a fresh UUID4 execution id and renamed branch_key", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    for (const sql of [
      `CREATE TABLE flow_step_timings (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        flow_id     TEXT    NOT NULL,
        pipeline_id TEXT,
        node_id     TEXT    NOT NULL,
        branch_item TEXT,
        entered_at  INTEGER NOT NULL,
        exited_at   INTEGER NOT NULL
      )`,
      `INSERT INTO flow_step_timings (flow_id, pipeline_id, node_id, branch_item, entered_at, exited_at)
        VALUES ('flow-old', 'pipe-old', 'SCORE', 'branch-old', 4000, 5000)`,
    ].filter((s) => s.trim())) {
      await raw.execute(sql);
    }
    raw.close();

    // v2 rebuilds flow_step_timings -> flow_node_executions (flat shape);
    // v3 immediately splits that into invocations + executions, since both
    // are unrun migrations on this fresh DB and apply in the same pass.
    const store = await createNodeExecutionsStore(dbPath);
    const rows = await rawExecutionRows(store, "flow-old");
    assert.equal(rows.length, 1);
    const row = rows[0];
    assert.match(row.id, UUID4_RE, "old integer PK should be replaced with a fresh UUID4 execution id");
    assert.equal(row.node_id, "SCORE");
    assert.equal(row.subflow_key, "branch-old", "branch_item value preserved under the renamed branch_key column");
    assert.equal(Number(row.entered_at), 4000);
    assert.equal(Number(row.exited_at), 5000);
    store.close();
  });

  test("old flow_step_timings table no longer exists post-migration", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    await raw.execute(`
      CREATE TABLE flow_step_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, flow_id TEXT NOT NULL, pipeline_id TEXT,
        node_id TEXT NOT NULL, branch_item TEXT, entered_at INTEGER NOT NULL, exited_at INTEGER NOT NULL
      )
    `);
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);
    const { rows } = await store._db.execute({ sql: "SELECT name FROM sqlite_master WHERE type='table'", args: [] });
    const tables = rows.map((r) => r.name);
    assert.ok(!tables.includes("flow_step_timings"));
    assert.ok(tables.includes("flow_node_executions"));
    assert.ok(tables.includes("flow_node_invocations"));
    assert.ok(!tables.includes("flow_node_executions_v3"), "intermediate v3-build table should be renamed away, not left behind");
    store.close();
  });

  test("v3 indexes exist on both the invocations and narrowed executions tables", async () => {
    const dbPath = tmpDb();
    const store = await createNodeExecutionsStore(dbPath);
    const { rows: invRows } = await store._db.execute({ sql: "SELECT name FROM pragma_index_list('flow_node_invocations')", args: [] });
    const invocationIdx = invRows.map((r) => r.name);
    assert.ok(invocationIdx.includes("idx_invocations_identity"));
    assert.ok(invocationIdx.includes("idx_invocations_flow"));
    assert.ok(invocationIdx.includes("idx_invocations_pipe_node"));
    const { rows: execRows } = await store._db.execute({ sql: "SELECT name FROM pragma_index_list('flow_node_executions')", args: [] });
    const executionIdx = execRows.map((r) => r.name);
    assert.ok(executionIdx.includes("idx_fne_invocation"));
    assert.ok(executionIdx.includes("idx_fne_exited"));
    store.close();
  });

  test("two retry-shaped rows sharing the same (flow_id,node_id,branch_key) tuple collapse into one invocation with two executions", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    await raw.execute(`
      CREATE TABLE flow_step_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, flow_id TEXT NOT NULL, pipeline_id TEXT,
        node_id TEXT NOT NULL, branch_item TEXT, entered_at INTEGER NOT NULL, exited_at INTEGER NOT NULL
      )
    `);
    await raw.execute({
      sql: `INSERT INTO flow_step_timings (flow_id, pipeline_id, node_id, branch_item, entered_at, exited_at)
        VALUES ('flow-retry', 'pipe-retry', 'SUBMIT', NULL, 1000, 1500)`,
      args: [],
    });
    await raw.execute({
      sql: `INSERT INTO flow_step_timings (flow_id, pipeline_id, node_id, branch_item, entered_at, exited_at)
        VALUES ('flow-retry', 'pipe-retry', 'SUBMIT', NULL, 2000, 2500)`,
      args: [],
    });
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);
    const { rows: invocations } = await store._db.execute({
      sql: "SELECT id FROM flow_node_invocations WHERE flow_id = 'flow-retry' AND node_id = 'SUBMIT'",
      args: [],
    });
    assert.equal(invocations.length, 1, "root-level retries (branch_key NULL) must collapse into ONE invocation — the COALESCE-based unique index is what makes this safe");
    const { rows: executions } = await store._db.execute({
      sql: "SELECT * FROM flow_node_executions WHERE invocation_id = ? ORDER BY entered_at ASC",
      args: [invocations[0].id],
    });
    assert.equal(executions.length, 2, "each retry attempt still gets its own execution row");
    assert.equal(Number(executions[0].entered_at), 1000);
    assert.equal(Number(executions[1].entered_at), 2000);
    store.close();
  });

  test("flow_branches string-shaped error backfills into a new row (content itself is dropped by v9 in the same boot)", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    await raw.execute(`
      CREATE TABLE flow_step_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, flow_id TEXT NOT NULL, pipeline_id TEXT,
        node_id TEXT NOT NULL, branch_item TEXT, entered_at INTEGER NOT NULL, exited_at INTEGER NOT NULL
      )
    `);
    await raw.execute(`
      CREATE TABLE flow_branches (
        flow_id TEXT NOT NULL, group_id TEXT NOT NULL, branch_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running', step TEXT NOT NULL DEFAULT '__start__',
        started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, done_at INTEGER,
        data TEXT, errors TEXT NOT NULL DEFAULT '[]', awaiting TEXT,
        display_title TEXT, display_url TEXT, retry_from TEXT,
        attempt_number INTEGER NOT NULL DEFAULT 0, has_error INTEGER NOT NULL DEFAULT 0,
        has_skip INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (flow_id, group_id, branch_key)
      )
    `);
    await raw.execute({
      sql: `INSERT INTO flow_branches (flow_id, group_id, branch_key, status, step, started_at, updated_at, done_at, errors)
        VALUES ('flow-b1', 'PROPOSE', 'item-string', 'failed', 'SUBMIT_CODE', 1000, 1050, 1050, '["SUBMIT_CODE: weekly-cap-reached"]')`,
      args: [],
    });
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);
    const rows = await rawExecutionRows(store, "flow-b1");
    assert.equal(rows.length, 1);
    const row = rows[0];
    assert.match(row.id, UUID4_RE);
    assert.equal(row.node_id, "SUBMIT_CODE");
    assert.equal(row.subflow_key, "item-string");
    assert.equal(row.pipeline_id, null, "pipeline_id intentionally left NULL for branch-backfilled rows");
    assert.equal(Number(row.entered_at), 1000);
    assert.equal(Number(row.exited_at), 1050);
    store.close();
  });

  test("flow_branches object-shaped error backfills a row (content itself is dropped by v9 in the same boot)", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    await raw.execute(`
      CREATE TABLE flow_step_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, flow_id TEXT NOT NULL, pipeline_id TEXT,
        node_id TEXT NOT NULL, branch_item TEXT, entered_at INTEGER NOT NULL, exited_at INTEGER NOT NULL
      )
    `);
    await raw.execute(`
      CREATE TABLE flow_branches (
        flow_id TEXT NOT NULL, group_id TEXT NOT NULL, branch_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running', step TEXT NOT NULL DEFAULT '__start__',
        started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, done_at INTEGER,
        data TEXT, errors TEXT NOT NULL DEFAULT '[]', awaiting TEXT,
        display_title TEXT, display_url TEXT, retry_from TEXT,
        attempt_number INTEGER NOT NULL DEFAULT 0, has_error INTEGER NOT NULL DEFAULT 0,
        has_skip INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (flow_id, group_id, branch_key)
      )
    `);
    await raw.execute({
      sql: `INSERT INTO flow_branches (flow_id, group_id, branch_key, status, step, started_at, updated_at, done_at, errors)
        VALUES ('flow-b2', 'WORK', 'item-object', 'failed', 'AGENT_STEP', 2000, 2010, 2010, '[{"node":"cancel","itemId":"item-object","message":"session abort","at":2005}]')`,
      args: [],
    });
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);
    const row = (await rawExecutionRows(store, "flow-b2"))[0];
    assert.equal(row.node_id, "AGENT_STEP");
    assert.equal(row.subflow_key, "item-object");
    store.close();
  });

  test("flow_branches row with empty errors array produces no backfilled row", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    await raw.execute(`
      CREATE TABLE flow_step_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, flow_id TEXT NOT NULL, pipeline_id TEXT,
        node_id TEXT NOT NULL, branch_item TEXT, entered_at INTEGER NOT NULL, exited_at INTEGER NOT NULL
      )
    `);
    await raw.execute(`
      CREATE TABLE flow_branches (
        flow_id TEXT NOT NULL, group_id TEXT NOT NULL, branch_key TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running', step TEXT NOT NULL DEFAULT '__start__',
        started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, done_at INTEGER,
        data TEXT, errors TEXT NOT NULL DEFAULT '[]', awaiting TEXT,
        display_title TEXT, display_url TEXT, retry_from TEXT,
        attempt_number INTEGER NOT NULL DEFAULT 0, has_error INTEGER NOT NULL DEFAULT 0,
        has_skip INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (flow_id, group_id, branch_key)
      )
    `);
    await raw.execute({
      sql: `INSERT INTO flow_branches (flow_id, group_id, branch_key, status, step, started_at, updated_at, done_at, errors)
        VALUES ('flow-clean', 'WORK', 'item-clean', 'succeeded', '__end__', 3000, 3010, 3010, '[]')`,
      args: [],
    });
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);
    const rows = await rawExecutionRows(store, "flow-clean");
    assert.equal(rows.length, 0, "a branch with no real errors should not produce a backfilled row");
    store.close();
  });

  test("migration completes cleanly when flow_branches does not exist (out-of-order boot)", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    await raw.execute(`
      CREATE TABLE flow_step_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, flow_id TEXT NOT NULL, pipeline_id TEXT,
        node_id TEXT NOT NULL, branch_item TEXT, entered_at INTEGER NOT NULL, exited_at INTEGER NOT NULL
      )
    `);
    await raw.execute({
      sql: `INSERT INTO flow_step_timings (flow_id, node_id, entered_at, exited_at) VALUES ('flow-solo', 'NODE', 1, 2)`,
      args: [],
    });
    raw.close();

    await assert.doesNotReject(async () => {
      const store = await createNodeExecutionsStore(dbPath);
      const { rows } = await store._db.execute({ sql: "SELECT * FROM flow_node_executions", args: [] });
      assert.equal(rows.length, 1, "the root-level row still migrates even with no flow_branches table");
      store.close();
    });
  });
});

// ─── v4 migration: drop group_id from flow_node_invocations ─────────────────

describe("createNodeExecutionsStore — v4 migration (group_id drop)", () => {
  test("drops group_id cleanly from a v3-shape DB, including genuine old-style group_id != node_id rows", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    const setupSqls = [
      `CREATE TABLE _schema_version (
        store TEXT NOT NULL, version INTEGER NOT NULL, applied_at INTEGER NOT NULL,
        PRIMARY KEY (store, version)
      )`,
      `INSERT INTO _schema_version (store, version, applied_at) VALUES ('step-timings', 3, 0)`,
      `CREATE TABLE flow_node_invocations (
        id          TEXT    PRIMARY KEY,
        flow_id     TEXT    NOT NULL,
        pipeline_id TEXT,
        node_id     TEXT    NOT NULL,
        group_id    TEXT,
        branch_key  TEXT,
        invoked_at  INTEGER NOT NULL
      )`,
      `CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(group_id, ''), COALESCE(branch_key, ''))`,
      `CREATE INDEX idx_invocations_flow      ON flow_node_invocations (flow_id)`,
      `CREATE INDEX idx_invocations_pipe_node ON flow_node_invocations (pipeline_id, node_id)`,
      `CREATE TABLE flow_node_executions (
        id            TEXT    PRIMARY KEY,
        invocation_id TEXT    NOT NULL,
        entered_at    INTEGER NOT NULL,
        exited_at     INTEGER NOT NULL,
        error         TEXT
      )`,
      `CREATE INDEX idx_fne_invocation ON flow_node_executions(invocation_id)`,
      `CREATE INDEX idx_fne_exited     ON flow_node_executions(exited_at)`,
      `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, group_id, branch_key, invoked_at)
        VALUES ('inv-old', 'flow-v4', 'pipe-v4', 'GATE', 'PROPOSE', 'item-1', 1000)`,
      `INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, error)
        VALUES ('exec-old', 'inv-old', 1000, 1050, 'gate timeout')`,
      `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, group_id, branch_key, invoked_at)
        VALUES ('inv-new', 'flow-v4', 'pipe-v4', 'PROPOSE', 'PROPOSE', 'item-2', 2000)`,
      `INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, error)
        VALUES ('exec-new', 'inv-new', 2000, 2050, NULL)`,
    ];
    for (const sql of setupSqls) await raw.execute(sql);
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);

    const { rows: colRows } = await store._db.execute({ sql: "PRAGMA table_info(flow_node_invocations)", args: [] });
    const cols = colRows.map((r) => r.name);
    assert.ok(!cols.includes("group_id"), "group_id column must be gone after the v4 migration");

    const oldInvocation = await store.getInvocation("inv-old");
    assert.equal(oldInvocation.nodeId, "GATE");
    assert.equal(oldInvocation.subflowKey, "item-1");
    assert.equal(oldInvocation.flowId, "flow-v4");

    const newInvocation = await store.getInvocation("inv-new");
    assert.equal(newInvocation.nodeId, "PROPOSE");
    assert.equal(newInvocation.subflowKey, "item-2");

    // The new unique index (flow_id, node_id, COALESCE(branch_key, '')) must
    // still enforce dedup post-migration — a duplicate insert attempt for the
    // new-style row's identity tuple should collapse via findOrCreateInvocation,
    // not create a second row.
    const dedupedId = await store.findOrCreateInvocation({ flowId: "flow-v4", pipelineId: "pipe-v4", nodeId: "PROPOSE", subflowKey: "item-2" });
    assert.equal(dedupedId, "inv-new");
    const { rows: countRows } = await store._db.execute({
      sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE flow_id = 'flow-v4'",
      args: [],
    });
    assert.equal(Number(countRows[0].n), 2, "no new invocation should have been created by the dedup check");

    store.close();
  });

  test("merges colliding invocations (same flow_id/node_id/branch_key, different group_id) instead of failing the unique index", async () => {
    // A real production shape caught by a dry-run against a runtime.sqlite
    // snapshot: two different loops (e.g. SCORE and PROPOSE) both wrote a
    // generic sub-node marker (node_id="__end__") for the SAME branch_key,
    // distinguished only by the now-dropped group_id. Once that column is
    // gone, (flow_id, node_id, branch_key) collides — the migration must
    // merge these into one invocation (same shape a retry already
    // produces) rather than crash on the new unique index.
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    const setupSqls = [
      `CREATE TABLE _schema_version (
        store TEXT NOT NULL, version INTEGER NOT NULL, applied_at INTEGER NOT NULL,
        PRIMARY KEY (store, version)
      )`,
      `INSERT INTO _schema_version (store, version, applied_at) VALUES ('step-timings', 3, 0)`,
      `CREATE TABLE flow_node_invocations (
        id          TEXT    PRIMARY KEY,
        flow_id     TEXT    NOT NULL,
        pipeline_id TEXT,
        node_id     TEXT    NOT NULL,
        group_id    TEXT,
        branch_key  TEXT,
        invoked_at  INTEGER NOT NULL
      )`,
      `CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(group_id, ''), COALESCE(branch_key, ''))`,
      `CREATE INDEX idx_invocations_flow      ON flow_node_invocations (flow_id)`,
      `CREATE INDEX idx_invocations_pipe_node ON flow_node_invocations (pipeline_id, node_id)`,
      `CREATE TABLE flow_node_executions (
        id            TEXT    PRIMARY KEY,
        invocation_id TEXT    NOT NULL,
        entered_at    INTEGER NOT NULL,
        exited_at     INTEGER NOT NULL,
        error         TEXT
      )`,
      `CREATE INDEX idx_fne_invocation ON flow_node_executions(invocation_id)`,
      `CREATE INDEX idx_fne_exited     ON flow_node_executions(exited_at)`,
      `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, group_id, branch_key, invoked_at)
        VALUES ('inv-scored', 'flow-collide', 'pipe', '__end__', 'scored', 'job-1', 1000)`,
      `INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, error)
        VALUES ('exec-scored', 'inv-scored', 1000, 1050, 'score-side error')`,
      `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, group_id, branch_key, invoked_at)
        VALUES ('inv-propose', 'flow-collide', 'pipe', '__end__', 'PROPOSE', 'job-1', 2000)`,
      `INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, error)
        VALUES ('exec-propose', 'inv-propose', 2000, 2050, 'propose-side error')`,
    ];
    for (const sql of setupSqls) await raw.execute(sql);
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);

    const { rows: invocations } = await store._db.execute({
      sql: "SELECT id FROM flow_node_invocations WHERE flow_id = 'flow-collide' AND node_id = '__end__'",
      args: [],
    });
    assert.equal(invocations.length, 1, "the two colliding invocations must merge into one");

    // error column is gone by v9 (same boot) — verify the merge by row
    // count + entered_at ordering instead of error content.
    const { rows: executions } = await store._db.execute({
      sql: "SELECT entered_at FROM flow_node_executions WHERE invocation_id = ? ORDER BY entered_at ASC",
      args: [invocations[0].id],
    });
    assert.equal(executions.length, 2, "both executions must survive the merge, reattached to the surviving invocation");
    assert.deepEqual(executions.map((e) => Number(e.entered_at)), [1000, 2000]);

    store.close();
  });
});

// ─── v5 migration: add visit_seq to flow_node_invocations ──────────────────────

describe("createNodeExecutionsStore — v5 migration (visit_seq add)", () => {
  test("backfills visit_seq=1 for every pre-existing row and the new index still enforces + allows dedup correctly", async () => {
    const dbPath = tmpDb();
    const raw = createClient({ url: `file:${dbPath}` });
    const setupSqls = [
      `CREATE TABLE _schema_version (
        store TEXT NOT NULL, version INTEGER NOT NULL, applied_at INTEGER NOT NULL,
        PRIMARY KEY (store, version)
      )`,
      `INSERT INTO _schema_version (store, version, applied_at) VALUES ('step-timings', 4, 0)`,
      `CREATE TABLE flow_node_invocations (
        id          TEXT    PRIMARY KEY,
        flow_id     TEXT    NOT NULL,
        pipeline_id TEXT,
        node_id     TEXT    NOT NULL,
        branch_key  TEXT,
        invoked_at  INTEGER NOT NULL
      )`,
      `CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(branch_key, ''))`,
      `CREATE INDEX idx_invocations_flow      ON flow_node_invocations (flow_id)`,
      `CREATE INDEX idx_invocations_pipe_node ON flow_node_invocations (pipeline_id, node_id)`,
      `CREATE TABLE flow_node_executions (
        id            TEXT    PRIMARY KEY,
        invocation_id TEXT    NOT NULL,
        entered_at    INTEGER NOT NULL,
        exited_at     INTEGER NOT NULL,
        error         TEXT
      )`,
      `CREATE INDEX idx_fne_invocation ON flow_node_executions(invocation_id)`,
      `CREATE INDEX idx_fne_exited     ON flow_node_executions(exited_at)`,
      `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, branch_key, invoked_at)
        VALUES ('inv-pre-v5', 'flow-v5', 'pipe-v5', 'DRAFT', 'item-1', 1000)`,
      `INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, error)
        VALUES ('exec-pre-v5', 'inv-pre-v5', 1000, 1050, 'pre-v5 error')`,
    ];
    for (const sql of setupSqls) await raw.execute(sql);
    raw.close();

    const store = await createNodeExecutionsStore(dbPath);

    const { rows: colRows } = await store._db.execute({ sql: "PRAGMA table_info(flow_node_invocations)", args: [] });
    const cols = colRows.map((r) => r.name);
    assert.ok(cols.includes("visit_seq"), "visit_seq column must exist after the v5 migration");

    const { rows: preRows } = await store._db.execute({
      sql: "SELECT visit_seq FROM flow_node_invocations WHERE id = 'inv-pre-v5'",
      args: [],
    });
    assert.equal(Number(preRows[0].visit_seq), 1, "pre-existing rows must backfill to visit_seq=1");

    // Same identity, no forceNew — must reuse the pre-existing round 1 invocation.
    const reused = await store.findOrCreateInvocation({ flowId: "flow-v5", pipelineId: "pipe-v5", nodeId: "DRAFT", subflowKey: "item-1" });
    assert.equal(reused, "inv-pre-v5");

    // Same identity, forceNew:true — must create a genuine round 2, not collide.
    const fresh = await store.findOrCreateInvocation({ flowId: "flow-v5", pipelineId: "pipe-v5", nodeId: "DRAFT", subflowKey: "item-1", forceNew: true });
    assert.notEqual(fresh, "inv-pre-v5");
    const { rows: freshRows } = await store._db.execute({
      sql: "SELECT visit_seq FROM flow_node_invocations WHERE id = ?",
      args: [fresh],
    });
    assert.equal(Number(freshRows[0].visit_seq), 2);

    // A raw duplicate-round insert attempt must still be rejected by the index.
    await assert.rejects(async () => {
      await store._db.execute({
        sql: `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, subflow_key, visit_seq, invoked_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: [randomUUID(), "flow-v5", "pipe-v5", "DRAFT", "item-1", 1, Date.now()],
      });
    }, /UNIQUE constraint failed/);

    store.close();
  });
});

// queryErrorsByFlow()/queryErrorsByFlowIds() moved to state/failure-store.js
// in Phase 1.5 — coverage now lives in failure-store.test.js.

// ── getExecution() ───────────────────────────────────────────────────────────
// No error/errorType fields since Phase 1.5 — this returns identity/timing
// only ({id, invocationId, enteredAt, exitedAt, kind}).

describe("getExecution()", () => {
  test("returns null for an unknown execution id", async () => {
    const store = await makeIsolatedStore();
    assert.equal(await store.getExecution(randomUUID()), null);
    store.close();
  });

  test("returns the full identity/timing row by execution id", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const written = await store.write({ flowId, nodeId: "SUBMIT_CODE", enteredAt: 100, exitedAt: 110, kind: "fresh" });
    await store.flush();
    const exec = await store.getExecution(written.executionId);
    assert.equal(exec.id, written.executionId);
    assert.equal(exec.invocationId, written.invocationId);
    assert.equal(exec.enteredAt, 100);
    assert.equal(exec.exitedAt, 110);
    assert.equal(exec.kind, "fresh");
    store.close();
  });
});

// ─── findOrCreateInvocation() ─────────────────────────────────────────────────

describe("findOrCreateInvocation()", () => {
  test("creates a new invocation and returns its id", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id = await store.findOrCreateInvocation({ flowId, pipelineId: "p", nodeId: "SUBMIT", subflowKey: "item-1" });
    assert.match(id, UUID4_RE);
    const { rows } = await store._db.execute({
      sql: "SELECT * FROM flow_node_invocations WHERE id = ?",
      args: [id],
    });
    const row = rows[0];
    assert.equal(row.flow_id, flowId);
    assert.equal(row.node_id, "SUBMIT");
    assert.equal(row.subflow_key, "item-1");
    store.close();
  });

  test("a second call with the same identity tuple returns the SAME id, not a new one", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id1 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    const id2 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    assert.equal(id1, id2);
    const { rows } = await store._db.execute({
      sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(Number(rows[0].n), 1, "must not create a duplicate invocation for the same identity tuple");
    store.close();
  });

  test("two root-level calls (subflowKey null) for the same flow+node also collapse into one invocation", async () => {
    // This is the exact case the COALESCE-based unique index exists to fix —
    // a bare UNIQUE(flow_id,node_id,branch_key) would treat two NULL rows as
    // distinct and silently create two invocations.
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id1 = await store.findOrCreateInvocation({ flowId, nodeId: "ROOT_NODE" });
    const id2 = await store.findOrCreateInvocation({ flowId, nodeId: "ROOT_NODE" });
    assert.equal(id1, id2);
    store.close();
  });

  test("different subflowKeys under the same node produce different invocations", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id1 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    const id2 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-2" });
    assert.notEqual(id1, id2);
    store.close();
  });

  test("race safety: a losing concurrent insert re-selects and returns the winner's id, not a throw", async () => {
    // Simulates two callers racing to create the same invocation: seed the
    // row directly (as if another process's INSERT already committed), then
    // confirm findOrCreateInvocation's insert-fails-then-reselect path
    // recovers cleanly rather than throwing or creating a duplicate.
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const winnerId = randomUUID();
    await store._db.execute({
      sql: `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, subflow_key, visit_seq, invoked_at)
       VALUES (?, ?, NULL, ?, ?, 1, ?)`,
      args: [winnerId, flowId, "SUBMIT", "item-1", Date.now()],
    });

    const foundId = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    assert.equal(foundId, winnerId, "loser must discover and return the already-committed row's id");
    const { rows } = await store._db.execute({
      sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(Number(rows[0].n), 1);
    store.close();
  });

  // ── forceNew / visit_seq ──────────────────────────────────────────────────

  test("forceNew:true creates a SECOND invocation (visit_seq=2) for the same identity tuple, not a reuse", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id1 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    const id2 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1", forceNew: true });
    assert.notEqual(id1, id2, "forceNew must not reuse the existing invocation");
    const { rows } = await store._db.execute({
      sql: "SELECT id, visit_seq FROM flow_node_invocations WHERE flow_id = ? ORDER BY visit_seq ASC",
      args: [flowId],
    });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].id, id1);
    assert.equal(Number(rows[0].visit_seq), 1);
    assert.equal(rows[1].id, id2);
    assert.equal(Number(rows[1].visit_seq), 2);
    store.close();
  });

  test("forceNew:false (default) keeps reusing the LATEST invocation regardless of how many rounds already exist", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const round1 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    const round2 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1", forceNew: true });
    const round3 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1", forceNew: true });
    assert.notEqual(round1, round2);
    assert.notEqual(round2, round3);

    const reused = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" });
    assert.equal(reused, round3, "a continuation must reuse the LATEST round, not the first one ever created");
    const { rows } = await store._db.execute({
      sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(Number(rows[0].n), 3, "no new invocation should have been created by the continuation call");
    store.close();
  });

  test("forceNew:true on a brand-new identity (no prior invocation at all) still just creates visit_seq=1", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id = await store.findOrCreateInvocation({ flowId, nodeId: "FIRST_EVER", forceNew: true });
    const { rows } = await store._db.execute({
      sql: "SELECT visit_seq FROM flow_node_invocations WHERE id = ?",
      args: [id],
    });
    assert.equal(Number(rows[0].visit_seq), 1);
    store.close();
  });

  // ── guard (cross-flow atomic dedup) ────────────────────────────────────
  //
  // The per-flow
  // unique index above only ever prevented the SAME flowId from double-
  // dispatching an identity — it never had an opinion on two DIFFERENT
  // parent flows (e.g. two overlapping scans)
  // both deciding to dispatch the same (nodeId, subflowKey). `guard` closes
  // that gap by running inside this call's own withSharedWrite turn, right
  // before the insert — see the function's own header comment.
  describe("guard", () => {
    test("guard returning false skips the insert and returns null — no row is created", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      const id = await store.findOrCreateInvocation({
        flowId, nodeId: "SCORE", subflowKey: "listing-1", guard: async () => false,
      });
      assert.equal(id, null);
      const { rows } = await store._db.execute({
        sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE flow_id = ?",
        args: [flowId],
      });
      assert.equal(Number(rows[0].n), 0);
      store.close();
    });

    test("guard returning true allows the insert through, same as no guard at all", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      const id = await store.findOrCreateInvocation({
        flowId, nodeId: "SCORE", subflowKey: "listing-1", guard: async () => true,
      });
      assert.match(id, UUID4_RE);
      store.close();
    });

    test("a rejected guard call means a DIFFERENT flowId can never create a duplicate identity for the same cross-flow subject", async () => {
      // Simulates the real race: flow A's dispatch commits first (no guard
      // needed — its own identity is brand new); flow B's dispatch for the
      // SAME subflowKey supplies a guard that checks whether ANY invocation
      // already exists for this subflowKey across flows — the exact shape
      // isListingInFlight provides in production.
      const store = await makeIsolatedStore();
      const flowA = randomUUID();
      const flowB = randomUUID();
      await store.findOrCreateInvocation({ flowId: flowA, nodeId: "SCORE", subflowKey: "listing-shared" });

      const crossFlowGuard = async () => {
        const { rows } = await store._db.execute({
          sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE node_id = ? AND subflow_key = ?",
          args: ["SCORE", "listing-shared"],
        });
        return Number(rows[0].n) === 0;
      };
      const idB = await store.findOrCreateInvocation({
        flowId: flowB, nodeId: "SCORE", subflowKey: "listing-shared", guard: crossFlowGuard,
      });
      assert.equal(idB, null, "flow B must be rejected — flow A already claimed this subflowKey");

      const { rows } = await store._db.execute({
        sql: "SELECT COUNT(*) AS n FROM flow_node_invocations WHERE node_id = ? AND subflow_key = ?",
        args: ["SCORE", "listing-shared"],
      });
      assert.equal(Number(rows[0].n), 1, "only flow A's invocation should exist");
      store.close();
    });

    test("guard is not consulted when an invocation already exists for THIS flow's own identity (forceNew:false reuse path)", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      const first = await store.findOrCreateInvocation({ flowId, nodeId: "SCORE", subflowKey: "listing-1" });
      let guardCalled = false;
      const second = await store.findOrCreateInvocation({
        flowId, nodeId: "SCORE", subflowKey: "listing-1",
        guard: async () => { guardCalled = true; return false; },
      });
      assert.equal(second, first, "an existing same-flow invocation must be reused without ever consulting guard");
      assert.equal(guardCalled, false);
      store.close();
    });
  });
});

describe("getInvocation()", () => {
  test("returns the invocation row for a known id", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id = await store.findOrCreateInvocation({ flowId, pipelineId: "p", nodeId: "SUBMIT", subflowKey: "item-1" });
    const row = await store.getInvocation(id);
    assert.equal(typeof row.invokedAt, "number");
    assert.deepEqual(row, {
      id, flowId, pipelineId: "p", nodeId: "SUBMIT", subflowKey: "item-1",
      visitSeq: 1, invokedAt: row.invokedAt, resultWrittenAt: null,
    });
    store.close();
  });

  test("resultWrittenAt is set once the invocation's result is written", async () => {
    const store = await makeIsolatedStore();
    const id = await store.findOrCreateInvocation({ flowId: randomUUID(), pipelineId: "p", nodeId: "SUBMIT", subflowKey: null });
    await store.writeResult(id, { ok: true });
    const row = await store.getInvocation(id);
    assert.equal(typeof row.resultWrittenAt, "number");
    store.close();
  });

  test("returns null for an unknown id", async () => {
    const store = await makeIsolatedStore();
    assert.equal(await store.getInvocation(randomUUID()), null);
    store.close();
  });
});

describe("getLatestInvocationForNode()", () => {
  test("returns the highest-visit_seq invocation for a (flowId, nodeId, subflowKey) lineage", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const round1 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "only" });
    const round2 = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "only", forceNew: true });

    const latest = await store.getLatestInvocationForNode(flowId, "SUBMIT", "only");
    assert.equal(latest.id, round2);
    assert.equal(latest.visitSeq, 2);
    assert.notEqual(latest.id, round1);
    store.close();
  });

  test("scopes by subflowKey — a different item's invocation never wins", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const itemA = await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-a" });
    await store.findOrCreateInvocation({ flowId, nodeId: "SUBMIT", subflowKey: "item-b", forceNew: true });

    const latest = await store.getLatestInvocationForNode(flowId, "SUBMIT", "item-a");
    assert.equal(latest.id, itemA);
    store.close();
  });

  test("returns null when no invocation exists for that identity", async () => {
    const store = await makeIsolatedStore();
    const result = await store.getLatestInvocationForNode(randomUUID(), "SUBMIT", "only");
    assert.equal(result, null);
    store.close();
  });
});

describe("getLatestInvocationInvokedAt()", () => {
  // Used by ios/live-bridge.js's snapshot() to backfill blockedAtMs for
  // gate items opened before requestedAt existed on wait_json items — the
  // exact moment the gate NODE was dispatched, independent of the flow's
  // own createdAt (which reflects the whole flow, not this specific node).
  test("returns the invokedAt of the highest-visit_seq invocation", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.findOrCreateInvocation({ flowId, nodeId: "GATE", subflowKey: "only" });
    await store.findOrCreateInvocation({ flowId, nodeId: "GATE", subflowKey: "only", forceNew: true });
    const { rows } = await store._db.execute({
      sql: "SELECT invoked_at FROM flow_node_invocations WHERE flow_id = ? ORDER BY visit_seq DESC LIMIT 1",
      args: [flowId],
    });
    const invokedAt = await store.getLatestInvocationInvokedAt(flowId, "GATE", "only");
    assert.equal(invokedAt, Number(rows[0].invoked_at));
    store.close();
  });

  test("returns null when no invocation exists for that identity", async () => {
    const store = await makeIsolatedStore();
    const invokedAt = await store.getLatestInvocationInvokedAt(randomUUID(), "GATE", "only");
    assert.equal(invokedAt, null);
    store.close();
  });

  test("is available while the invocation is still unresolved (result_written_at null) — the gate-blocked case", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.findOrCreateInvocation({ flowId, nodeId: "GATE", subflowKey: null });
    const invokedAt = await store.getLatestInvocationInvokedAt(flowId, "GATE", null);
    assert.equal(typeof invokedAt, "number");
    store.close();
  });
});

describe("listInvocationIdsForNode()", () => {
  test("returns every child-spawn invocation id (subflowKey set) for one loop node's firing", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const id1 = await store.findOrCreateInvocation({ flowId, nodeId: "LOOP", subflowKey: "item-1" });
    const id2 = await store.findOrCreateInvocation({ flowId, nodeId: "LOOP", subflowKey: "item-2" });
    // A different flow's invocation under the same-named node must not leak in.
    await store.findOrCreateInvocation({ flowId: randomUUID(), nodeId: "LOOP", subflowKey: "item-1" });

    const ids = (await store.listInvocationIdsForNode(flowId, "LOOP")).sort();
    assert.deepEqual(ids, [id1, id2].sort());
    store.close();
  });

  test("excludes an ordinary root-level invocation for the same node (subflowKey null)", async () => {
    // branch_key IS NOT NULL is the sole "this is a loop-child invocation"
    // signal post-group_id-drop (see node-executions.js's header comment on
    // _listInvocationIdsForNodeStmt). A plain, non-loop dispatch of a node
    // that happens to share the loop's own id (subflowKey null) must not be
    // mistaken for one of its children.
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const childSpawnId = await store.findOrCreateInvocation({ flowId, nodeId: "LOOP", subflowKey: "item-1" });
    await store.findOrCreateInvocation({ flowId, nodeId: "LOOP" }); // subflowKey null — not a child

    const ids = await store.listInvocationIdsForNode(flowId, "LOOP");
    assert.deepEqual(ids, [childSpawnId]);
    store.close();
  });

  test("returns an empty array when the node has no invocations", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listInvocationIdsForNode(randomUUID(), "LOOP"), []);
    store.close();
  });
});

describe("listSubflowNodeIdsForFlow()", () => {
  test("returns every loop node this flow has spawned real children under", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.findOrCreateInvocation({ flowId, nodeId: "LOOP_A", subflowKey: "item-1" });
    await store.findOrCreateInvocation({ flowId, nodeId: "LOOP_B", subflowKey: "item-1" });
    // Root-level (subflowKey null) invocations must not appear.
    await store.findOrCreateInvocation({ flowId, nodeId: "ROOT_NODE" });

    const nodeIds = (await store.listSubflowNodeIdsForFlow(flowId)).sort();
    assert.deepEqual(nodeIds, ["LOOP_A", "LOOP_B"]);
    store.close();
  });

  test("returns an empty array for a flow with no loop invocations", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listSubflowNodeIdsForFlow(randomUUID()), []);
    store.close();
  });
});

describe("no-op store — new query methods", () => {
  test("identity/timing queries degrade safely", async () => {
    const blockingFile = path.join(os.tmpdir(), `gf-block-errq-${randomUUID()}`);
    writeFileSync(blockingFile, "not a directory");
    const badPath = path.join(blockingFile, "node-executions.sqlite");
    const store = await createNodeExecutionsStore(badPath);
    assert.equal(await store.getInvocation("x"), null);
    assert.deepEqual(await store.listInvocationIdsForNode("f", "g"), []);
    assert.deepEqual(await store.listSubflowNodeIdsForFlow("f"), []);
    assert.equal(await store.getLatestExecutionForNode({ flowId: "f", nodeId: "g" }), null);
  });
});

describe("getLatestExecutionForNode()", () => {
  test("returns null when no execution exists for the tuple", async () => {
    const store = await makeIsolatedStore();
    assert.equal(await store.getLatestExecutionForNode({ flowId: randomUUID(), nodeId: "X" }), null);
    store.close();
  });

  test("returns the most recent execution's ids across multiple writes to the same tuple", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "DRAFT", enteredAt: 100, exitedAt: 110, kind: "fresh" });
    const latest = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 200, exitedAt: 210, kind: "retry" });
    await store.flush();

    const result = await store.getLatestExecutionForNode({ flowId, nodeId: "DRAFT" });
    assert.equal(result.invocationId, latest.invocationId);
    assert.equal(result.executionId, latest.executionId);
    assert.equal(result.exitedAt, 210);
    store.close();
  });

  test("scopes by subflowKey — a root (null-branch) write does not match a branch-scoped lookup", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "SUBMIT", enteredAt: 1, exitedAt: 2, kind: "fresh" });

    assert.equal(await store.getLatestExecutionForNode({ flowId, nodeId: "SUBMIT", subflowKey: "item-1" }), null);
    store.close();
  });

  test("does not match a different flow's execution for the same node", async () => {
    const store = await makeIsolatedStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    await store.write({ flowId: flowA, nodeId: "X", enteredAt: 1, exitedAt: 2, kind: "fresh" });

    assert.equal(await store.getLatestExecutionForNode({ flowId: flowB, nodeId: "X" }), null);
    store.close();
  });
});

// summarizeChildrenByFlowIds — the dual-mode counterpart to
// branchService.summarizeByFlowIds, sourced from real flow_runs children
// instead of flow_branches. flow_node_invocations and flow_runs live in
// the same physical runtime.sqlite file, so these tests point a
// createRunStore and a createNodeExecutionsStore at the SAME tmp path
// (same pattern as engine/executor-loop-children.test.js's harness) and
// wire real children via spawn-child.js's resolveOrCreateChild — the
// exact path dispatchLoopChildren uses in production.
describe("summarizeChildrenByFlowIds()", () => {
  async function makeDualStore() {
    const dbPath = tmpDb();
    const runStore = createRunStore({ dbPath });
    await runStore.ready();
    const nodeExecutionsStore = createNodeExecutionsStore(dbPath);
    return { runStore, nodeExecutionsStore, dbPath };
  }

  async function spawnChild({ runStore, nodeExecutionsStore }, { flowId, nodeId, subflowKey, status }) {
    const store = {
      create: ({ pipelineId, initialState, parentInvocationId }) => {
        const childFlowId = randomUUID();
        runStore.insert({
          flowId: childFlowId, ownerKey: "test-owner", pipelineId, revision: 0,
          status, currentStep: null, lastStep: null, stateJson: initialState ?? {},
          createdAt: Date.now(), updatedAt: Date.now(), parentInvocationId,
        });
        return { flowId: childFlowId };
      },
    };
    return resolveOrCreateChild({
      flowId, pipelineId: "team_pipe", nodeId, subflowKey,
      childPipelineId: `team_pipe::${nodeId}`, initialState: {},
      store, runStore, nodeExecutionsStore,
    });
  }

  test("returns an empty Map for an empty or non-array input", async () => {
    const { nodeExecutionsStore, runStore } = await makeDualStore();
    assert.deepEqual(await nodeExecutionsStore.summarizeChildrenByFlowIds([]), new Map());
    assert.deepEqual(await nodeExecutionsStore.summarizeChildrenByFlowIds(undefined), new Map());
    runStore.close(); nodeExecutionsStore.close();
  });

  test("buckets real children by status, matching branchService.summarizeByFlowIds' shape", async () => {
    const harness = await makeDualStore();
    const { runStore, nodeExecutionsStore } = harness;
    const flowId = randomUUID();
    await spawnChild(harness, { flowId, nodeId: "SCORE", subflowKey: "a", status: "succeeded" });
    await spawnChild(harness, { flowId, nodeId: "SCORE", subflowKey: "b", status: "succeeded" });
    await spawnChild(harness, { flowId, nodeId: "SCORE", subflowKey: "c", status: "failed" });
    await spawnChild(harness, { flowId, nodeId: "SCORE", subflowKey: "d", status: "blocked" });
    await spawnChild(harness, { flowId, nodeId: "SCORE", subflowKey: "e", status: "skipped" });

    const result = await nodeExecutionsStore.summarizeChildrenByFlowIds([flowId]);
    const summary = result.get(flowId);
    assert.ok(summary);
    assert.equal(summary.total, 5);
    assert.equal(summary.succeeded, 2);
    assert.equal(summary.failed, 1);
    assert.equal(summary.pending, 1, "blocked folds into pending, same as branchService");
    assert.equal(summary.skipped, 1);
    runStore.close(); nodeExecutionsStore.close();
  });

  test("a flowId with no real children is absent from the Map (falls through to old-model/EMPTY_BRANCHES upstream)", async () => {
    const harness = await makeDualStore();
    const { runStore, nodeExecutionsStore } = harness;
    const flowWithChildren = randomUUID();
    const flowWithoutChildren = randomUUID();
    await spawnChild(harness, { flowId: flowWithChildren, nodeId: "SCORE", subflowKey: "a", status: "succeeded" });

    const result = await nodeExecutionsStore.summarizeChildrenByFlowIds([flowWithChildren, flowWithoutChildren]);
    assert.equal(result.has(flowWithChildren), true);
    assert.equal(result.has(flowWithoutChildren), false);
    runStore.close(); nodeExecutionsStore.close();
  });

  test("covers multiple flows and multiple loop nodes on the same flow in one call", async () => {
    const harness = await makeDualStore();
    const { runStore, nodeExecutionsStore } = harness;
    const flowA = randomUUID();
    const flowB = randomUUID();
    await spawnChild(harness, { flowId: flowA, nodeId: "SCORE", subflowKey: "a1", status: "succeeded" });
    await spawnChild(harness, { flowId: flowA, nodeId: "PROPOSE", subflowKey: "a2", status: "running" });
    await spawnChild(harness, { flowId: flowB, nodeId: "SCORE", subflowKey: "b1", status: "failed" });

    const result = await nodeExecutionsStore.summarizeChildrenByFlowIds([flowA, flowB]);
    assert.equal(result.get(flowA).total, 2, "sums across every loop node on the flow, not just one group");
    assert.equal(result.get(flowA).succeeded, 1);
    assert.equal(result.get(flowA).pending, 1);
    assert.equal(result.get(flowB).total, 1);
    assert.equal(result.get(flowB).failed, 1);
    runStore.close(); nodeExecutionsStore.close();
  });
});

// ─── listInvocationsForFlow() / listExecutionsForInvocation() ─────────────────
// Data source for the run-detail node tree (routes/runs.js's
// GET /manifold/runs/:flowId/tree[/expand]) — invocation-granular rows with
// a drill-in to full per-execution-attempt history.

describe("listInvocationsForFlow()", () => {
  test("returns [] for a flow with no invocations", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listInvocationsForFlow(randomUUID()), []);
    store.close();
  });

  test("one invocation, one execution — latest fields mirror the single execution", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const written = await store.write({ flowId, nodeId: "CACHE_CHECK", enteredAt: 100, exitedAt: 140, kind: "fresh" });
    await store.flush();
    const rows = await store.listInvocationsForFlow(flowId);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].nodeId, "CACHE_CHECK");
    assert.equal(rows[0].subflowKey, null);
    assert.equal(rows[0].executionCount, 1);
    assert.equal(rows[0].enteredAt, 100);
    assert.equal(rows[0].exitedAt, 140);
    assert.equal(rows[0].latestExecutionId, written.executionId);
    assert.equal(rows[0].kind, "fresh");
    assert.equal(typeof rows[0].invocationId, "string");
    store.close();
  });

  test("multiple executions on one invocation collapse to one row — executionCount reflects all attempts, fields reflect the LATEST", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "SCORE_LLM", enteredAt: 100, exitedAt: 110, kind: "fresh" });
    await store.write({ flowId, nodeId: "SCORE_LLM", enteredAt: 200, exitedAt: 210, kind: "retry" });
    const third = await store.write({ flowId, nodeId: "SCORE_LLM", enteredAt: 300, exitedAt: 320, kind: "retry" });
    await store.flush();
    const rows = await store.listInvocationsForFlow(flowId);
    assert.equal(rows.length, 1, "still one row — same invocation, three attempts");
    assert.equal(rows[0].executionCount, 3);
    assert.equal(rows[0].enteredAt, 300, "latest attempt's entered_at");
    assert.equal(rows[0].latestExecutionId, third.executionId, "reflects the LATEST attempt's own execution id");
    assert.equal(rows[0].kind, "retry");
    store.close();
  });

  // everRetriedOrRevived. `kind` above is the LATEST execution only; this
  // field looks across the WHOLE history, so a retry/revival buried behind
  // later "recheck" (the normal, structural re-entry every subflow node
  // gets once its children finish) is still visible.
  describe("everRetriedOrRevived", () => {
    test("single fresh execution — false", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      await store.write({ flowId, nodeId: "PROPOSE", enteredAt: 100, exitedAt: 110, kind: "fresh" });
      const rows = await store.listInvocationsForFlow(flowId);
      assert.equal(rows[0].everRetriedOrRevived, false);
      store.close();
    });

    test("fresh -> recheck, no retry ever happened — false (the routine case, not noise)", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      await store.write({ flowId, nodeId: "PROPOSE", enteredAt: 100, exitedAt: 110, kind: "fresh" });
      await store.write({ flowId, nodeId: "PROPOSE", enteredAt: 200, exitedAt: 210, kind: "recheck" });
      await store.flush();
      const rows = await store.listInvocationsForFlow(flowId);
      assert.equal(rows[0].kind, "recheck", "sanity: latest kind really is recheck");
      assert.equal(rows[0].everRetriedOrRevived, false);
      store.close();
    });

    test("fresh -> retry -> recheck — true, even though latest kind (recheck) would mask it", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      await store.write({ flowId, nodeId: "PROPOSE", enteredAt: 100, exitedAt: 110, kind: "fresh" });
      await store.write({ flowId, nodeId: "PROPOSE", enteredAt: 200, exitedAt: 210, kind: "retry" });
      await store.write({ flowId, nodeId: "PROPOSE", enteredAt: 300, exitedAt: 310, kind: "recheck" });
      await store.flush();
      const rows = await store.listInvocationsForFlow(flowId);
      assert.equal(rows[0].kind, "recheck", "sanity: latest kind really is recheck");
      assert.equal(rows[0].everRetriedOrRevived, true);
      store.close();
    });

    test("fresh -> revival — true", async () => {
      const store = await makeIsolatedStore();
      const flowId = randomUUID();
      await store.write({ flowId, nodeId: "ENSURE_LOGIN", enteredAt: 100, exitedAt: 110, kind: "fresh" });
      await store.write({ flowId, nodeId: "ENSURE_LOGIN", enteredAt: 200, exitedAt: 210, kind: "revival" });
      await store.flush();
      const rows = await store.listInvocationsForFlow(flowId);
      assert.equal(rows[0].everRetriedOrRevived, true);
      store.close();
    });
  });

  test("multiple distinct nodes produce multiple rows, ordered by invoked_at (invocation-creation time, not enteredAt)", async () => {
    // invoked_at is stamped Date.now() inside findOrCreateInvocation at
    // write() time, independent of whatever enteredAt the caller passes —
    // so ordering here must follow real call order, not a fabricated
    // enteredAt value. This is also the semantically correct choice for the
    // tree: a later RETRY of an already-invoked node keeps invoked_at
    // pinned to its first attempt, so a retried node stays at its original
    // position in the tree instead of jumping to wherever the retry
    // happened (see listInvocationsForFlow's own comment).
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "FIRST", enteredAt: 100, exitedAt: 110, kind: "fresh" });
    await store.write({ flowId, nodeId: "SECOND", enteredAt: 200, exitedAt: 210, kind: "fresh" });
    const rows = await store.listInvocationsForFlow(flowId);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].nodeId, "FIRST", "ordered by real call order (invoked_at)");
    assert.equal(rows[1].nodeId, "SECOND");
    store.close();
  });

  test("excludes subflow-item invocations (subflow_key set) — those are branch-item spawn points, not root-level dispatches", async () => {
    // A subflow-kind node's OWN dispatch (subflow_key=null) and each branch
    // item's own spawn invocation (subflow_key=<item key>) share the same
    // node_id but are semantically different things — the tree only wants
    // the former at root level; the latter is resolved separately via
    // listInvocationIdsForNode when a subflow row is expanded. Regression
    // guard: without this filter, a node that fanned out to N items
    // surfaced as N separate root-level rows instead of one.
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "PROPOSE", subflowKey: "listing-42", enteredAt: 100, exitedAt: 110, kind: "fresh" });
    await store.write({ flowId, nodeId: "PROPOSE", subflowKey: null, enteredAt: 200, exitedAt: 210, kind: "fresh" });
    const rows = await store.listInvocationsForFlow(flowId);
    assert.equal(rows.length, 1, "only the null-subflowKey (node's own) invocation appears");
    assert.equal(rows[0].subflowKey, null);
    store.close();
  });

  test("scoped to the given flowId — another flow's invocations are excluded", async () => {
    const store = await makeIsolatedStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    await store.write({ flowId: flowA, nodeId: "A_NODE", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.write({ flowId: flowB, nodeId: "B_NODE", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const rows = await store.listInvocationsForFlow(flowA);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].nodeId, "A_NODE");
    store.close();
  });

  test("no-op store returns []", () => {
    const noopStore = {
      write: () => {}, listInvocationsForFlow: () => [], listExecutionsForInvocation: () => [], close: () => {},
    };
    assert.deepEqual(noopStore.listInvocationsForFlow("any-flow"), []);
  });
});

describe("listExecutionsForInvocation()", () => {
  test("returns [] for an invocation with no executions (or that doesn't exist)", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listExecutionsForInvocation(randomUUID()), []);
    store.close();
  });

  test("returns every attempt in chronological order, each with its own kind/timing", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "SUBMIT", enteredAt: 100, exitedAt: 110, kind: "fresh" });
    await store.write({ flowId, nodeId: "SUBMIT", enteredAt: 200, exitedAt: 215, kind: "retry" });
    await store.flush();
    const [{ invocationId }] = await store.listInvocationsForFlow(flowId);
    const executions = await store.listExecutionsForInvocation(invocationId);
    assert.equal(executions.length, 2);
    assert.equal(executions[0].kind, "fresh");
    assert.equal(executions[0].enteredAt, 100);
    assert.equal(executions[1].kind, "retry");
    assert.equal(executions[1].enteredAt, 200);
    assert.ok(executions.every((e) => typeof e.executionId === "string"));
    store.close();
  });

  test("scoped to the given invocationId — another invocation's executions are excluded", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "NODE_A", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.write({ flowId, nodeId: "NODE_B", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.flush();
    const rows = await store.listInvocationsForFlow(flowId);
    const nodeAInvocation = rows.find((r) => r.nodeId === "NODE_A").invocationId;
    const executions = await store.listExecutionsForInvocation(nodeAInvocation);
    assert.equal(executions.length, 1);
    store.close();
  });
});

describe("hasInvocations()", () => {
  test("returns an empty Set for empty or non-array input", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.hasInvocations([]), new Set());
    assert.deepEqual(await store.hasInvocations(null), new Set());
    store.close();
  });

  test("includes only flowIds that have dispatched at least one node", async () => {
    const store = await makeIsolatedStore();
    const withInvocation = randomUUID();
    const without = randomUUID();
    await store.write({ flowId: withInvocation, nodeId: "A", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const result = await store.hasInvocations([withInvocation, without]);
    assert.equal(result.has(withInvocation), true);
    assert.equal(result.has(without), false);
    store.close();
  });

  test("no-op store returns an empty Set", () => {
    const noopStore = { write: () => {}, hasInvocations: () => new Set(), close: () => {} };
    assert.deepEqual(noopStore.hasInvocations(["any-flow"]), new Set());
  });
});

describe("listVisitedNodeIdsByFlow()", () => {
  test("returns an empty Map for empty or non-array input", async () => {
    const store = await makeIsolatedStore();
    assert.deepEqual(await store.listVisitedNodeIdsByFlow([]), new Map());
    assert.deepEqual(await store.listVisitedNodeIdsByFlow(null), new Map());
    store.close();
  });

  test("groups distinct top-level node ids per flow, keyed by flowId", async () => {
    const store = await makeIsolatedStore();
    const flowA = randomUUID();
    const flowB = randomUUID();
    await store.write({ flowId: flowA, nodeId: "PRIME", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.write({ flowId: flowA, nodeId: "DRAFT", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    await store.write({ flowId: flowB, nodeId: "SCAN", enteredAt: 5, exitedAt: 6, kind: "fresh" });
    const result = await store.listVisitedNodeIdsByFlow([flowA, flowB]);
    assert.deepEqual(result.get(flowA), new Set(["PRIME", "DRAFT"]));
    assert.deepEqual(result.get(flowB), new Set(["SCAN"]));
    store.close();
  });

  test("excludes per-item subflow rows (subflow_key set) — only the flow's own top-level dispatches", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    await store.write({ flowId, nodeId: "PROPOSE", subflowKey: null, enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.write({ flowId, nodeId: "PROPOSE", subflowKey: "listing-1", enteredAt: 3, exitedAt: 4, kind: "fresh" });
    const result = await store.listVisitedNodeIdsByFlow([flowId]);
    // Both rows share nodeId "PROPOSE" here, so this alone wouldn't catch a
    // missing filter — the point is documented intent, matching
    // findCurrentStep/listInvocationsForFlow's same scoping.
    assert.deepEqual(result.get(flowId), new Set(["PROPOSE"]));
    store.close();
  });

  test("a retried node (multiple executions, same invocation) still counts once", async () => {
    const store = await makeIsolatedStore();
    const flowId = randomUUID();
    const { invocationId } = await store.write({ flowId, nodeId: "DRAFT", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    await store.write({ flowId, nodeId: "DRAFT", enteredAt: 3, exitedAt: 4, kind: "retry", invocationId, isNewInvocation: false });
    const result = await store.listVisitedNodeIdsByFlow([flowId]);
    assert.deepEqual(result.get(flowId), new Set(["DRAFT"]));
    store.close();
  });

  test("a flowId with no invocations is absent from the Map, not an empty-Set entry", async () => {
    const store = await makeIsolatedStore();
    const withVisit = randomUUID();
    const without = randomUUID();
    await store.write({ flowId: withVisit, nodeId: "A", enteredAt: 1, exitedAt: 2, kind: "fresh" });
    const result = await store.listVisitedNodeIdsByFlow([withVisit, without]);
    assert.equal(result.has(withVisit), true);
    assert.equal(result.has(without), false);
    store.close();
  });

  test("no-op store returns an empty Map", () => {
    const noopStore = { write: () => {}, listVisitedNodeIdsByFlow: () => new Map(), close: () => {} };
    assert.deepEqual(noopStore.listVisitedNodeIdsByFlow(["any-flow"]), new Map());
  });
});
