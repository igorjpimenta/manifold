// Unit tests for state/batch-writer.js — enqueue/flush/flushAll, size- and
// time-triggered flush, FIFO ordering (last-enqueued-wins on same-row
// writes), failure isolation (a bad batch is swallowed, not thrown), and
// per-dbPath queue isolation.
//
// Uses flow_node_executions (schema.js) as the fixture table since it's the
// first real call site this module gets wired into (node-executions.js's
// write()) — createNodeExecutionsStore(dbPath) triggers the same migration
// that store uses in production, so the table exists before batch-writer
// writes to it directly.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { getSharedDb } from "../../../state/db.js";
import { flowNodeExecutions } from "../../../state/schema.js";
import { eq } from "drizzle-orm";
import { enqueue, enqueueRaw, flush, flushAll, _resetForTests } from "../../../state/batch-writer.js";

function tmpDb() {
  return path.join(os.tmpdir(), `gf-bw-${randomUUID()}.sqlite`);
}

// batch-writer itself doesn't run migrations — tests need a store's boot-
// time migrator to have created flow_node_executions first, same as
// production (node-executions.js's own store-creation path does this before
// batch-writer ever enqueues against the same dbPath). Returns both the
// store (for read-back assertions via getExecution()) and the shared
// drizzle `db` handle (so enqueue() gets the same query-builder instance
// production code would) — both backed by the same getSharedDb() connection.
async function migratedDb(dbPath) {
  const store = await createNodeExecutionsStore(dbPath);
  // createNodeExecutionsStore() returns a lazy proxy synchronously — awaiting
  // it does NOT wait for the underlying migration to finish (its "then" trap
  // returns undefined, so `await` on the proxy resolves immediately without
  // waiting on the store's real setup promise). store.ready() is the actual
  // "migration has completed" signal; skipping it races enqueue()'s writes
  // against CREATE TABLE and intermittently fails with "no such table".
  await store.ready();
  const { db } = await getSharedDb(dbPath);
  return { store, db };
}

function insertExecQuery(db, overrides = {}) {
  return db.insert(flowNodeExecutions).values({
    id: randomUUID(),
    invocationId: overrides.invocationId ?? randomUUID(),
    enteredAt: overrides.enteredAt ?? Date.now(),
    exitedAt: overrides.exitedAt ?? Date.now(),
    kind: overrides.kind ?? "fresh",
    ...overrides,
  });
}

describe("batch-writer — enqueue defers the write", () => {
  test("a row is not visible until flush() runs", async () => {
    const dbPath = tmpDb();
    const { store, db } = await migratedDb(dbPath);
    const id = randomUUID();

    enqueue(dbPath, insertExecQuery(db, { id }), { flushIntervalMs: 60_000 }); // long interval so the timer won't fire during the test
    const before = await store.getExecution(id);
    assert.equal(before, null, "row must not be visible before flush");

    await flush(dbPath);
    const after = await store.getExecution(id);
    assert.ok(after, "row must be visible after flush");
    assert.equal(after.id, id);

    _resetForTests(dbPath);
  });
});

describe("batch-writer — size-triggered flush", () => {
  test("reaching maxBatchSize flushes without waiting for the timer", async () => {
    const dbPath = tmpDb();
    const { store, db } = await migratedDb(dbPath);
    const ids = [randomUUID(), randomUUID(), randomUUID()];

    for (const id of ids) {
      enqueue(dbPath, insertExecQuery(db, { id }), { maxBatchSize: 3, flushIntervalMs: 60_000 });
    }

    // The 3rd enqueue() should have triggered an async flush already — wait
    // for whatever's in flight rather than asserting instantly, since the
    // flush itself is an unawaited microtask chain from enqueue()'s
    // perspective.
    await flush(dbPath);

    for (const id of ids) {
      const row = await store.getExecution(id);
      assert.ok(row, `row ${id} should exist after size-triggered flush`);
    }

    _resetForTests(dbPath);
  });
});

describe("batch-writer — time-triggered flush", () => {
  test("a queue below maxBatchSize still flushes after flushIntervalMs", async () => {
    const dbPath = tmpDb();
    const { store, db } = await migratedDb(dbPath);
    const id = randomUUID();

    enqueue(dbPath, insertExecQuery(db, { id }), { maxBatchSize: 50, flushIntervalMs: 30 });
    await new Promise((resolve) => setTimeout(resolve, 80));

    const row = await store.getExecution(id);
    assert.ok(row, "row should be visible after the flush timer fires");

    _resetForTests(dbPath);
  });
});

describe("batch-writer — FIFO ordering", () => {
  test("two writes to the same row apply last-enqueued-wins", async () => {
    const dbPath = tmpDb();
    const { store, db } = await migratedDb(dbPath);
    const id = randomUUID();
    const invocationId = randomUUID();

    // First insert, then an UPDATE on the same row queued in the same
    // window — db.batch() runs its array in enqueue order inside one
    // transaction, so the UPDATE (enqueued second) should win.
    enqueue(dbPath, insertExecQuery(db, { id, invocationId, kind: "fresh" }), { flushIntervalMs: 60_000 });
    enqueue(dbPath, db.update(flowNodeExecutions).set({ kind: "retry" }).where(eq(flowNodeExecutions.id, id)), { flushIntervalMs: 60_000 });

    await flush(dbPath);

    const row = await store.getExecution(id);
    assert.ok(row);
    assert.equal(row.kind, "retry", "later-enqueued UPDATE should win over the earlier INSERT's default");

    _resetForTests(dbPath);
  });
});

describe("batch-writer — failure isolation", () => {
  test("a batch that fails is logged and swallowed, not thrown", async () => {
    const dbPath = tmpDb();
    const { db } = await migratedDb(dbPath);

    const warnings = [];
    const logger = { warn: (msg) => warnings.push(msg) };

    // invocationId is NOT NULL — omitting it should make the batch fail.
    const badQuery = db.insert(flowNodeExecutions).values({
      id: randomUUID(),
      invocationId: null,
      enteredAt: Date.now(),
      exitedAt: Date.now(),
      kind: "fresh",
    });

    enqueue(dbPath, badQuery, { flushIntervalMs: 60_000, logger });
    await assert.doesNotReject(() => flush(dbPath));
    assert.ok(warnings.length > 0, "a failed flush should log a warning");
    assert.match(warnings[0], /batch-writer: flush failed/);

    _resetForTests(dbPath);
  });
});

describe("batch-writer — per-dbPath isolation", () => {
  test("flushAll() drains every queued dbPath independently", async () => {
    const dbPathA = tmpDb();
    const dbPathB = tmpDb();
    const { store: storeA, db: dbA } = await migratedDb(dbPathA);
    const { store: storeB, db: dbB } = await migratedDb(dbPathB);
    const idA = randomUUID();
    const idB = randomUUID();

    enqueue(dbPathA, insertExecQuery(dbA, { id: idA }), { flushIntervalMs: 60_000 });
    enqueue(dbPathB, insertExecQuery(dbB, { id: idB }), { flushIntervalMs: 60_000 });

    await flushAll();

    assert.ok(await storeA.getExecution(idA), "dbPathA's row should be flushed");
    assert.ok(await storeB.getExecution(idB), "dbPathB's row should be flushed");

    _resetForTests(dbPathA);
    _resetForTests(dbPathB);
  });
});

describe("batch-writer — raw statements (enqueueRaw)", () => {
  test("a raw {sql, args} statement is deferred and flushed via the client's own batch()", async () => {
    const dbPath = tmpDb();
    const { store } = await migratedDb(dbPath);
    const id = randomUUID();
    const invocationId = randomUUID();

    enqueueRaw(
      dbPath,
      "INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, kind) VALUES (?, ?, ?, ?, ?)",
      [id, invocationId, Date.now(), Date.now(), "fresh"],
      { flushIntervalMs: 60_000 },
    );
    assert.equal(await store.getExecution(id), null, "raw statement must not be visible before flush");

    await flush(dbPath);
    const row = await store.getExecution(id);
    assert.ok(row, "raw statement should be visible after flush");
    assert.equal(row.id, id);

    _resetForTests(dbPath);
  });

  test("a flush can contain both Drizzle and raw items in the same window (two round-trips, one flush call)", async () => {
    const dbPath = tmpDb();
    const { store, db } = await migratedDb(dbPath);
    const drizzleId = randomUUID();
    const rawId = randomUUID();
    const invocationId = randomUUID();

    enqueue(dbPath, insertExecQuery(db, { id: drizzleId }), { flushIntervalMs: 60_000 });
    enqueueRaw(
      dbPath,
      "INSERT INTO flow_node_executions (id, invocation_id, entered_at, exited_at, kind) VALUES (?, ?, ?, ?, ?)",
      [rawId, invocationId, Date.now(), Date.now(), "fresh"],
      { flushIntervalMs: 60_000 },
    );

    await flush(dbPath);

    assert.ok(await store.getExecution(drizzleId), "the Drizzle-shaped item should be flushed");
    assert.ok(await store.getExecution(rawId), "the raw-shaped item should be flushed");

    _resetForTests(dbPath);
  });
});
