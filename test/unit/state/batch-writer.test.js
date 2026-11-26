import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { createRunStore } from "../../../state/run-store.js";
import { getSharedDb } from "../../../state/db.js";
import { flowNodeExecutions, flowNodeInvocations } from "../../../state/schema.js";
import { eq } from "drizzle-orm";
import { enqueue, enqueueRaw, flush, flushAll, _resetForTests } from "../../../state/batch-writer.js";

function tmpDb() {
  return path.join(os.tmpdir(), `gf-bw-${randomUUID()}.sqlite`);
}

let fixtureInvocationId = null;

async function migratedDb(dbPath) {
  const store = await createNodeExecutionsStore(dbPath);
  await store.ready();
  const { db } = await getSharedDb(dbPath);
  const runStore = await createRunStore({ dbPath });
  const flowId = randomUUID();
  await runStore.insert({ flowId, status: "running" });
  fixtureInvocationId = randomUUID();
  await db.insert(flowNodeInvocations).values({
    id: fixtureInvocationId, flowId, nodeId: "NODE", visitSeq: 1, invokedAt: Date.now(),
  });
  return { store, db };
}

async function seedInvocation(dbPath) {
  const runStore = await createRunStore({ dbPath });
  const flowId = randomUUID();
  await runStore.insert({ flowId, status: "running" });
  const { db } = await getSharedDb(dbPath);
  const invocationId = randomUUID();
  await db.insert(flowNodeInvocations).values({
    id: invocationId, flowId, nodeId: "NODE", visitSeq: 1, invokedAt: Date.now(),
  });
  return invocationId;
}

function insertExecQuery(db, overrides = {}) {
  return db.insert(flowNodeExecutions).values({
    id: randomUUID(),
    invocationId: overrides.invocationId ?? fixtureInvocationId ?? randomUUID(),
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

    enqueue(dbPath, insertExecQuery(db, { id }), { flushIntervalMs: 60_000 });
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
    const invocationId = await seedInvocation(dbPath);

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
    const invocationA = await seedInvocation(dbPathA);
    const invocationB = await seedInvocation(dbPathB);

    enqueue(dbPathA, insertExecQuery(dbA, { id: idA, invocationId: invocationA }), { flushIntervalMs: 60_000 });
    enqueue(dbPathB, insertExecQuery(dbB, { id: idB, invocationId: invocationB }), { flushIntervalMs: 60_000 });

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
    const invocationId = await seedInvocation(dbPath);

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
    const invocationId = await seedInvocation(dbPath);

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
