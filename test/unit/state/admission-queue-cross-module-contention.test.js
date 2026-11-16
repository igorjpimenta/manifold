// Empirical test for a residual risk flagged during a post-migration audit
// of the flow-admission-queue rearchitecture: admission-queue-store.js was
// the ONLY module in this codebase that opened a `client.transaction(...)`
// object against runtime.sqlite. Its own internal write-queue serializer
// only protected against ITS OWN concurrent callers — it had zero
// awareness of, or coordination with, the OTHER modules sharing the exact
// same @libsql/client connection via state/db.js's getSharedDb() cache:
// state/run-store.js's CAS writes (single-shot db.update()) and state/
// batch-writer.js's periodic flush() (used by state/node-executions.js —
// single-shot db.batch()/client.batch()).
//
// Known @libsql/client hazards this serialization works around:
//   1. A plain client.execute()/db.update()/db.batch() issued on the SAME
//      connection while a transaction() (or a raw BEGIN/COMMIT) is open can
//      fail INSTANTLY, bypassing the busy_timeout PRAGMA entirely, and can
//      corrupt the transaction HOLDER's own commit ("cannot commit
//      transaction - SQL statements in progress").
//   2. Even across two SEPARATE connections to the same file (which does
//      avoid #1's corruption), multiple concurrent calls queued on ONE
//      client object each pay close to the FULL busy_timeout independently
//      rather than exiting as soon as the real, much shorter lock clears.
//
// Resolved for the writers this test exercises: admission-queue-store,
// run-store, batch-writer (covering node-executions' batched path), and
// failure-store now route their writes through state/db.js's
// withSharedWrite(dbPath, fn) — one in-process queue plus SQLITE_BUSY
// retry, keyed per dbPath rather than owned by any one store, so they all
// serialize against each other instead of racing on the same connection.
// node-health-store.js, session-index.js, discuss-store.js, and
// pipeline-registry.js also share this connection via getSharedDb() but
// are NOT yet migrated onto withSharedWrite — the same corruption risk
// this test checks for is still open between them and everything above
// until they are.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createAdmissionQueueStore } from "../../../state/admission-queue-store.js";
import { createRunStore } from "../../../state/run-store.js";
import { createNodeExecutionsStore } from "../../../state/node-executions.js";
import { flush as flushBatchWriter } from "../../../state/batch-writer.js";
import { createFailureStore } from "../../../state/failure-store.js";

function tmpDb(prefix) {
  return path.join(os.tmpdir(), `${prefix}-${randomUUID()}.sqlite`);
}

describe("admission-queue-store.js vs. other state/*.js writers — real cross-module contention on the SAME shared connection", () => {
  test("concurrent admissionQueueStore admit()/release(), runStore CAS writes, nodeExecutionsStore batched writes, and failureStore writes against the same dbPath never throw a transaction-corruption error, and admission counts stay exact", async () => {
    const dbPath = tmpDb("cross-module-contention");
    // Constructed and awaited-ready SEQUENTIALLY, not concurrently — boot
    // opens share one connection, and overlapping transactions on it fail
    // ("cannot start a transaction within a transaction") regardless of
    // which API opened them. Keeping boot sequential isolates what THIS
    // test checks, which is LIVE-OPERATION contention after all stores
    // are already up and their schemas exist.
    const admissionQueueStore = createAdmissionQueueStore({ dbPath, getLimit: () => 5 });
    await admissionQueueStore.ready();
    const runStore = createRunStore({ dbPath });
    await runStore.ready();
    const nodeExecutionsStore = createNodeExecutionsStore(dbPath);
    await nodeExecutionsStore.ready();
    // No explicit ready() on this store's Proxy — every method call already
    // awaits its own async init internally on first use. A harmless read
    // forces it to finish here, sequentially, before the concurrent round
    // below.
    const failureStore = createFailureStore({
      dbPath,
      nodeExecutionsStore: { getInvocation: () => null },
    });
    await failureStore.list({ limit: 1 });

    // Real flow_runs rows for runStore's CAS writes to have a genuine
    // target — an update against a nonexistent row would just return
    // {applied:false} harmlessly, which wouldn't exercise the write path
    // for real (no actual row-lock/UPDATE statement would need to run).
    const flowIds = Array.from({ length: 20 }, () => randomUUID());
    await Promise.all(flowIds.map((flowId) => runStore.insert({
      flowId, ownerKey: "test", pipelineId: "p", revision: 0, status: "running",
      lastStep: "__start__", stateJson: {},
    })));

    const errors = [];
    const CALLER_COUNT = 30;
    const ROUNDS = 5;

    for (let round = 0; round < ROUNDS; round++) {
      const ops = [];

      // Category 1: real admission-queue-store.js transactions — the ONLY
      // client.transaction() user in the codebase. Mix of admit + release
      // so both the read-then-INSERT path and the delete-then-promote path
      // (release()'s own internal SELECT + UPDATE inside the same
      // transaction) both fire concurrently.
      const admissions = [];
      for (let i = 0; i < CALLER_COUNT; i++) {
        ops.push(
          admissionQueueStore.admit({ kind: "child-dispatch", flowId: flowIds[i % flowIds.length], nodeId: `N-${round}-${i}` })
            .then((r) => { admissions.push(r); })
            .catch((err) => errors.push(`admit: ${err?.message ?? err}`)),
        );
      }

      // Category 2: real run-store.js single-shot CAS writes (db.update(),
      // no held-open transaction) against real rows, fired at the exact
      // same time as category 1's open transactions.
      for (let i = 0; i < CALLER_COUNT; i++) {
        const flowId = flowIds[i % flowIds.length];
        ops.push(
          runStore.casSetRunning(flowId, { expectedRevision: 0, lastStep: `STEP_${round}_${i}`, stateJson: {} })
            .catch((err) => errors.push(`casSetRunning: ${err?.message ?? err}`)),
        );
      }

      // Category 3: real node-executions.js writes — routed through
      // batch-writer.js's enqueue()/db.batch(), the other real consumer of
      // this shared connection. Deliberately not awaited individually
      // (matches every real call site's own fire-and-forget contract) —
      // collected via the returned promise-less call, then force-flushed
      // below to guarantee the db.batch() round-trip actually overlaps
      // this round's open admission-queue-store transactions instead of
      // happening to land in some later, uncontended window.
      for (let i = 0; i < CALLER_COUNT; i++) {
        ops.push(
          nodeExecutionsStore.write({
            flowId: flowIds[i % flowIds.length], nodeId: `N${i}`, kind: "fresh",
            enteredAt: Date.now(), exitedAt: Date.now(),
          }).catch((err) => errors.push(`nodeExecutions.write: ${err?.message ?? err}`)),
        );
      }
      // Force the batch-writer flush to actually run NOW, concurrently
      // with the still-in-flight admit()/casSetRunning calls above, rather
      // than waiting for its own flush timer (which could land after this
      // round's transactions have all already committed, silently
      // defeating the point of this test).
      ops.push(flushBatchWriter(dbPath).catch((err) => errors.push(`flushBatchWriter: ${err?.message ?? err}`)));

      // Category 4: real failure-store.js writes (recordEngineFailure ->
      // upsertFailureRow) mixed into the same contention round — a burst
      // of these must not corrupt an unrelated admission-queue-store
      // release() transaction.
      for (let i = 0; i < CALLER_COUNT; i++) {
        ops.push(
          failureStore.recordEngineFailure({
            flowId: flowIds[i % flowIds.length], pipelineId: "p",
            failureNode: `N${i}`, message: `contention failure ${round}-${i}`,
          }).catch((err) => errors.push(`recordEngineFailure: ${err?.message ?? err}`)),
        );
      }

      await Promise.all(ops);

      // Release everything admitted this round so the pool doesn't fill up
      // across rounds and start masking real admit() behavior as "queued"
      // for a reason unrelated to what this test is actually checking.
      await Promise.all(admissions.map((a) => admissionQueueStore.release(a.id).catch((err) => errors.push(`release: ${err?.message ?? err}`))));
    }

    assert.deepEqual(errors, [], "no cross-module transaction-corruption error (e.g. 'cannot commit transaction - SQL statements in progress') across 5 rounds of 30-way concurrent admission-queue-store + run-store + node-executions + failure-store writes on the same connection");

    // Sanity: the pool is empty again (every admission from every round was
    // released) — confirms admission-queue-store.js's own bookkeeping
    // stayed internally consistent throughout, not just error-free.
    assert.deepEqual(await admissionQueueStore._stats(), { admitted: 0, queued: 0 });
  });
});
