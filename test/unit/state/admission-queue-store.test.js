// Unit tests for state/admission-queue-store.js — the single durable
// admission table for flow runs (kind:"child-dispatch", kind:"flow-start").
//
// Real tmp-sqlite instances throughout (same convention as run-store.test.js),
// not mocks — the whole point of this store is a genuine atomicity
// guarantee under real concurrent access, which a mock can't verify.

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
import { getSharedDb } from "../../../state/db.js";

function tmpDb() {
  return path.join(os.tmpdir(), `admission-queue-test-${randomUUID()}.sqlite`);
}

function makeStore(limit, overrides = {}) {
  const dbPath = tmpDb();
  const store = createAdmissionQueueStore({ dbPath, getLimit: () => limit, ...overrides });
  const runStore = createRunStore({ dbPath });
  const admit = store.admit.bind(store);
  return new Proxy(store, {
    get(target, key) {
      if (key === "admit") {
        return async (args) => {
          const runs = await runStore;
          await runs.insert({ flowId: args.flowId, status: "running" });
          return admit(args);
        };
      }
      const value = target[key];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

describe("admission-queue-store — factory", () => {
  test("throws synchronously-visible-on-first-call when getLimit is not a function", async () => {
    const store = createAdmissionQueueStore({ dbPath: tmpDb() });
    await assert.rejects(
      () => store.admit({ kind: "flow-start", flowId: "f1" }),
      /getLimit\(\) function is required/,
    );
  });
});

describe("admission-queue-store — admit()", () => {
  test("admits up to the limit, queues the rest", async () => {
    const store = makeStore(2);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    const b = await store.admit({ kind: "flow-start", flowId: "f2" });
    const c = await store.admit({ kind: "flow-start", flowId: "f3" });
    assert.equal(a.status, "admitted");
    assert.equal(b.status, "admitted");
    assert.equal(c.status, "queued");
    assert.deepEqual(await store._stats(), { admitted: 2, queued: 1 });
  });

  test("the combined pool is shared across kinds — not one budget per kind", async () => {
    const store = makeStore(2);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    const b = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N" });
    const c = await store.admit({ kind: "child-dispatch", flowId: "f2", nodeId: "C" });
    assert.equal(a.status, "admitted");
    assert.equal(b.status, "admitted");
    assert.equal(c.status, "queued", "third entry queues regardless of kind — one shared pool, not three");
  });

  test("throws on missing kind or flowId", async () => {
    const store = makeStore(2);
    await assert.rejects(() => store.admit({ flowId: "f1" }), /kind is required/);
    await assert.rejects(() => store.admit({ kind: "flow-start" }), /flowId is required/);
  });

  test("getLimit is re-read on every call, not cached at store creation", async () => {
    let limit = 1;
    const store = makeStore(0, { getLimit: () => limit });
    // getLimit passed via override replaces makeStore's fixed-limit closure.
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    assert.equal(a.status, "admitted");
    const b = await store.admit({ kind: "flow-start", flowId: "f2" });
    assert.equal(b.status, "queued", "limit=1, second entry queues");
    limit = 5;
    const c = await store.admit({ kind: "flow-start", flowId: "f3" });
    assert.equal(c.status, "admitted", "raised limit takes effect on the very next admit(), no restart needed");
  });

  test("each admit() call gets its own distinct id, even for the same flowId", async () => {
    const store = makeStore(5);
    const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
    const b = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N2" });
    assert.notEqual(a.id, b.id, "a flow legitimately cycles through admit()/release() many times over its life");
  });

  // Idempotency: a caller re-entering the same logical request (e.g. a
  // resume racing a revival for the same flow) must not create a second
  // live row — that would double-admit and permanently shrink the shared
  // pool by one slot.
  describe("idempotency — re-admitting the same (kind, flowId, nodeId) while a live row exists", () => {
    test("a repeated admit() for the same admitted tuple returns the SAME row, not a new one", async () => {
      const store = makeStore(5);
      const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      assert.equal(a.status, "admitted");
      const b = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      assert.deepEqual(b, a, "same id and status — no second row created");
      assert.deepEqual(await store._stats(), { admitted: 1, queued: 0 });
    });

    test("a repeated admit() for the same still-queued tuple returns the SAME row, not a new one", async () => {
      const store = makeStore(0); // pool full immediately — everything queues
      const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      assert.equal(a.status, "queued");
      const b = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      assert.deepEqual(b, a);
      assert.deepEqual(await store._stats(), { admitted: 0, queued: 1 });
    });

    test("does not dedupe across DIFFERENT flowIds or nodeIds — only the exact tuple", async () => {
      const store = makeStore(5);
      const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      const b = await store.admit({ kind: "flow-start", flowId: "f2", nodeId: "N1" });
      const c = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N2" });
      const d = await store.admit({ kind: "child-dispatch", flowId: "f1", nodeId: "N1" });
      assert.equal(new Set([a.id, b.id, c.id, d.id]).size, 4, "different flowId, nodeId, or kind are all genuinely distinct requests");
    });

    test("null nodeId (flow-start/child-dispatch) dedupes correctly against another null, not against a real nodeId", async () => {
      const store = makeStore(5);
      const a = await store.admit({ kind: "flow-start", flowId: "f1" }); // nodeId defaults to null
      const b = await store.admit({ kind: "flow-start", flowId: "f1" });
      assert.deepEqual(b, a, "two admit() calls with no nodeId for the same flow+kind must dedupe");
      const c = await store.admit({ kind: "child-dispatch", flowId: "f1", nodeId: "REAL_NODE" });
      assert.notEqual(c.id, a.id, "a real nodeId must never match a null one");
    });

    test("once the original row is released, a fresh admit() for the same tuple creates a genuinely NEW row", async () => {
      const store = makeStore(5);
      const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      await store.release(a.id);
      const b = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N1" });
      assert.notEqual(b.id, a.id, "no live row remains, so this is a legitimate new request, not a duplicate");
      assert.equal(b.status, "admitted");
    });
  });
});

describe("admission-queue-store — release()", () => {
  test("releasing an admitted entry with nothing queued just removes it", async () => {
    const store = makeStore(1);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    const result = await store.release(a.id);
    assert.equal(result.released, true);
    assert.equal(result.promoted, null);
    assert.deepEqual(await store._stats(), { admitted: 0, queued: 0 });
  });

  test("releasing an admitted entry promotes the oldest queued entry", async () => {
    const store = makeStore(1);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    const b = await store.admit({ kind: "flow-start", flowId: "f2", nodeId: "N" });
    assert.equal(a.status, "admitted");
    assert.equal(b.status, "queued");

    const result = await store.release(a.id);
    assert.equal(result.released, true);
    assert.equal(result.promoted.id, b.id);
    assert.equal(result.promoted.kind, "flow-start");
    assert.equal(result.promoted.flowId, "f2");
    assert.equal(result.promoted.nodeId, "N");

    const row = await store.get(b.id);
    assert.equal(row.status, "admitted", "the promoted row's own status is actually flipped, not just returned");
    assert.ok(row.admittedAt, "admittedAt is stamped on promotion");
  });

  test("promotion is FIFO across the whole pool, not scoped to the releasing entry's own kind", async () => {
    const store = makeStore(1);
    const held = await store.admit({ kind: "flow-start", flowId: "holder" });
    const first = await store.admit({ kind: "child-dispatch", flowId: "f1", nodeId: "C1" });
    await new Promise((r) => setTimeout(r, 5)); // ensure a distinct created_at ordering
    const second = await store.admit({ kind: "flow-start", flowId: "f2", nodeId: "N2" });
    assert.equal(first.status, "queued");
    assert.equal(second.status, "queued");

    const result = await store.release(held.id);
    assert.equal(result.promoted.id, first.id, "oldest queued entry wins, regardless of its kind differing from the released entry's kind");
  });

  test("releasing a queued (never-admitted) entry does not trigger a promotion", async () => {
    const store = makeStore(1);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    const b = await store.admit({ kind: "flow-start", flowId: "f2" });
    assert.equal(b.status, "queued");

    const result = await store.release(b.id);
    assert.equal(result.released, true);
    assert.equal(result.promoted, null, "removing a queued entry frees nothing — only releasing an ADMITTED entry can promote");
    assert.deepEqual(await store._stats(), { admitted: 1, queued: 0 });
  });

  test("releasing a nonexistent id is a safe no-op, never throws", async () => {
    const store = makeStore(1);
    const result = await store.release(randomUUID());
    assert.deepEqual(result, { released: false, promoted: null });
  });

  test("releasing the same id twice — the second call is a safe no-op", async () => {
    const store = makeStore(1);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    const first = await store.release(a.id);
    const second = await store.release(a.id);
    assert.equal(first.released, true);
    assert.equal(second.released, false);
  });

  test("throws on missing id", async () => {
    const store = makeStore(1);
    await assert.rejects(() => store.release(), /id is required/);
  });
});

describe("admission-queue-store — releaseByFlow()", () => {
  test("releases the live admitted entry for (flowId, kind), promoting the oldest queued entry", async () => {
    const store = makeStore(1);
    const a = await store.admit({ kind: "child-dispatch", flowId: "child-1", nodeId: "SCORE" });
    const b = await store.admit({ kind: "flow-start", flowId: "f2", nodeId: "N" });
    assert.equal(a.status, "admitted");
    assert.equal(b.status, "queued");

    const result = await store.releaseByFlow({ flowId: "child-1", kind: "child-dispatch" });
    assert.equal(result.released, true);
    assert.equal(result.promoted.id, b.id);
    assert.equal((await store.get(a.id)), null, "the released row itself is gone, not just marked");
    assert.equal((await store.get(b.id)).status, "admitted");
  });

  test("is a safe no-op for a flowId/kind pair that never held an entry — the common case for ordinary flows", async () => {
    const store = makeStore(1);
    const result = await store.releaseByFlow({ flowId: "never-admitted", kind: "child-dispatch" });
    assert.deepEqual(result, { released: false, promoted: null });
  });

  test("does not match a live entry under a DIFFERENT kind for the same flowId", async () => {
    const store = makeStore(2);
    const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N" });
    const result = await store.releaseByFlow({ flowId: "f1", kind: "child-dispatch" });
    assert.deepEqual(result, { released: false, promoted: null });
    assert.equal((await store.get(a.id)).status, "admitted", "the flow-start entry must be untouched");
  });

  test("releasing a queued (not yet admitted) entry does not promote anything", async () => {
    const store = makeStore(1);
    await store.admit({ kind: "flow-start", flowId: "holder" });
    const b = await store.admit({ kind: "child-dispatch", flowId: "child-1", nodeId: "SCORE" });
    assert.equal(b.status, "queued");

    const result = await store.releaseByFlow({ flowId: "child-1", kind: "child-dispatch" });
    assert.equal(result.released, true);
    assert.equal(result.promoted, null);
  });

  test("throws on missing flowId or kind", async () => {
    const store = makeStore(1);
    await assert.rejects(() => store.releaseByFlow({ kind: "child-dispatch" }), /flowId is required/);
    await assert.rejects(() => store.releaseByFlow({ flowId: "f1" }), /kind is required/);
  });
});

describe("admission-queue-store — get() / listByStatus()", () => {
  test("get() returns null for an unknown id", async () => {
    const store = makeStore(1);
    assert.equal(await store.get(randomUUID()), null);
  });

  test("get() returns the full row shape", async () => {
    const store = makeStore(1);
    const a = await store.admit({ kind: "flow-start", flowId: "f1", nodeId: "N" });
    const row = await store.get(a.id);
    assert.equal(row.kind, "flow-start");
    assert.equal(row.flowId, "f1");
    assert.equal(row.nodeId, "N");
    assert.equal(row.status, "admitted");
    assert.equal(typeof row.createdAt, "number");
    assert.equal(typeof row.admittedAt, "number");
  });

  test("listByStatus('queued') lists oldest-first", async () => {
    const store = makeStore(0);
    const a = await store.admit({ kind: "flow-start", flowId: "f1" });
    await new Promise((r) => setTimeout(r, 5));
    const b = await store.admit({ kind: "flow-start", flowId: "f2" });
    assert.equal(a.status, "queued");
    assert.equal(b.status, "queued");

    const list = await store.listByStatus("queued");
    assert.equal(list.length, 2);
    assert.equal(list[0].id, a.id);
    assert.equal(list[1].id, b.id);
  });

  test("listByStatus('admitted') is empty when limit is 0", async () => {
    const store = makeStore(0);
    await store.admit({ kind: "flow-start", flowId: "f1" });
    assert.deepEqual(await store.listByStatus("admitted"), []);
  });
});

// ── the test that actually proves the race is closed ────────────────────────
//
// Fires many real, genuinely concurrent admit() calls (Promise.all, not
// sequential awaits) against one real sqlite file, with a limit well below
// the caller count. If admit()'s count-then-claim weren't atomic, this
// would overshoot the limit under real concurrent load — the same way any
// count-then-claim check that doesn't happen inside admit()'s own
// transaction would.

describe("admission-queue-store — concurrent admit() under real contention", () => {
  test("N concurrent callers, limit L (L < N): exactly L admitted, the rest queued, never an overshoot", async () => {
    const store = makeStore(5);
    const CALLER_COUNT = 30;
    const results = await Promise.all(
      Array.from({ length: CALLER_COUNT }, (_, i) =>
        store.admit({ kind: "flow-start", flowId: `f${i}`, nodeId: "N" }))
    );
    const admitted = results.filter((r) => r.status === "admitted");
    const queued = results.filter((r) => r.status === "queued");
    assert.equal(admitted.length, 5, "exactly the limit, no overshoot despite 30 truly concurrent callers");
    assert.equal(queued.length, 25);
    const stats = await store._stats();
    assert.deepEqual(stats, { admitted: 5, queued: 25 }, "the DB's own count agrees with what admit() reported to each caller");
  });

  test("concurrent admit() + release() interleaved: pool size is never exceeded at any observed point", async () => {
    const store = makeStore(3);
    const ids = [];
    // Fill to capacity first (sequential, to get real ids to release below).
    for (let i = 0; i < 3; i++) {
      const r = await store.admit({ kind: "flow-start", flowId: `seed${i}` });
      assert.equal(r.status, "admitted");
      ids.push(r.id);
    }
    // Now fire a burst of concurrent releases (freeing slots) and concurrent
    // new admits (claiming them) at the same time — the contended case that
    // actually matters: a slot freeing up while multiple new requests race
    // for it.
    const [releaseResults, admitResults] = await Promise.all([
      Promise.all(ids.map((id) => store.release(id))),
      Promise.all(Array.from({ length: 6 }, (_, i) => store.admit({ kind: "flow-start", flowId: `new${i}`, nodeId: "N" }))),
    ]);
    assert.equal(releaseResults.filter((r) => r.released).length, 3);
    const stats = await store._stats();
    assert.equal(stats.admitted, 3, "pool never exceeds its limit even mid-churn — 3 freed, exactly 3 of the 6 new admits claimed them");
    assert.equal(stats.queued, 3);
    void admitResults; // shape already covered by the aggregate stats assertion above
  });
});

describe("admission-queue-store — fillFreeSlots()", () => {
  test("promotes oldest queued rows into free capacity, up to the limit, and returns them", async () => {
    let limit = 1;
    const dbPath = tmpDb();
    const store = createAdmissionQueueStore({ dbPath, getLimit: () => limit });
    const runs = await createRunStore({ dbPath });
    for (const flowId of ["held", "q1", "q2", "q3"]) {
      await runs.insert({ flowId, status: "running" });
    }
    await store.admit({ kind: "flow-start", flowId: "held" });
    const q1 = await store.admit({ kind: "flow-start", flowId: "q1" });
    await new Promise((r) => setTimeout(r, 5));
    const q2 = await store.admit({ kind: "child-dispatch", flowId: "q2", nodeId: "N" });
    await new Promise((r) => setTimeout(r, 5));
    const q3 = await store.admit({ kind: "flow-start", flowId: "q3" });
    assert.deepEqual(await store._stats(), { admitted: 1, queued: 3 });

    limit = 3; // two slots free, three rows queued
    const promoted = await store.fillFreeSlots();

    assert.deepEqual(promoted.map((p) => p.id), [q1.id, q2.id], "oldest first, only as many as are free");
    assert.deepEqual(promoted.map((p) => p.flowId), ["q1", "q2"]);
    assert.equal(promoted[1].kind, "child-dispatch");
    assert.equal((await store.get(q3.id)).status, "queued");
    assert.deepEqual(await store._stats(), { admitted: 3, queued: 1 });
  });

  test("is a no-op when the pool is already full or nothing is queued", async () => {
    const store = makeStore(1);
    await store.admit({ kind: "flow-start", flowId: "a" });
    await store.admit({ kind: "flow-start", flowId: "b" });
    assert.deepEqual(await store.fillFreeSlots(), [], "full pool");

    const empty = makeStore(4);
    await empty.admit({ kind: "flow-start", flowId: "only" });
    assert.deepEqual(await empty.fillFreeSlots(), [], "nothing queued");
  });
});

// ── cross-module contention on the shared connection ────────────────────────
//
// Two hazards the write serialization has to work around:
//   1. A plain execute()/update()/batch() issued on a connection while a
//      transaction() (or a raw BEGIN/COMMIT) is open fails instantly,
//      bypassing busy_timeout, and can corrupt the holder's own commit.
//   2. Across separate connections to the same file, concurrent calls queued
//      on one client each pay close to the full busy_timeout instead of
//      exiting as soon as the real lock clears.
// admission-queue-store, run-store, batch-writer (node-executions' path) and
// failure-store serialize through state/db.js's withSharedWrite. The other
// getSharedDb() users — node-health-store, session-index, discuss-store,
// pipeline-registry — are not on it yet.

describe("admission-queue-store.js vs. other state/*.js writers — real cross-module contention on the SAME shared connection", () => {
  test("concurrent admissionQueueStore admit()/release(), runStore CAS writes, nodeExecutionsStore batched writes, and failureStore writes against the same dbPath never throw a transaction-corruption error, and admission counts stay exact", async () => {
    const dbPath = tmpDb();
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
