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
import { getSharedDb } from "../../../state/db.js";

function tmpDb() {
  return path.join(os.tmpdir(), `admission-queue-test-${randomUUID()}.sqlite`);
}

function makeStore(limit, overrides = {}) {
  return createAdmissionQueueStore({ dbPath: tmpDb(), getLimit: () => limit, ...overrides });
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
    const store = createAdmissionQueueStore({ dbPath: tmpDb(), getLimit: () => limit });
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

