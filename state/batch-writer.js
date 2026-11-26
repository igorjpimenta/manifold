// batch-writer.js — decouples "write happened" from "write persisted" for
// fire-and-forget SQLite writes, coalescing many of them into one
// db.batch() round-trip (one transaction, one write-lock acquisition).
//
// Only enqueue a write here if it's genuinely fire-and-forget: nothing
// that reads the row back, and no caller inspecting a return value.
// Anything with a compare-and-swap / read-after-write dependency
// (run-store's casSet*, node-executions.enter()/writeResult(),
// failure-store.upsertFailureRow()) must NOT be routed through this —
// deferring those breaks the correctness they depend on.
//
// Ordering: one FIFO queue per dbPath. db.batch() executes its array in
// order, inside one transaction, so same-row writes queued in the same
// flush window apply last-enqueued-wins, same as sequential awaits would.

import { getSharedDb, withSharedWrite } from "./db.js";

const DEFAULT_MAX_BATCH_SIZE = 50;
const DEFAULT_FLUSH_INTERVAL_MS = 200;

const _queues = new Map(); // dbPath -> { items: Array<query>, timer, maxBatchSize, flushIntervalMs, logger, flushing: Promise|null }

function _queueFor(dbPath, opts) {
  let q = _queues.get(dbPath);
  if (!q) {
    q = {
      items: [],
      timer: null,
      maxBatchSize: opts?.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE,
      flushIntervalMs: opts?.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      logger: opts?.logger ?? null,
      flushing: null,
    };
    _queues.set(dbPath, q);
  } else if (opts) {
    // Later enqueue() calls can supply a logger/tuning even if an earlier
    // call created the queue first — last one wins.
    if (opts.logger) q.logger = opts.logger;
  }
  return q;
}

async function _flush(dbPath) {
  const q = _queues.get(dbPath);
  if (!q || q.items.length === 0) return;
  if (q.timer) { clearTimeout(q.timer); q.timer = null; }

  const batch = q.items;
  q.items = [];

  const run = (async () => {
    try {
      const { db, client } = await getSharedDb(dbPath);
      // Two item shapes share one queue: un-awaited Drizzle query builders
      // and raw {__raw, sql, args} statements (enqueueRaw). They can't be
      // mixed in one batch() call — Drizzle's db.batch() only accepts its
      // own BatchItem type, the raw client's only {sql, args} — so a
      // mixed flush sends two round-trips instead of one. In practice a
      // given dbPath's queue is homogeneous.
      const drizzleItems = batch.filter((item) => !(item && item.__raw));
      const rawItems = batch.filter((item) => item && item.__raw);
      // Each batch() call is its own transaction — serialized and retried
      // on SQLITE_BUSY via withSharedWrite, so this flush also serializes
      // against every other writer sharing the connection (CAS writes,
      // admission-queue transactions), not just other flushes.
      if (drizzleItems.length > 0) await withSharedWrite(dbPath, () => db.batch(drizzleItems));
      if (rawItems.length > 0) await withSharedWrite(dbPath, () => client.batch(rawItems.map((item) => ({ sql: item.sql, args: item.args }))));
    } catch (err) {
      // Swallow-on-error matches every batchable call site: a batch
      // failure is logged, never thrown to the caller that enqueued.
      q.logger?.warn?.(`batch-writer: flush failed for ${dbPath} (${batch.length} item(s) dropped): ${err?.message ?? err}`);
    }
  })();

  q.flushing = run;
  await run;
  if (q.flushing === run) q.flushing = null;
}

// enqueue(dbPath, query, opts?) — query is an UN-AWAITED Drizzle query
// builder. Do not await it before passing it in — awaiting executes it
// immediately instead of deferring. For raw client.execute()-shaped
// statements use enqueueRaw() — the two aren't interchangeable at flush
// time. opts are only consulted the first time a dbPath's queue is
// created.
export function enqueue(dbPath, query, opts) {
  const q = _queueFor(dbPath, opts);
  q.items.push(query);

  if (q.items.length >= q.maxBatchSize) {
    // Size-triggered flush; enqueue() itself stays synchronous so call
    // sites don't need to await it.
    void _flush(dbPath);
    return;
  }

  if (!q.timer) {
    q.timer = setTimeout(() => { void _flush(dbPath); }, q.flushIntervalMs);
    q.timer.unref?.();
  }
}

// Same contract as enqueue(), for raw {sql, args} statements instead of
// Drizzle builders — flushed via the raw client's own batch().
export function enqueueRaw(dbPath, sql, args, opts) {
  enqueue(dbPath, { __raw: true, sql, args }, opts);
}

// Force-drain one dbPath's queue now. Awaits any flush already in flight
// first, so enqueue-then-immediately-flush doesn't race an in-progress
// batch. For tests (assert-after-write) and the shutdown hook.
export async function flush(dbPath) {
  const q = _queues.get(dbPath);
  if (q?.flushing) await q.flushing;
  await _flush(dbPath);
}

export async function flushAll() {
  await Promise.all([..._queues.keys()].map((dbPath) => flush(dbPath)));
}

// Clears in-memory queue state without flushing, so test suites using
// isolated per-test dbPaths don't leak queued items (or timers) into a
// later test that happens to reuse a temp path.
export function _resetForTests(dbPath) {
  const q = _queues.get(dbPath);
  if (q?.timer) clearTimeout(q.timer);
  _queues.delete(dbPath);
}
