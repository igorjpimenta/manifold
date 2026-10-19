// batch-writer.js — decouples "write happened" from "write persisted" for
// fire-and-forget SQLite writes, and coalesces many of them into one
// db.batch() round-trip (one transaction, one write-lock acquisition)
// instead of one round-trip per write.
//
// Why: state/*.js stores issue one INSERT/UPDATE per event (one per node
// dispatch, one per LLM call, ...) as its own immediate, individually-
// awaited statement. Under concurrent pipeline load that's N separate write-
// lock acquisitions competing against the same file's busy_timeout (db.js).
// Every call site this is wired into was ALREADY fire-and-forget by its own
// contract (errors swallowed, no caller inspects a return value or re-reads
// the row in the same call) — see each store's own "fire-and-forget" header
// comments. This module doesn't change that contract; it just changes N
// individual transactions into ceil(N / maxBatchSize) batched ones.
//
// Only enqueue a write here if it's genuinely fire-and-forget: nothing that
// reads the row back, and no caller inspecting a return value. Anything
// with a compare-and-swap / read-after-write dependency (run-store's
// casSet*, node-executions.enter()/writeResult(), failure-store
// .upsertFailureRow()) must NOT be routed through this — deferring those
// breaks the correctness they depend on.
//
// Ordering: one FIFO queue per dbPath. db.batch() executes its array in
// order, inside one transaction, so same-row writes queued in the same
// flush window still apply last-enqueued-wins, same as today's sequential
// awaits would.

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
    // call created the queue first — last one wins, matches how most of
    // this codebase's optional-opts-on-a-shared-resource pattern behaves
    // elsewhere (e.g. getSharedDb's cache doesn't re-validate opts either).
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
      // Two item shapes share one queue/timer per dbPath: un-awaited
      // Drizzle query builders (db.insert(table).values(...), the majority
      // — node-executions.js etc.) and raw {__raw, sql, args} statements
      // (enqueueRaw(), for stores like telemetry/store.js that never touch
      // Drizzle at all). They can't be mixed in one batch() call — Drizzle's
      // db.batch() only accepts its own BatchItem type, and the raw
      // client's batch() only accepts {sql, args}-shaped statements — so a
      // flush that happens to contain both partitions and sends two
      // round-trips instead of one. In practice a given dbPath's queue is
      // always homogeneous (each store only ever produces one item shape),
      // so this split costs nothing in the common case.
      const drizzleItems = batch.filter((item) => !(item && item.__raw));
      const rawItems = batch.filter((item) => item && item.__raw);
      // Each batch() call is already its own transaction — serialized and
      // retried on SQLITE_BUSY independently via withSharedWrite (state/
      // db.js), so a retry of one can never re-run the other's already-
      // committed statements, and this flush now serializes against every
      // other writer sharing this connection (run-store.js's CAS writes,
      // admission-queue-store.js's transactions), not just other flushes.
      if (drizzleItems.length > 0) await withSharedWrite(dbPath, () => db.batch(drizzleItems));
      if (rawItems.length > 0) await withSharedWrite(dbPath, () => client.batch(rawItems.map((item) => ({ sql: item.sql, args: item.args }))));
    } catch (err) {
      // Matches the swallow-on-error contract every batchable call site
      // already has (writes here were already fire-and-forget with errors
      // swallowed before batching existed) — a batch failure is logged, not
      // thrown, and the caller that originally enqueued each item never
      // sees it, same as before.
      q.logger?.warn?.(`batch-writer: flush failed for ${dbPath} (${batch.length} item(s) dropped): ${err?.message ?? err}`);
    }
  })();

  q.flushing = run;
  await run;
  if (q.flushing === run) q.flushing = null;
}

// enqueue(dbPath, query, opts?) — query is an UN-AWAITED Drizzle query
// builder (e.g. db.insert(table).values({...})). Do not await the query
// before passing it in — awaiting it executes it immediately instead of
// deferring. For raw client.execute()-shaped statements, use enqueueRaw()
// below instead — the two aren't interchangeable at flush time.
//
// opts: { maxBatchSize?, flushIntervalMs?, logger? } — only consulted the
// first time a dbPath's queue is created; later callers share the same
// queue/timer.
export function enqueue(dbPath, query, opts) {
  const q = _queueFor(dbPath, opts);
  q.items.push(query);

  if (q.items.length >= q.maxBatchSize) {
    // Size-triggered: flush now rather than waiting for the timer. Fire-
    // and-forget from the caller's perspective — enqueue() itself stays
    // synchronous so call sites don't need to await it (matching the
    // un-awaited flow_node_executions.write() call sites this was built for).
    void _flush(dbPath);
    return;
  }

  if (!q.timer) {
    q.timer = setTimeout(() => { void _flush(dbPath); }, q.flushIntervalMs);
    q.timer.unref?.();
  }
}

// enqueueRaw(dbPath, sql, args, opts?) — same contract as enqueue(), for
// stores that issue raw client.execute({sql, args}) calls instead of going
// through Drizzle (e.g. telemetry/store.js). Flushed via the raw client's
// own batch() method, not Drizzle's — see _flush()'s partitioning comment.
export function enqueueRaw(dbPath, sql, args, opts) {
  enqueue(dbPath, { __raw: true, sql, args }, opts);
}

// flush(dbPath) — force-drain one dbPath's queue now. Awaits any flush
// already in flight first, so a caller that enqueues then immediately
// flushes doesn't race an in-progress batch. For tests (assert-after-write)
// and the shutdown hook (drain before the gateway exits).
export async function flush(dbPath) {
  const q = _queues.get(dbPath);
  if (q?.flushing) await q.flushing;
  await _flush(dbPath);
}

// flushAll() — drain every dbPath's queue. Used by the shutdown service hook.
export async function flushAll() {
  await Promise.all([..._queues.keys()].map((dbPath) => flush(dbPath)));
}

// Exposed for tests only — clears in-memory queue state without flushing,
// so test suites using isolated per-test dbPaths don't leak queued items
// (or timers) into a later test that happens to reuse a temp path.
export function _resetForTests(dbPath) {
  const q = _queues.get(dbPath);
  if (q?.timer) clearTimeout(q.timer);
  _queues.delete(dbPath);
}
