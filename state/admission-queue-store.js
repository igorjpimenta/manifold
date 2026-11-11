// admission-queue-store.js — the single durable admission table for flow
// RUNS: starting a flow (kind:"flow-start") and dispatching a subflow child
// (kind:"child-dispatch"). One row per run; nothing inside a running flow
// (agent nodes included) takes a slot of its own — a run executes one node
// at a time, so the run cap already bounds everything it does.
//
// ONE combined pool, not one per kind — deliberately. Both kinds compete
// for the same shared count (no `kind` filter on the admitted-count query
// or the promotion query below). This only works because admission tracks
// flow_runs.status, not just start/end: a flow-run's own admission entry
// (whichever kind spawned it) is held only while it's genuinely "running" —
// the moment it goes "blocked" (a human-gate wait, or its own
// children-pending wait while a subflow fans out), state-store.js's
// setWaiting releases that entry via releaseByFlow(), so it stops occupying
// a shared slot for however long it's actually idle. Without that, a parent
// would hold its slot for its ENTIRE lifetime while its children ALSO need
// slots — one fan-out could alone exhaust the whole pool.
//
// child-dispatch releases on blocked/terminal but deliberately
// does NOT re-request admission when a blocked child later resumes — the
// mechanism it replaced never re-checked the cap at resume time either
// (the cap only ever governed fan-out DISPATCH, not a continuously-
// enforced running ceiling), so this is exact behavioral parity, not a new
// gap. Whether flow-start (a whole run's own admission, not yet wired)
// needs real re-admission-on-resume is still open — later stage's call.
//
// Atomicity: every decision (count admitted, then claim admitted-or-queued)
// runs inside one client.transaction("write") — BEGIN IMMEDIATE, acquiring
// SQLite's writer lock before the SELECT even runs. A bare
// `await count-query` then, much later, a separate write is two non-atomic
// steps, racy across concurrent callers; this is the primitive that closes
// the window.

import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

// Same Proxy-wrapped async-ready factory idiom as run-store.js/state-
// store.js — every method call transparently awaits store construction
// (migration included) before running.
export function createAdmissionQueueStore(opts = {}) {
  const _p = _createImpl(opts);
  return new Proxy({}, {
    get(_, k) {
      if (k === "then" || typeof k === "symbol") return undefined;
      if (k === "ready") return () => _p.then(() => {});
      return (...args) => _p.then((s) => {
        if (typeof s[k] !== "function") throw new TypeError(`admission-queue-store: ${String(k)} is not a function`);
        return s[k](...args);
      });
    },
  });
}

async function _createImpl({ dbPath = defaultDbPath(), getLimit } = {}) {
  if (typeof getLimit !== "function") {
    throw new TypeError("admission-queue-store: getLimit() function is required (e.g. global-concurrency.js's getGlobalConcurrencyLimit)");
  }
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { client } = await getSharedDb(dbPath);

  // Runs `fn(tx)` inside one BEGIN IMMEDIATE transaction, committing on
  // success and always closing (which rolls back if not committed) — the
  // exact try/finally shape @libsql/client's own docs specify as required.
  // Serialized and retried on SQLITE_BUSY via state/db.js's
  // withSharedWrite(), keyed by dbPath: @libsql/client's local sqlite3
  // backend does NOT safely support multiple Transaction objects in flight
  // concurrently on one shared connection — commit() itself fails with
  // "cannot commit transaction - SQL statements in
  // progress", consistent with concurrent Transaction objects interleaving
  // execute() calls over the same underlying native connection and
  // corrupting each other's in-flight statement state; a client-library
  // limitation, not a SQLite-level one — BEGIN IMMEDIATE itself would have
  // serialized this correctly at the file-lock level. Corroborated by this
  // store's own test suite, which drives 30 genuinely concurrent admit()
  // callers through it.
  //
  // Because withSharedWrite's queue is keyed by dbPath rather than private
  // to this store, every OTHER writer sharing this same connection via
  // db.js's getSharedDb (run-store.js's CAS writes, batch-writer.js's
  // flushes for node-executions.js/node-health-store.js) serializes against
  // this store's transactions too, once it also calls withSharedWrite for
  // the same dbPath — closing the corruption/stall path this store's own
  // private serializer could not (see test/unit/
  // state/admission-queue-cross-module-contention.test.js): a plain
  // client.execute()/db.update()/db.batch() issued on the same connection
  // while a transaction() is open can fail instantly, bypassing the
  // busy_timeout PRAGMA entirely, and can corrupt the transaction holder's
  // own commit. Until every writer on this connection routes through the
  // same primitive, that gap remains open for whichever writers haven't
  // adopted it yet.
  //
  // Safe to retry the whole fn(tx) on a fresh transaction: an uncommitted
  // transaction has zero durable effect (SQLite's own atomicity), so
  // re-running every one of fn's statements against a new tx is not a
  // partial-redo risk.
  async function withWriteTx(fn) {
    return withSharedWrite(dbPath, async () => {
      const tx = await client.transaction("write");
      try {
        const result = await fn(tx);
        await tx.commit();
        return result;
      } finally {
        tx.close();
      }
    });
  }

  // Shared by release(id) and releaseByFlow() — deletes the row and, if it
  // was occupying an admitted slot, promotes the oldest queued entry across
  // the whole combined pool. Must run inside the caller's own withWriteTx
  // callback (not its own transaction) so both release paths stay a single
  // atomic decision.
  async function releaseRowById(tx, id) {
    const { rows: selfRows } = await tx.execute({
      sql: `SELECT status FROM flow_admission_queue WHERE id = ?`,
      args: [id],
    });
    const self = selfRows[0] ?? null;
    if (!self) return { released: false, promoted: null };

    await tx.execute({ sql: `DELETE FROM flow_admission_queue WHERE id = ?`, args: [id] });

    let promoted = null;
    if (self.status === "admitted") {
      const { rows: nextRows } = await tx.execute({
        sql: `SELECT id, kind, flow_id, node_id FROM flow_admission_queue
              WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1`,
        args: [],
      });
      const next = nextRows[0] ?? null;
      if (next) {
        const now = Date.now();
        await tx.execute({
          sql: `UPDATE flow_admission_queue SET status = 'admitted', admitted_at = ? WHERE id = ?`,
          args: [now, next.id],
        });
        promoted = { id: next.id, kind: next.kind, flowId: next.flow_id, nodeId: next.node_id };
      }
    }
    return { released: true, promoted };
  }

  return {
    // Decides admitted-vs-queued for one new entry, atomically against the
    // CURRENT combined count of admitted rows (all kinds pooled — see this
    // file's header). Returns { id, status }. Never throws on contention —
    // "queued" is a normal, expected outcome, not an error.
    //
    // Idempotent per (kind, flowId, nodeId): if a live (admitted or queued)
    // row already exists for this exact tuple, returns THAT row instead of
    // inserting a second one. Without this, any caller that re-enters the
    // same logical request (e.g. a resume racing a revival for the same
    // flow) creates a genuine duplicate row, permanently shrinking the
    // shared pool by one slot per occurrence.
    //
    // A queued caller is never woken in-process: when a slot frees,
    // release() promotes the row and the releasing call site drives the
    // flow forward through wakePromotedRelease (engine/admission-promotion-
    // resume.js) — flip to 'running', then resume.
    async admit({ kind, flowId, nodeId = null }) {
      if (!kind) throw new TypeError("admission-queue-store.admit: kind is required");
      if (!flowId) throw new TypeError("admission-queue-store.admit: flowId is required");
      const limit = getLimit();
      return withWriteTx(async (tx) => {
        const { rows: existingRows } = await tx.execute({
          sql: `SELECT id, status FROM flow_admission_queue
                WHERE kind = ? AND flow_id = ? AND (node_id = ? OR (? IS NULL AND node_id IS NULL))
                  AND status IN ('admitted', 'queued')
                LIMIT 1`,
          args: [kind, flowId, nodeId, nodeId],
        });
        if (existingRows[0]) {
          const { id, status } = existingRows[0];
          return { id, status };
        }
        const { rows } = await tx.execute({
          sql: `SELECT COUNT(*) AS n FROM flow_admission_queue WHERE status = 'admitted'`,
          args: [],
        });
        const admittedCount = Number(rows[0]?.n ?? 0);
        const id = randomUUID();
        const now = Date.now();
        const status = admittedCount < limit ? "admitted" : "queued";
        await tx.execute({
          sql: `INSERT INTO flow_admission_queue (id, kind, flow_id, node_id, status, created_at, admitted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)`,
          args: [id, kind, flowId, nodeId, status, now, status === "admitted" ? now : null],
        });
        return { id, status };
      });
    },

    // Releases one entry by id. If it was occupying an admitted slot,
    // atomically promotes the oldest queued entry — FIFO across the WHOLE
    // combined pool, not scoped to the releasing entry's own kind, matching
    // the one-pool design. Returns { released, promoted }; promoted is
    // either null or { id, flowId, nodeId, kind } for the caller to wake
    // via wakePromotedRelease. A release() for an id that doesn't exist
    // (already released, or never existed) is a safe no-op —
    // { released: false, promoted: null } — never throws, since a caller
    // racing its own cleanup against a process restart shouldn't have to
    // special-case "already gone".
    async release(id) {
      if (!id) throw new TypeError("admission-queue-store.release: id is required");
      return withWriteTx(async (tx) => releaseRowById(tx, id));
    },

    // Releases the CURRENT (admitted or queued) entry for a given
    // (flowId, kind) pair, without the caller needing to carry its own
    // admission id around — the shape a status-store hook wants: state-
    // store.js's setWaiting/finish/skip/fail/cancel know the flowId that
    // just left "running" (or went terminal), not any admission id, since
    // the flow could have crashed and been revived by a different process
    // between admit() and now. Same promotion/notify semantics as
    // release(id); a no-op ({ released: false, promoted: null }) when this
    // flow never held a live entry for this kind (true for every ordinary
    // flow that never participated in this kind's admission at all — the
    // common case, deliberately cheap: one indexed SELECT).
    async releaseByFlow({ flowId, kind }) {
      if (!flowId) throw new TypeError("admission-queue-store.releaseByFlow: flowId is required");
      if (!kind) throw new TypeError("admission-queue-store.releaseByFlow: kind is required");
      return withWriteTx(async (tx) => {
        const { rows } = await tx.execute({
          sql: `SELECT id FROM flow_admission_queue
                WHERE flow_id = ? AND kind = ? AND status IN ('admitted', 'queued')
                ORDER BY created_at DESC LIMIT 1`,
          args: [flowId, kind],
        });
        const self = rows[0] ?? null;
        if (!self) return { released: false, promoted: null };
        return releaseRowById(tx, self.id);
      });
    },

    // Promotes oldest queued rows into any free capacity, until the pool is
    // full or nothing is queued. Returns the promoted entries ({ id, kind,
    // flowId, nodeId }) for the caller to wake via wakePromotedRelease.
    // Normal operation never needs this — every release promotes exactly
    // one row into the slot it frees — but a pool that loses admitted rows
    // without a release (a migration deleting a whole kind, a crash between
    // a delete and its promotion) would otherwise sit below its limit with
    // work still queued, until enough unrelated releases happened.
    async fillFreeSlots() {
      const limit = getLimit();
      return withWriteTx(async (tx) => {
        const { rows } = await tx.execute({
          sql: `SELECT COUNT(*) AS n FROM flow_admission_queue WHERE status = 'admitted'`,
          args: [],
        });
        const free = limit - Number(rows[0]?.n ?? 0);
        if (free <= 0) return [];
        const { rows: nextRows } = await tx.execute({
          sql: `SELECT id, kind, flow_id, node_id FROM flow_admission_queue
                WHERE status = 'queued' ORDER BY created_at ASC LIMIT ?`,
          args: [free],
        });
        const now = Date.now();
        const promoted = [];
        for (const next of nextRows) {
          await tx.execute({
            sql: `UPDATE flow_admission_queue SET status = 'admitted', admitted_at = ? WHERE id = ?`,
            args: [now, next.id],
          });
          promoted.push({ id: next.id, kind: next.kind, flowId: next.flow_id, nodeId: next.node_id });
        }
        return promoted;
      });
    },

    // Observability / test hook — counts across the whole combined pool,
    // not scoped to one kind (matching admit()/release()'s own scope).
    async _stats() {
      const { rows } = await client.execute(
        `SELECT status, COUNT(*) AS n FROM flow_admission_queue GROUP BY status`
      );
      const out = { admitted: 0, queued: 0 };
      for (const r of rows) out[r.status] = Number(r.n);
      return out;
    },

    // Lists queued entries oldest-first — general observability / test hook.
    // Not currently called from any boot-revival or dispatch path (see
    // boot/flow-revival.js's own skip-direct-dispatch comment for how
    // queued orphans are actually handled today).
    async listByStatus(status) {
      const { rows } = await client.execute({
        sql: `SELECT id, kind, flow_id, node_id, created_at, admitted_at
              FROM flow_admission_queue WHERE status = ? ORDER BY created_at ASC`,
        args: [status],
      });
      return rows.map((r) => ({
        id: r.id, kind: r.kind, flowId: r.flow_id, nodeId: r.node_id,
        createdAt: Number(r.created_at),
        admittedAt: r.admitted_at != null ? Number(r.admitted_at) : null,
      }));
    },

    // Fetches one entry by id — callers releasing/inspecting their own
    // admission handle without going through listByStatus's full scan.
    async get(id) {
      const { rows } = await client.execute({
        sql: `SELECT id, kind, flow_id, node_id, status, created_at, admitted_at
              FROM flow_admission_queue WHERE id = ?`,
        args: [id],
      });
      const r = rows[0];
      if (!r) return null;
      return {
        id: r.id, kind: r.kind, flowId: r.flow_id, nodeId: r.node_id, status: r.status,
        createdAt: Number(r.created_at),
        admittedAt: r.admitted_at != null ? Number(r.admitted_at) : null,
      };
    },
  };
}
