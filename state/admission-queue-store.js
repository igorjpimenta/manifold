// admission-queue-store.js — the single durable admission table for flow
// RUNS (kind:"flow-start" for a start, "child-dispatch" for a subflow
// child): one row per run; a run executes one node at a time, so the run
// cap already bounds everything it does.
//
// ONE combined pool, not one per kind — both kinds compete for the same
// shared count. This works because admission tracks flow_runs.status: a
// flow's entry is held only while genuinely "running" — the moment it
// leaves running, state-store's hooks release it via releaseByFlow(), so a
// parent never holds a slot for its entire lifetime while its children
// also need slots (one fan-out could otherwise exhaust the whole pool).
//
// Atomicity: every decision (count admitted, then claim) runs inside one
// client.transaction("write") — BEGIN IMMEDIATE acquires SQLite's writer
// lock before the SELECT; a separate count-then-write would be racy across
// concurrent callers.

import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

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
  // success and always closing (rollback if not committed) — the
  // try/finally shape @libsql/client requires. Serialized and retried on
  // SQLITE_BUSY via withSharedWrite(), keyed by dbPath: libsql's local
  // sqlite3 backend does NOT safely support multiple Transaction objects
  // in flight on one shared connection (commit fails with "cannot commit
  // transaction - SQL statements in progress"), and a plain execute() on
  // the same connection while a transaction is open can corrupt the
  // holder's commit — a client-library limitation, not a SQLite-level one.
  //
  // Safe to retry the whole fn(tx) on a fresh transaction: an uncommitted
  // transaction has zero durable effect, so re-running every statement is
  // not a partial-redo risk.
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

  // Shared by release(id) and releaseByFlow(). Must run inside the
  // caller's own withWriteTx callback (not its own transaction) so both
  // release paths stay a single atomic decision.
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
    // Idempotent per (kind, flowId, nodeId): a live row for this exact
    // tuple is returned instead of inserting a second one — otherwise a
    // re-entrant request (e.g. a resume racing a revival) would
    // permanently shrink the shared pool by one slot per occurrence.
    // "queued" is a normal, expected outcome, not an error.
    //
    // A queued caller is never woken in-process: when a slot frees,
    // release() promotes the row and the releasing call site drives the
    // flow forward through wakePromotedRelease.
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

    // Promotes FIFO across the WHOLE combined pool, not just the
    // releasing entry's kind. An id that no longer exists is a safe
    // no-op — never throws, so a caller racing its own cleanup against a
    // process restart needn't special-case "already gone".
    async release(id) {
      if (!id) throw new TypeError("admission-queue-store.release: id is required");
      return withWriteTx(async (tx) => releaseRowById(tx, id));
    },

    // Releases the CURRENT entry for a (flowId, kind) without the caller
    // carrying an admission id — the flow could have crashed and been
    // revived by a different process between admit() and now. No-op when
    // this flow never held a live entry for this kind.
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

    // Normal operation never needs this — every release promotes exactly
    // one row — but a pool that loses admitted rows without a release
    // (a migration deleting a whole kind, a crash between a delete and
    // its promotion) would otherwise sit below its limit with work still
    // queued, until enough unrelated releases happened.
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

    async limit() {
      return getLimit();
    },

    async _stats() {
      const { rows } = await client.execute(
        `SELECT status, COUNT(*) AS n FROM flow_admission_queue GROUP BY status`
      );
      const out = { admitted: 0, queued: 0 };
      for (const r of rows) out[r.status] = Number(r.n);
      return out;
    },

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
