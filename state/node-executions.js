// node-executions.js — per-node invocation + execution-attempt records
// (flow_node_invocations, flow_node_executions), renamed/split from the
// original flow_step_timings.
//
// Lazy singleton: first caller opens the DB and creates the schema; subsequent
// calls return the cached store. Writes are fire-and-forget (errors swallowed)
// so a timing/error-recording failure never disrupts flow execution.
//
// Two-table split (Phase 2):
//   flow_node_invocations — one row per LOGICAL node dispatch (root-level
//     node-per-flow-visit, or subflow-item), created exactly once and never
//     duplicated by retries. flow_runs.parent_invocation_id points here.
//   flow_node_executions — one row per EXECUTION ATTEMPT of an invocation
//     (one per retry), referencing its invocation via invocation_id.
// The single flat table this split replaces conflated these two concerns —
// retries legitimately write multiple rows sharing the same (flow_id,
// node_id, group_id, branch_key) tuple, so that tuple could never safely be
// used as a stable identity/anchor on its own (see design doc "Schema:
// splitting invocation from execution" for the full reasoning).

import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { eq, and, inArray, isNull, isNotNull, desc, asc } from "drizzle-orm";
import { getSharedDb, withSharedWrite } from "./db.js";
import { flowNodeInvocations, flowNodeExecutions } from "./schema.js";
import { createMigrator } from "./migrator.js";
import { enqueue, flush as flushBatchWriter } from "./batch-writer.js";

const DEFAULT_DB_PATH = path.join(os.homedir(), ".openclaw", "runtime.sqlite");

// No size cap on resultJson: no cap width is both safe for real payloads
// and small enough to matter, so writeResult() always stores the full
// result. resultTooLargeBytes and the legacy-row handling in
// result-replay.js/needs-resolver.js stay in place read-only, for
// pre-existing rows whose capped result cannot be recovered.

const MIGRATIONS = [
  // v1: original flow_step_timings shape.
  `CREATE TABLE IF NOT EXISTS flow_step_timings (
     id          INTEGER PRIMARY KEY AUTOINCREMENT,
     flow_id     TEXT    NOT NULL,
     pipeline_id TEXT,
     node_id     TEXT    NOT NULL,
     branch_item TEXT,
     entered_at  INTEGER NOT NULL,
     exited_at   INTEGER NOT NULL
   );
   CREATE INDEX IF NOT EXISTS idx_fst_pipe_node ON flow_step_timings(pipeline_id, node_id);
   CREATE INDEX IF NOT EXISTS idx_fst_flow      ON flow_step_timings(flow_id);
   CREATE INDEX IF NOT EXISTS idx_fst_exited    ON flow_step_timings(exited_at)`,

  // v2: renamed flow_step_timings -> flow_node_executions.
  async (client) => {
    await client.execute(`
      CREATE TABLE flow_node_executions (
        id          TEXT    PRIMARY KEY,
        flow_id     TEXT    NOT NULL,
        pipeline_id TEXT,
        node_id     TEXT    NOT NULL,
        branch_key  TEXT,
        entered_at  INTEGER NOT NULL,
        exited_at   INTEGER NOT NULL,
        group_id    TEXT,
        error       TEXT
      )
    `);
    const { rows } = await client.execute(
      `SELECT flow_id, pipeline_id, node_id, branch_item, entered_at, exited_at FROM flow_step_timings`
    );
    for (const r of rows) {
      await client.execute({
        sql: `INSERT INTO flow_node_executions (id, flow_id, pipeline_id, node_id, branch_key, entered_at, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: [randomUUID(), r.flow_id, r.pipeline_id, r.node_id, r.branch_item, r.entered_at, r.exited_at],
      });
    }
    // Branch-error backfill: flow_branches lives in this same runtime.sqlite
    // file — legacy old-model storage, so guarded by an existence check: it
    // may simply not exist on a database that never had a branch model.
    const { rows: hasBranchTable } = await client.execute(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='flow_branches'`
    );
    if (hasBranchTable.length > 0) {
      const { rows: branchRows } = await client.execute(
        `SELECT flow_id, group_id, branch_key, step, started_at, updated_at, done_at, errors
           FROM flow_branches WHERE errors IS NOT NULL AND errors != '[]'`
      );
      for (const b of branchRows) {
        const error = lastErrorMessage(b.errors);
        if (error == null) continue;
        await client.execute({
          sql: `INSERT INTO flow_node_executions (id, flow_id, pipeline_id, node_id, branch_key, group_id, entered_at, exited_at, error) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)`,
          args: [randomUUID(), b.flow_id, b.step, b.branch_key, b.group_id, b.started_at, b.done_at ?? b.updated_at, error],
        });
      }
    }
    await client.execute(`DROP TABLE flow_step_timings`);
    await client.execute(`CREATE INDEX idx_fst_pipe_node ON flow_node_executions(pipeline_id, node_id)`);
    await client.execute(`CREATE INDEX idx_fst_flow ON flow_node_executions(flow_id)`);
    await client.execute(`CREATE INDEX idx_fst_exited ON flow_node_executions(exited_at)`);
  },

  // v3 (Phase 2): split flow_node_executions into flow_node_invocations
  // (one row per logical dispatch, created once) + a narrowed
  // flow_node_executions (one row per attempt, FK to its invocation).
  async (client) => {
    await client.execute(`
      CREATE TABLE flow_node_invocations (
        id          TEXT    PRIMARY KEY,
        flow_id     TEXT    NOT NULL,
        pipeline_id TEXT,
        node_id     TEXT    NOT NULL,
        group_id    TEXT,
        branch_key  TEXT,
        invoked_at  INTEGER NOT NULL
      )
    `);
    await client.execute(`
      CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(group_id, ''), COALESCE(branch_key, ''))
    `);
    await client.execute(`CREATE INDEX idx_invocations_flow ON flow_node_invocations (flow_id)`);
    await client.execute(`CREATE INDEX idx_invocations_pipe_node ON flow_node_invocations (pipeline_id, node_id)`);

    const { rows } = await client.execute(
      `SELECT id, flow_id, pipeline_id, node_id, branch_key, group_id, entered_at, exited_at, error
         FROM flow_node_executions ORDER BY entered_at ASC`
    );

    await client.execute(`
      CREATE TABLE flow_node_executions_v3 (
        id            TEXT    PRIMARY KEY,
        invocation_id TEXT    NOT NULL,
        entered_at    INTEGER NOT NULL,
        exited_at     INTEGER NOT NULL,
        error         TEXT
      )
    `);

    const invocationIdByTuple = new Map();
    for (const r of rows) {
      const tupleKey = `${r.flow_id} ${r.node_id} ${r.group_id ?? ""} ${r.branch_key ?? ""}`;
      let invocationId = invocationIdByTuple.get(tupleKey);
      if (!invocationId) {
        invocationId = randomUUID();
        await client.execute({
          sql: `INSERT INTO flow_node_invocations (id, flow_id, pipeline_id, node_id, group_id, branch_key, invoked_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
          args: [invocationId, r.flow_id, r.pipeline_id, r.node_id, r.group_id, r.branch_key, r.entered_at],
        });
        invocationIdByTuple.set(tupleKey, invocationId);
      }
      await client.execute({
        sql: `INSERT INTO flow_node_executions_v3 (id, invocation_id, entered_at, exited_at, error) VALUES (?, ?, ?, ?, ?)`,
        args: [randomUUID(), invocationId, r.entered_at, r.exited_at, r.error],
      });
    }

    await client.execute(`DROP TABLE flow_node_executions`);
    await client.execute(`ALTER TABLE flow_node_executions_v3 RENAME TO flow_node_executions`);
    await client.execute(`CREATE INDEX idx_fne_invocation ON flow_node_executions(invocation_id)`);
    await client.execute(`CREATE INDEX idx_fne_exited ON flow_node_executions(exited_at)`);
  },

  // v4: drop group_id from flow_node_invocations (made redundant by
  // childFlows being universal). Merges historical duplicates first —
  // __end__ sub-node rows that share (flow_id, node_id, branch_key) but
  // differ only in group_id would collide once group_id is gone.
  async (client) => {
    const { rows: dupes } = await client.execute(`
      SELECT flow_id, node_id, COALESCE(branch_key, '') AS bk, MIN(id) AS winner_id
        FROM flow_node_invocations
       GROUP BY flow_id, node_id, bk
      HAVING COUNT(*) > 1
    `);
    for (const d of dupes) {
      const { rows: losers } = await client.execute({
        sql: `SELECT id FROM flow_node_invocations WHERE flow_id = ? AND node_id = ? AND COALESCE(branch_key, '') = ? AND id != ?`,
        args: [d.flow_id, d.node_id, d.bk, d.winner_id],
      });
      for (const loser of losers) {
        await client.execute({ sql: `UPDATE flow_node_executions SET invocation_id = ? WHERE invocation_id = ?`, args: [d.winner_id, loser.id] });
        await client.execute({ sql: `DELETE FROM flow_node_invocations WHERE id = ?`, args: [loser.id] });
      }
    }
    await client.execute(`DROP INDEX idx_invocations_identity`);
    await client.execute(`ALTER TABLE flow_node_invocations DROP COLUMN group_id`);
    await client.execute(`
      CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(branch_key, ''))
    `);
  },

  // v5: add visit_seq to distinguish fresh passes from
  // continuations (retries, revivals, re-checks).
  async (client) => {
    await client.execute(`ALTER TABLE flow_node_invocations ADD COLUMN visit_seq INTEGER NOT NULL DEFAULT 1`);
    await client.execute(`DROP INDEX idx_invocations_identity`);
    await client.execute(`
      CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(branch_key, ''), visit_seq)
    `);
  },

  // v6: kind on flow_node_executions — why this execution happened.
  async (client) => {
    await client.execute(`ALTER TABLE flow_node_executions ADD COLUMN kind TEXT NOT NULL DEFAULT 'legacy'`);
  },

  // v7: rename branch_key -> subflow_key. Drop and recreate the expression
  // index since RENAME COLUMN doesn't rewrite index expressions.
  async (client) => {
    await client.execute(`DROP INDEX idx_invocations_identity`);
    await client.execute(`ALTER TABLE flow_node_invocations RENAME COLUMN branch_key TO subflow_key`);
    await client.execute(`
      CREATE UNIQUE INDEX idx_invocations_identity
        ON flow_node_invocations (flow_id, node_id, COALESCE(subflow_key, ''), visit_seq)
    `);
  },

  // v8 ("Eradicate soft errors"):
  // error_type on flow_node_executions, set when a thrown error is a
  // NodeFailure (engine/executor-errors.js) — reuses the existing
  // never_attempted|confirmed_absent|inconclusive|infra_unavailable|blocked
  // vocabulary, moved from a soft-returned field to a thrown error's
  // property. Null for a plain Error or no error at all.
  async (client) => {
    await client.execute(`ALTER TABLE flow_node_executions ADD COLUMN error_type TEXT`);
  },

  // v9 (Phase 1.5, "Eradicate soft errors"): error content moves entirely
  // to failure_events, which now gets a permanent row on every single
  // throw regardless of what a cascade edge decides next — v8's error_type
  // lasted one phase. This table goes back to being a pure
  // execution-window/identity ledger. Plain columns, no constraints
  // reference either — safe to drop directly, no table-recreate-and-swap
  // needed.
  async (client) => {
    await client.execute(`ALTER TABLE flow_node_executions DROP COLUMN error`);
    await client.execute(`ALTER TABLE flow_node_executions DROP COLUMN error_type`);
  },

  // v10 ("Option B", Phase B0):
  // resultJson on flow_node_invocations — this table's first payload
  // column, written once per row on completion via writeResult() below.
  // resultWrittenAt distinguishes "not dispatched" (no row)/"dispatched,
  // not complete" (row exists, resultWrittenAt NULL)/"complete" (set) —
  // a bare nullable resultJson alone can't tell those apart.
  // resultTooLargeBytes is the write-time size-cap marker: above the cap,
  // resultJson stays NULL and this carries the real byte count instead of
  // silently inlining an oversized payload. Purely additive — nothing
  // reads these columns yet; B0 only starts building history for the
  // later assembly phase (B1+).
  async (client) => {
    await client.execute(`ALTER TABLE flow_node_invocations ADD COLUMN result_json TEXT`);
    await client.execute(`ALTER TABLE flow_node_invocations ADD COLUMN result_written_at INTEGER`);
    await client.execute(`ALTER TABLE flow_node_invocations ADD COLUMN result_too_large_bytes INTEGER`);
  },

  // v11: gate_records — one row per (flow, human-gate node), holding
  // exactly what executor-nodes.js's human-gate branch already tracks in
  // state.gates[nodeId] (pending/items/openedAt/lastOpenedAt/
  // consumedAt). state.gates itself is untouched by this — this is a
  // parallel, additive write, same pattern the original resultJson columns
  // used. The reason it needs its OWN identity-keyed table rather than
  // riding along in resultJson: multiple different gate nodes in the same
  // flow all write into the SAME top-level state.gates key via a full-
  // snapshot embed (required by state_json's own shallow-merge accumulation
  // model), which makes a later gate's own resultJson row contain an
  // EARLIER gate's now-stale copy of that shared key — replaying it can
  // silently erase a sibling gate's real record. A dedicated table keyed
  // by (flow_id, node_id) has no shared key to go stale: each gate node
  // only ever touches its own row, so a point lookup is always correct,
  // no replay/merge involved.
  `CREATE TABLE IF NOT EXISTS gate_records (
     flow_id     TEXT    NOT NULL,
     node_id     TEXT    NOT NULL,
     record_json TEXT    NOT NULL,
     updated_at  INTEGER NOT NULL,
     PRIMARY KEY (flow_id, node_id)
   )`,

  // v13: index for freelance-filter-node.js's in-flight dedup query
  // (workspace/.shared/common/lib/freelance-filter-node.js), which filters
  // flow_node_invocations by node_id and correlates on subflow_key (both
  // in its own WHERE clause and in a NOT EXISTS subquery added alongside
  // it). Neither column was covered by an existing index — idx_invocations_
  // pipe_node leads with pipeline_id, which that query never filters on —
  // so SQLite falls back to a full scan of flow_runs (driving the join)
  // plus a second per-row scan inside the correlated subquery; the query
  // effectively never completes. This index turns both the outer filter
  // and the subquery's exact (node_id, subflow_key) lookup into direct
  // index searches instead. IF NOT EXISTS: the migrator tracks applied
  // versions in _schema_version, not actual schema state, so a database
  // that already carries this index (e.g. patched out-of-band ahead of the
  // migration) would throw "index already exists" without it.
  async (client) => {
    await client.execute(`CREATE INDEX IF NOT EXISTS idx_invocations_node_subflow ON flow_node_invocations (node_id, subflow_key)`);
  },
];

const VALID_EXECUTION_KINDS = new Set(["fresh", "retry", "revival", "recheck"]);

// Normalizes flow_branches.errors' confirmed mixed shape (bare strings vs
// {node, itemId, message, at} objects) — only the last entry is taken.
// One-time migration-time helper, not permanent runtime logic.
export function lastErrorMessage(errorsJson) {
  let arr;
  try { arr = JSON.parse(errorsJson); } catch { return null; }
  if (!Array.isArray(arr) || arr.length === 0) return null;
  const last = arr[arr.length - 1];
  if (typeof last === "string") return last;
  if (last && typeof last === "object") return last.message ?? JSON.stringify(last);
  return String(last);
}

let _singleton = null;

function _makeStoreProxy(_p) {
  return new Proxy({}, { get(_, k) {
    if (k === "then" || typeof k === "symbol") return undefined;
    if (k === "ready") return () => _p.then(() => {});
    if (k === "_db") return new Proxy({}, { get(__, method) {
      if (method === "then" || typeof method === "symbol") return undefined;
      return (...a) => _p.then(s => s._db[method](...a));
    } });
    return (...a) => _p.then(s => { if (typeof s[k] !== "function") throw new TypeError(`node-executions: ${String(k)} is not a function`); return s[k](...a); });
  } });
}

export function getNodeExecutionsStore(dbPath = DEFAULT_DB_PATH) {
  if (!_singleton) {
    _singleton = _makeStoreProxy(_createStore(dbPath));
  }
  return _singleton;
}

// Exported for tests: creates an independent store (bypasses the singleton).
export function createNodeExecutionsStore(dbPath) {
  return _makeStoreProxy(_createStore(dbPath));
}

async function _createStore(dbPath) {
  try {
    mkdirSync(path.dirname(dbPath), { recursive: true });
    const { db, client } = await getSharedDb(dbPath);
    // Tracking key deliberately kept as "step-timings" — pure version-history
    // bookkeeping, unrelated to the table's current name/shape.
    await createMigrator(client, { name: "step-timings", migrations: MIGRATIONS });

    // ── helpers ────────────────────────────────────────────────────────────

    async function _findLatestInvocation(flowId, nodeId, subflowKey) {
      const nullGuard = subflowKey == null
        ? isNull(flowNodeInvocations.subflowKey)
        : eq(flowNodeInvocations.subflowKey, subflowKey);
      const rows = await db.select({
        id: flowNodeInvocations.id,
        visitSeq: flowNodeInvocations.visitSeq,
        invokedAt: flowNodeInvocations.invokedAt,
      })
        .from(flowNodeInvocations)
        .where(and(eq(flowNodeInvocations.flowId, flowId), eq(flowNodeInvocations.nodeId, nodeId), nullGuard))
        .orderBy(desc(flowNodeInvocations.visitSeq))
        .limit(1);
      return rows[0] ?? null;
    }

    // Find-or-create the invocation for a logical node dispatch. See MIGRATIONS
    // v5 comment: forceNew=true (kind="fresh") creates a new visit_seq row;
    // forceNew=false reuses the latest. Race-safe via INSERT OR IGNORE +
    // re-select.
    //
    // `guard`, if supplied, is an async () => boolean evaluated INSIDE the
    // same withSharedWrite(dbPath, ...) turn as the insert — that queue
    // already serializes every write against this dbPath to one at a time
    // (state/db.js), so running the check there closes the gap a caller-side
    // pre-check can't: two concurrent findOrCreateInvocation calls for
    // DIFFERENT flowIds but the SAME cross-flow identity (e.g. the same
    // listing's SCORE/PROPOSE dispatch, decided by two parent flow-start
    // runs that both read "nothing in flight yet" before either committed)
    // are forced through this one queue, so the second caller's guard always
    // observes the first caller's already-committed row. A false result
    // skips the insert entirely — findOrCreateInvocation returns null rather
    // than creating a duplicate identity for a different flowId. guard has
    // no opinion on same-flow identity (that's the latest/visitSeq check
    // above, unchanged) — it only ever runs for a genuinely new insert.
    async function findOrCreateInvocation({ flowId, pipelineId = null, nodeId, subflowKey = null, forceNew = false, guard }) {
      const latest = await _findLatestInvocation(flowId, nodeId, subflowKey);
      if (!forceNew && latest) return latest.id;
      const nextVisitSeq = (Number(latest?.visitSeq ?? 0)) + 1;
      const id = randomUUID();
      const r = await withSharedWrite(dbPath, async () => {
        if (guard && !(await guard())) return { rowsAffected: 0, guardRejected: true };
        return db.insert(flowNodeInvocations).values({
          id, flowId, pipelineId, nodeId, subflowKey: subflowKey ?? null, visitSeq: nextVisitSeq, invokedAt: Date.now(),
        }).onConflictDoNothing();
      });
      if (r.guardRejected) return null;
      if (r.rowsAffected > 0) return id;
      // Lost the race — re-select.
      const row = await _findLatestInvocation(flowId, nodeId, subflowKey);
      if (row) return row.id;
      throw new Error(`node-executions: findOrCreateInvocation lost the race and found nothing on re-select (${flowId}/${nodeId})`);
    }

    // ── store API ──────────────────────────────────────────────────────────

    return {
      _db: client,

      // write() defers its INSERT through batch-writer (see there) instead
      // of awaiting it immediately — production callers don't need this
      // (they never awaited write()'s DB completion either), but a test
      // asserting read-after-write needs an explicit drain point. Not meant
      // for production use.
      async flush() {
        await flushBatchWriter(dbPath);
      },

      findOrCreateInvocation,

      // Public wrapper around the internal _findLatestInvocation helper —
      // used by engine/child-resume-trigger.js's repair-propagation guard
      // to confirm a settling child's spawning invocation is still the
      // CURRENT invocation of its node/subflowKey lineage before
      // resurrecting a terminal 'failed' parent — a stale invocation
      // (superseded by a fresh top-level retry or a loop-back re-entry)
      // must not repair a flow that's since moved on independently.
      // Returns `{id, visitSeq}` or null.
      async getLatestInvocationForNode(flowId, nodeId, subflowKey) {
        return _findLatestInvocation(flowId, nodeId, subflowKey);
      },

      // ios/live-bridge.js's snapshot() uses this to backfill blockedAtMs
      // for gate items opened before requestedAt existed on wait_json
      // items — invokedAt is the exact moment the gate NODE itself was
      // dispatched (written eagerly at invocation start, independent of
      // the node resolving/blocking), a much tighter bound than
      // flow.createdAt, which reflects the whole flow's creation and can
      // predate the gate node's actual dispatch by however long earlier
      // steps in the same flow took to run.
      async getLatestInvocationInvokedAt(flowId, nodeId, subflowKey) {
        const row = await _findLatestInvocation(flowId, nodeId, subflowKey);
        return row?.invokedAt != null ? Number(row.invokedAt) : null;
      },

      async getInvocation(id) {
        try {
          const rows = await db.select().from(flowNodeInvocations)
            .where(eq(flowNodeInvocations.id, id));
          const row = rows[0] ?? null;
          if (!row) return null;
          return {
            id: row.id,
            flowId: row.flowId,
            pipelineId: row.pipelineId,
            nodeId: row.nodeId,
            subflowKey: row.subflowKey,
            visitSeq: Number(row.visitSeq),
            invokedAt: Number(row.invokedAt),
            resultWrittenAt: row.resultWrittenAt != null ? Number(row.resultWrittenAt) : null,
          };
        } catch { return null; }
      },

      async listInvocationIdsForNode(flowId, nodeId) {
        try {
          const rows = await db.select({ id: flowNodeInvocations.id }).from(flowNodeInvocations)
            .where(and(
              eq(flowNodeInvocations.flowId, flowId),
              eq(flowNodeInvocations.nodeId, nodeId),
              isNotNull(flowNodeInvocations.subflowKey),
            ));
          return rows.map((r) => r.id);
        } catch { return []; }
      },

      async listInvocationIdsForNodes(flowId, nodeIds) {
        if (!nodeIds?.length) return new Map();
        try {
          const rows = await db.select({ id: flowNodeInvocations.id, nodeId: flowNodeInvocations.nodeId })
            .from(flowNodeInvocations)
            .where(and(
              eq(flowNodeInvocations.flowId, flowId),
              inArray(flowNodeInvocations.nodeId, nodeIds),
              isNotNull(flowNodeInvocations.subflowKey),
            ));
          const out = new Map();
          for (const row of rows) {
            if (!out.has(row.nodeId)) out.set(row.nodeId, []);
            out.get(row.nodeId).push(row.id);
          }
          return out;
        } catch { return new Map(); }
      },

      async listSubflowNodeIdsForFlow(flowId) {
        try {
          const rows = await db.selectDistinct({ nodeId: flowNodeInvocations.nodeId }).from(flowNodeInvocations)
            .where(and(eq(flowNodeInvocations.flowId, flowId), isNotNull(flowNodeInvocations.subflowKey)));
          return rows.map((r) => r.nodeId);
        } catch { return []; }
      },

      // Create the invocation row before execution starts. Returns
      // { invocationId, isNew } on success, null on DB error (fire-safe).
      // Callers must thread both values through to write() so the post-
      // execution completion row links to the same invocation and the
      // fresh/re-entry invariant check can be satisfied without a second
      // _findLatestInvocation lookup.
      async enter({ flowId, pipelineId = null, nodeId, subflowKey = null, kind }) {
        if (!VALID_EXECUTION_KINDS.has(kind)) {
          throw new Error(`node-executions: enter() requires a valid kind (fresh|retry|revival|recheck), got ${JSON.stringify(kind)}`);
        }
        const forceNew = kind === "fresh";
        try {
          const priorLatest = await _findLatestInvocation(flowId, nodeId, subflowKey);
          const invocationId = await findOrCreateInvocation({ flowId, pipelineId, nodeId, subflowKey, forceNew });
          const isNew = !priorLatest || (forceNew && invocationId !== priorLatest.id);
          return { invocationId, isNew };
        } catch {
          return null;
        }
      },

      // The node_id of the most recently created invocation for this flow.
      // After enter() is called pre-execution, this correctly identifies the
      // in-flight node even during execution, not just after completion.
      // Returns null if no invocations exist yet.
      //
      // subflow_key IS NULL scopes this to the flow's own top-level dispatch
      // sequence — enter() (this store's own write path, called from
      // executor.js's runFlow loop) always passes subflowKey: null, so this
      // is the same slice of rows that path ever creates. Per-item child-
      // spawn bookkeeping rows (subflow_key set, written by
      // resolveOrCreateChild/spawn-child.js, never by enter()) are a
      // separate concern and are excluded here on purpose — an unscoped
      // query would occasionally return one of those instead of the node's
      // own identity row, which happens to carry the same node_id today (so
      // callers reading only node_id were never actually wrong) but isn't a
      // designed guarantee. This makes it correct by construction instead.
      async findCurrentStep(flowId) {
        try {
          // rowid DESC breaks ties on invoked_at (Date.now(), millisecond
          // resolution — two rapid successive dispatches for the same flow
          // can genuinely land in the same millisecond). rowid is SQLite's
          // own implicit, strictly-insertion-order column on this table
          // (never declared WITHOUT ROWID) — the only reliable tiebreaker,
          // since `id` is a random UUID with no correlation to insert order.
          // Found via a real, reproducible test flake (not hypothetical):
          // ORDER BY invoked_at DESC LIMIT 1 alone returned either of two
          // same-millisecond rows nondeterministically.
          const { rows } = await client.execute({
            sql: `SELECT node_id FROM flow_node_invocations WHERE flow_id = ? AND subflow_key IS NULL ORDER BY invoked_at DESC, rowid DESC LIMIT 1`,
            args: [flowId],
          });
          return rows[0]?.node_id ?? null;
        } catch { return null; }
      },

      // `kind` is required and self-validating. Invariant violations throw
      // OUTSIDE any try/catch — must fail loud in tests/dev. Genuine DB
      // failures keep the fire-and-forget contract: swallowed, not thrown.
      // When invocationId + isNewInvocation are provided (from a prior enter()
      // call), the internal findOrCreateInvocation is skipped and those values
      // are trusted directly — saving one DB round-trip per dispatch.
      async write({ flowId, pipelineId = null, nodeId, subflowKey = null, enteredAt, exitedAt, kind, invocationId: providedId = null, isNewInvocation = null }) {
        if (!VALID_EXECUTION_KINDS.has(kind)) {
          throw new Error(`node-executions: write() requires a valid kind (fresh|retry|revival|recheck), got ${JSON.stringify(kind)}`);
        }
        const forceNew = kind === "fresh";
        let wasCreated, invocationId;
        try {
          if (providedId != null && isNewInvocation != null) {
            invocationId = providedId;
            wasCreated = isNewInvocation;
          } else {
            const priorLatest = await _findLatestInvocation(flowId, nodeId, subflowKey);
            invocationId = await findOrCreateInvocation({ flowId, pipelineId, nodeId, subflowKey, forceNew });
            wasCreated = !priorLatest || (forceNew && invocationId !== priorLatest.id);
          }
        } catch {
          return;
        }
        if (kind === "fresh" && !wasCreated) {
          throw new Error(`node-executions: kind="fresh" but invocation ${invocationId} already existed (flow=${flowId} node=${nodeId})`);
        }
        if (kind !== "fresh" && wasCreated) {
          throw new Error(`node-executions: kind="${kind}" but this is the first execution for a brand-new invocation (flow=${flowId} node=${nodeId}) — should be "fresh"`);
        }
        // Routed through batch-writer instead of an immediate awaited
        // INSERT: this is the highest-volume write in the codebase (one per
        // node execution attempt), already fire-and-forget by contract (see
        // this file's header) and every real call site (executor.js) fires
        // it without awaiting the result — safe to defer and coalesce with
        // other pending writes into one db.batch() round-trip instead of
        // one write-lock acquisition per call. executionId is generated
        // synchronously up front (a random UUID, not derived from the row
        // actually landing), so the caller-facing return value doesn't
        // depend on the deferred write having completed yet.
        const executionId = randomUUID();
        enqueue(dbPath, db.insert(flowNodeExecutions).values({
          id: executionId, invocationId, enteredAt, exitedAt, kind,
        }));
        return { invocationId, executionId };
      },

      // Writes an invocation's own resultJson once it completes, no size
      // cap (see this file's own header comment). resultTooLargeBytes is
      // always written null here — it only carries a real value on legacy
      // rows that predate the cap's removal.
      //
      // Two things this deliberately isn't: (1) it doesn't throw on
      // ordinary failure — same fire-and-forget contract as write() above,
      // a result-recording failure must never disrupt flow execution; (2)
      // it isn't a blind UPDATE. `flow_node_invocations` has no CAS/
      // revision of its own (unlike flow_runs), so an unconditional write
      // here would silently accept last-write-wins on a genuine double-
      // completion race with zero signal — exactly the detection
      // flow_runs.revision's own conflict handling already provides at the
      // flow level. This does the per-row equivalent: read the invocation's
      // current resultWrittenAt, then write conditioned on that same value
      // still being current. A 0-row UPDATE means something else won this
      // invocation's write between the read and the write — the caller
      // gets { ok: false, reason: "race" } back and is expected to log it
      // loudly (this module has no logger of its own — see file header;
      // executor.js does).
      async writeResult(invocationId, result) {
        if (!invocationId) return { ok: false, reason: "no-invocation-id" };
        let serialized;
        try {
          serialized = JSON.stringify(result ?? null);
        } catch {
          return { ok: false, reason: "unserializable" };
        }
        const byteLength = Buffer.byteLength(serialized, "utf8");
        const now = Date.now();
        try {
          // Read-then-conditional-write, retried as one unit on SQLITE_BUSY:
          // a retry re-reads resultWrittenAt fresh and re-attempts the
          // conditional update against that current value, rather than
          // blindly retrying just the UPDATE against a now-possibly-stale
          // `expected` captured before the retry.
          return await withSharedWrite(dbPath, async () => {
            const rows = await db.select({ resultWrittenAt: flowNodeInvocations.resultWrittenAt })
              .from(flowNodeInvocations)
              .where(eq(flowNodeInvocations.id, invocationId));
            if (rows.length === 0) return { ok: false, reason: "no-such-invocation" };
            const expected = rows[0].resultWrittenAt ?? null;
            const whereExpected = expected === null
              ? and(eq(flowNodeInvocations.id, invocationId), isNull(flowNodeInvocations.resultWrittenAt))
              : and(eq(flowNodeInvocations.id, invocationId), eq(flowNodeInvocations.resultWrittenAt, expected));
            const r = await db.update(flowNodeInvocations)
              .set({ resultJson: serialized, resultWrittenAt: now, resultTooLargeBytes: null })
              .where(whereExpected);
            if (r.rowsAffected === 0) return { ok: false, reason: "race" };
            return { ok: true, bytes: byteLength };
          });
        } catch {
          return { ok: false, reason: "db-error" };
        }
      },

      // Single execution row by id — used by failure-store.js's read-time
      // resolved-status derivation to check whether a failure's linked
      // execution recorded a genuine thrown error (error IS NOT NULL,
      // eligible for same-flow auto-resolve) vs a soft state-marker with
      // no throw at all (error IS NULL, never eligible for same-flow
      // auto-resolve — only cross-flow supersession applies to those).
      async getExecution(id) {
        try {
          const rows = await db.select().from(flowNodeExecutions).where(eq(flowNodeExecutions.id, id));
          const row = rows[0] ?? null;
          if (!row) return null;
          return { id: row.id, invocationId: row.invocationId, enteredAt: Number(row.enteredAt), exitedAt: Number(row.exitedAt), kind: row.kind };
        } catch { return null; }
      },

      // Latest execution outcome for a (pipelineId, nodeId, subflowKey)
      // tuple ACROSS ANY FLOW — not scoped to one flow_id. Used by
      // failure-store.js's cross-flow "auto:branch-success" derivation:
      // when a later flow supersedes an earlier one's failure for the
      // same item (e.g. a re-shortlisted listing that succeeds on a
      // fresh flow), this is how the earlier failure is found to now be
      // resolved. Returns identity + timing only, not error content
      // (Phase 1.5 — this table no longer knows success vs failure at
      // all; the caller checks failure_events for that execution_id
      // instead: no row there means it succeeded).
      async getLatestExecutionOutcomeForItem({ pipelineId, nodeId, subflowKey }) {
        try {
          const { rows } = await client.execute({
            sql: `SELECT e.id AS execution_id, e.exited_at
                    FROM flow_node_executions e JOIN flow_node_invocations i ON e.invocation_id = i.id
                   WHERE i.pipeline_id = ? AND i.node_id = ? AND i.subflow_key IS ?
                   ORDER BY e.exited_at DESC LIMIT 1`,
            args: [pipelineId, nodeId, subflowKey ?? null],
          });
          const row = rows[0] ?? null;
          return row ? { executionId: row.execution_id, exitedAt: Number(row.exited_at) } : null;
        } catch { return null; }
      },

      async getLatestExecutionForNode({ flowId, nodeId, subflowKey = null }) {
        try {
          const { rows } = await client.execute({
            sql: `SELECT e.id AS execution_id, e.invocation_id, e.exited_at
                    FROM flow_node_executions e JOIN flow_node_invocations i ON e.invocation_id = i.id
                   WHERE i.flow_id = ? AND i.node_id = ? AND i.subflow_key IS ?
                   ORDER BY e.entered_at DESC LIMIT 1`,
            args: [flowId, nodeId, subflowKey ?? null],
          });
          const row = rows[0] ?? null;
          return row
            ? { invocationId: row.invocation_id, executionId: row.execution_id, exitedAt: Number(row.exited_at) }
            : null;
        } catch { return null; }
      },

      async queryByPipeline({ pipelineId, sinceMs = null, untilMs = null } = {}) {
        const conditions = ["i.pipeline_id = ?"];
        const args = [pipelineId];
        if (sinceMs != null) { conditions.push("e.exited_at >= ?"); args.push(sinceMs); }
        if (untilMs != null) { conditions.push("e.exited_at <= ?"); args.push(untilMs); }
        const { rows } = await client.execute({
          sql: `SELECT i.node_id, e.entered_at, e.exited_at
                  FROM flow_node_executions e JOIN flow_node_invocations i ON e.invocation_id = i.id
                 WHERE ${conditions.join(" AND ")}`,
          args,
        });
        return rows;
      },

      // queryErrorsByFlow/queryErrorsByFlowIds moved to state/failure-store.js
      // in Phase 1.5 ("Eradicate soft errors") — this table no longer
      // stores error content at all, so it has nothing left to query for
      // "which nodes errored." failure_events is now the only source; see
      // failureStore.queryErrorsByFlow(Ids).

      async summarizeChildrenByFlowIds(flowIds) {
        if (!Array.isArray(flowIds) || flowIds.length === 0) return new Map();
        try {
          const placeholders = flowIds.map(() => "?").join(", ");
          const { rows } = await client.execute({
            sql: `SELECT i.flow_id, fr.status, COUNT(*) AS cnt
                    FROM flow_node_invocations i
                    JOIN flow_runs fr ON fr.parent_invocation_id = i.id
                   WHERE i.flow_id IN (${placeholders})
                   GROUP BY i.flow_id, fr.status`,
            args: flowIds,
          });
          const result = new Map();
          for (const { flow_id, status, cnt } of rows) {
            if (!result.has(flow_id)) result.set(flow_id, { total: 0, pending: 0, failed: 0, skipped: 0, succeeded: 0 });
            const c = result.get(flow_id);
            const count = Number(cnt);
            c.total += count;
            if (status === "running" || status === "blocked" || status === "queued") c.pending += count;
            else if (status === "failed")    c.failed += count;
            else if (status === "skipped")   c.skipped += count;
            else if (status === "succeeded") c.succeeded += count;
          }
          return result;
        } catch { return new Map(); }
      },

      async listInvocationsForFlow(flowId) {
        const invocations = await db.select({
          id: flowNodeInvocations.id,
          nodeId: flowNodeInvocations.nodeId,
          subflowKey: flowNodeInvocations.subflowKey,
          visitSeq: flowNodeInvocations.visitSeq,
          invokedAt: flowNodeInvocations.invokedAt,
        }).from(flowNodeInvocations)
          .where(and(eq(flowNodeInvocations.flowId, flowId), isNull(flowNodeInvocations.subflowKey)))
          .orderBy(asc(flowNodeInvocations.invokedAt));
        if (invocations.length === 0) return [];
        const executions = await db.select().from(flowNodeExecutions)
          .where(inArray(flowNodeExecutions.invocationId, invocations.map((i) => i.id)))
          .orderBy(asc(flowNodeExecutions.enteredAt));
        const byInvocation = new Map();
        for (const e of executions) {
          if (!byInvocation.has(e.invocationId)) byInvocation.set(e.invocationId, []);
          byInvocation.get(e.invocationId).push(e);
        }
        // No error field here (Phase 1.5 — this table doesn't know success
        // vs failure at all anymore). latestExecutionId is exposed so
        // callers can cross-reference failureStore.queryErrorsByFlow(Ids)
        // themselves to determine whether the MOST RECENT attempt of this
        // invocation failed — see routes/runs.js's buildTreeNodeRows.
        return invocations.map((inv) => {
          const execs = byInvocation.get(inv.id) ?? [];
          const latest = execs.length > 0 ? execs[execs.length - 1] : null;
          return {
            invocationId: inv.id,
            nodeId: inv.nodeId,
            subflowKey: inv.subflowKey,
            visitSeq: Number(inv.visitSeq),
            invokedAt: Number(inv.invokedAt),
            executionCount: execs.length,
            enteredAt: latest ? Number(latest.enteredAt) : null,
            exitedAt:  latest ? Number(latest.exitedAt)  : null,
            latestExecutionId: latest?.id ?? null,
            kind:  latest?.kind  ?? null,
            // `kind` above is the LATEST execution only — a subflow node
            // re-entered via "recheck" once its children finish always
            // masks an earlier "retry" in its own history. Derived from
            // the same `execs` array already loaded above — no extra
            // query.
            everRetriedOrRevived: execs.some((e) => e.kind === "retry" || e.kind === "revival"),
          };
        });
      },

      // Option B, Phase B1 — one resultJson row per top-level node, ready
      // for assembleState
      // (engine/executor-helpers.js) to replay into a working state
      // object. Two things this does that listInvocationsForFlow (above)
      // deliberately doesn't:
      //   - Reduces to exactly one row per node_id — the chronologically
      //     LATEST occurrence (a genuine loop-back creates a new row with
      //     a higher visit_seq; a retry reuses the same row in place). The
      //     dedup preserves that latest occurrence's own position in
      //     invocation order, not its first-seen position — so if node A
      //     re-fires (loop-back) AFTER node B already ran, A's replay
      //     position correctly moves to after B's, matching what actually
      //     happened chronologically. (Retries reuse one row and so never
      //     move position at all — no ordering question there.)
      //   - Orders by invoked_at then rowid, the same tiebreak
      //     findCurrentStep uses — invoked_at alone (Date.now(), millisecond
      //     resolution) can't distinguish two dispatches landing in the
      //     same millisecond; rowid (SQLite's own strictly-insertion-order
      //     column, unlike the random-UUID id) breaks the tie deterministically.
      async listResultsForFlow(flowId) {
        try {
          // result_written_at IS NOT NULL is load-bearing, not an
          // optimization: without it, a node that re-enters itself (a
          // loop-back, or any revisit) creates a NEW in-flight invocation
          // whose invoked_at is later than its own prior completed
          // invocation — the per-node dedup below keeps only the latest
          // row BY INVOCATION, so it would keep that in-flight,
          // not-yet-written row and silently drop the earlier COMPLETED
          // one entirely, even though the caller (assembleState(), read
          // from INSIDE that very re-entry's own dispatch) needs exactly
          // that prior value to compute its own next result. The failure
          // mode is silent and livelocks: a LOOP node's edge condition
          // never advances and the node re-dispatches forever.
          const { rows } = await client.execute({
            sql: `SELECT node_id, visit_seq, result_json, result_written_at, result_too_large_bytes
                    FROM flow_node_invocations
                   WHERE flow_id = ? AND subflow_key IS NULL AND result_written_at IS NOT NULL
                   ORDER BY invoked_at ASC, rowid ASC`,
            args: [flowId],
          });
          const byNode = new Map();
          for (const r of rows) {
            byNode.delete(r.node_id);
            byNode.set(r.node_id, r);
          }
          return [...byNode.values()].map((r) => ({
            nodeId: r.node_id,
            visitSeq: Number(r.visit_seq),
            resultWrittenAt: r.result_written_at != null ? Number(r.result_written_at) : null,
            resultTooLargeBytes: r.result_too_large_bytes != null ? Number(r.result_too_large_bytes) : null,
            resultJson: r.result_json,
          }));
        } catch { return []; }
      },

      // Single-node-scoped sibling of listResultsForFlow — Option B's
      // "needs:" accessor (engine/needs-resolver.js) only ever wants one
      // named node's latest result from a (possibly ancestor) flow, not
      // that flow's entire result set, so this skips fetching/deduping
      // every other node's rows just to throw them away. Same latest-
      // visit_seq convention (ORDER BY invoked_at DESC, rowid DESC LIMIT 1)
      // as findCurrentStep/listResultsForFlow. Returns null — not a
      // resultWrittenAt:null row — when there's no invocation at all for
      // this (flowId, nodeId), so a caller can tell "never dispatched"
      // apart from "dispatched, not yet complete" without a second query.
      async getResultForNode(flowId, nodeId) {
        try {
          const { rows } = await client.execute({
            sql: `SELECT node_id, visit_seq, result_json, result_written_at, result_too_large_bytes
                    FROM flow_node_invocations
                   WHERE flow_id = ? AND node_id = ? AND subflow_key IS NULL
                   ORDER BY invoked_at DESC, rowid DESC
                   LIMIT 1`,
            args: [flowId, nodeId],
          });
          if (rows.length === 0) return null;
          const r = rows[0];
          return {
            nodeId: r.node_id,
            visitSeq: Number(r.visit_seq),
            resultWrittenAt: r.result_written_at != null ? Number(r.result_written_at) : null,
            resultTooLargeBytes: r.result_too_large_bytes != null ? Number(r.result_too_large_bytes) : null,
            resultJson: r.result_json,
          };
        } catch { return null; }
      },

      // gate_records — see MIGRATIONS v11's comment for why this needs its
      // own identity-keyed table rather than riding along in resultJson.
      // Point upsert: a gate node only ever touches its own (flowId, nodeId)
      // row, so there's no merge/replay involved and no staleness possible.
      async writeGateRecord(flowId, nodeId, record) {
        try {
          const recordJson = JSON.stringify(record ?? null);
          await withSharedWrite(dbPath, () => client.execute({
            sql: `INSERT INTO gate_records (flow_id, node_id, record_json, updated_at)
                    VALUES (?, ?, ?, ?)
                  ON CONFLICT(flow_id, node_id) DO UPDATE SET
                    record_json = excluded.record_json,
                    updated_at = excluded.updated_at`,
            args: [flowId, nodeId, recordJson, Date.now()],
          }));
          return { ok: true };
        } catch (err) {
          return { ok: false, reason: err?.message ?? String(err) };
        }
      },

      // Point lookup — always the real, current value for this one gate,
      // never a historical snapshot. Returns null if this gate has never
      // written a record (not the same as "record exists but is empty").
      async getGateRecord(flowId, nodeId) {
        try {
          const { rows } = await client.execute({
            sql: `SELECT record_json FROM gate_records WHERE flow_id = ? AND node_id = ?`,
            args: [flowId, nodeId],
          });
          if (rows.length === 0) return null;
          try {
            return JSON.parse(rows[0].record_json ?? "null");
          } catch {
            return null;
          }
        } catch { return null; }
      },

      // Batch sibling of getGateRecord — "every gate this flow has ever
      // opened," for a consumer that needs to display the whole flow's
      // gate state at once (e.g. routes/runs.js's run-detail response,
      // once B2.5 cuts it over) rather than one specific node. Each row
      // is independently correct (no shared key, no merge), so building
      // the {[nodeId]: record} dict here is just a fan-in of already-
      // correct point values — nothing about batching reopens the
      // staleness risk assembleState()'s own gates merge has.
      async listGateRecordsForFlow(flowId) {
        try {
          const { rows } = await client.execute({
            sql: `SELECT node_id, record_json FROM gate_records WHERE flow_id = ?`,
            args: [flowId],
          });
          const out = {};
          for (const r of rows) {
            try {
              out[r.node_id] = JSON.parse(r.record_json ?? "null");
            } catch { /* skip a corrupted row rather than fail the whole batch */ }
          }
          return out;
        } catch { return {}; }
      },

      // No error field here either — same Phase 1.5 reasoning. Callers
      // needing per-attempt failure content cross-reference executionId
      // against failureStore.queryErrorsByFlow(Ids) themselves (see
      // routes/runs.js's sendInvocationDetail).
      async listExecutionsForInvocation(invocationId) {
        const rows = await db.select().from(flowNodeExecutions)
          .where(eq(flowNodeExecutions.invocationId, invocationId))
          .orderBy(asc(flowNodeExecutions.enteredAt));
        return rows.map((e) => ({
          executionId: e.id,
          enteredAt:   Number(e.enteredAt),
          exitedAt:    Number(e.exitedAt),
          kind:  e.kind,
        }));
      },

      async hasInvocations(flowIds) {
        if (!Array.isArray(flowIds) || flowIds.length === 0) return new Set();
        try {
          const rows = await db.selectDistinct({ flowId: flowNodeInvocations.flowId }).from(flowNodeInvocations)
            .where(inArray(flowNodeInvocations.flowId, flowIds));
          return new Set(rows.map((r) => r.flowId));
        } catch { return new Set(); }
      },

      // Batched version of "which top-level nodes did this flow visit" —
      // backing the dashboard auto-funnel's per-node visit check without
      // one HTTP/DB round-trip per run. subflow_key IS NULL for the same
      // reason listInvocationsForFlow/findCurrentStep filter on it —
      // only the flow's own top-level dispatch sequence, not per-item
      // child-spawn bookkeeping rows.
      async listVisitedNodeIdsByFlow(flowIds) {
        if (!Array.isArray(flowIds) || flowIds.length === 0) return new Map();
        try {
          const rows = await db.selectDistinct({
            flowId: flowNodeInvocations.flowId,
            nodeId: flowNodeInvocations.nodeId,
          }).from(flowNodeInvocations)
            .where(and(inArray(flowNodeInvocations.flowId, flowIds), isNull(flowNodeInvocations.subflowKey)));
          const result = new Map();
          for (const { flowId, nodeId } of rows) {
            if (!result.has(flowId)) result.set(flowId, new Set());
            result.get(flowId).add(nodeId);
          }
          return result;
        } catch { return new Map(); }
      },

      close() { try { client.close(); } catch { /* ignore */ } },
    };
  } catch (err) {
    // If the DB can't open, return a no-op store so callers don't need try/catch.
    const noop = () => {};
    return {
      _db: null, flush: async () => {}, findOrCreateInvocation: () => { throw new Error("node-executions store unavailable"); },
      getInvocation: async () => null, listInvocationIdsForNode: async () => [], listSubflowNodeIdsForFlow: async () => [],
      write: noop, queryByPipeline: async () => [],
      writeResult: async () => ({ ok: false, reason: "db-error" }),
      listResultsForFlow: async () => [],
      getResultForNode: async () => null,
      writeGateRecord: async () => ({ ok: false, reason: "db-error" }),
      getGateRecord: async () => null,
      listGateRecordsForFlow: async () => ({}),
      getLatestExecutionForNode: async () => null,
      getLatestInvocationForNode: async () => null,
      getLatestInvocationInvokedAt: async () => null,
      getExecution: async () => null, getLatestExecutionOutcomeForItem: async () => null,
      summarizeChildrenByFlowIds: async () => new Map(),
      listInvocationsForFlow: async () => [], listExecutionsForInvocation: async () => [],
      hasInvocations: async () => new Set(),
      listVisitedNodeIdsByFlow: async () => new Map(),
      close: noop,
    };
  }
}
