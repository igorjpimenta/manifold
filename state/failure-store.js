// failure-store.js — durable record of flow / branch failures, for
// operator triage via a consumer's Failures surface + deep links.
//
// Storage:  ~/.openclaw/runtime.sqlite (see DEFAULT_DB_PATH).
//
// Why a separate db file rather than another table in registry.sqlite:
// schema isolation. failure_events is queried independently from
// flow_runs (the dashboard's /api/failures route doesn't need flow state,
// just the snapshot we stamp here). One file = one writer story; no
// cross-table transactions to coordinate.
//
// Schema: this table is deliberately thin — id, flow_id, failed_at,
// failure_summary, invocation_id, execution_id, error_type. Everything else
// a caller might want (pipeline_id, branch_item_id, failure_node,
// recovery_hint, whether it's currently resolved) is reconstructed at READ
// time via deriveFailureContext, joining through invocation_id/execution_id
// to flow_node_invocations/flow_node_executions — the engine's own
// node-dispatch identity system, which this table deliberately does NOT
// duplicate in parallel.
//
// Identity: execution_id is the real conflict target now (partial UNIQUE,
// WHERE execution_id IS NOT NULL) — a flow whose retry path re-enters and
// re-fails the same node updates the existing row instead of piling up
// duplicates. The handful of structurally-unknown-dispatch failures (no
// execution_id at all — pipeline never loaded, dead edge, unknown next-node
// id) fall back to a manual dedup-by-flow_id (see upsertFailureRow) —
// SQLite's NULL != NULL unique-index semantics mean the DB-level conflict
// target can't cover this case on its own.
//
// error_type — the sole classification axis now. A node's own thrown
// NodeFailure carries one of: never_attempted | confirmed_absent |
// inconclusive | infra_unavailable | blocked. A subflow node whose exit
// mode wasn't satisfied gets "subflow-mode-not-satisfied" instead — not a
// thrown error at all, a policy verdict computed by
// engine/child-resume-trigger.js once every spawned child has gone
// terminal. null means an ordinary engine-caught throw with no further
// node-level classification (the common case — still safe to retry).

import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { getSharedDb, withSharedWrite } from "./db.js";
import { flowRuns } from "./schema.js";
import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { createMigrator } from "./migrator.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { failureEmitter } from "../events.js";
import { getNodeExecutionsStore } from "./node-executions.js";

const DEFAULT_DB_PATH = path.join(os.homedir(), ".openclaw", "runtime.sqlite");

const MIGRATIONS = [
  // v1: initial schema. The acked_at/created_at indexes are created only
  // when their columns exist — baseline-shaped tables (which never had
  // those columns) skip them; nothing reads them there.
  async (client) => {
    await client.execute(`CREATE TABLE IF NOT EXISTS failure_events (
     id              TEXT    PRIMARY KEY,
     flow_id         TEXT    NOT NULL,
     pipeline_id     TEXT    NOT NULL,
     branch_item_id  TEXT,
     failed_at       INTEGER NOT NULL,
     failure_node    TEXT,
     failure_kind    TEXT    NOT NULL,
     failure_summary TEXT    NOT NULL,
     state_excerpt   TEXT,
     acked_at        INTEGER,
     acked_via       TEXT,
     created_at      INTEGER NOT NULL,
     UNIQUE(flow_id, branch_item_id, failure_node)
   )`);
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('failure_events')");
    const cols = rows.map((r) => r.name);
    if (cols.includes("acked_at")) {
      await client.execute(`CREATE INDEX IF NOT EXISTS idx_failures_acked   ON failure_events(acked_at)`);
    }
    if (cols.includes("created_at")) {
      await client.execute(`CREATE INDEX IF NOT EXISTS idx_failures_created ON failure_events(created_at)`);
    }
    await client.execute(`CREATE INDEX IF NOT EXISTS idx_failures_flow    ON failure_events(flow_id)`);
  },
  // v2–v10: additive columns. Each uses a function migration guarded by
  // pragma_table_info so existing installs with columns already applied
  // don't throw "duplicate column name" on first-time migrator adoption.
  ...[
    ["resolved_at",     "INTEGER"],
    ["resolved_via",    "TEXT"],
    ["recovery_hint",   "TEXT"],
    ["ignored_at",      "INTEGER"],
    ["ignored_by",      "TEXT"],
    ["retry_count",     "INTEGER DEFAULT 0"],
    ["last_retried_at", "INTEGER"],
    ["invocation_id",   "TEXT"],
    ["execution_id",    "TEXT"],
  ].map(([col, type]) => async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('failure_events')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes(col)) await client.execute(`ALTER TABLE failure_events ADD COLUMN ${col} ${type}`);
  }),
  // v11: index for invocation-keyed lookups. Self-healing (never just a
  // bare CREATE INDEX); harmless no-op on any install that already has
  // invocation_id and no child_flow_id.
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('failure_events')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes("invocation_id")) await client.execute("ALTER TABLE failure_events ADD COLUMN invocation_id TEXT");
    if (cols.includes("child_flow_id")) await client.execute("ALTER TABLE failure_events DROP COLUMN child_flow_id");
    await client.execute("CREATE INDEX IF NOT EXISTS idx_failures_invocation ON failure_events(invocation_id)");
  },
  // v12 (schema redesign): drop the 14 columns that duplicated
  // data reachable via invocation_id/execution_id, or that supported the
  // now-removed Skip action / server-side ignore tracking, or that were
  // already confirmed dead. Re-anchor identity on execution_id.
  //
  // Table-recreate-and-swap, not a sequence of ALTER TABLE DROP COLUMN —
  // SQLite refuses to drop branch_item_id/failure_node via plain DROP
  // COLUMN because they're part of the original inline UNIQUE(flow_id,
  // branch_item_id, failure_node) table constraint (dropping a column a
  // constraint references isn't just an index problem, it needs the whole
  // table rebuilt). Same technique node-executions.js's v3 migration
  // already used for exactly this reason — precedented, not novel.
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('failure_events')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes("pipeline_id")) return; // already migrated (idempotent re-run)
    await client.execute(`
      CREATE TABLE failure_events_v12 (
        id              TEXT    PRIMARY KEY,
        flow_id         TEXT    NOT NULL,
        failed_at       INTEGER NOT NULL,
        failure_kind    TEXT    NOT NULL,
        failure_summary TEXT    NOT NULL,
        invocation_id   TEXT,
        execution_id    TEXT
      )
    `);
    // Copy every row with no execution_id unconditionally — the new
    // partial UNIQUE index only covers non-null execution_id, so these
    // can't collide with it regardless of how many share a null value.
    await client.execute(`
      INSERT INTO failure_events_v12 (id, flow_id, failed_at, failure_kind, failure_summary, invocation_id, execution_id)
      SELECT id, flow_id, failed_at, failure_kind, failure_summary, invocation_id, execution_id
      FROM failure_events WHERE execution_id IS NULL
    `);
    // Rows WITH an execution_id: dedupe before the new UNIQUE index can
    // even be created. The old (flow_id, branch_item_id, failure_node)
    // identity let genuine duplicates through whenever branch_item_id was
    // recorded inconsistently for the same real event — exactly the
    // identity mismatch this whole redesign exists to fix. Keep the row
    // with the latest failed_at per execution_id (most current snapshot of
    // that failure), drop the rest.
    await client.execute(`
      INSERT INTO failure_events_v12 (id, flow_id, failed_at, failure_kind, failure_summary, invocation_id, execution_id)
      SELECT id, flow_id, failed_at, failure_kind, failure_summary, invocation_id, execution_id FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY execution_id ORDER BY failed_at DESC, id DESC) AS rn
        FROM failure_events WHERE execution_id IS NOT NULL
      ) WHERE rn = 1
    `);
    await client.execute("DROP TABLE failure_events");
    await client.execute("ALTER TABLE failure_events_v12 RENAME TO failure_events");
    await client.execute("CREATE INDEX IF NOT EXISTS idx_failures_flow ON failure_events(flow_id)");
    await client.execute("CREATE INDEX IF NOT EXISTS idx_failures_invocation ON failure_events(invocation_id)");
    await client.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_failures_execution ON failure_events(execution_id) WHERE execution_id IS NOT NULL");
  },
  // v13 (Phase 1.5, "Eradicate soft errors"): error_type moves here from
  // flow_node_executions, which no longer stores error content at all —
  // this table is now the sole source. Same vocabulary as before: never_attempted|confirmed_absent|
  // inconclusive|infra_unavailable|blocked, or null.
  // Guarded: skips when the column already exists (baseline-shaped).
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('failure_events')");
    if (rows.map((r) => r.name).includes("error_type")) return;
    await client.execute("ALTER TABLE failure_events ADD COLUMN error_type TEXT");
  },
  // v14: drop failure_kind. Table-recreate-and-swap, not ALTER TABLE DROP
  // COLUMN, same reason as v12 (SQLite can rebuild around a dropped column
  // fine, but a plain DROP COLUMN on some SQLite builds still balks at the
  // NOT NULL constraint failure_kind carried — recreating sidesteps the
  // question entirely).
  // Every existing "subflow-mode-not-satisfied" row had error_type NULL
  // (nothing ever set both at once), so its one bit of real information is
  // carried forward into error_type here; every other historical
  // failure_kind value (node-error, submit-blocked, etc.) simply drops,
  // since nothing has read failure_kind for any purpose other than
  // that one comparison since the redesign.
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('failure_events')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes("failure_kind")) return; // already migrated (idempotent re-run)
    await client.execute(`
      CREATE TABLE failure_events_v14 (
        id              TEXT    PRIMARY KEY,
        flow_id         TEXT    NOT NULL,
        failed_at       INTEGER NOT NULL,
        failure_summary TEXT    NOT NULL,
        invocation_id   TEXT,
        execution_id    TEXT,
        error_type      TEXT
      )
    `);
    await client.execute(`
      INSERT INTO failure_events_v14 (id, flow_id, failed_at, failure_summary, invocation_id, execution_id, error_type)
      SELECT id, flow_id, failed_at, failure_summary, invocation_id, execution_id,
             COALESCE(error_type, CASE WHEN failure_kind = 'subflow-mode-not-satisfied' THEN failure_kind ELSE NULL END)
      FROM failure_events
    `);
    await client.execute("DROP TABLE failure_events");
    await client.execute("ALTER TABLE failure_events_v14 RENAME TO failure_events");
    await client.execute("CREATE INDEX IF NOT EXISTS idx_failures_flow ON failure_events(flow_id)");
    await client.execute("CREATE INDEX IF NOT EXISTS idx_failures_invocation ON failure_events(invocation_id)");
    await client.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_failures_execution ON failure_events(execution_id) WHERE execution_id IS NOT NULL");
  },
];

// Maps a failure's (error_type, summary) to the operator action the iOS
// Inbox should surface for this row:
//   retry-step  — system-recoverable; one-tap retry the failed step
//   skip-only   — work is moot (already done, listing closed); skip removes
//   investigate — unknown / engineer-level, or a policy-level verdict with
//                 no single thrown error to point at; operator looks
//                 before acting
//
// error_type is null for the common case — an ordinary engine-caught throw
// with no further node-level classification — which is still a plain
// "just retry it" situation. "blocked" is the one error_type whose summary
// text actually distinguishes a genuinely different case (a listing
// already closed/awarded — "workana-forbidden" — vs. an unrecognized
// block, which stays investigate rather than guessing). "redraft"/"wait"
// aren't reachable today: nothing currently sets an error_type/summary
// combination that would justify either — see git history if a producer
// for either ever comes back.
//
// Computed fresh at read time (see deriveFailureContext) — not stored.
function classifyRecoveryHint(errorType, summary) {
  if (errorType == null) return "retry-step";
  if (errorType === "infra_unavailable") return "retry-step";
  if (errorType === "blocked") {
    const s = String(summary ?? "").toLowerCase();
    return s.includes("workana-forbidden") ? "skip-only" : "investigate";
  }
  // confirmed_absent, inconclusive, never_attempted, subflow-mode-not-
  // satisfied, and any future error_type this hasn't been taught yet —
  // safer to over-surface as "needs a look" than to silently misroute.
  return "investigate";
}

function summarize(s, maxLen = 280) {
  if (typeof s !== "string") s = String(s ?? "");
  s = s.replace(/\s+/g, " ").trim();
  return s.length > maxLen ? s.slice(0, maxLen - 1) + "…" : s;
}

// ─── Public API ─────────────────────────────────────────────────────────

export function createFailureStore(opts = {}) {
  const _p = _createFailureStoreImpl(opts);
  return new Proxy({}, { get(_, k) {
    if (k === "then" || typeof k === "symbol") return undefined;
    if (k === "_db") return new Proxy({}, { get(__, method) {
      if (method === "then" || typeof method === "symbol") return undefined;
      return (...a) => _p.then(s => s._db[method](...a));
    } });
    return (...a) => _p.then(s => { if (typeof s[k] !== "function") throw new TypeError(`failure-store: ${String(k)} is not a function`); return s[k](...a); });
  } });
}

async function _createFailureStoreImpl({ dbPath = DEFAULT_DB_PATH, logger, nodeExecutionsStore } = {}) {
  // Injectable for tests (defaults to the real singleton in production) —
  // same DI pattern as engine/executor.js.
  const nodeExecutions = nodeExecutionsStore ?? await getNodeExecutionsStore();
  mkdirSync(path.dirname(dbPath), { recursive: true });
  const { db, client } = await getSharedDb(dbPath);
  await createMigrator(client, { name: "failure-store", migrations: MIGRATIONS });
  // Generated snapshots apply after the legacy chain (see state/migrate.js).
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });

  // ── Write-side identity ────────────────────────────────────────────
  // execution_id is the real conflict target (partial UNIQUE index,
  // v12). When there's no execution_id to anchor to (a handful of
  // structurally-unknown-dispatch engine failures — pipeline never
  // loaded, dead edge, unknown next-node id — or a soft-marker whose
  // best-effort execution lookup came up empty), fall back to a manual
  // dedup by flow_id: SQLite's NULL != NULL unique-index semantics mean
  // the DB-level conflict target can't cover a null execution_id on its
  // own.
  // Returns the row's id on a successful write, null on no-op — the id lets
  // callers re-fetch the fully-derived row (pipeline_id/branch_item_id/
  // failure_node/recovery_hint) before emitting "failure.created", instead
  // of emitting the bare write-time fields the push/WS consumers don't
  // understand.
  // Wrapped in withSharedWrite (state/db.js) — every other writer sharing
  // this same runtime.sqlite connection (run-store.js, admission-queue-
  // store.js) already routes through it, and a bare client.execute() here
  // can throw SQLITE_BUSY into a CONCURRENT writer's own transaction rather
  // than just itself.
  async function upsertFailureRow({ flowId, failedAt, failureSummary, invocationId, executionId, errorType = null }) {
    return withSharedWrite(dbPath, async () => {
      if (executionId) {
        const r = await client.execute({
          sql: `
            INSERT INTO failure_events (id, flow_id, failed_at, failure_summary, invocation_id, execution_id, error_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(execution_id) WHERE execution_id IS NOT NULL DO UPDATE SET
              failed_at       = excluded.failed_at,
              failure_summary = excluded.failure_summary,
              invocation_id   = excluded.invocation_id,
              error_type      = excluded.error_type
          `,
          args: [randomUUID(), flowId, failedAt, failureSummary, invocationId ?? null, executionId, errorType ?? null],
        });
        if (r.rowsAffected === 0) return null;
        const { rows } = await client.execute({ sql: "SELECT id FROM failure_events WHERE execution_id = ?", args: [executionId] });
        return rows[0]?.id ?? null;
      }
      const existing = await client.execute({
        sql: "SELECT id FROM failure_events WHERE flow_id = ? AND execution_id IS NULL LIMIT 1",
        args: [flowId],
      });
      if (existing.rows.length > 0) {
        const r = await client.execute({
          sql: "UPDATE failure_events SET failed_at = ?, failure_summary = ?, invocation_id = ?, error_type = ? WHERE id = ?",
          args: [failedAt, failureSummary, invocationId ?? null, errorType ?? null, existing.rows[0].id],
        });
        return r.rowsAffected > 0 ? existing.rows[0].id : null;
      }
      const newId = randomUUID();
      const r = await client.execute({
        sql: `INSERT INTO failure_events (id, flow_id, failed_at, failure_summary, invocation_id, execution_id, error_type)
              VALUES (?, ?, ?, ?, ?, NULL, ?)`,
        args: [newId, flowId, failedAt, failureSummary, invocationId ?? null, errorType ?? null],
      });
      return r.rowsAffected > 0 ? newId : null;
    });
  }

  // ── Read-side derivation ────────────────────────────────────────────
  // Reconstructs everything NOT stored directly on the row:
  // pipeline_id (via flow_id -> flow_runs, always available), branch_item_id
  // / failure_node (via invocation_id -> flow_node_invocations, only when a
  // real dispatch happened), recovery_hint (pure function, computed fresh),
  // and whether this failure currently reads as resolved.
  async function deriveFailureContext(row) {
    let pipelineId = null;
    try {
      const flowRows = await db.select({ pipelineId: flowRuns.pipelineId }).from(flowRuns).where(eq(flowRuns.flowId, row.flow_id)).limit(1);
      pipelineId = flowRows[0]?.pipelineId ?? null;
    } catch { /* best-effort */ }

    let failureNode = null, inv = null;
    if (row.invocation_id) {
      inv = await nodeExecutions.getInvocation(row.invocation_id);
      if (inv) failureNode = inv.nodeId ?? null;
    }

    // branchItemId ("which item, if this failure happened inside a
    // subflow's spawned child") almost never lives on `inv` directly —
    // see resolveItemInvocation's own comment for why — so it's derived
    // via the same parent-invocation walk, purely for display now
    // (computeResolvedAt no longer uses item identity for resolution).
    const itemInv = await resolveItemInvocation(row, inv);
    const branchItemId = itemInv?.subflowKey ?? null;

    const recoveryHint = classifyRecoveryHint(row.error_type, row.failure_summary);
    const resolvedAt = await computeResolvedAt(row);

    return {
      id: row.id,
      flow_id: row.flow_id,
      pipeline_id: pipelineId,
      branch_item_id: branchItemId,
      failed_at: Number(row.failed_at),
      failure_node: failureNode,
      failure_summary: row.failure_summary,
      recovery_hint: recoveryHint,
      resolved_at: resolvedAt,
      invocation_id: row.invocation_id,
      execution_id: row.execution_id,
      error_type: row.error_type ?? null,
    };
  }

  // Resolves the "item identity" (pipelineId/nodeId/subflowKey of the
  // subflow dispatch this failure's flow was spawned FOR) used by both
  // deriveFailureContext's branch_item_id display field and
  // computeResolvedAt's cross-flow supersession check below.
  //
  // Almost never on the failing node's OWN invocation: engine/executor.js's
  // ordinary dispatch loop always writes subflowKey:null (every real
  // failure site — SUBMIT_CODE, SCORE_LLM, a subflow's own inner nodes —
  // runs INSIDE a subflow's spawned child flow, not as the subflow node
  // itself). The tag lives on the PARENT's own invocation of the subflow
  // node that spawned this failure's flow, reachable via this flow's own
  // flow_runs.parent_invocation_id — the exact lookup child-resume-
  // trigger.js's maybeResumeParent already does to find its own parent.
  // `inv` (the failing node's own invocation) is checked first only for
  // the (currently theoretical) case of a node whose own invocation
  // somehow already carries a subflowKey directly.
  async function resolveItemInvocation(row, inv) {
    if (inv?.subflowKey) return inv;
    try {
      const childRows = await db.select({ parentInvocationId: flowRuns.parentInvocationId }).from(flowRuns).where(eq(flowRuns.flowId, row.flow_id)).limit(1);
      const parentInvocationId = childRows[0]?.parentInvocationId ?? null;
      if (parentInvocationId) {
        const parentInv = await nodeExecutions.getInvocation(parentInvocationId);
        if (parentInv?.subflowKey) return parentInv;
      }
    } catch { /* best-effort */ }
    return null;
  }

  // A failure resolves only via same-flow completion: this exact flow_id
  // reached a clean terminal status (succeeded/skipped) despite this
  // failure. Applies identically to root and subflow-child flows — no
  // special-cased mechanism for children. Returns a timestamp (ms) when
  // resolved, null when still open — not just a boolean, since iOS sorts
  // the Done section by "most recently resolved" and windows it to the
  // last 7 days.
  //
  // Do not extend resolution to cross-flow supersession — matching a
  // LATER invocation of the same (pipeline_id, node_id, subflow_key)
  // tuple in ANY flow:
  // subflow_key isn't always a real per-item identity — single-item
  // retry-cascade nodes use a constant literal key for every dispatch —
  // so tuple matching collapses distinct items and can falsely mark one
  // job's failure resolved because a different job's dispatch of the same
  // node succeeded later. Every flow's own failure stays open until that
  // exact flow_id itself resolves.
  async function computeResolvedAt(row) {
    try {
      const flowRows = await db.select({ status: flowRuns.status, updatedAt: flowRuns.updatedAt }).from(flowRuns).where(eq(flowRuns.flowId, row.flow_id)).limit(1);
      const flow = flowRows[0];
      if (flow && (flow.status === "succeeded" || flow.status === "skipped")) return Number(flow.updatedAt);
    } catch { /* best-effort */ }
    return null;
  }

  // Failures persist to failure_events directly from the engine's
  // throw-catch paths (recordEngineFailure); no terminal-state soft-marker
  // scan feeds this table.

  // ── Reads ──────────────────────────────────────────────────────────
  // `resolved` is computed at read time now (see deriveFailureContext),
  // not a stored column — filtering happens in JS after derivation, not
  // in the SQL WHERE clause. At current table scale (low thousands, no
  // pagination anywhere else on this table either) this is an acceptable
  // tradeoff for the correctness win of not duplicating derivable state;
  // revisit if this table's row count grows enough for per-row derivation
  // cost to matter.
  //   undefined / false → only open failures (the default "Recover" view)
  //   true              → only resolved failures (the "Done" view)
  //   null              → both
  // `flowId` — the cascade-retry walk (routes/operator-retry.js) needs
  // "this specific flow's own open failure, with its real id/error_type" —
  // queryErrorsByFlow returns neither (ErrorEntry shape, for display/
  // errorNodes membership only), so this is the reuse point rather than a
  // bespoke third read method.
  async function list({ resolved, limit = 50, since, flowId } = {}) {
    const where = [];
    const args = [];
    if (since != null) { where.push("failed_at >= ?"); args.push(since); }
    if (flowId != null) { where.push("flow_id = ?"); args.push(flowId); }
    const sqlText = `
      SELECT * FROM failure_events
      ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
      ORDER BY failed_at DESC
    `;
    const { rows } = await client.execute({ sql: sqlText, args });
    const withContext = await Promise.all(rows.map((r) => deriveFailureContext(r)));
    let filtered = withContext;
    if (resolved === undefined || resolved === false) filtered = withContext.filter((r) => r.resolved_at == null);
    else if (resolved === true) filtered = withContext.filter((r) => r.resolved_at != null);
    return filtered.slice(0, limit);
  }

  async function get(id) {
    const { rows } = await client.execute({ sql: "SELECT * FROM failure_events WHERE id = ?", args: [id] });
    const row = rows[0] ?? null;
    return row ? await deriveFailureContext(row) : null;
  }

  // Maps a joined failure_events+flow_node_invocations row to the
  // canonical ErrorEntry shape shared across every consumer.
  function _toErrorEntry(row) {
    return {
      nodeId: row.node_id ?? null,
      subflowKey: row.subflow_key ?? null,
      message: row.failure_summary,
      errorType: row.error_type ?? null,
      at: Number(row.failed_at),
      executionId: row.execution_id ?? null,
      invocationId: row.invocation_id ?? null,
    };
  }

  // flow_node_executions no longer stores error content at all, so "which
  // errors happened on this flow" can only be answered from failure_events
  // now. Same ErrorEntry shape, same "every error for the flow, caller
  // filters by subflowKey if it wants root-level only" contract.
  async function queryErrorsByFlow(flowId) {
    const { rows } = await client.execute({
      sql: `SELECT f.failed_at, f.failure_summary, f.error_type, f.execution_id, f.invocation_id, i.node_id, i.subflow_key
              FROM failure_events f LEFT JOIN flow_node_invocations i ON f.invocation_id = i.id
             WHERE f.flow_id = ?
             ORDER BY f.failed_at ASC`,
      args: [flowId],
    });
    return rows.map(_toErrorEntry);
  }

  async function queryErrorsByFlowIds(flowIds) {
    if (!Array.isArray(flowIds) || flowIds.length === 0) return new Map();
    const placeholders = flowIds.map(() => "?").join(", ");
    const { rows } = await client.execute({
      sql: `SELECT f.flow_id, f.failed_at, f.failure_summary, f.error_type, f.execution_id, f.invocation_id, i.node_id, i.subflow_key
              FROM failure_events f LEFT JOIN flow_node_invocations i ON f.invocation_id = i.id
             WHERE f.flow_id IN (${placeholders})
             ORDER BY f.failed_at ASC`,
      args: flowIds,
    });
    const out = new Map();
    for (const r of rows) {
      if (!out.has(r.flow_id)) out.set(r.flow_id, []);
      out.get(r.flow_id).push(_toErrorEntry(r));
    }
    return out;
  }

  // ── Mutations ──────────────────────────────────────────────────────
  // recordRetry/markIgnored/markUnignored/resolveOpenForBranch and the
  // Skip action they supported are gone. retry_count/
  // last_retried_at are derivable (COUNT/MAX over flow_node_executions
  // WHERE kind='retry' for the invocation) but confirmed not wanted in
  // the UI at all, so no replacement is computed here either. Ignore
  // state moved to iOS device-local storage entirely.

  // Engine-failed recorder — called both when a flow definitively fails
  // (node threw with no cascade left to try, edge resolved to nothing,
  // unknown next-node id, pipeline-not-loaded) AND, as of Phase 1.5,
  // immediately on every cascade-leg throw regardless of whether the
  // cascade goes on to retry/escalate/succeed — every throw gets a
  // permanent row here, no exceptions. errorType, when known, is the
  // node's own domain-specific reason (never_attempted|confirmed_absent|
  // inconclusive|infra_unavailable|blocked), or "subflow-mode-not-
  // satisfied" for the one non-throw producer (engine/child-resume-
  // trigger.js). null means an ordinary engine-caught throw with no
  // further classification — still safe to retry. computeResolvedAt
  // answers "did this ultimately resolve" separately, at read time.
  async function recordEngineFailure({ flowId, pipelineId, failureNode, message, state, invocationId = null, executionId = null, errorType = null }) {
    if (!flowId || !pipelineId) return { recorded: 0 };
    const now = Date.now();
    const summary = summarize(message ?? "engine-failed (no message)");
    try {
      const insertedId = await upsertFailureRow({
        flowId, failedAt: now, failureSummary: summary,
        invocationId, executionId, errorType,
      });
      if (insertedId) {
        logger?.info?.(`manifold failure-store: ${errorType ?? "engine-failed"} ${flowId.slice(0, 8)} (${pipelineId}) at ${failureNode || "<engine>"}`);
        const context = await get(insertedId);
        if (context) failureEmitter.emit("failure.created", context);
        return { recorded: 1 };
      }
      return { recorded: 0 };
    } catch (err) {
      logger?.warn?.(`manifold failure-store: recordEngineFailure failed: ${err?.message ?? err}`);
      return { recorded: 0 };
    }
  }

  return {
    recordEngineFailure,
    list,
    get,
    queryErrorsByFlow,
    queryErrorsByFlowIds,
    close() { try { client.close(); } catch { /* ignore */ } },
    _db: client, // for tests
  };
}
