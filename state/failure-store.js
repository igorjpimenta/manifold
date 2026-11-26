// failure-store.js — durable flow/branch failure records (runtime.sqlite),
// for operator triage via the Failures surface + deep links.

import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { flowRuns } from "./schema.js";
import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { failureEmitter } from "../events.js";
import { getNodeExecutionsStore } from "./node-executions.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

// Maps (error_type, summary) to the operator action the Inbox offers:
//   retry-step  — system-recoverable; one-tap retry the failed step
//   skip-only   — work is moot (already done, listing closed); skip removes
//   investigate — unknown / engineer-level, or a policy verdict with no
//                 single thrown error to point at
// Computed fresh at read time (see deriveFailureContext) — not stored.
function classifyRecoveryHint(errorType, summary) {
  if (errorType == null) return "retry-step";
  if (errorType === "infra_unavailable") return "retry-step";
  if (errorType === "blocked") {
    const s = String(summary ?? "").toLowerCase();
    return s.includes("workana-forbidden") ? "skip-only" : "investigate";
  }
  // Unknown/untaught error_type — over-surface as "needs a look" rather
  // than silently misroute.
  return "investigate";
}

function summarize(s, maxLen = 280) {
  if (typeof s !== "string") s = String(s ?? "");
  s = s.replace(/\s+/g, " ").trim();
  return s.length > maxLen ? s.slice(0, maxLen - 1) + "…" : s;
}

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

async function _createFailureStoreImpl({ dbPath = defaultDbPath(), logger, nodeExecutionsStore, emit } = {}) {
  const nodeExecutions = nodeExecutionsStore ?? await getNodeExecutionsStore();
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  // execution_id is the conflict target (partial UNIQUE, WHERE
  // execution_id IS NOT NULL): a retry re-entering the same node updates
  // the existing row instead of piling up duplicates. Failures with no
  // execution_id (pipeline never loaded, dead edge, unknown next-node id)
  // fall back to a manual dedup by flow_id — SQLite's NULL != NULL
  // unique-index semantics can't cover a null execution_id.
  //
  // Wrapped in withSharedWrite: a bare client.execute() here can throw
  // SQLITE_BUSY into a CONCURRENT writer's own transaction on this shared
  // connection, not just into itself.
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

    // branch_item_id is display-only — derived here, never used for
    // resolution.
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

  // The item identity (subflowKey) almost never lives on the failing
  // node's own invocation — the engine's dispatch loop always writes
  // subflowKey:null there, because real failure sites run INSIDE a
  // subflow's spawned child flow. The tag lives on the PARENT's
  // invocation of the subflow node that spawned this flow, reachable via
  // flow_runs.parent_invocation_id. `inv` is checked first only for a
  // node whose own invocation somehow carries a subflowKey directly.
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
  // reached succeeded/skipped. Returns a timestamp (ms), not a boolean —
  // the Done view sorts by most-recently-resolved and windows to 7 days.
  //
  // Do NOT extend resolution to cross-flow supersession (matching a later
  // invocation of the same (pipeline_id, node_id, subflow_key) tuple in
  // any flow): subflow_key isn't always a real per-item identity —
  // single-item retry-cascade nodes use a constant literal key for every
  // dispatch — so tuple matching collapses distinct items and can falsely
  // mark one job's failure resolved because a different job's dispatch of
  // the same node succeeded later.
  async function computeResolvedAt(row) {
    try {
      const flowRows = await db.select({ status: flowRuns.status, updatedAt: flowRuns.updatedAt }).from(flowRuns).where(eq(flowRuns.flowId, row.flow_id)).limit(1);
      const flow = flowRows[0];
      if (flow && (flow.status === "succeeded" || flow.status === "skipped")) return Number(flow.updatedAt);
    } catch { /* best-effort */ }
    return null;
  }

  // Reads: `resolved` is computed at read time (deriveFailureContext),
  // not a stored column — filtering happens in JS after derivation.
  // undefined/false → open failures (default "Recover" view);
  // true → resolved ("Done"); null → both.
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

  // Called when a flow definitively fails (node threw with no cascade
  // left, edge resolved to nothing, unknown next-node id, pipeline-not-
  // loaded) AND immediately on every cascade-leg throw regardless of
  // whether the cascade later retries/escalates/succeeds — every throw
  // gets a permanent row. errorType, when known, is the node's own
  // domain-specific reason; null means an ordinary engine-caught throw.
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
        // Fire-and-forget: delivery must never fail the recording.
        try {
          await emit?.("failure.recorded", {
            failureId: insertedId,
            flowId,
            pipelineId: pipelineId ?? null,
            failureNode: failureNode ?? null,
            failureSummary: summary,
            errorType: errorType ?? null,
          });
        } catch {}
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
