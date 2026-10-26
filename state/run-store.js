// run-store.js — manifold-owned flow_runs in runtime.sqlite.
//
// This is the sole read/write store for manifold flows. No dual-write;
// registry.sqlite is never touched. All executor transitions go through the
// CAS methods here.
//
// CAS pattern: every active-state write is revision-checked.
//   UPDATE ... WHERE flow_id = ? AND revision = ? AND status NOT IN (terminals)
// If rowsAffected === 0 → disambiguate via SELECT revision, status to return the
// right code: not_found | terminal | revision_conflict.

import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { eq, and, notInArray, inArray, isNull, isNotNull, ne, desc, sql } from "drizzle-orm";
import { alias } from "drizzle-orm/sqlite-core";
import { getSharedDb, withSharedWrite, defaultRuntimeDbPath } from "./db.js";
import { flowRuns, flowNodeInvocations } from "./schema.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { enqueue, flush as flushBatchWriter } from "./batch-writer.js";

const DEFAULT_DB_PATH = defaultRuntimeDbPath();

// Exported so callers deciding "is this flow cancellable / already done"
// (e.g. the CLI's and dashboard's own cancel handlers) check against the
// real terminal set instead of hand-rolling their own allowlist of active
// statuses — a `status !== "running" && status !== "blocked"` check misses
// "queued" (an admission-pool wait, very much non-terminal) and wrongly
// refuses to cancel it.
export const TERMINAL_STATUSES = ["succeeded", "failed", "cancelled", "skipped"];
export const TERMINAL_STATUS_SET = new Set(TERMINAL_STATUSES);
const TERMINAL_SET = TERMINAL_STATUS_SET;

// ── store factory ────────────────────────────────────────────────────────────

export function createRunStore(opts = {}) {
  const _p = _createRunStoreImpl(opts);
  return new Proxy({}, { get(_, k) {
    if (k === "then" || typeof k === "symbol") return undefined;
    if (k === "ready") return () => _p.then(() => {});
    return (...a) => _p.then(s => { if (typeof s[k] !== "function") throw new TypeError(`run-store: ${String(k)} is not a function`); return s[k](...a); });
  } });
}

async function _createRunStoreImpl({ logger, dbPath = DEFAULT_DB_PATH } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  // ── CAS failure classifier ───────────────────────────────────────────────

  async function _casClassify(flowId) {
    const rows = await db.select({ revision: flowRuns.revision, status: flowRuns.status })
      .from(flowRuns).where(eq(flowRuns.flowId, flowId));
    const row = rows[0] ?? null;
    if (!row) return { applied: false, code: "not_found" };
    if (TERMINAL_SET.has(row.status)) return { applied: false, code: "terminal" };
    return { applied: false, code: "revision_conflict" };
  }

  // ── public API ───────────────────────────────────────────────────────────

  return {
    async insert({ flowId, ownerKey, pipelineId, revision, status,
                   lastStep, stateJson, createdAt, updatedAt, parentInvocationId }) {
      try {
        await withSharedWrite(dbPath, () => db.insert(flowRuns).values({
          flowId,
          ownerKey:           ownerKey ?? "",
          pipelineId:         pipelineId ?? null,
          revision:           revision ?? 0,
          status,
          lastStep:           lastStep ?? null,
          stateJson:          _str(stateJson) ?? "{}",
          createdAt:          createdAt ?? Date.now(),
          updatedAt:          updatedAt ?? Date.now(),
          parentInvocationId: parentInvocationId ?? null,
        }).onConflictDoNothing());
      } catch {}
    },

    // CAS: advance to next running step (clears wait state).
    async casSetRunning(flowId, { expectedRevision, lastStep, stateJson }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "running",
            revision:    sql`${flowRuns.revision} + 1`,
            lastStep:    lastStep ?? null,
            stateJson:   _str(stateJson),
            waitJson:    null,
            statusReason: null,
            updatedAt:   now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          return await _casClassify(flowId);
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // CAS: block on external signal (human-gate).
    async casSetWaiting(flowId, { expectedRevision, lastStep, blockingNodeId, stateJson, waitJson, statusReason }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "blocked",
            revision:    sql`${flowRuns.revision} + 1`,
            lastStep:    lastStep ?? null,
            stateJson:   _str(stateJson),
            waitJson:    _str(waitJson),
            statusReason: statusReason ?? null,
            updatedAt:   now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          return await _casClassify(flowId);
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // CAS: block on an internal resource (a shared admission-pool slot,
    // cross-process lock) — same shape as casSetWaiting, distinct status so
    // consumers can tell "waiting on an operator decision" from "waiting
    // on a resource that will free itself" without inspecting wait_json.kind.
    async casSetQueued(flowId, { expectedRevision, lastStep, blockingNodeId, stateJson, waitJson, statusReason }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "queued",
            revision:    sql`${flowRuns.revision} + 1`,
            lastStep:    lastStep ?? null,
            stateJson:   _str(stateJson),
            waitJson:    _str(waitJson),
            statusReason: statusReason ?? null,
            updatedAt:   now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          return await _casClassify(flowId);
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // CAS: terminal success.
    async casFinish(flowId, { expectedRevision, stateJson }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "succeeded",
            revision:    sql`${flowRuns.revision} + 1`,
            stateJson:   _str(stateJson),
            waitJson:    null,
            statusReason: null,
            updatedAt:   now,
            endedAt:     now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          return await _casClassify(flowId);
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // CAS: terminal skip (pipeline ran, nothing to output).
    async casSkip(flowId, { expectedRevision, stateJson, reason }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "skipped",
            revision:    sql`${flowRuns.revision} + 1`,
            stateJson:   _str(stateJson),
            waitJson:    null,
            statusReason: reason ?? null,
            updatedAt:   now,
            endedAt:     now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          return await _casClassify(flowId);
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // CAS: terminal failure. current_step stays frozen at the failing node.
    async casFail(flowId, { expectedRevision, stateJson, statusReason }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "failed",
            revision:    sql`${flowRuns.revision} + 1`,
            stateJson:   _str(stateJson),
            statusReason: statusReason ?? null,
            waitJson:    null,
            updatedAt:   now,
            endedAt:     now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          return await _casClassify(flowId);
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // CAS: force a terminal flow back to 'blocked'.
    // Returns { applied: false, code: "not_terminal" } when the row exists,
    // the revision matches, but its status is not in TERMINAL_STATUSES.
    async forceBlock(flowId, { expectedRevision, stateJson, statusReason, ownerKey }) {
      try {
        return await withSharedWrite(dbPath, async () => {
          const now = Date.now();
          const r = await db.update(flowRuns).set({
            status:      "blocked",
            revision:    sql`${flowRuns.revision} + 1`,
            stateJson:   _str(stateJson),
            waitJson:    null,
            statusReason: statusReason ?? null,
            endedAt:     null,
            ownerKey:    ownerKey ?? "",
            updatedAt:   now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            eq(flowRuns.revision, expectedRevision),
            inArray(flowRuns.status, TERMINAL_STATUSES),
          ));
          if (r.rowsAffected > 0) return { applied: true };
          const rows = await db.select({ revision: flowRuns.revision, status: flowRuns.status })
            .from(flowRuns).where(eq(flowRuns.flowId, flowId));
          const row = rows[0] ?? null;
          if (!row) return { applied: false, code: "not_found" };
          if (Number(row.revision) !== expectedRevision) return { applied: false, code: "revision_conflict" };
          return { applied: false, code: "not_terminal" };
        });
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    // Non-CAS cancel: terminal guard only. Returns { cancelled: boolean }.
    async cancelFlow(flowId, reason = null) {
      try {
        const now = Date.now();
        const r = await withSharedWrite(dbPath, () => db.update(flowRuns).set({
          status:      "cancelled",
          waitJson:    null,
          statusReason: reason ?? null,
          updatedAt:   now,
          endedAt:     now,
        }).where(and(
          eq(flowRuns.flowId, flowId),
          notInArray(flowRuns.status, TERMINAL_STATUSES),
        )));
        return { cancelled: r.rowsAffected > 0 };
      } catch {
        return { cancelled: false };
      }
    },

    // Stamps state_json before the cancel status flip.
    async stampStateJson(flowId, stateJson) {
      try {
        await withSharedWrite(dbPath, () => db.update(flowRuns)
          .set({ stateJson: _str(stateJson) })
          .where(eq(flowRuns.flowId, flowId)));
      } catch {}
    },

    // Atomic combination of stampStateJson() + cancelFlow(), for
    // state-store.js's cancel(): the state_json stamp and the cancel
    // status flip must commit together — as two separate awaited writes a
    // crash between them could leave state_json updated but status not
    // flipped to 'cancelled', or vice versa. One db.batch() call commits
    // both inside a single transaction instead.
    // Returns the same { cancelled } shape cancelFlow() alone returns,
    // read from that statement's own rowsAffected within the batch —
    // db.batch() preserves each query's normal per-statement result shape,
    // not just an overall success flag.
    async cancelWithStateJson(flowId, stateJson, reason = null) {
      try {
        const results = await withSharedWrite(dbPath, () => {
          const now = Date.now();
          const queries = [];
          if (stateJson != null) {
            queries.push(db.update(flowRuns)
              .set({ stateJson: _str(stateJson) })
              .where(eq(flowRuns.flowId, flowId)));
          }
          queries.push(db.update(flowRuns).set({
            status:       "cancelled",
            waitJson:     null,
            statusReason: reason ?? null,
            updatedAt:    now,
            endedAt:      now,
          }).where(and(
            eq(flowRuns.flowId, flowId),
            notInArray(flowRuns.status, TERMINAL_STATUSES),
          )));
          return db.batch(queries);
        });
        const cancelResult = results[results.length - 1];
        return { cancelled: cancelResult.rowsAffected > 0 };
      } catch {
        return { cancelled: false };
      }
    },

    // Stamps cancel_requested_at flag (no revision check — just a signal).
    // Deferred via batch-writer: requestCancel() (state-store.js) awaits
    // this purely to sequence before returning { applied: true } — it
    // never inspects a result, so there's no read-after-write to preserve.
    // NOT the same as cancelFlow()/stampStateJson() below — those feed a
    // caller-visible { cancelled } result that gates real side effects
    // (event emission, resume triggers), so they stay immediate. The
    // deferred batch itself is protected at flush time — see batch-writer.js.
    stampCancelRequested(flowId) {
      const now = Date.now();
      enqueue(dbPath, db.update(flowRuns)
        .set({ cancelRequestedAt: now, updatedAt: now })
        .where(eq(flowRuns.flowId, flowId)));
    },

    // Revision-checked state_json update for flows that are in the DB but not
    // in the executor's in-process Map (post-restart blocked flows).
    async updateStateJsonCAS(flowId, expectedRevision, stateJson) {
      try {
        const r = await withSharedWrite(dbPath, () => db.update(flowRuns).set({
          stateJson: _str(stateJson),
          revision:  sql`${flowRuns.revision} + 1`,
          updatedAt: Date.now(),
        }).where(and(
          eq(flowRuns.flowId, flowId),
          eq(flowRuns.revision, expectedRevision),
        )));
        if (r.rowsAffected === 0) return { applied: false, code: "revision_conflict" };
        return { applied: true, via: "run-store-cas" };
      } catch (err) {
        return { applied: false, code: "run_store_failed", error: err?.message ?? String(err) };
      }
    },

    async get(flowId) {
      try {
        const rows = await db.select().from(flowRuns).where(eq(flowRuns.flowId, flowId));
        return mapRow(rows[0] ?? null);
      } catch { return null; }
    },

    async list() {
      try {
        const rows = await db.select().from(flowRuns)
          .where(isNull(flowRuns.parentInvocationId))
          .orderBy(desc(flowRuns.updatedAt));
        return rows.map(mapRow);
      } catch { return []; }
    },

    async listFor(pipelineId) {
      try {
        const rows = await db.select().from(flowRuns)
          .where(eq(flowRuns.pipelineId, pipelineId))
          .orderBy(desc(flowRuns.updatedAt));
        return rows.map(mapRow);
      } catch { return []; }
    },

    // The child spawned for one subflow-item invocation, if it exists.
    async getByParentInvocationId(invocationId) {
      try {
        const rows = await db.select().from(flowRuns)
          .where(eq(flowRuns.parentInvocationId, invocationId));
        return mapRow(rows[0] ?? null);
      } catch { return null; }
    },

    // Every child spawned for a set of sibling invocations.
    async listForParentInvocations(invocationIds) {
      if (!Array.isArray(invocationIds) || invocationIds.length === 0) return [];
      try {
        const rows = await db.select().from(flowRuns)
          .where(inArray(flowRuns.parentInvocationId, invocationIds));
        return rows.map(mapRow);
      } catch { return []; }
    },

    // ── Read-only dashboard/route queries ────────────────────────

    // list() with optional includeChildren flag (default: top-level only).
    // The no-arg form is identical to the original list() above.
    async list({ includeChildren = false } = {}) {
      const q = includeChildren
        ? db.select().from(flowRuns).orderBy(desc(flowRuns.updatedAt))
        : db.select().from(flowRuns)
            .where(isNull(flowRuns.parentInvocationId))
            .orderBy(desc(flowRuns.updatedAt));
      return (await q).map(mapRow);
    },

    // Returns all non-terminal flows (ended_at IS NULL) across all pipelines,
    // including children. Used by live-bridge's one-shot startup seed —
    // only active flows need tracking set entries, so O(active) not O(all).
    async listActive() {
      return (await db.select().from(flowRuns)
        .where(isNull(flowRuns.endedAt))
        .orderBy(desc(flowRuns.updatedAt))).map(mapRow);
    },

    // { [status]: count } for top-level runs, optionally filtered by pipeline.
    // Additive dashboard data: a failed count must degrade to no counts, the
    // same contract as get/list — GET /manifold/runs renders the runs list
    // whatever this returns, and letting it throw turns an additive card
    // into a 500 for the whole response.
    async listStatusCounts({ pipelineId } = {}) {
      try {
        const conditions = [isNull(flowRuns.parentInvocationId)];
        if (pipelineId) conditions.push(eq(flowRuns.pipelineId, pipelineId));
        const rows = await db.select({
          status: flowRuns.status,
          n:      sql`COUNT(*)`.mapWith(Number),
        }).from(flowRuns)
          .where(and(...conditions))
          .groupBy(flowRuns.status);
        const out = {};
        for (const { status, n } of rows) out[status ?? "unknown"] = n;
        return out;
      } catch {
        return {};
      }
    },

    // Aggregate stats for a single pipeline in two queries.
    async statsFor(pipelineId, { todayStartMs = 0, sevenDaysMs = 0 } = {}) {
      const where = and(eq(flowRuns.pipelineId, pipelineId), isNull(flowRuns.parentInvocationId));
      const [row] = await db.select({
        totalRuns:       sql`COUNT(*)`.mapWith(Number),
        lastRunAtMs:     sql`MAX(${flowRuns.updatedAt})`.mapWith(Number),
        todayTotal:      sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} THEN 1 ELSE 0 END)`.mapWith(Number),
        todaySucceeded:  sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} = 'succeeded' THEN 1 ELSE 0 END)`.mapWith(Number),
        todayFailed:     sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} = 'failed' THEN 1 ELSE 0 END)`.mapWith(Number),
        todayActive:     sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} NOT IN ('succeeded','failed','skipped','cancelled') THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dTotal:     sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dSucceeded: sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} AND ${flowRuns.status} = 'succeeded' THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dFailed:    sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} AND ${flowRuns.status} = 'failed' THEN 1 ELSE 0 END)`.mapWith(Number),
      }).from(flowRuns).where(where);
      const [lastRow] = await db.select({ status: flowRuns.status })
        .from(flowRuns).where(where)
        .orderBy(desc(flowRuns.updatedAt)).limit(1);
      return {
        totalRuns:       row?.totalRuns       ?? 0,
        lastRunAtMs:     row?.lastRunAtMs      ?? null,
        lastRunStatus:   lastRow?.status       ?? null,
        todayTotal:      row?.todayTotal       ?? 0,
        todaySucceeded:  row?.todaySucceeded   ?? 0,
        todayFailed:     row?.todayFailed      ?? 0,
        todayActive:     row?.todayActive      ?? 0,
        last7dTotal:     row?.last7dTotal      ?? 0,
        last7dSucceeded: row?.last7dSucceeded  ?? 0,
        last7dFailed:    row?.last7dFailed     ?? 0,
      };
    },

    // Narrow 5-column rows for succeeded peers of a pipeline.
    async listPeers({ pipelineId, excludeFlowId } = {}) {
      const where = excludeFlowId
        ? and(eq(flowRuns.pipelineId, pipelineId), eq(flowRuns.status, "succeeded"), isNull(flowRuns.parentInvocationId), ne(flowRuns.flowId, excludeFlowId))
        : and(eq(flowRuns.pipelineId, pipelineId), eq(flowRuns.status, "succeeded"), isNull(flowRuns.parentInvocationId));
      return db.select({
        flowId:    flowRuns.flowId,
        createdAt: flowRuns.createdAt,
        updatedAt: flowRuns.updatedAt,
        endedAt:   flowRuns.endedAt,
        revision:  flowRuns.revision,
      }).from(flowRuns).where(where).orderBy(desc(flowRuns.endedAt));
    },

    // Queued flows with parent context via LEFT JOIN. Raw client — Drizzle
    // JOIN merges both tables' columns and the alias approach is verbose enough
    // that the raw SQL is clearer here.
    async listQueued() {
      try {
        const { rows } = await client.execute(`
          SELECT
            fr.flow_id AS flowId,
            fr.last_step AS lastStep, fr.status_reason AS statusReason,
            fr.wait_json AS waitJson, fr.created_at AS createdAt,
            fr.updated_at AS updatedAt,
            COALESCE(pr.pipeline_id, fr.pipeline_id) AS pipelineId,
            inv.flow_id AS parentFlowId, inv.node_id AS nodeId,
            inv.subflow_key AS subflowKey
          FROM flow_runs fr
          LEFT JOIN flow_node_invocations inv ON inv.id = fr.parent_invocation_id
          LEFT JOIN flow_runs pr ON pr.flow_id = inv.flow_id
          WHERE fr.status = 'queued'
          ORDER BY fr.created_at
        `);
        return rows.map(row => {
          let waitJson = null;
          try { if (row.waitJson) waitJson = JSON.parse(row.waitJson); } catch {}
          return {
            flowId:       row.flowId,
            pipelineId:   row.pipelineId ?? null,
            parentFlowId: row.parentFlowId ?? null,
            nodeId:       row.nodeId ?? null,
            subflowKey:   row.subflowKey ?? null,
            lastStep:     row.lastStep ?? null,
            statusReason: row.statusReason,
            waitJson,
            createdAt:    row.createdAt,
            updatedAt:    row.updatedAt,
          };
        });
      } catch { return []; }
    },

    // Cursor-paginated run list for iOS — includes JSON_EXTRACT display fields.
    async listRunsPage({ pipelineId, status, limit = 50, beforeMs, beforeId } = {}) {
      const conditions = [isNull(flowRuns.parentInvocationId)];
      if (pipelineId) conditions.push(eq(flowRuns.pipelineId, pipelineId));
      if (status) conditions.push(eq(flowRuns.status, status));
      if (beforeMs != null && beforeId != null) {
        conditions.push(sql`(${flowRuns.createdAt} < ${beforeMs} OR (${flowRuns.createdAt} = ${beforeMs} AND ${flowRuns.flowId} > ${beforeId}))`);
      }
      const rows = await db.select({
        flowId:         flowRuns.flowId,
        pipelineId:     flowRuns.pipelineId,
        status:         flowRuns.status,
        lastStep:       flowRuns.lastStep,
        statusReason:   flowRuns.statusReason,
        createdAtMs:    flowRuns.createdAt,
        updatedAtMs:    flowRuns.updatedAt,
        endedAtMs:      flowRuns.endedAt,
        displayTitle:   sql`JSON_EXTRACT(${flowRuns.stateJson}, '$._display.title')`,
        displayMetrics: sql`JSON_EXTRACT(${flowRuns.stateJson}, '$._display.metrics')`,
      }).from(flowRuns)
        .where(and(...conditions))
        .orderBy(desc(flowRuns.createdAt), flowRuns.flowId)
        .limit(limit);
      return rows.map(row => {
        let metrics = null;
        if (row.displayMetrics != null) {
          try { metrics = JSON.parse(row.displayMetrics); } catch {}
        }
        return { ...row, displayMetrics: metrics };
      });
    },

    // Map<pipelineId, stats> for ALL pipelines — one GROUP BY aggregate + one
    // window-function query for last-run status (raw client: Drizzle has no
    // window function support).
    async listAllPipelineStats({ todayStartMs = 0, sevenDaysMs = 0 } = {}) {
      const agg = await db.select({
        pipelineId:      flowRuns.pipelineId,
        totalRuns:       sql`COUNT(*)`.mapWith(Number),
        lastRunAtMs:     sql`MAX(${flowRuns.updatedAt})`.mapWith(Number),
        todayTotal:      sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} THEN 1 ELSE 0 END)`.mapWith(Number),
        todaySucceeded:  sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} = 'succeeded' THEN 1 ELSE 0 END)`.mapWith(Number),
        todayFailed:     sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} = 'failed' THEN 1 ELSE 0 END)`.mapWith(Number),
        todaySkipped:    sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} IN ('skipped','cancelled') THEN 1 ELSE 0 END)`.mapWith(Number),
        todayActive:     sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${todayStartMs} AND ${flowRuns.status} IN ('running','blocked') THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dTotal:     sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dSucceeded: sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} AND ${flowRuns.status} = 'succeeded' THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dFailed:    sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} AND ${flowRuns.status} = 'failed' THEN 1 ELSE 0 END)`.mapWith(Number),
        last7dSkipped:   sql`SUM(CASE WHEN ${flowRuns.createdAt} >= ${sevenDaysMs} AND ${flowRuns.status} IN ('skipped','cancelled') THEN 1 ELSE 0 END)`.mapWith(Number),
      }).from(flowRuns)
        .where(and(isNull(flowRuns.parentInvocationId), isNotNull(flowRuns.pipelineId)))
        .groupBy(flowRuns.pipelineId);

      const { rows: lastStatusRows } = await client.execute(`
        SELECT pipeline_id, status FROM (
          SELECT pipeline_id, status,
            ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY updated_at DESC) AS rn
          FROM flow_runs
          WHERE parent_invocation_id IS NULL AND pipeline_id IS NOT NULL
        ) WHERE rn = 1
      `);
      const lastStatusMap = new Map(lastStatusRows.map(r => [r.pipeline_id, r.status]));

      const statsMap = new Map();
      for (const row of agg) {
        if (!row.pipelineId) continue;
        statsMap.set(row.pipelineId, {
          totalRuns:       row.totalRuns,
          lastRunAtMs:     row.lastRunAtMs || null,
          lastRunStatus:   lastStatusMap.get(row.pipelineId) ?? null,
          todayTotal:      row.todayTotal,
          todaySucceeded:  row.todaySucceeded,
          todayFailed:     row.todayFailed,
          todaySkipped:    row.todaySkipped,
          todayActive:     row.todayActive,
          last7dTotal:     row.last7dTotal,
          last7dSucceeded: row.last7dSucceeded,
          last7dFailed:    row.last7dFailed,
          last7dSkipped:   row.last7dSkipped,
        });
      }
      return statsMap;
    },

    // Narrow 5-column snapshot for cost analysis.
    async listCostSnapshot() {
      return db.select({
        flowId:     flowRuns.flowId,
        pipelineId: flowRuns.pipelineId,
        status:     flowRuns.status,
        createdAt:  flowRuns.createdAt,
        updatedAt:  flowRuns.updatedAt,
      }).from(flowRuns).where(isNull(flowRuns.parentInvocationId));
    },

    // stampCancelRequested() defers through batch-writer — for tests that
    // assert read-after-write. Not meant for production use.
    async flush() {
      await flushBatchWriter(dbPath);
    },

    close() { try { client.close(); } catch {} },
    _db: client,
  };
}

// ── helpers ───────────────────────────────────────────────────────────────────

function _str(v) {
  if (v == null) return null;
  if (typeof v === "string") return v;
  return JSON.stringify(v);
}

// Drizzle returns camelCase keys matching the schema field names.
// JSON columns (stateJson, waitJson) are still stored as text — parse them here.
function mapRow(row) {
  if (!row) return null;
  let stateJson = null, waitJson = null;
  try { if (row.stateJson) stateJson = JSON.parse(row.stateJson); } catch {}
  try { if (row.waitJson)  waitJson  = JSON.parse(row.waitJson);  } catch {}
  return {
    flowId:             row.flowId,
    ownerKey:           row.ownerKey,
    pipelineId:         row.pipelineId,
    revision:           Number(row.revision),
    status:             row.status,
    lastStep:           row.lastStep ?? null,
    statusReason:       row.statusReason ?? null,
    stateJson,
    waitJson,
    cancelRequestedAt:  row.cancelRequestedAt ?? null,
    createdAt:          Number(row.createdAt),
    updatedAt:          Number(row.updatedAt),
    endedAt:            row.endedAt ?? null,
    parentInvocationId: row.parentInvocationId ?? null,
  };
}
