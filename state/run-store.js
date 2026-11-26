// run-store.js — sole read/write store for manifold flow_runs
// (runtime.sqlite); all executor transitions go through the CAS methods.

import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { eq, and, notInArray, inArray, isNull, isNotNull, ne, desc, sql } from "drizzle-orm";
import { alias } from "drizzle-orm/sqlite-core";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { flowRuns, flowNodeInvocations, pipelines } from "./schema.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { enqueue, flush as flushBatchWriter } from "./batch-writer.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

// Exported so callers check against the real terminal set instead of
// hand-rolling one — a `status !== "running" && status !== "blocked"`
// allowlist misses "queued" (an admission-pool wait, non-terminal) and
// wrongly refuses to cancel it.
export const TERMINAL_STATUSES = ["succeeded", "failed", "cancelled", "skipped"];
export const TERMINAL_STATUS_SET = new Set(TERMINAL_STATUSES);
const TERMINAL_SET = TERMINAL_STATUS_SET;

export function createRunStore(opts = {}) {
  const _p = _createRunStoreImpl(opts);
  return new Proxy({}, { get(_, k) {
    if (k === "then" || typeof k === "symbol") return undefined;
    if (k === "ready") return () => _p.then(() => {});
    return (...a) => _p.then(s => { if (typeof s[k] !== "function") throw new TypeError(`run-store: ${String(k)} is not a function`); return s[k](...a); });
  } });
}

async function _createRunStoreImpl({ logger, dbPath = defaultDbPath() } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  async function _casClassify(flowId) {
    const rows = await db.select({ revision: flowRuns.revision, status: flowRuns.status })
      .from(flowRuns).where(eq(flowRuns.flowId, flowId));
    const row = rows[0] ?? null;
    if (!row) return { applied: false, code: "not_found" };
    if (TERMINAL_SET.has(row.status)) return { applied: false, code: "terminal" };
    return { applied: false, code: "revision_conflict" };
  }

  return {
    async insert({ flowId, ownerKey, pipelineId, revision, status,
                   lastStep, stateJson, createdAt, updatedAt, parentInvocationId }) {
      try {
        await withSharedWrite(dbPath, async () => {
          if (pipelineId != null) {
            const now = Date.now();
            await db.insert(pipelines).values({
              id: pipelineId, name: pipelineId, createdAt: now, updatedAt: now,
            }).onConflictDoNothing();
          }
          await db.insert(flowRuns).values({
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
          }).onConflictDoNothing();
        });
      } catch {}
    },

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

    // Distinct status from 'blocked' so consumers can tell "waiting on an
    // operator decision" from "waiting on a resource that will free
    // itself" without inspecting wait_json.kind.
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

    // last_step is intentionally NOT cleared — it stays frozen at the
    // failing node.
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

    async stampStateJson(flowId, stateJson) {
      try {
        await withSharedWrite(dbPath, () => db.update(flowRuns)
          .set({ stateJson: _str(stateJson) })
          .where(eq(flowRuns.flowId, flowId)));
      } catch {}
    },

    // The state_json stamp and the cancel flip must commit together — as
    // two separate writes a crash between them could leave state_json
    // updated but status not 'cancelled' (or vice versa). One db.batch()
    // commits both in a single transaction; db.batch() preserves each
    // query's per-statement result, so `cancelled` reads that statement's
    // own rowsAffected.
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

    // No revision check — just a signal. Deferred via batch-writer:
    // requestCancel() awaits this purely to sequence before returning, and
    // never inspects a result. NOT deferred for cancelFlow()/stampStateJson()
    // — those feed a caller-visible { cancelled } that gates real side
    // effects, so they stay immediate.
    stampCancelRequested(flowId) {
      const now = Date.now();
      enqueue(dbPath, db.update(flowRuns)
        .set({ cancelRequestedAt: now, updatedAt: now })
        .where(eq(flowRuns.flowId, flowId)));
    },

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

    // ── Read-only dashboard/route queries ──

    // list() with optional includeChildren flag (default: top-level only).
    async list({ includeChildren = false } = {}) {
      const q = includeChildren
        ? db.select().from(flowRuns).orderBy(desc(flowRuns.updatedAt))
        : db.select().from(flowRuns)
            .where(isNull(flowRuns.parentInvocationId))
            .orderBy(desc(flowRuns.updatedAt));
      return (await q).map(mapRow);
    },

    async listActive() {
      return (await db.select().from(flowRuns)
        .where(isNull(flowRuns.endedAt))
        .orderBy(desc(flowRuns.updatedAt))).map(mapRow);
    },

    // Additive dashboard data: must degrade to no counts on failure —
    // GET /manifold/runs renders the runs list whatever this returns, and
    // letting it throw would turn an additive card into a 500 for the
    // whole response.
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

    // Raw client: Drizzle's JOIN merges both tables' columns, and the
    // alias workaround is verbose enough that raw SQL is clearer here.
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

    // Raw client for the last-run query: Drizzle has no window-function
    // support.
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

    async listCostSnapshot() {
      return db.select({
        flowId:     flowRuns.flowId,
        pipelineId: flowRuns.pipelineId,
        status:     flowRuns.status,
        createdAt:  flowRuns.createdAt,
        updatedAt:  flowRuns.updatedAt,
      }).from(flowRuns).where(isNull(flowRuns.parentInvocationId));
    },

    // Drain point for stampCancelRequested()'s deferred batch — for tests
    // that assert read-after-write. Not meant for production use.
    async flush() {
      await flushBatchWriter(dbPath);
    },

    close() { try { client.close(); } catch {} },
    _db: client,
  };
}

function _str(v) {
  if (v == null) return null;
  if (typeof v === "string") return v;
  return JSON.stringify(v);
}

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
