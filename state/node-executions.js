// flow_node_invocations: one row per logical dispatch (retries reuse it).
// flow_node_executions: one row per attempt, keyed to its invocation.

import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import path from "node:path";
import os from "node:os";
import { eq, and, inArray, isNull, isNotNull, desc, asc } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { flowNodeInvocations, flowNodeExecutions } from "./schema.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { enqueue, flush as flushBatchWriter } from "./batch-writer.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

const VALID_EXECUTION_KINDS = new Set(["fresh", "retry", "revival", "recheck"]);

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

export function getNodeExecutionsStore(dbPath = defaultDbPath()) {
  if (!_singleton) {
    _singleton = _makeStoreProxy(_createStore(dbPath));
  }
  return _singleton;
}

export function createNodeExecutionsStore(dbPath) {
  return _makeStoreProxy(_createStore(dbPath));
}

async function _createStore(dbPath) {
  try {
    mkdirSync(path.dirname(dbPath), { recursive: true });
    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
    const { db, client } = await getSharedDb(dbPath);

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

    // Find-or-create the invocation for a logical node dispatch.
    // forceNew=true (kind="fresh") creates a new visit_seq row;
    // forceNew=false reuses the latest. Race-safe via INSERT OR IGNORE +
    // re-select.
    //
    // `guard`, if supplied, is evaluated INSIDE the withSharedWrite turn —
    // that queue serializes every write against this dbPath (state/db.js),
    // so a cross-flow identity race (two flow-starts both reading "nothing
    // in flight" before either commits) is forced through one queue: the
    // second caller's guard always observes the first caller's committed
    // row. A false result skips the insert and returns null. guard only
    // ever runs for a genuinely new insert — same-flow reuse is decided by
    // the latest/visitSeq check above.
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
      const row = await _findLatestInvocation(flowId, nodeId, subflowKey);
      if (row) return row.id;
      throw new Error(`node-executions: findOrCreateInvocation lost the race and found nothing on re-select (${flowId}/${nodeId})`);
    }

    return {
      _db: client,

      async flush() {
        await flushBatchWriter(dbPath);
      },

      findOrCreateInvocation,

      async getLatestInvocationForNode(flowId, nodeId, subflowKey) {
        return _findLatestInvocation(flowId, nodeId, subflowKey);
      },

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

      // Callers must thread {invocationId, isNew} from enter() through to
      // write() so the completion row links to the same invocation without
      // a second _findLatestInvocation lookup.
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

      // subflow_key IS NULL scopes this to the flow's own top-level
      // dispatch sequence: child-spawn bookkeeping rows (subflow_key set,
      // written by spawn-child.js, never by enter()) can carry the same
      // node_id and must not be returned here.
      async findCurrentStep(flowId) {
        try {
          // rowid DESC breaks invoked_at ties (Date.now() has ms
          // resolution — two dispatches can land in the same millisecond).
          // rowid is SQLite's insertion-order column; `id` is a random UUID
          // with no correlation to insert order.
          const { rows } = await client.execute({
            sql: `SELECT node_id FROM flow_node_invocations WHERE flow_id = ? AND subflow_key IS NULL ORDER BY invoked_at DESC, rowid DESC LIMIT 1`,
            args: [flowId],
          });
          return rows[0]?.node_id ?? null;
        } catch { return null; }
      },

      // `kind` violations throw OUTSIDE try/catch (fail loud in tests/dev);
      // genuine DB failures keep the fire-and-forget contract. When
      // invocationId + isNewInvocation come from a prior enter() call they
      // are trusted directly, skipping the internal findOrCreateInvocation.
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
        // Deferred via batch-writer (highest-volume write; call sites never
        // await it). executionId is generated up front so the return value
        // doesn't depend on the deferred insert landing.
        const executionId = randomUUID();
        enqueue(dbPath, db.insert(flowNodeExecutions).values({
          id: executionId, invocationId, enteredAt, exitedAt, kind,
        }));
        return { invocationId, executionId };
      },

      // resultTooLargeBytes stays null here — a non-null marks a row whose
      // result was never stored.
      //
      // flow_node_invocations has no CAS/revision of its own, so the write
      // is conditioned on resultWrittenAt still matching the value read
      // under the write lock: a 0-row UPDATE means another writer won the
      // double-completion race, and the caller gets
      // { ok: false, reason: "race" } to log loudly.
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
          // Retried as one unit on SQLITE_BUSY: the retry re-reads
          // resultWrittenAt fresh rather than re-attempting the UPDATE
          // against a now-stale `expected`.
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

      async getExecution(id) {
        try {
          const rows = await db.select().from(flowNodeExecutions).where(eq(flowNodeExecutions.id, id));
          const row = rows[0] ?? null;
          if (!row) return null;
          return { id: row.id, invocationId: row.invocationId, enteredAt: Number(row.enteredAt), exitedAt: Number(row.exitedAt), kind: row.kind };
        } catch { return null; }
      },

      // ACROSS ANY FLOW — keyed by (pipelineId, nodeId, subflowKey), not
      // flowId. Returns identity + timing only; success/failure comes from
      // failure_events for that execution_id.
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
            // `kind` is the LATEST attempt only — a "recheck" after
            // children finish masks an earlier "retry"; this flag scans
            // every execution instead.
            everRetriedOrRevived: execs.some((e) => e.kind === "retry" || e.kind === "revival"),
          };
        });
      },

      // One row per node_id: the chronologically LATEST completed
      // invocation. Re-setting the Map key moves it to the end, so a
      // loop-back re-fire lands at its actual chronological position, not
      // first-seen position (retries reuse one row and never move).
      async listResultsForFlow(flowId) {
        try {
          // result_written_at IS NOT NULL is load-bearing: a re-entry
          // creates a NEW in-flight invocation whose invoked_at is later
          // than its own prior completed one — the per-node dedup below
          // would keep that in-flight row and silently drop the completed
          // value assembleState() needs. Failure mode is a silent
          // livelock: a LOOP node's edge condition never advances.
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

      // null when no invocation exists for this (flowId, nodeId) —
      // distinct from a resultWrittenAt:null row ("dispatched, not yet
      // complete"), so callers can tell the two apart without a second
      // query.
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

      // null = this gate has never written a record — distinct from a
      // record that exists but is empty.
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
    // DB couldn't open: no-op store so callers need no try/catch.
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
