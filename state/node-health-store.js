// node-health-store.js — circuit breaker for any node's agent dispatch.
//
// Storage: ~/.openclaw/runtime.sqlite (sibling of flow_branches, failure_events).
//
// Any "agent"-kind node can opt in via node.circuitBreaker: true, checked by
// the ordinary agent dispatch path regardless of which kind of flow it's
// running inside. When the agent dispatch fails, we record it here so any
// subsequent flow that reaches the same (pipelineId, nodeId) fails
// immediately instead of paying for another doomed LLM call — until an
// operator resets it.
//
// Column shape mirrors flow_runs (state/run-store.js / registry.sqlite): one
// `status` column drives state instead of nullable timestamp columns doing
// double duty as flags, plus generic created_at/updated_at/ended_at. A given
// (pipeline_id, node_id) can have many historical rows across separate block
// episodes; only one may be status='blocked' at a time, enforced by a partial
// unique index rather than a primary key on (pipeline_id, node_id) — resets
// flag the row resolved rather than deleting it, so history survives.

import { randomUUID } from "node:crypto";
import { eq, and } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { enqueue, flush as flushBatchWriter } from "./batch-writer.js";
import { nodeHealth } from "./schema.js";
import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

const INSERT_BLOCKED_SQL = `
  INSERT OR IGNORE INTO node_health
    (id, pipeline_id, node_id, status, reason, blocked_by_flow_id, created_at, updated_at)
  VALUES (?, ?, ?, 'blocked', ?, ?, ?, ?)
`;
const RESOLVE_SQL = `
  UPDATE node_health
  SET status = 'resolved', resolved_via = ?, updated_at = ?, ended_at = ?
  WHERE pipeline_id = ? AND node_id = ? AND status = 'blocked'
`;
const IS_BLOCKED_SQL = `
  SELECT 1 FROM node_health WHERE pipeline_id = ? AND node_id = ? AND status = 'blocked' LIMIT 1
`;

export async function createNodeHealthStore({ logger, dbPath = defaultDbPath() } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  async function isBlocked(pipelineId, nodeId) {
    if (!pipelineId || !nodeId) return false;
    const rows = await db.select({ x: nodeHealth.id }).from(nodeHealth)
      .where(and(eq(nodeHealth.pipelineId, pipelineId), eq(nodeHealth.nodeId, nodeId), eq(nodeHealth.status, "blocked")))
      .limit(1);
    return rows.length > 0;
  }

  // First failure opens a new blocked row. A failure while already blocked
  // is a silent no-op — the partial unique index rejects a second active
  // row for the same (pipelineId, nodeId), so INSERT OR IGNORE just drops it.
  //
  // Deferred via batch-writer: its only caller (executor-nodes.js) awaits
  // this purely to sequence before re-throwing the original dispatch error
  // and never inspects a return value. rowsAffected (and therefore whether
  // this was a genuinely new block vs. a no-op against an already-blocked
  // row) isn't known until flush, so the old conditional "marked unhealthy"
  // log is dropped rather than logged on a guess. reset() below stays
  // immediate — its CLI caller reports a different message to the operator
  // based on the real rowsAffected, so it can't be deferred the same way.
  function markUnhealthy(pipelineId, nodeId, { reason, flowId } = {}) {
    if (!pipelineId || !nodeId) return;
    const now = Date.now();
    enqueue(dbPath, db.insert(nodeHealth).values({
      id: randomUUID(), pipelineId, nodeId, status: "blocked",
      reason: reason ?? "unknown failure", blockedByFlowId: flowId ?? null,
      createdAt: now, updatedAt: now,
    }).onConflictDoNothing());
  }

  async function reset(pipelineId, nodeId, { via } = {}) {
    if (!pipelineId || !nodeId) return { reset: 0 };
    const now = Date.now();
    const r = await withSharedWrite(dbPath, () =>
      db.update(nodeHealth)
        .set({ status: "resolved", resolvedVia: via ?? "operator", updatedAt: now, endedAt: now })
        .where(and(eq(nodeHealth.pipelineId, pipelineId), eq(nodeHealth.nodeId, nodeId), eq(nodeHealth.status, "blocked")))
    );
    if (r.rowsAffected > 0) {
      logger?.info?.(`manifold node-health: ${pipelineId}/${nodeId} reset (${via ?? "operator"})`);
    }
    return { reset: r.rowsAffected };
  }

  async function list(pipelineId, { status } = {}) {
    const where = [];
    const args = [];
    if (pipelineId) { where.push("pipeline_id = ?"); args.push(pipelineId); }
    if (status) { where.push("status = ?"); args.push(status); }
    const sql = `
      SELECT * FROM node_health
      ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
      ORDER BY created_at DESC
    `;
    const { rows } = await client.execute({ sql, args });
    return rows;
  }

  function close() {
    client.close();
  }

  // markUnhealthy() defers through batch-writer — for tests that assert
  // read-after-write. Not meant for production use.
  async function flush() {
    await flushBatchWriter(dbPath);
  }

  return { isBlocked, markUnhealthy, reset, list, flush, close, _db: client };
}
