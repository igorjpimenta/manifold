// session-index.js — durable flowId→sessionKey index. Adapter-facing state:
// it stores GATEWAY keys (`agent:<agent>:subagent:mf-…`), never core's opaque
// `mf-…` handles. Nothing in the pull world writes new rows — record() is
// kept for interface compatibility; reads (hasSessionForNode,
// flowIdForSessionKey) answer from history for boot revival and taskflow
// delegation. Reads take gateway keys verbatim — callers holding task_runs
// rows already have that shape, no conversion needed.
//
// Storage: defaultDbPath() below (resolveStorePaths().runtimeDb —
// package-owned, see state/paths.js).
//
// INSERT OR IGNORE via UNIQUE(flow_id, session_key) — recording the same
// session key twice (e.g. on error-retry with the same key) is a no-op.

import { mkdirSync } from "node:fs";
import { eq, and } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { flowSessions } from "./schema.js";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

export async function createSessionIndex({ dbPath = defaultDbPath(), logger } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  let db, client;
  try {
    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
    ({ db, client } = await getSharedDb(dbPath));
  } catch (err) {
    logger?.warn?.(`manifold session-index: failed to open DB: ${err?.message ?? err}`);
    // Return a no-op shim so callers don't need to null-check.
    return { async record() {}, async hasSessionForNode() { return false; }, async flowIdForSessionKey() { return null; }, close() {} };
  }

  // `agent` is accepted and ignored: retained so older callers passing it
  // keep working.
  async function record({ flowId, sessionKey, nodeId = null, branchItemId = null, agent = null } = {}) {
    void agent;
    if (!flowId || !sessionKey) return;
    try {
      await withSharedWrite(dbPath, () =>
        db.insert(flowSessions).values({
          id: randomUUID(), flowId, sessionKey,
          nodeId: nodeId ?? null, branchItemId: branchItemId ?? null,
          spawnedAt: Date.now(),
        }).onConflictDoNothing()
      );
    } catch (err) {
      logger?.warn?.(`manifold session-index: record failed: ${err?.message ?? err}`);
    }
  }

  // Was a session ever recorded for this (flowId, nodeId)? Answers from
  // history only — nothing in the pull world writes new rows. Used by
  // boot/taskflow-revive.js to resolve a task_runs session key back to its
  // flow.
  async function hasSessionForNode(flowId, nodeId) {
    if (!flowId || !nodeId) return false;
    try {
      const rows = await db.select({ x: flowSessions.id }).from(flowSessions)
        .where(and(eq(flowSessions.flowId, flowId), eq(flowSessions.nodeId, nodeId)))
        .limit(1);
      return rows.length > 0;
    } catch (err) {
      logger?.warn?.(`manifold session-index: hasSessionForNode failed: ${err?.message ?? err}`);
      return false;
    }
  }

  // Reverse of the above: given a session key alone (no flowId in hand —
  // e.g. a task_runs row from boot/taskflow-revive.js's orphan scan, which
  // only carries child_session_key), find which flow it belongs to. Used
  // to delegate a manifold-owned orphan to the flow's own revival path
  // instead of touching the session directly.
  async function flowIdForSessionKey(sessionKey) {
    if (!sessionKey) return null;
    try {
      const rows = await db.select({ x: flowSessions.flowId }).from(flowSessions)
        .where(eq(flowSessions.sessionKey, sessionKey))
        .limit(1);
      return rows[0]?.x ?? null;
    } catch (err) {
      logger?.warn?.(`manifold session-index: flowIdForSessionKey failed: ${err?.message ?? err}`);
      return null;
    }
  }

  return {
    record,
    hasSessionForNode,
    flowIdForSessionKey,
    close() { try { client.close(); } catch { /* ignore */ } },
    _db: client,
  };
}
