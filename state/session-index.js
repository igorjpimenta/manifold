// session-index.js — durable flowId→sessionKey index. Adapter-facing state:
// it stores GATEWAY keys (`agent:<agent>:subagent:mf-…`), never core's opaque
// `mf-…` handles — NO schema migration was needed because the stored shape
// is unchanged, only the prefix core mints changed. Core's dispatch path
// reaches this store through the adapter's conversion proxy (which maps the
// opaque dispatch handle to its gateway key via the `agent` param on
// record()); direct record() calls must already carry gateway keys.
// Reads (flowIdForSessionKey) take gateway keys verbatim — callers holding
// task_runs rows already have that shape, no conversion needed.
//
// Storage: ~/.openclaw/runtime.sqlite (see defaultDbPath()).
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

  // `agent` is accepted and ignored: the adapter proxy consumes it to map
  // the opaque handle to its gateway key before delegating here.
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

  // Was a subagent dispatch ever actually ATTEMPTED for this (flowId, nodeId)?
  // record() (above) is called at the top of dispatchAgentWithRetry, always
  // before subagent.run() is awaited. This answers "did dispatch
  // start" independent of whether it finished, blocked, or is still in
  // flight. Used by boot/flow-revival.js to distinguish a flow that crashed
  // mid-turn (revive via continuation prompt) from one that was merely queued
  // waiting for a slot and never actually started (revive via a fresh
  // dispatch instead).
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
  // e.g. a session key observed on the event queue with no flow attached),
  // find which flow it belongs to.
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
