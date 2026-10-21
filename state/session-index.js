// session-index.js — durable flowId→sessionKey index.
//
// Written once per subagent spawn so post-mortem queries can answer
// "which agent sessions did flow X touch?" without walking the full
// ~/.openclaw/agents/ directory tree. NOTE: the dashboard's transcript
// endpoint (routes/runs.js's sendTranscriptsList) does NOT read this
// table — it still walks ~/.openclaw/agents/*/sessions/sessions.json
// on disk directly. This index is written but currently has no reader
// in production code.
//
// Storage: ~/.openclaw/runtime.sqlite (consolidated store; see
// DEFAULT_DB_PATH). ~/.openclaw/flows/sessions.sqlite is only consulted
// as a legacy migration source.
//
// INSERT OR IGNORE via UNIQUE(flow_id, session_key) — recording the same
// session key twice (e.g. on error-retry with the same key) is a no-op.

import { mkdirSync } from "node:fs";
import { eq, and } from "drizzle-orm";
import { getSharedDb, withSharedWrite } from "./db.js";
import { flowSessions } from "./schema.js";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { createMigrator } from "./migrator.js";
import { migrateFromLegacy } from "./consolidate.js";

const DEFAULT_DB_PATH = path.join(os.homedir(), ".openclaw", "runtime.sqlite");
const LEGACY_DB_PATH  = path.join(os.homedir(), ".openclaw", "flows", "sessions.sqlite");

const MIGRATIONS = [
  `CREATE TABLE IF NOT EXISTS flow_sessions (
     id              TEXT    PRIMARY KEY,
     flow_id         TEXT    NOT NULL,
     session_key     TEXT    NOT NULL,
     node_id         TEXT,
     branch_item_id  TEXT,
     spawned_at      INTEGER NOT NULL
   );
   CREATE INDEX IF NOT EXISTS idx_flow_sessions_flow_id
     ON flow_sessions(flow_id);
   CREATE UNIQUE INDEX IF NOT EXISTS idx_flow_sessions_uniq
     ON flow_sessions(flow_id, session_key)`,
];


export async function createSessionIndex({ dbPath = DEFAULT_DB_PATH, legacyDbPath = LEGACY_DB_PATH, logger } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  let db, client;
  try {
    ({ db, client } = await getSharedDb(dbPath));
    await createMigrator(client, { name: "session-index", migrations: MIGRATIONS });
    if (legacyDbPath) await migrateFromLegacy(client, legacyDbPath, ["flow_sessions"], logger);
  } catch (err) {
    logger?.warn?.(`manifold session-index: failed to open DB: ${err?.message ?? err}`);
    // Return a no-op shim so callers don't need to null-check.
    return { async record() {}, async hasSessionForNode() { return false; }, async flowIdForSessionKey() { return null; }, close() {} };
  }

  async function record({ flowId, sessionKey, nodeId = null, branchItemId = null } = {}) {
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
