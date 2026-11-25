// session-index.js — durable flowId→session index. Sessions are the
// ids the dispatch used — stored and read verbatim, with no assumed
// shape.
//
// Storage: the store home's runtime.sqlite (resolveStorePaths().runtimeDb).
//
// INSERT OR IGNORE via UNIQUE(flow_id, session_key) — recording the same
// session twice (e.g. on error-retry with the same session) is a no-op.

import { mkdirSync } from "node:fs";
import { eq, and } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { flowSessions, discussThreads } from "./schema.js";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { ensureSessionKeys } from "./session-keys.js";
import { ensureFlowItems } from "./attribution-registries.js";

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
    return { async record() {}, async hasSessionForNode() { return false; }, async flowIdForSessionKey() { return null; }, async listForFlow() { return []; }, async recordThread() {}, async findThread() { return null; }, close() {} };
  }

  async function record({ flowId, session, nodeId = null, branchItemId = null } = {}) {
    if (!flowId || !session) return;
    try {
      await withSharedWrite(dbPath, async () => {
        await ensureSessionKeys(client, [session]);
        await ensureFlowItems(client, [[flowId, branchItemId]]);
        return db.insert(flowSessions).values({
          id: randomUUID(), flowId, session,
          nodeId: nodeId ?? null, branchItemId: branchItemId ?? null,
          spawnedAt: Date.now(),
        }).onConflictDoNothing();
      });
    } catch (err) {
      logger?.warn?.(`manifold session-index: record failed: ${err?.message ?? err}`);
    }
  }

  // Was an agent dispatch ever actually ATTEMPTED for this (flowId, nodeId)?
  // record() (above) is called at the top of dispatchAgentWithRetry, always
  // before the turn is run. This answers "did dispatch
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

  // Reverse of the above: given a session alone (no flowId in hand —
  // e.g. a session observed on the event queue with no flow attached),
  // find which flow it belongs to.
  async function flowIdForSessionKey(session) {
    if (!session) return null;
    try {
      const rows = await db.select({ x: flowSessions.flowId }).from(flowSessions)
        .where(eq(flowSessions.session, session))
        .limit(1);
      return rows[0]?.x ?? null;
    } catch (err) {
      logger?.warn?.(`manifold session-index: flowIdForSessionKey failed: ${err?.message ?? err}`);
      return null;
    }
  }

  // Every session ever recorded for one flow, in spawn order.
  // Transcript listing joins these rows to per-node results (dispatches)
  // and discuss threads (branchItemId = itemId) so attribution comes from
  // stored columns, never from parsing the session itself.
  async function listForFlow(flowId) {
    if (!flowId) return [];
    try {
      const rows = await db.select({
        session: flowSessions.session,
        nodeId: flowSessions.nodeId,
        branchItemId: flowSessions.branchItemId,
        spawnedAt: flowSessions.spawnedAt,
      }).from(flowSessions)
        .where(eq(flowSessions.flowId, flowId))
        .orderBy(flowSessions.spawnedAt);
      return rows.map((r) => ({
        session: r.session,
        nodeId: r.nodeId ?? null,
        branchItemId: r.branchItemId ?? null,
        spawnedAt: Number(r.spawnedAt),
      }));
    } catch (err) {
      logger?.warn?.(`manifold session-index: listForFlow failed: ${err?.message ?? err}`);
      return [];
    }
  }

  // Gate discuss thread identity: one row per (flow, gate, item), upserted
  // on first turn. The roster is the thread map — the harness owns the id.
  // Latest write wins on conflict.
  async function recordThread({ flowId, gateNodeId, itemId, agentId = null, session } = {}) {
    if (!flowId || !gateNodeId || !itemId || !session) return;
    try {
      await withSharedWrite(dbPath, async () => {
        await ensureSessionKeys(client, [session]);
        return db.insert(discussThreads).values({
          flowId, gateNodeId, itemId,
          agentId: agentId ?? null, session,
          updatedAt: Date.now(),
        }).onConflictDoUpdate({
          target: [discussThreads.flowId, discussThreads.gateNodeId, discussThreads.itemId],
          set: {
            agentId: agentId ?? null, session,
            updatedAt: Date.now(),
          },
        });
      });
    } catch (err) {
      logger?.warn?.(`manifold session-index: recordThread failed: ${err?.message ?? err}`);
    }
  }

  // Thread stamp for (flow, gate, item), or null when no turn ran yet.
  async function findThread(flowId, gateNodeId, itemId) {
    if (!flowId || !gateNodeId || !itemId) return null;
    try {
      const rows = await db.select({
        session: discussThreads.session,
        agentId: discussThreads.agentId,
      }).from(discussThreads)
        .where(and(
          eq(discussThreads.flowId, flowId),
          eq(discussThreads.gateNodeId, gateNodeId),
          eq(discussThreads.itemId, itemId),
        ))
        .limit(1);
      if (!rows.length) return null;
      return { session: rows[0].session, agentId: rows[0].agentId ?? null };
    } catch (err) {
      logger?.warn?.(`manifold session-index: findThread failed: ${err?.message ?? err}`);
      return null;
    }
  }

  return {
    record,
    hasSessionForNode,
    flowIdForSessionKey,
    listForFlow,
    recordThread,
    findThread,
    close() { try { client.close(); } catch { /* ignore */ } },
    _db: client,
  };
}
