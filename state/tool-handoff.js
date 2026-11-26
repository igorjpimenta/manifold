// tool-handoff.js — sqlite-backed bridge between a dispatched agent's
// tool call and the dispatching node's apply(). Also published for
// harness use (package.json "./tool-handoff").
//
// Durable, not in-memory: the delivering side (an MCP tool call) and the
// consuming side (the engine's executor loop) may live in different
// processes, and an in-process Map would silently drop results across
// that boundary. Rows are keyed by the harness-stamped session both sides
// know (neither needs the other's invocationId), upserted latest-wins,
// and consumed-and-cleared by the dispatch so a retry never reads a stale
// result from a prior attempt. One row per session at most, so the table
// cannot grow unbounded.

import { eq } from "drizzle-orm";
import { getSharedDb, withSharedWrite } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { toolResults, flowRuns } from "./schema.js";
import { resolveStorePaths } from "./paths.js";
import { ensureSessionKeys } from "./session-keys.js";

// One shared client per DB path, migrated once: concurrent first callers
// share the same promise so the migration never races itself.
const _clients = new Map();

async function toolDb(dbPath) {
  let pending = _clients.get(dbPath);
  if (!pending) {
    pending = (async () => {
      await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
      return getSharedDb(dbPath);
    })().catch((err) => {
      _clients.delete(dbPath);
      throw err;
    });
    _clients.set(dbPath, pending);
  }
  return pending;
}

function storeDbPath() {
  return resolveStorePaths().runtimeDb;
}

function toRow(r) {
  if (!r) return null;
  let payload = null;
  try { payload = JSON.parse(r.payloadJson); } catch { payload = null; }
  return {
    session: r.session,
    runId: r.runId ?? null,
    agentId: r.agentId ?? null,
    kind: r.kind ?? null,
    payload,
  };
}

/** Latest-wins upsert for one session. */
export async function writePendingResult({ session, runId = null, agentId = null, kind = null, payload, dbPath = null } = {}) {
  if (!session || payload === undefined) return;
  const path = dbPath ?? storeDbPath();
  const { db, client } = await toolDb(path);
  let payloadJson;
  try { payloadJson = JSON.stringify(payload); } catch { return; }
  await withSharedWrite(path, async () => {
    await ensureSessionKeys(client, [session]);
    // run_id carries an FK; a result may legitimately arrive for a run
    // this process never saw (late delivery, resumed session, different
    // worker). Keep the result and the session rendezvous, drop only the
    // dangling pointer — the FK must not turn a late write into a
    // constraint error.
    let safeRunId = runId ?? null;
    if (safeRunId !== null) {
      const known = await db.select({ flowId: flowRuns.flowId }).from(flowRuns)
        .where(eq(flowRuns.flowId, safeRunId)).limit(1);
      if (known.length === 0) safeRunId = null;
    }
    return db.insert(toolResults).values({
      session, runId: safeRunId, agentId: agentId ?? null,
      kind: kind ?? null, payloadJson, updatedAt: Date.now(),
    }).onConflictDoUpdate({
      target: toolResults.session,
      set: {
        runId: safeRunId, agentId: agentId ?? null, kind: kind ?? null,
        payloadJson, updatedAt: Date.now(),
      },
    });
  });
}

export async function readPendingResult(session, { dbPath = null } = {}) {
  if (!session) return null;
  const path = dbPath ?? storeDbPath();
  const { db, client } = await toolDb(path);
  const rows = await db.select().from(toolResults).where(eq(toolResults.session, session)).limit(1);
  return toRow(rows[0] ?? null);
}

export async function clearPendingResult(session, { dbPath = null } = {}) {
  if (!session) return;
  const path = dbPath ?? storeDbPath();
  const { db, client } = await toolDb(path);
  await withSharedWrite(path, () =>
    db.delete(toolResults).where(eq(toolResults.session, session))
  );
}

// Poll-based waiter: checks once up front (a result written before the
// waiter starts is never missed), then polls. Returns { promise, cancel }
// — the caller MUST call cancel() once the race is decided, or the
// interval keeps the event loop alive for nothing.
export function waitForToolResult(session, { pollMs = 250, dbPath = null } = {}) {
  let timer = null;
  let settled = false;
  let resolveFn;
  const promise = new Promise((resolve) => { resolveFn = resolve; });
  const finish = (row) => {
    if (settled) return;
    settled = true;
    if (timer) clearInterval(timer);
    resolveFn(row);
  };
  const cancel = () => {
    if (settled) return;
    settled = true;
    if (timer) clearInterval(timer);
  };
  (async () => {
    try {
      const first = await readPendingResult(session, { dbPath });
      if (first) { finish(first); return; }
      timer = setInterval(async () => {
        try {
          const row = await readPendingResult(session, { dbPath });
          if (row) finish(row);
        } catch { /* transient read failure: keep polling */ }
      }, Math.max(10, pollMs));
      if (timer.unref) timer.unref();
    } catch {
      // Store unavailable: leave the promise pending; the caller's own
      // race settles the dispatch either way.
    }
  })();
  return { promise, cancel };
}
