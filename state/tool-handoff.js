// tool-handoff.js — sqlite-backed bridge between a dispatched subagent's
// tool call and the dispatching node's apply(). The tools live outside core
// (oc-config submits results over MCP); core owns only this registry and
// the engine side that reads it. It is also published for harness use
// (package.json "./tool-handoff").
//
// runSubagentTurn (executor-subagent.js) only ever returns {text, runId} to
// apply() — never a tool call's structured payload — so a tool has no other
// channel to hand structured data back to the node that spawned it.
//
// This is deliberately durable, not in-memory: the delivering side (an MCP
// tool call) and the consuming side (the engine's executor loop) may live
// in different processes — an in-process Map would silently drop results
// across that boundary the way it already did across dual module
// registries. A `tool_results` row keyed by the harness-stamped session key
// is the rendezvous both sides can reach: the agent knows its session key,
// the dispatch knows it too, neither needs the other's invocationId.
//
// Writes upsert (latest wins per session); the dispatch consumes its row
// and clears it, so a retry never reads a stale result from a prior
// attempt. Rows are keyed by session, not invocation: one row per session
// at most, so the table cannot grow unbounded over gateway uptime.
//
// Lifecycle (tool side: MCP deliver_result; engine side: the executor loop):
//   writePendingResult({ sessionKey, runId?, agentId?, kind?, payload })
//   readPendingResult(sessionKey)        — the row, or null
//   clearPendingResult(sessionKey)       — the dispatch, once apply() has
//     consumed the payload (also covers the failure path — a failed turn
//     never retries into a stale row)
//   waitForToolResult(sessionKey, { pollMs? }) — { promise, cancel }; the
//     promise resolves with the row the moment one exists (checked once up
//     front, then polled), so a result written before the waiter starts is
//     never missed

import { eq } from "drizzle-orm";
import { getSharedDb, withSharedWrite } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { toolResults } from "./schema.js";
import { resolveStorePaths } from "./paths.js";

// One shared client per DB path, migrated once. Concurrent first callers
// share the same promise, so the migration never races itself. Cross-
// process races resolve the same way every other store's do: each process
// migrates idempotently, then shares its own connection.
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
    sessionKey: r.sessionKey,
    runId: r.runId ?? null,
    agentId: r.agentId ?? null,
    kind: r.kind ?? null,
    payload,
  };
}

/** Latest-wins upsert for one session. No session key, no write. */
export async function writePendingResult({ sessionKey, runId = null, agentId = null, kind = null, payload, dbPath = null } = {}) {
  if (!sessionKey || payload === undefined) return;
  const path = dbPath ?? storeDbPath();
  const { db } = await toolDb(path);
  let payloadJson;
  try { payloadJson = JSON.stringify(payload); } catch { return; }
  await withSharedWrite(path, () =>
    db.insert(toolResults).values({
      sessionKey, runId: runId ?? null, agentId: agentId ?? null,
      kind: kind ?? null, payloadJson, updatedAt: Date.now(),
    }).onConflictDoUpdate({
      target: toolResults.sessionKey,
      set: {
        runId: runId ?? null, agentId: agentId ?? null, kind: kind ?? null,
        payloadJson, updatedAt: Date.now(),
      },
    })
  );
}

/** The unconsumed result for one session, or null. */
export async function readPendingResult(sessionKey, { dbPath = null } = {}) {
  if (!sessionKey) return null;
  const path = dbPath ?? storeDbPath();
  const { db } = await toolDb(path);
  const rows = await db.select().from(toolResults).where(eq(toolResults.sessionKey, sessionKey)).limit(1);
  return toRow(rows[0] ?? null);
}

/** Forget one session's result. Idempotent — unknown keys are a no-op. */
export async function clearPendingResult(sessionKey, { dbPath = null } = {}) {
  if (!sessionKey) return;
  const path = dbPath ?? storeDbPath();
  const { db } = await toolDb(path);
  await withSharedWrite(path, () =>
    db.delete(toolResults).where(eq(toolResults.sessionKey, sessionKey))
  );
}

// Poll-based waiter: resolves with the row the moment one exists for the
// session (checked once up front, so a result written before the waiter
// starts is never missed). Returns { promise, cancel } — the caller MUST
// call cancel() once the race is decided, or the interval keeps the event
// loop alive for nothing.
export function waitForToolResult(sessionKey, { pollMs = 250, dbPath = null } = {}) {
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
      const first = await readPendingResult(sessionKey, { dbPath });
      if (first) { finish(first); return; }
      timer = setInterval(async () => {
        try {
          const row = await readPendingResult(sessionKey, { dbPath });
          if (row) finish(row);
        } catch { /* transient read failure: keep polling */ }
      }, Math.max(10, pollMs));
      if (timer.unref) timer.unref();
    } catch {
      // Store unavailable: leave the promise pending; the waiter's own
      // race (waitForRun) still settles the dispatch either way.
    }
  })();
  return { promise, cancel };
}
