// tool-handoff.js — sqlite-backed bridge between a dispatched agent's
// tool call and the dispatching node's apply(). Core owns only this registry
// and the engine side that reads it. It is also published for harness use
// (package.json "./tool-handoff").
//
// The harness turn (the plugin's run-turn.js) only ever returns
// {text, runId, toolResult?} to apply() — a turn's own reply, never a tool
// call's structured payload — so a tool has no other channel to hand
// structured data back to the node that spawned it.
//
// This is deliberately durable, not in-memory: the delivering side (an MCP
// tool call) and the consuming side (the engine's executor loop) may live
// in different processes, and an in-process Map would silently drop results
// across that boundary. A `tool_results` row keyed by the harness-stamped
// session is the rendezvous both sides can reach: the agent knows its
// session, the dispatch knows it too, neither needs the other's
// invocationId.
//
// Writes upsert (latest wins per session); the dispatch consumes its row
// and clears it, so a retry never reads a stale result from a prior
// attempt. Rows are keyed by session, not invocation: one row per session
// at most, so the table cannot grow unbounded over gateway uptime.
//
// Lifecycle (tool side: MCP deliver_result; engine side: the executor loop):
//   writePendingResult({ session, runId?, agentId?, kind?, payload })
//   readPendingResult(session)        — the row, or null
//   clearPendingResult(session)       — the dispatch, once apply() has
//     consumed the payload (also covers the failure path — a failed turn
//     never retries into a stale row)
//   waitForToolResult(session, { pollMs? }) — { promise, cancel }; the
//     promise resolves with the row the moment one exists (checked once up
//     front, then polled), so a result written before the waiter starts is
//     never missed

import { eq } from "drizzle-orm";
import { getSharedDb, withSharedWrite } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { toolResults, flowRuns } from "./schema.js";
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
    session: r.session,
    runId: r.runId ?? null,
    agentId: r.agentId ?? null,
    kind: r.kind ?? null,
    payload,
  };
}

/** Latest-wins upsert for one session. No session key, no write. */
export async function writePendingResult({ session, runId = null, agentId = null, kind = null, payload, dbPath = null } = {}) {
  if (!session || payload === undefined) return;
  const path = dbPath ?? storeDbPath();
  const { db } = await toolDb(path);
  let payloadJson;
  try { payloadJson = JSON.stringify(payload); } catch { return; }
  await withSharedWrite(path, async () => {
    // run_id carries an FK; a result may legitimately arrive for a run this
    // process never saw (late delivery, resumed session, different worker).
    // Keep the result and the session rendezvous, drop only the dangling
    // pointer — the FK must not turn a late write into a constraint error.
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

/** The unconsumed result for one session, or null. */
export async function readPendingResult(session, { dbPath = null } = {}) {
  if (!session) return null;
  const path = dbPath ?? storeDbPath();
  const { db } = await toolDb(path);
  const rows = await db.select().from(toolResults).where(eq(toolResults.session, session)).limit(1);
  return toRow(rows[0] ?? null);
}

/** Forget one session's result. Idempotent — unknown keys are a no-op. */
export async function clearPendingResult(session, { dbPath = null } = {}) {
  if (!session) return;
  const path = dbPath ?? storeDbPath();
  const { db } = await toolDb(path);
  await withSharedWrite(path, () =>
    db.delete(toolResults).where(eq(toolResults.session, session))
  );
}

// Poll-based waiter: resolves with the row the moment one exists for the
// session (checked once up front, so a result written before the waiter
// starts is never missed). Returns { promise, cancel } — the caller MUST
// call cancel() once the race is decided, or the interval keeps the event
// loop alive for nothing.
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
      // Store unavailable: leave the promise pending; the waiter's own
      // race (waitForRun) still settles the dispatch either way.
    }
  })();
  return { promise, cancel };
}
