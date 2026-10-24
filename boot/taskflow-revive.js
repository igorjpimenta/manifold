// Boot-time orphan cancel + re-engage for interrupted agent tasks.
//
// On gateway boot, any task_runs row at status='running' is guaranteed-dead:
// no prior process is still alive. Cancel each orphan then re-engage its
// session so the agent resumes under a fresh task_run lifecycle.
// Boot-only — no periodic polling — to avoid false-positives on healthy
// long-running tasks whose last_event_at hasn't been written mid-flight.

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { getSharedDb } from "../state/db.js";
import os from "node:os";
import { REVIVAL_MSG } from "../engine/revival-messages.js";
import { isManifoldOwnedSessionKey } from "../engine/executor-helpers.js";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";

export { REVIVAL_MSG };

const DEFAULT_AGENTS_ROOT = path.join(os.homedir(), ".openclaw", "agents");
// "graph-flow" is manifold's pre-rename plugin id — sessions created before
// the rename still carry it and are just as much manifold's to own.
const MANIFOLD_PLUGIN_OWNER_IDS = new Set(["manifold", "graph-flow"]);

function loadSessionsStore(filePath) {
  try {
    if (!existsSync(filePath)) return null;
    return JSON.parse(readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Authoritative ownership check: reads `pluginOwnerId` off the session
 * store's own record for `sessionKey` — the field openclaw core itself
 * stamps at session creation from the dispatching plugin's id (not
 * caller-supplied) and relies on for its own access control (sessions.delete
 * rejects cross-plugin deletes on a mismatch). Falls back to the
 * sessionKey-shape heuristic (isManifoldOwnedSessionKey) when the record
 * can't be read — missing agentId, missing/corrupt sessions.json, or a
 * session that isn't in the store — which is exactly the check this
 * function replaces, so an unavailable lookup is a no-op, not a regression.
 *
 * `cache` (a Map, optional) lets one scanAndCleanup() pass reuse a single
 * parsed sessions.json per agentId instead of re-reading it once per row —
 * the store can be several MB after long agent history.
 */
export function isManifoldOwnedSession({ agentId, sessionKey }, { agentsRoot = DEFAULT_AGENTS_ROOT, cache } = {}) {
  if (agentId && sessionKey) {
    let store = cache?.get(agentId);
    if (store === undefined) {
      store = loadSessionsStore(path.join(agentsRoot, agentId, "sessions", "sessions.json"));
      cache?.set(agentId, store);
    }
    const ownerId = store?.[sessionKey]?.pluginOwnerId;
    if (ownerId != null) return MANIFOLD_PLUGIN_OWNER_IDS.has(ownerId);
  }
  return isManifoldOwnedSessionKey(sessionKey);
}

export async function queryRunningOrphans(dbPath) {
  if (!existsSync(dbPath)) return [];
  // Shared, never closed: runs.sqlite's writer is the tasks runtime's own
  // connection, and closing a libsql handle here checkpoints and unlinks the
  // -wal under it — the other driver then reads a stale shm until restart.
  const { client } = await getSharedDb(dbPath, { readonly: true });
  const { rows } = await client.execute(
    `SELECT task_id, agent_id, child_session_key, runtime, last_event_at
       FROM task_runs
      WHERE status = 'running'
      ORDER BY last_event_at DESC`,
  );
  return rows;
}

export async function cancelOrphan(api, sessionKey, taskId) {
  const cfg = api?.runtime?.config?.current?.() ?? api?.config;
  if (!cfg) return { ok: false, error: "no cfg available" };
  const runs = api?.runtime?.tasks?.runs;
  if (!runs || typeof runs.bindSession !== "function") {
    return { ok: false, error: "runtime.tasks.runs.bindSession unavailable" };
  }
  try {
    const bound = runs.bindSession({ sessionKey });
    const result = await bound.cancel({ taskId, cfg });
    return { ok: result.cancelled === true, found: result.found, reason: result.reason };
  } catch (err) {
    return { ok: false, error: err?.message ?? String(err) };
  }
}

export async function reviveSession(subagent, sessionKey) {
  if (!subagent || typeof subagent.run !== "function") {
    return { ok: false, error: "subagent.run unavailable" };
  }
  try {
    const result = await subagent.run({ sessionKey, message: REVIVAL_MSG });
    return { ok: true, runId: result?.runId };
  } catch (err) {
    return { ok: false, error: err?.message ?? String(err) };
  }
}

// A manifold-owned orphan doesn't get this scan's own generic revival
// (a bare subagent.run() with no flow awareness) — it gets routed through
// the SAME function manifold's own boot-time flow revival uses,
// engine/executor.js's reviveAgentKindOrphan(). That function resolves the
// pipeline/node, dispatches a continuation prompt, and writes the result
// back into the flow's own state; a bare reviveSession() call here would
// know nothing about any of that and would leave the flow itself stuck
// even if the session technically got a reply. Sharing the one real
// revival path also means either boot service can correctly revive a
// manifold-owned orphan on its own, rather than depending on both
// services having started successfully.
//
// Requires sessionIndex (state/session-index.js, to resolve the session
// key back to a flowId) and runStore + executor (to fetch the flow row and
// call its own revival function) — all optional constructor args on
// scanAndCleanup. Any of them missing, or the flow not resolvable/already
// terminal, returns {ok:false} rather than throwing; the caller's fallback
// for a failed delegation is to skip the row entirely, never to fall
// through to the generic cancel+reviveSession path this exists to replace
// for manifold-owned sessions.
export async function reviveViaFlowRevival(row, { sessionIndex, runStore, executor } = {}) {
  if (!sessionIndex || !runStore || typeof executor?.reviveAgentKindOrphan !== "function") {
    return { ok: false, reason: "flow_revival_deps_unavailable" };
  }
  const flowId = await sessionIndex.flowIdForSessionKey(row.child_session_key);
  if (!flowId) return { ok: false, reason: "flow_id_not_found" };
  const flow = await runStore.get(flowId);
  if (!flow) return { ok: false, reason: "flow_not_found" };
  if (TERMINAL_STATUS_SET.has(flow.status)) return { ok: false, reason: "flow_terminal" };
  await executor.reviveAgentKindOrphan(flow, flow.waitJson?.nodeId);
  return { ok: true, flowId };
}

export async function scanAndCleanup(api, dbPath, startupDelayMs, logger, { sessionIndex, runStore, executor } = {}) {
  await new Promise((r) => setTimeout(r, startupDelayMs));
  let rows;
  try {
    rows = await queryRunningOrphans(dbPath);
  } catch (err) {
    logger.warn(`taskflow-revive: query failed: ${err?.message ?? String(err)}`);
    return;
  }
  if (rows.length === 0) {
    logger.info("taskflow-revive: no pre-boot orphans");
    return;
  }
  logger.info(`taskflow-revive: found ${rows.length} pre-boot orphan(s)`);

  // Cancels stale task_run bookkeeping and classifies which rows need a
  // real revival dispatch, below. cancelOrphan goes through
  // api.runtime.tasks.runs, not the subagent runtime, and the
  // manifold-ownership check is a pure session-store lookup.
  let cancelled = 0;
  const ownerCache = new Map();
  const toRevive = [];
  const toDelegate = [];
  for (const row of rows) {
    // A manifold flow node's session (engine/executor-helpers.js's
    // defaultAgentSessionKey) is owned by engine/executor.js's
    // reviveAgentKindOrphan() — the one function that actually knows how
    // to resolve the flow's pipeline/node and feed a reply back into flow
    // state. This scan must not cancel that session (see
    // reviveViaFlowRevival's own header for why: cancelling by session key
    // can tear down a fresh dispatch the flow-revival service already
    // started under this exact deterministic key) or dispatch its own
    // flow-unaware reviveSession() against it — it delegates instead.
    if (row.child_session_key && isManifoldOwnedSession({ agentId: row.agent_id, sessionKey: row.child_session_key }, { cache: ownerCache })) {
      toDelegate.push(row);
      continue;
    }
    const ageSec = row.last_event_at ? Math.round((Date.now() - row.last_event_at) / 1000) : null;
    const cancelResult = await cancelOrphan(api, row.child_session_key, row.task_id);
    if (cancelResult.ok) {
      cancelled += 1;
      logger.info(`taskflow-revive: cancelled ${row.task_id.slice(0, 8)} (runtime=${row.runtime}, age=${ageSec}s)`);
    } else {
      logger.warn(`taskflow-revive: cancel skipped for ${row.task_id.slice(0, 8)}: ${cancelResult.error ?? cancelResult.reason ?? "unknown"}`);
    }
    if (!row.child_session_key) {
      logger.warn(`taskflow-revive: ${row.task_id.slice(0, 8)} missing child_session_key; cannot revive`);
      continue;
    }
    toRevive.push(row);
  }

  let revived = 0;
  let delegated = 0;
  if (toRevive.length > 0 || toDelegate.length > 0) {
    // The executor's subagent is gated (engine/gateway-subagent.js): a
    // revival dispatched before the gateway is ready waits for it there.
    const subagent = executor?.getSubagent?.() ?? api?.runtime?.subagent;
    for (const row of toRevive) {
      const reviveResult = await reviveSession(subagent, row.child_session_key);
      if (reviveResult.ok) {
        revived += 1;
        logger.info(`taskflow-revive: revived ${row.child_session_key} (runId=${reviveResult.runId ?? "?"})`);
      } else {
        logger.warn(`taskflow-revive: revival failed for ${row.child_session_key}: ${reviveResult.error}`);
      }
    }
    for (const row of toDelegate) {
      // A failed delegation (missing deps, flow not found, already
      // terminal) skips the row entirely — it never falls through to
      // cancelOrphan/reviveSession, which is exactly the race this
      // delegation exists to avoid for a manifold-owned session.
      const result = await reviveViaFlowRevival(row, { sessionIndex, runStore, executor });
      if (result.ok) {
        delegated += 1;
        logger.info(`taskflow-revive: delegated ${row.task_id.slice(0, 8)} (session=${row.child_session_key}) to flow revival [${result.flowId.slice(0, 8)}]`);
      } else {
        logger.info(`taskflow-revive: skipping ${row.task_id.slice(0, 8)} entirely — session ${row.child_session_key} is owned by manifold flow revival (${result.reason})`);
      }
    }
  }
  logger.info(`taskflow-revive[boot]: scanned=${rows.length} cancelled=${cancelled} revived=${revived} delegated=${delegated}`);
}
