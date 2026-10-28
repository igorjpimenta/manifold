// One-shot backfill — walks every trajectory.jsonl in ~/.openclaw/agents,
// resolves lineage for each session, and writes a row. Idempotent: rows
// already written by hooks (`source ≠ 'backfill'`) are not overwritten —
// see the ON CONFLICT clause in store.js.
//
// Also optional: --cost extracts every assistant-message usage.cost from
// the same jsonls and writes cost_by_call rows, stamped with the lineage.

import { resolveCost } from "./pricer.js";
import { getSharedDb } from "../state/db.js";
import { resolveStorePaths } from "../state/paths.js";
import { execAll } from "./http-helpers.js";

// Gateway file reads (trajectory walk, session text) arrive via `sources`,
// injected by the harness-side adapter. Core keeps the lineage loop and the
// runtime.sqlite edge query over injected paths.
export async function runBackfill({ store, resolver, logger, includeCost = false, force = false, sources = null, agentsDir, runtimeDbPath = resolveStorePaths().runtimeDb } = {}) {
  const t0 = Date.now();
  const stats = {
    sessionsScanned: 0, lineageWritten: 0, lineageSkipped: 0,
    costScanned: 0, costWritten: 0, errors: 0,
    childFlowEdgesScanned: 0, childFlowEdgesWritten: 0, childFlowEdgesSkipped: 0, childFlowEdgeErrors: 0,
  };

  if (!sources) { logger?.warn("backfill: no gateway sources injected — skipping"); return stats; }
  let pairs;
  try { pairs = sources.listTrajectoryPairs({ agentsDir }); }
  catch (e) { logger?.warn(`backfill: cannot read trajectories: ${e.message}`); return stats; }

  // Process in a single transaction for speed — rolls back cleanly on crash
  await store._client.execute("BEGIN");
  try {
    for (const { trajPath, sessionFile } of pairs) {
      stats.sessionsScanned++;
      try {
        const head = sources.readTrajectoryHead(trajPath);
        if (!head) continue;
        const d = JSON.parse(head);
        const sessionKey = d.sessionKey;
        const sessionId = d.sessionId;
        if (!sessionKey || !sessionId) continue;

        // Lineage row
        if (!force) {
          const existing = await store.getLineage(sessionKey);
          if (existing) { stats.lineageSkipped++; }
          else {
            const row = await resolver.resolve(sessionKey, {
              source: "backfill",
              firstSeenAt: Date.parse(d.ts) || Date.now(),
            });
            if (row) { await store.upsertLineage(row); stats.lineageWritten++; }
          }
        } else {
          const row = await resolver.resolve(sessionKey, {
            source: "backfill",
            firstSeenAt: Date.parse(d.ts) || Date.now(),
          });
          if (row) { await store.upsertLineage(row); stats.lineageWritten++; }
        }

        // Cost rows
        if (includeCost) {
          const sessionText = sources.readSessionText(sessionFile);
          if (sessionText == null) continue;
          const lineage = await store.getLineage(sessionKey);
          await extractCostFromSession(sessionText, sessionId, sessionKey, lineage, store, stats);
        }
      } catch (err) {
        stats.errors++;
        if (stats.errors <= 5) logger?.warn(`backfill: ${trajPath}: ${err.message}`);
      }
    }
    await store._client.execute("COMMIT");
  } catch (err) {
    await store._client.execute("ROLLBACK");
    throw err;
  }

  // Child-flow trigger edges: a childFlows:true subflow node's fan-out
  // (executor-subflow-children.js) spawns real per-item flow_runs, each with
  // its own flow_id. Before that path called recordTrigger (fixed alongside
  // this backfill), no `flow:<childFlowId>` session_lineage row was ever
  // written for these — recordSpawn still pointed the child's node-agent
  // sessions at that key regardless, so the resolver walk dead-ended there
  // instead of continuing up to the parent flow's already-resolved root
  // (cron, operator, etc), and their cost collapsed into an unlinked
  // "unknown" bucket in the cost tree. Reconstruct the missing edges from
  // runtime.sqlite's flow_runs.parent_invocation_id -> flow_node_invocations
  // linkage — a separate pass/transaction from the trajectory walk above
  // since it reads a different database and one failing doesn't need to
  // roll back the other.
  try {
    const childStats = await backfillChildFlowTriggerEdges({ store, resolver, logger, force, runtimeDbPath });
    stats.childFlowEdgesScanned = childStats.scanned;
    stats.childFlowEdgesWritten = childStats.written;
    stats.childFlowEdgesSkipped = childStats.skipped;
    stats.childFlowEdgeErrors = childStats.errors;
  } catch (err) {
    logger?.warn(`backfill: child-flow trigger edges failed: ${err.message}`);
    stats.childFlowEdgeErrors++;
  }

  stats.elapsedMs = Date.now() - t0;
  return stats;
}

// Oldest-parent-first: a multi-level fan-out's own parent edge (itself
// possibly a backfilled child) must exist by the time its child row is
// processed, and a parent flow_run's created_at is always <= its child's.
async function backfillChildFlowTriggerEdges({ store, resolver, logger, force = false, runtimeDbPath = resolveStorePaths().runtimeDb } = {}) {
  const stats = { scanned: 0, written: 0, skipped: 0, errors: 0 };
  const { existsSync } = await import("node:fs");
  if (!existsSync(runtimeDbPath)) return stats;

  let rows;
  rows = await execAll((await getSharedDb(runtimeDbPath, { readonly: true })).client, `
      SELECT fr.flow_id AS child_flow_id, fr.pipeline_id AS child_pipeline_id,
             fr.created_at AS created_at, fni.flow_id AS parent_flow_id
        FROM flow_runs fr
        JOIN flow_node_invocations fni ON fni.id = fr.parent_invocation_id
       WHERE fr.parent_invocation_id IS NOT NULL
       ORDER BY fr.created_at ASC
    `);

  await store._client.execute("BEGIN");
  try {
    for (const r of rows) {
      stats.scanned++;
      try {
        const childKey = `flow:${r.child_flow_id}`;
        if (!force && await store.getLineage(childKey)) { stats.skipped++; continue; }

        const parentKey = `flow:${r.parent_flow_id}`;
        const chain = await resolver.walk(parentKey);
        const rootFrame = chain[chain.length - 1];

        await store.upsertLineage({
          child_session_key:  childKey,
          parent_session_key: parentKey,
          root_session_key:   rootFrame?.rootKey ?? rootFrame?.key ?? parentKey,
          kind:               "flow-triggered",
          root_kind:          rootFrame?.rootKind ?? rootFrame?.kind ?? "unknown",
          root_label:         rootFrame?.rootLabel ?? null,
          agent_id:           null,
          pipeline_id:        r.child_pipeline_id ?? null,
          flow_id:            r.child_flow_id,
          run_id:             null,
          node_id:            null,
          item_key:           null,
          first_seen_at:      r.created_at ?? Date.now(),
          source:             "trigger_backfill",
        });
        stats.written++;
      } catch (err) {
        stats.errors++;
        if (stats.errors <= 5) logger?.warn(`backfill: child-flow edge ${r.child_flow_id}: ${err.message}`);
      }
    }
    await store._client.execute("COMMIT");
  } catch (err) {
    await store._client.execute("ROLLBACK");
    throw err;
  }
  return stats;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async function extractCostFromSession(sessionText, sessionId, sessionKey, lineage, store, stats) {
  // Scan the jsonl line by line (some sessions are tens of MB).
  const data = sessionText;
  let start = 0;
  while (start < data.length) {
    const nl = data.indexOf("\n", start);
    const line = nl === -1 ? data.slice(start) : data.slice(start, nl);
    start = nl === -1 ? data.length : nl + 1;
    if (!line || line.length < 30 || line.indexOf('"cost"') === -1) continue;
    let d;
    try { d = JSON.parse(line); } catch { continue; }
    if (d.type !== "assistant" && d.role !== "assistant") {
      // Some shapes nest the assistant message under `message`
      if (!d.message || (d.message.role !== "assistant" && d.type !== "assistant")) continue;
    }
    stats.costScanned++;
    const msg = d.message || d;
    const usage = msg.usage || d.usage;
    if (!usage) continue;
    const provider = msg.provider ?? d.provider ?? null;
    const model = msg.model ?? d.model ?? null;
    // Use pricer to compute when source cost is missing or zero-but-tokens
    const cost = resolveCost({ provider, model, usage });
    const msgId = d.id || msg.id || `${sessionId}-${d.timestamp || ""}`;
    const ts = Date.parse(d.timestamp || msg.timestamp || "") || Date.now();
    try {
      await store.insertCost({
        session_id: sessionId, msg_id: msgId, ts,
        provider, model,
        input_tokens: usage.input ?? null,
        output_tokens: usage.output ?? null,
        cache_read_tokens: usage.cacheRead ?? null,
        cache_write_tokens: usage.cacheWrite ?? null,
        total_tokens: usage.totalTokens ?? usage.total ?? null,
        cost_input: cost.input ?? null,
        cost_output: cost.output ?? null,
        cost_cache_read: cost.cacheRead ?? null,
        cost_cache_write: cost.cacheWrite ?? null,
        cost_total: cost.total,
        session_key: sessionKey,
        agent_id: lineage?.agent_id ?? null,
        root_session_key: lineage?.root_session_key ?? sessionKey,
        root_kind: lineage?.root_kind ?? null,
        root_label: lineage?.root_label ?? null,
        pipeline_id: lineage?.pipeline_id ?? null,
        flow_id: lineage?.flow_id ?? null,
        node_id: lineage?.node_id ?? null,
        item_key: lineage?.item_key ?? null,
      });
      stats.costWritten++;
    } catch { stats.errors++; }
  }
}
