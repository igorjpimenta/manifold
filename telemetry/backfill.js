// Child-flow trigger-edge backfill — reconstructs missing `flow:<childFlowId>`
// session_lineage rows from runtime.sqlite's flow_runs.parent_invocation_id
// -> flow_node_invocations linkage. A childFlows:true subflow node's fan-out
// spawns real per-item flow_runs without a recordTrigger call, so recordSpawn
// leaves the child's node-agent sessions pointing at a key with no trigger
// row and the resolver walk dead-ends there instead of continuing to the
// parent flow's already-resolved root. Idempotent: rows already present are
// skipped unless `force` is set.

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { getSharedDb } from "../state/db.js";
import { execAll } from "./http-helpers.js";

const RUNTIME_DB_PATH = path.join(os.homedir(), ".openclaw", "runtime.sqlite");

export async function runBackfill({ store, resolver, logger, force = false } = {}) {
  const t0 = Date.now();
  const stats = await backfillChildFlowTriggerEdges({ store, resolver, logger, force });
  stats.elapsedMs = Date.now() - t0;
  return stats;
}

// Oldest-parent-first: a multi-level fan-out's own parent edge (itself
// possibly a backfilled child) must exist by the time its child row is
// processed, and a parent flow_run's created_at is always <= its child's.
async function backfillChildFlowTriggerEdges({ store, resolver, logger, force = false, runtimeDbPath = RUNTIME_DB_PATH } = {}) {
  const stats = { scanned: 0, written: 0, skipped: 0, errors: 0 };
  if (!fs.existsSync(runtimeDbPath)) return stats;

  const rows = await execAll((await getSharedDb(runtimeDbPath, { readonly: true })).client, `
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
