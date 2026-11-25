// Attribution registries: item keys, node names and turn-run ids are written
// by engine and telemetry frames, so no table of ours owns the key space.
// These helpers materialize the key set — writers register what they are
// about to attribute, in the same write; the migration backfills the union
// so historical rows keep their attribution and the FKs are satisfiable.
//
// A writer that skips the guard fails its insert instead of writing an
// untracked pointer. Rows are append-only: pruning a flow or a branch must
// not erase the attribution that names it.

export const ENSURE_FLOW_ITEM_SQL =
  "INSERT OR IGNORE INTO flow_items (flow_id, item_key, first_seen_at) VALUES (?, ?, ?)";
export const ENSURE_TURN_RUN_SQL =
  "INSERT OR IGNORE INTO turn_runs (run_id, first_seen_at) VALUES (?, ?)";
export const ENSURE_PIPELINE_NODE_SQL =
  "INSERT OR IGNORE INTO pipeline_nodes (pipeline_id, node_name, first_seen_at) VALUES (?, ?, ?)";

// Composite keys are skipped whole when any part is null: a partial pointer
// cannot be checked, and SQLite would treat the row as satisfying the FK
// anyway — registering half of it would only create noise.
function pairs(rows) {
  const seen = new Map();
  for (const [a, b] of rows) {
    if (typeof a !== "string" || !a || typeof b !== "string" || !b) continue;
    seen.set(`${a}\u0000${b}`, [a, b]);
  }
  return [...seen.values()];
}

export async function ensureFlowItems(client, rows, now = Date.now()) {
  const keys = pairs(rows);
  if (keys.length === 0) return;
  await client.batch(keys.map(([flowId, itemKey]) => ({
    sql: ENSURE_FLOW_ITEM_SQL, args: [flowId, itemKey, now],
  })), "write");
}

export async function ensureTurnRuns(client, runIds, now = Date.now()) {
  const unique = [...new Set(runIds.filter((r) => typeof r === "string" && r.length > 0))];
  if (unique.length === 0) return;
  await client.batch(unique.map((runId) => ({
    sql: ENSURE_TURN_RUN_SQL, args: [runId, now],
  })), "write");
}

export async function ensurePipelineNodes(client, rows, now = Date.now()) {
  const keys = pairs(rows);
  if (keys.length === 0) return;
  await client.batch(keys.map(([pipelineId, nodeName]) => ({
    sql: ENSURE_PIPELINE_NODE_SQL, args: [pipelineId, nodeName, now],
  })), "write");
}

// Union backfill, one pass. Node names also come from the version registry
// so a pipeline's declared nodes are known before their first turn. Each
// batch item is one statement.
export const BACKFILL_FLOW_ITEMS_SQL = `
  INSERT OR IGNORE INTO flow_items (flow_id, item_key, first_seen_at)
  SELECT flow_id, item_key, ? FROM (
    SELECT flow_id, item_key FROM session_lineage WHERE item_key IS NOT NULL AND flow_id IS NOT NULL
    UNION SELECT flow_id, item_key FROM cost_by_call WHERE item_key IS NOT NULL AND flow_id IS NOT NULL
    UNION SELECT flow_id, branch_item_id FROM flow_sessions WHERE branch_item_id IS NOT NULL
  )
`;

export const BACKFILL_TURN_RUNS_SQL = `
  INSERT OR IGNORE INTO turn_runs (run_id, first_seen_at)
  SELECT run_id, ? FROM session_lineage WHERE run_id IS NOT NULL
`;

export const BACKFILL_PIPELINE_NODES_SQL = `
  INSERT OR IGNORE INTO pipeline_nodes (pipeline_id, node_name, first_seen_at)
  SELECT pipeline_id, node_name, ? FROM (
    SELECT pipeline_id, node_name FROM pipeline_node_versions WHERE node_name IS NOT NULL
    UNION SELECT pipeline_id, node_id FROM session_lineage WHERE node_id IS NOT NULL AND pipeline_id IS NOT NULL
    UNION SELECT pipeline_id, node_id FROM cost_by_call WHERE node_id IS NOT NULL AND pipeline_id IS NOT NULL
    UNION SELECT pipeline_id, node FROM queue_items WHERE node IS NOT NULL AND pipeline_id IS NOT NULL
    UNION SELECT pipeline_id, node_id FROM node_health WHERE node_id IS NOT NULL
  )
`;

export const BACKFILL_ATTRIBUTION_SQL = [BACKFILL_FLOW_ITEMS_SQL, BACKFILL_TURN_RUNS_SQL, BACKFILL_PIPELINE_NODES_SQL];

export async function backfillAttribution(client, now = Date.now()) {
  const statements = [...BACKFILL_ATTRIBUTION_SQL];
  await client.batch(statements.map((sql) => ({ sql, args: [now] })), "write");
}
