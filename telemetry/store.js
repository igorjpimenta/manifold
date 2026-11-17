// SQLite store for lineage + cost_by_call. WAL mode so the hook write path
// doesn't block on dashboard reads. Schema migrations are append-only — each
// MIGRATIONS entry is one applied step; never edit a past entry in place.

import path from "node:path";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "../state/db.js";
import fs from "node:fs";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "../state/migrate.js";
import { enqueueRaw, flush as flushBatchWriter } from "../state/batch-writer.js";
import { ensureSessionKeys, ENSURE_SESSION_SQL } from "../state/session-keys.js";
import { ensureFlowItems, ensureTurnRuns, ensurePipelineNodes } from "../state/attribution-registries.js";

// Lineage/cost now live in runtime.sqlite so flow_id and pipeline_id can
// carry FKs; resolve per call so MANIFOLD_DATA_DIR set after import wins.
export function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

const UPSERT_LINEAGE_SQL = `
  INSERT INTO session_lineage
    (child_session_key, parent_session_key, root_session_key, kind, root_kind, root_label,
     agent_id, pipeline_id, flow_id, run_id, node_id, item_key, first_seen_at, source)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  ON CONFLICT(child_session_key) DO UPDATE SET
    parent_session_key = COALESCE(excluded.parent_session_key, session_lineage.parent_session_key),
    root_session_key   = excluded.root_session_key,
    kind               = excluded.kind,
    root_kind          = excluded.root_kind,
    root_label         = excluded.root_label,
    agent_id           = COALESCE(excluded.agent_id, session_lineage.agent_id),
    pipeline_id        = COALESCE(excluded.pipeline_id, session_lineage.pipeline_id),
    flow_id            = COALESCE(excluded.flow_id, session_lineage.flow_id),
    run_id             = COALESCE(excluded.run_id, session_lineage.run_id),
    node_id            = COALESCE(excluded.node_id, session_lineage.node_id),
    item_key           = COALESCE(excluded.item_key, session_lineage.item_key),
    source             = excluded.source
  WHERE session_lineage.source = 'backfill' OR excluded.source != 'backfill'
`;

const GET_LINEAGE_SQL = `SELECT * FROM session_lineage WHERE child_session_key = ?`;

const INSERT_COST_SQL = `
  INSERT OR REPLACE INTO cost_by_call
    (session_id, msg_id, ts, provider, model,
     input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, total_tokens,
     cost_input, cost_output, cost_cache_read, cost_cache_write, cost_total,
     session_key, agent_id,
     root_session_key, root_kind, root_label,
     pipeline_id, flow_id, node_id, item_key)
  VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?, ?,?,?, ?,?,?,?)
`;

export async function openStore({ dbPath = defaultDbPath() } = {}) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { client } = await getSharedDb(dbPath);
  return makeStore(client, dbPath);
}

function makeStore(client, dbPath) {
  return {
    _client: client,
    async upsertLineage(row) {
      try {
        await withSharedWrite(dbPath, async () => {
          await ensureSessionKeys(client, [row.child_session_key, row.parent_session_key, row.root_session_key]);
          await ensureFlowItems(client, [[row.flow_id, row.item_key]]);
          await ensureTurnRuns(client, [row.run_id]);
          await ensurePipelineNodes(client, [[row.pipeline_id, row.node_id]]);
          return client.execute({
            sql: UPSERT_LINEAGE_SQL,
            args: [
              row.child_session_key,
              row.parent_session_key ?? null,
              row.root_session_key,
              row.kind,
              row.root_kind,
              row.root_label ?? null,
              row.agent_id ?? null,
              row.pipeline_id ?? null,
              row.flow_id ?? null,
              row.run_id ?? null,
              row.node_id ?? null,
              row.item_key ?? null,
              row.first_seen_at ?? Date.now(),
              row.source,
            ],
          });
        });
      } catch { /* best-effort */ }
    },
    async getLineage(childKey) {
      try {
        const { rows } = await client.execute({ sql: GET_LINEAGE_SQL, args: [childKey] });
        return rows[0] ?? null;
      } catch { return null; }
    },
    // Deferred via batch-writer: one call per LLM call with cost data
    // (telemetry/index.js's before_message_write hook), likely the
    // highest-volume write in runtime.sqlite. Already best-effort (errors
    // swallowed, no caller inspects a return value or re-reads the row in
    // the same hook invocation — unlike upsertLineage() above, which stays
    // immediate because a closely-timed child session's getLineage() can
    // depend on a parent's just-written row).
    insertCost(row) {
      const now = Date.now();
      for (const k of [row.session_key, row.root_session_key]) {
        if (typeof k === "string" && k.length > 0) enqueueRaw(dbPath, ENSURE_SESSION_SQL, [k, now]);
      }
      if (row.flow_id && row.item_key) enqueueRaw(dbPath, "INSERT OR IGNORE INTO flow_items (flow_id, item_key, first_seen_at) VALUES (?, ?, ?)", [row.flow_id, row.item_key, now]);
      if (row.pipeline_id && row.node_id) enqueueRaw(dbPath, "INSERT OR IGNORE INTO pipeline_nodes (pipeline_id, node_name, first_seen_at) VALUES (?, ?, ?)", [row.pipeline_id, row.node_id, now]);
      enqueueRaw(dbPath, INSERT_COST_SQL, [
        row.session_id,
        row.msg_id,
        row.ts,
        row.provider ?? null,
        row.model ?? null,
        row.input_tokens ?? null,
        row.output_tokens ?? null,
        row.cache_read_tokens ?? null,
        row.cache_write_tokens ?? null,
        row.total_tokens ?? null,
        row.cost_input ?? null,
        row.cost_output ?? null,
        row.cost_cache_read ?? null,
        row.cost_cache_write ?? null,
        row.cost_total,
        row.session_key ?? null,
        row.agent_id ?? null,
        row.root_session_key ?? null,
        row.root_kind ?? null,
        row.root_label ?? null,
        row.pipeline_id ?? null,
        row.flow_id ?? null,
        row.node_id ?? null,
        row.item_key ?? null,
      ]);
    },
    // insertCost() defers through batch-writer — for tests that assert
    // read-after-write. Not meant for production use.
    async flush() {
      await flushBatchWriter(dbPath);
    },
    close() { try { client.close(); } catch { /* ignore */ } },
  };
}

