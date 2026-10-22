// SQLite store for lineage + cost_by_call. WAL mode so the hook write path
// doesn't block on dashboard reads. Schema migrations are append-only — each
// MIGRATIONS entry is one applied step; never edit a past entry in place.

import path from "node:path";
import { getSharedDb, withSharedWrite } from "../state/db.js";
import os from "node:os";
import fs from "node:fs";
import { createMigrator } from "../state/migrator.js";
import { migrateDatabase, OPS_MIGRATIONS_FOLDER } from "../state/migrate.js";
import { enqueueRaw, flush as flushBatchWriter } from "../state/batch-writer.js";

const DEFAULT_DB_PATH = path.join(os.homedir(), ".openclaw", "ops.sqlite");

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

const MIGRATIONS = [
  // 1: initial schema
  `
  CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS session_lineage (
    child_session_key   TEXT PRIMARY KEY,
    parent_session_key  TEXT,
    root_session_key    TEXT NOT NULL,
    kind                TEXT NOT NULL,
    root_kind           TEXT NOT NULL,
    root_label          TEXT,
    agent_id            TEXT,
    pipeline_id         TEXT,
    flow_id             TEXT,
    node_id             TEXT,
    item_key            TEXT,
    first_seen_at       INTEGER NOT NULL,
    source              TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_lineage_root      ON session_lineage(root_session_key);
  CREATE INDEX IF NOT EXISTS idx_lineage_parent    ON session_lineage(parent_session_key);
  CREATE INDEX IF NOT EXISTS idx_lineage_pipeline  ON session_lineage(pipeline_id);
  CREATE INDEX IF NOT EXISTS idx_lineage_flow      ON session_lineage(flow_id);
  CREATE INDEX IF NOT EXISTS idx_lineage_root_kind ON session_lineage(root_kind);

  CREATE TABLE IF NOT EXISTS cost_by_call (
    session_id          TEXT NOT NULL,
    msg_id              TEXT NOT NULL,
    ts                  INTEGER NOT NULL,
    provider            TEXT,
    model               TEXT,
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    cache_read_tokens   INTEGER,
    cache_write_tokens  INTEGER,
    total_tokens        INTEGER,
    cost_input          REAL,
    cost_output         REAL,
    cost_cache_read     REAL,
    cost_cache_write    REAL,
    cost_total          REAL NOT NULL,
    session_key         TEXT,
    agent_id            TEXT,
    root_session_key    TEXT,
    root_kind           TEXT,
    root_label          TEXT,
    pipeline_id         TEXT,
    flow_id             TEXT,
    node_id             TEXT,
    item_key            TEXT,
    PRIMARY KEY (session_id, msg_id)
  );
  CREATE INDEX IF NOT EXISTS idx_cost_ts            ON cost_by_call(ts);
  CREATE INDEX IF NOT EXISTS idx_cost_root_key      ON cost_by_call(root_session_key);
  CREATE INDEX IF NOT EXISTS idx_cost_root_kind_ts  ON cost_by_call(root_kind, ts);
  CREATE INDEX IF NOT EXISTS idx_cost_pipeline_ts   ON cost_by_call(pipeline_id, ts);
  CREATE INDEX IF NOT EXISTS idx_cost_flow_node     ON cost_by_call(flow_id, node_id);
  CREATE INDEX IF NOT EXISTS idx_cost_model_ts      ON cost_by_call(model, ts);
  CREATE INDEX IF NOT EXISTS idx_cost_session       ON cost_by_call(session_id);
  `,
  // 2: add run_id to session_lineage
  async (client) => {
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('session_lineage')");
    const cols = rows.map((r) => r.name);
    if (!cols.includes("run_id")) {
      await client.execute("ALTER TABLE session_lineage ADD COLUMN run_id TEXT");
      await client.execute("CREATE INDEX IF NOT EXISTS idx_lineage_run ON session_lineage(run_id)");
    }
  },
];

export async function openStore({ dbPath = DEFAULT_DB_PATH } = {}) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const { client } = await getSharedDb(dbPath);
  await createMigrator(client, { name: "telemetry", migrations: MIGRATIONS });
  // Generated snapshots apply after the legacy chain (see state/migrate.js).
  await migrateDatabase({ dbPath, migrationsFolder: OPS_MIGRATIONS_FOLDER });
  await attachFlowsDb(client);
  return makeStore(client, dbPath);
}

/** Cross-DB JOIN target: manifold's flow_runs lives in a sibling sqlite.
 *  Attached read-only so ROI queries (status × cost) work in a single hop.
 *  Silent if absent — installations without manifold keep working. */
async function attachFlowsDb(client) {
  const flowsPath = path.join(os.homedir(), ".openclaw", "runtime.sqlite");
  if (!fs.existsSync(flowsPath)) return;
  try {
    await client.execute(`ATTACH DATABASE '${flowsPath.replace(/'/g, "''")}' AS flows`);
  } catch { /* attach failed — ROI queries will skip the JOIN */ }
}

function makeStore(client, dbPath) {
  return {
    _client: client,
    async upsertLineage(row) {
      try {
        await withSharedWrite(dbPath, () => client.execute({
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
        }));
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
    // highest-volume write in ops.sqlite. Already best-effort (errors
    // swallowed, no caller inspects a return value or re-reads the row in
    // the same hook invocation — unlike upsertLineage() above, which stays
    // immediate because a closely-timed child session's getLineage() can
    // depend on a parent's just-written row).
    insertCost(row) {
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
    DEFAULT_DB_PATH,
  };
}

export { DEFAULT_DB_PATH };
