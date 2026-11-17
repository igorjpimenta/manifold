// ops-merge.js — one-time copy of the lineage/cost tables out of ops.sqlite.
//
// Those tables now live in runtime.sqlite because flow_id and pipeline_id
// need FKs, and SQLite cannot constrain a column against a table in another
// file. The copy runs inside migrateDatabase() after the runtime snapshot,
// and is idempotent: rows already in runtime win (INSERT OR IGNORE), and a
// non-empty runtime table means the merge already happened.
//
// Columns are intersected rather than assumed equal: the live ops file
// predates a column its baseline migration declares (queue_items.kind), and
// CREATE TABLE IF NOT EXISTS never added it. A missing column merges as its
// default/NULL instead of failing the whole boot.

import path from "node:path";
import { existsSync } from "node:fs";
import Database from "better-sqlite3";

const TABLES = {
  queue_items:     { flow: "flow_id", pipeline: "pipeline_id" },
  session_lineage: { flow: "flow_id", pipeline: "pipeline_id" },
  cost_by_call:    { flow: "flow_id", pipeline: "pipeline_id" },
};

function columnsOf(db, schema, table) {
  // prepare().columns() is schema-accurate; `SELECT name FROM ops.pragma_table_info(?)`
  // silently reports the main-schema table of the same name.
  return db.prepare(`SELECT * FROM "${schema}"."${table}" LIMIT 0`).columns().map((c) => c.name);
}

export function mergeOpsTables({ runtimeDbPath, logger = null } = {}) {
  // Only the canonical pair is migrated; alternate paths (tests, one-off
  // stores) would otherwise look for an unrelated sibling ops.sqlite.
  if (path.basename(runtimeDbPath) !== "runtime.sqlite") {
    return { merged: false, reason: "not-canonical" };
  }
  const opsPath = path.join(path.dirname(runtimeDbPath), "ops.sqlite");
  if (!existsSync(opsPath)) return { merged: false, reason: "no-ops-db" };

  const db = new Database(runtimeDbPath);
  try {
    db.exec("PRAGMA busy_timeout = 30000;");
    const existing = Object.keys(TABLES).reduce(
      (n, t) => n + db.prepare(`SELECT COUNT(*) AS n FROM main.${t}`).get().n, 0,
    );
    if (existing > 0) return { merged: false, reason: "already-populated" };

    db.prepare("ATTACH DATABASE ? AS ops").run(opsPath);
    const present = Object.keys(TABLES).filter(
      (t) => db.prepare("SELECT 1 FROM ops.sqlite_master WHERE type='table' AND name=?").get(t),
    );
    if (present.length === 0) return { merged: false, reason: "no-source-tables" };

    db.exec("PRAGMA foreign_keys = OFF;");
    db.exec("BEGIN");
    const copied = {};
    try {
      for (const table of present) {
        const runtimeCols = columnsOf(db, "main", table);
        const opsCols = new Set(columnsOf(db, "ops", table));
        const cols = runtimeCols.filter((c) => opsCols.has(c));
        const list = cols.map((c) => `"${c}"`).join(", ");
        db.exec(`INSERT OR IGNORE INTO main."${table}" (${list}) SELECT ${list} FROM ops."${table}"`);
        copied[table] = db.prepare(`SELECT COUNT(*) AS n FROM main."${table}"`).get().n;
      }

      // Historical rows whose run/pipeline was pruned predate the constraints.
      // Null the pointer, keep the telemetry row; the FK is for writes.
      for (const [table, cols] of Object.entries(TABLES)) {
        if (!present.includes(table)) continue;
        for (const [kind, col] of Object.entries(cols)) {
          const parent = kind === "flow" ? "flow_runs" : "pipelines";
          const parentCol = kind === "flow" ? "flow_id" : "id";
          db.exec(`UPDATE main."${table}" SET "${col}" = NULL
                   WHERE "${col}" IS NOT NULL
                     AND "${col}" NOT IN (SELECT "${parentCol}" FROM main."${parent}")`);
        }
      }
      db.exec("COMMIT");
    } catch (err) {
      db.exec("ROLLBACK");
      throw err;
    }
    db.exec("PRAGMA foreign_keys = ON;");

    logger?.info?.(`ops-merge: copied ${JSON.stringify(copied)} from ${opsPath}`);
    return { merged: true, copied };
  } catch (err) {
    logger?.warn?.(`ops-merge: skipped (${err?.message ?? err})`);
    return { merged: false, reason: "error" };
  } finally {
    db.close();
  }
}
