// Removes the ops-side copies of the tables that moved to runtime.sqlite
// (queue_items, session_lineage, cost_by_call). There is no safe way to do
// this in a snapshot: SQL cannot ask whether the runtime copy holds every
// row, and a DROP that runs before the copy is a one-way data loss.
//
// So it runs here, after the runtime sibling has been migrated (and
// therefore merged and backfilled by state/migrate.js), under two guards:
// the canonical names must be in place, and each table is only dropped when
// runtime has at least as many rows as ops. A table that fails the count is
// left alone and logged — that only happens when an older binary is still
// writing to ops, which is exactly when the rows must be kept.

import path from "node:path";
import { existsSync } from "node:fs";
import Database from "better-sqlite3";

const MOVED_TABLES = ["queue_items", "session_lineage", "cost_by_call"];
const CANONICAL_OPS = "ops.sqlite";
const CANONICAL_RUNTIME = "runtime.sqlite";

function count(db, table) {
  try {
    return db.prepare(`SELECT COUNT(*) AS n FROM main.${table}`).get().n;
  } catch {
    return null;
  }
}

export function dropMovedOpsTables(opsDbPath, { runtimeDbPath, logger } = {}) {
  const runtime = runtimeDbPath ?? path.join(path.dirname(opsDbPath), CANONICAL_RUNTIME);
  if (path.basename(opsDbPath) !== CANONICAL_OPS) return { dropped: [], skipped: [], reason: "not-canonical" };
  if (path.basename(runtime) !== CANONICAL_RUNTIME) return { dropped: [], skipped: [], reason: "not-canonical" };
  if (!existsSync(runtime)) return { dropped: [], skipped: [], reason: "no-runtime" };
  if (!existsSync(opsDbPath)) return { dropped: [], skipped: [], reason: "no-ops" };

  const rt = new Database(runtime, { readonly: true });
  const ops = new Database(opsDbPath);
  const dropped = [];
  const skipped = [];
  try {
    const eligible = [];
    for (const table of MOVED_TABLES) {
      const inOps = count(ops, table);
      if (inOps === null) continue;
      const inRuntime = count(rt, table);
      if (inRuntime === null || inRuntime < inOps) {
        skipped.push(table);
        logger?.warn?.(`ops-drop: keeping ${table} (runtime ${inRuntime ?? "missing"} < ops ${inOps})`);
        continue;
      }
      eligible.push(table);
    }
    if (eligible.length === 0) return { dropped, skipped };
    ops.exec("PRAGMA foreign_keys = OFF;");
    ops.exec("PRAGMA busy_timeout = 30000;");
    ops.exec("BEGIN IMMEDIATE");
    try {
      for (const table of eligible) ops.exec(`DROP TABLE IF EXISTS main.${table}`);
      ops.exec("COMMIT");
    } catch (err) {
      ops.exec("ROLLBACK");
      throw err;
    }
    dropped.push(...eligible);
    logger?.info?.(`ops-drop: removed ${eligible.join(", ")} from ${opsDbPath}`);
    return { dropped, skipped };
  } finally {
    rt.close();
    ops.close();
  }
}
