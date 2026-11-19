// state/migrate.js — better-sqlite3 migrate-on-open, exactly winch's src/db.js
// shape: schema DDL lives in state/schema.js (runtime.sqlite) and
// state/schema.ops.js (ops.sqlite); `npm run db:generate` snapshots them
// into .migrations/*/<n>.sql; drizzle's migrate() applies pending snapshots.
//
// Call sites run this BEFORE opening any libsql handle for the file, and it
// runs at most once per file per process (migratedPaths): a second driver
// writing under live handles is the one ordering that was observed to
// poison them, so the first opener applies pending snapshots while no
// handle exists yet and later opens find the journal current. Cross-process
// safety comes from the drizzle journal transaction + busy_timeout, same
// posture as winch.
//
// Snapshots are the sole schema path. A snapshot that fails throws here,
// failing the open fast with the file named, instead of surfacing as
// missing-column errors on first query later.
//
// The 0000 baseline files carry hand-added IF NOT EXISTS on every CREATE
// (one-time anchor edit — drizzle-kit emits plain CREATE). Future
// snapshots must keep that property; the snapshot hygiene tests fail the
// build otherwise. Never hand-edit snapshots to add DROP/RENAME.
import path from "node:path";
import { existsSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { migrate } from "drizzle-orm/better-sqlite3/migrator";
import * as runtimeSchema from "./schema.js";
import * as opsSchema from "./schema.ops.js";
import { mergeOpsTables } from "./ops-merge.js";
import { SESSION_KEYS_FROM_ROWS_SQL } from "./session-keys.js";
import { BACKFILL_ATTRIBUTION_SQL, BACKFILL_LEGACY_BRANCH_ITEMS_SQL } from "./attribution-registries.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, "..");

export const RUNTIME_MIGRATIONS_FOLDER = path.join(REPO_ROOT, ".migrations", "runtime");
export const OPS_MIGRATIONS_FOLDER = path.join(REPO_ROOT, ".migrations", "ops");

const SCHEMAS = new Map([
  [RUNTIME_MIGRATIONS_FOLDER, runtimeSchema],
  [OPS_MIGRATIONS_FOLDER, opsSchema],
]);

const migratedPaths = new Set();

export function migrateDatabase({ dbPath, migrationsFolder, logger } = {}) {
  if (migratedPaths.has(dbPath)) return { applied: false };
  // ops/0001 drops the tables that moved to runtime.sqlite, so the copy has
  // to exist first. The runtime sibling is migrated here — including on an
  // install where only ops exists — because that run is what performs the
  // merge and the registry backfill. Non-canonical paths (tests, one-off
  // stores) are left alone: they have no sibling to copy from.
  if (migrationsFolder === OPS_MIGRATIONS_FOLDER && path.basename(dbPath) === "ops.sqlite") {
    const runtimeDbPath = path.join(path.dirname(dbPath), "runtime.sqlite");
    migrateDatabase({ dbPath: runtimeDbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER, logger });
  }
  const dir = path.dirname(dbPath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const sqlite = new Database(dbPath);
  try {
    sqlite.exec("PRAGMA journal_mode = WAL;");
    // Snapshot rebuilds copy rows that predate the constraints (SQLite cannot
    // add an FK in place), and drizzle wraps every snapshot in one
    // transaction — a PRAGMA inside it is a no-op, so the posture has to be
    // set here, around the whole run. Enforcement resumes before the handle
    // closes; opening a migrated file never re-validates existing rows.
    sqlite.exec("PRAGMA foreign_keys = OFF;");
    // 30000 to match the house busy posture (state/db.js), not winch's
    // 5000 — the contended moment is a second process mid-migration.
    sqlite.exec("PRAGMA busy_timeout = 30000;");
    migrate(drizzle(sqlite, { schema: SCHEMAS.get(migrationsFolder) ?? {} }), { migrationsFolder });
    sqlite.exec("PRAGMA foreign_keys = ON;");
  } catch (err) {
    throw new Error(
      `migrate: ${migrationsFolder} snapshot failed on ${dbPath}: ${err?.message ?? err}`,
    );
  } finally {
    sqlite.close();
  }
  if (migrationsFolder === RUNTIME_MIGRATIONS_FOLDER) {
    mergeOpsTables({ runtimeDbPath: dbPath, logger });
    // The registry must hold every key already referenced before the FKs
    // are consulted; rows copied by mergeOpsTables carry keys of their own,
    // so this runs after it rather than inside the migration snapshot.
    const backfill = new Database(dbPath);
    try {
      const now = Date.now();
      backfill.prepare(SESSION_KEYS_FROM_ROWS_SQL).run(now);
      for (const sql of BACKFILL_ATTRIBUTION_SQL) backfill.prepare(sql).run(now);
      const hasBranches = backfill
        .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'flow_branches'")
        .get();
      if (hasBranches) backfill.prepare(BACKFILL_LEGACY_BRANCH_ITEMS_SQL).run(now);
    } catch (err) {
      throw new Error(`migrate: sessions backfill failed on ${dbPath}: ${err?.message ?? err}`);
    } finally {
      backfill.close();
    }
  }
  migratedPaths.add(dbPath);
  logger?.info?.(`migrate: snapshots current at ${dbPath}`);
  return { applied: true };
}
