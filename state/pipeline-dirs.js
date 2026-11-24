// pipeline-dirs.js — per-harness pipeline directory overrides.
//
// Resolution order (resolvePipelinesDir): table[harness] →
// MANIFOLD_PIPELINES_DIR env → adapter config `pipelinesDir` →
// <dataDir>/pipelines default. Table wins so an operator can
// relocate one harness without touching env or config; env wins over config
// so a one-shot override doesn't need a config edit.
//
// Sync better-sqlite3 open-per-operation (same posture as migrate.js's own
// open/close): no persistent handle, so the gateway and one-shot CLI
// processes never share lock state — WAL + busy_timeout carry the
// cross-process safety. Every op runs pending migrations first, so a fresh
// file self-creates the table.

import path from "node:path";
import { existsSync, mkdirSync, statSync } from "node:fs";
import Database from "better-sqlite3";
import { resolveDataDir, pipelinesDir as defaultPipelinesDir } from "./paths.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

export function createPipelineDirsStore({ dbPath, logger = null } = {}) {
  function withDb(fn) {
    mkdirSync(path.dirname(dbPath), { recursive: true });
    migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER, logger });
    const db = new Database(dbPath);
    try {
      db.exec("PRAGMA busy_timeout = 30000;");
      return fn(db);
    } finally {
      db.close();
    }
  }

  // Upsert. Throws when the directory does not exist — a registered path
  // that isn't there is always an operator typo, never a valid override.
  function register({ harness, dirPath } = {}) {
    const harnessName = String(harness ?? "").trim();
    if (!harnessName) throw new Error("pipeline-dirs: register needs a non-empty harness");
    const resolved = path.resolve(String(dirPath ?? ""));
    let st = null;
    try { st = statSync(resolved); } catch { st = null; }
    if (!st?.isDirectory()) throw new Error(`pipeline-dirs: not a directory: ${resolved}`);
    const now = Date.now();
    withDb((db) => {
      // pipeline_dirs.harness carries an FK; an operator registering the
      // first override for a harness must not hit a constraint error.
      db.transaction(() => {
        db.prepare(
          `INSERT INTO harnesses (id, created_at, updated_at) VALUES (?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at`
        ).run(harnessName, now, now);
        db.prepare(
          `INSERT INTO pipeline_dirs (harness, path, created_at) VALUES (?, ?, ?)
           ON CONFLICT(harness) DO UPDATE SET path = excluded.path`
        ).run(harnessName, resolved, now);
      })();
    });
    return { harness: harnessName, path: resolved };
  }

  function list() {
    try {
      return withDb((db) =>
        db.prepare("SELECT harness, path, created_at FROM pipeline_dirs ORDER BY harness").all()
      );
    } catch (err) {
      logger?.warn?.(`pipeline-dirs: list failed: ${err?.message ?? err}`);
      return [];
    }
  }

  function get(harness) {
    if (!harness) return null;
    try {
      return withDb((db) =>
        db.prepare("SELECT harness, path, created_at FROM pipeline_dirs WHERE harness = ?").get(String(harness)) ?? null
      );
    } catch (err) {
      logger?.warn?.(`pipeline-dirs: get failed: ${err?.message ?? err}`);
      return null;
    }
  }

  // Pure precedence over an explicit lookup + env + config + default — the
  // store method below delegates here so tests pin the order without a DB.
  function resolve({ harness = null, env = process.env, dataDir = null, configDir = null, lookup = null } = {}) {
    const fromTable = typeof lookup === "function" ? lookup(harness) : null;
    if (fromTable?.path) return { dir: fromTable.path, source: "table" };
    if (env?.MANIFOLD_PIPELINES_DIR) return { dir: env.MANIFOLD_PIPELINES_DIR, source: "env" };
    if (configDir) return { dir: configDir, source: "config" };
    return { dir: defaultPipelinesDir(dataDir ?? resolveDataDir({ env })), source: "default" };
  }

  function resolveDir({ harness = null, env = process.env, dataDir = null, configDir = null } = {}) {
    return resolve({ harness, env, dataDir, configDir, lookup: (h) => get(h) });
  }

  return { register, list, get, resolve, resolveDir, close() {} };
}
