// sidecar/paths.js — manifold owns its data directory. The store belongs
// to the manifold sidecar, not to whichever harness happens to launch it.
//
// Precedence: MANIFOLD_DATA_DIR, then ~/.manifold/. Legacy harness-managed
// files (~/.openclaw/runtime.sqlite, ~/.openclaw/ops.sqlite) are copied in
// once on first boot and never touched again afterwards — no harness, keeper
// script, or shell probe may open the package-owned copies.
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";

const require = createRequire(import.meta.url);

export function defaultDataDir(home = os.homedir()) {
  return path.join(home, ".manifold");
}

export function resolveDataDir({ env = process.env, home = os.homedir() } = {}) {
  return env.MANIFOLD_DATA_DIR || defaultDataDir(home);
}

export function runtimeDbPath(dataDir) {
  return path.join(dataDir, "runtime.sqlite");
}

export function opsDbPath(dataDir) {
  return path.join(dataDir, "ops.sqlite");
}

export function apiTokenPath(dataDir) {
  return path.join(dataDir, ".api-token");
}

function migrationMarkerPath(dataDir) {
  return path.join(dataDir, ".migrated.json");
}

function legacyDbPath(home, name) {
  return path.join(home, ".openclaw", name);
}

// One-time copy from harness-managed paths into the package-owned data dir.
// Copy (never move): the legacy files stay where they are for the old
// gateway-hosted manifold until cutover; after cutover nothing reads them.
// Integrity-checked after copy; throws on a damaged source so boot refuses
// loudly instead of serving a torn store. Idempotent via the marker file.
export function ensureMigrated({ dataDir, home = os.homedir(), logger = null } = {}) {
  mkdirSync(dataDir, { recursive: true });
  if (existsSync(migrationMarkerPath(dataDir))) return { migrated: false };
  const runtime = runtimeDbPath(dataDir);
  const ops = opsDbPath(dataDir);
  if (existsSync(runtime) && existsSync(ops)) {
    writeFileSync(migrationMarkerPath(dataDir), JSON.stringify({ at: new Date().toISOString(), from: null }) + "\n");
    return { migrated: false };
  }
  const legacyRuntime = legacyDbPath(home, "runtime.sqlite");
  const legacyOps = legacyDbPath(home, "ops.sqlite");
  const sources = [];
  if (existsSync(legacyRuntime) && !existsSync(runtime)) sources.push([legacyRuntime, runtime]);
  if (existsSync(legacyOps) && !existsSync(ops)) sources.push([legacyOps, ops]);
  if (sources.length === 0) {
    // Fresh install: nothing to copy. The marker records that the decision
    // was made, so a later-appearing legacy file is never picked up mid-life
    // (that would fork the store).
    writeFileSync(migrationMarkerPath(dataDir), JSON.stringify({ at: new Date().toISOString(), from: null, fresh: true }) + "\n");
    return { migrated: false };
  }
  for (const [src, dest] of sources) {
    copyFileSync(src, dest);
    logger?.info?.(`sidecar: migrated ${src} -> ${dest}`);
  }
  verifyCopies(sources.map(([, dest]) => dest));
  writeFileSync(
    migrationMarkerPath(dataDir),
    JSON.stringify({ at: new Date().toISOString(), from: sources.map(([src]) => src) }) + "\n",
  );
  return { migrated: true, from: sources.map(([src]) => src) };
}

// quick_check (not full integrity_check — the multi-GB primaries would add
// minutes to first boot) on every copied file. Lazy native import so this
// module stays light for tests and for callers that never migrate.
function verifyCopies(destPaths) {
  for (const dest of destPaths) {
    let Database;
    try {
      const mod = require("better-sqlite3");
      Database = mod?.default ?? mod;
    } catch {
      throw new Error(`sidecar: cannot verify migrated copy ${dest} (better-sqlite3 unavailable)`);
    }
    const db = new Database(dest, { readonly: true });
    try {
      const row = db.prepare("PRAGMA quick_check;").get();
      const out = row ? Object.values(row)[0] : null;
      if (out !== "ok") throw new Error(`sidecar: migrated copy failed quick_check: ${dest}: ${out}`);
    } finally {
      db.close();
    }
  }
}

export function readMigrationMarker(dataDir) {
  try {
    return JSON.parse(readFileSync(migrationMarkerPath(dataDir), "utf8"));
  } catch {
    return null;
  }
}
