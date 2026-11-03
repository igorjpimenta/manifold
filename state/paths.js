// state/paths.js — manifold owns its data directory. The store belongs to
// manifold core, not to whichever harness happens to launch it.
//
// Precedence: MANIFOLD_DATA_DIR, then ~/.manifold/. Legacy harness-managed
// files (~/.openclaw/{runtime.sqlite,ops.sqlite}) are copied in once on
// first boot and never touched again afterwards.
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// Lazy CJS binding (not a static named import) so ESM test mocks of
// node:fs that only stub the functions they intercept keep linking.
const fs = require("node:fs");
const { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } = fs;

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

export function pendingDir(dataDir) {
  return path.join(dataDir, "discuss-pending");
}

export function locksDir(dataDir) {
  return path.join(dataDir, "locks");
}

export function pipelinesDir(dataDir) {
  return path.join(dataDir, "workspace", ".pipelines");
}

export function discussSessionsDir(dataDir) {
  return path.join(dataDir, "discuss-sessions");
}

export function discussAudioDir(dataDir) {
  return path.join(dataDir, "discuss-audio");
}

// Pre-store-home locations, read-only migration fallback: sidecars written
// before the store-home move live here. Never written to — the only writer
// is the store-home dir above. Centralized here so no other file joins the
// harness home to its own path.
export function legacyDiscussSessionsDir(home = os.homedir()) {
  return path.join(home, ".openclaw", "discuss-sessions");
}

export function legacyDiscussAudioDir(home = os.homedir()) {
  return path.join(home, ".openclaw", "discuss-audio");
}

export function stateFile(dataDir) {
  return path.join(dataDir, "state", "manifold-paused.json");
}

function migrationMarkerPath(dataDir) {
  return path.join(dataDir, ".migrated.json");
}

function legacyDbPath(home, name) {
  return path.join(home, ".openclaw", name);
}

// One-time copy from harness-managed paths into the core-owned data dir.
// Copy (never move): the legacy files stay where they are for the old
// gateway-hosted manifold until cutover; after cutover nothing reads them.
// Integrity-checked after copy; throws on a damaged source so boot refuses
// loudly instead of serving a torn store. Idempotent via the marker file.
// Any other failure (missing native module, unwritable dir) is logged, not
// thrown — boot must not fail on a best-effort migration.
export function ensureMigrated({ dataDir, home = os.homedir(), logger = null } = {}) {
  try {
    mkdirSync(dataDir, { recursive: true });
  } catch (err) {
    logger?.warn?.(`paths: cannot create data dir ${dataDir}: ${err?.message ?? err}`);
    return { migrated: false };
  }
  if (existsSync(migrationMarkerPath(dataDir))) return { migrated: false };
  const runtime = runtimeDbPath(dataDir);
  const ops = opsDbPath(dataDir);
  if (existsSync(runtime) && existsSync(ops)) {
    try {
      writeFileSync(migrationMarkerPath(dataDir), JSON.stringify({ at: new Date().toISOString(), from: null }) + "\n");
    } catch (err) {
      logger?.warn?.(`paths: cannot write migration marker: ${err?.message ?? err}`);
    }
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
    try {
      writeFileSync(migrationMarkerPath(dataDir), JSON.stringify({ at: new Date().toISOString(), from: null, fresh: true }) + "\n");
    } catch (err) {
      logger?.warn?.(`paths: cannot write migration marker: ${err?.message ?? err}`);
    }
    return { migrated: false };
  }
  for (const [src, dest] of sources) {
    copyFileSync(src, dest);
    logger?.info?.(`paths: migrated ${src} -> ${dest}`);
  }
  // Loud throw on a damaged source is the one allowed throw: boot must
  // refuse a torn store rather than serve it. An unavailable verifier is
  // a loud log, not a throw — the marker records the decision either way
  // so a later-appearing legacy file can never fork the store.
  let verified = true;
  try {
    verifyCopies(sources.map(([, dest]) => dest));
  } catch (err) {
    if (err?.code !== "MANIFOLD_VERIFY_UNAVAILABLE") throw err;
    verified = false;
    logger?.error?.(`paths: migrated copies UNVERIFIED: ${err?.message ?? err}`);
  }
  try {
    writeFileSync(
      migrationMarkerPath(dataDir),
      JSON.stringify({ at: new Date().toISOString(), from: sources.map(([src]) => src), verified }) + "\n",
    );
  } catch (err) {
    logger?.warn?.(`paths: cannot write migration marker: ${err?.message ?? err}`);
  }
  return { migrated: true, from: sources.map(([src]) => src) };
}

// quick_check (not full integrity_check — multi-GB primaries would add
// minutes to first boot) on every copied file. Lazy native import so this
// module stays light for callers that never migrate.
function verifyCopies(destPaths) {
  for (const dest of destPaths) {
    let Database;
    try {
      const mod = require("better-sqlite3");
      Database = mod?.default ?? mod;
    } catch {
      const err = new Error(`paths: cannot verify migrated copy ${dest} (better-sqlite3 unavailable)`);
      err.code = "MANIFOLD_VERIFY_UNAVAILABLE";
      throw err;
    }
    const db = new Database(dest, { readonly: true });
    try {
      const row = db.prepare("PRAGMA quick_check;").get();
      const out = row ? Object.values(row)[0] : null;
      if (out !== "ok") throw new Error(`paths: migrated copy failed quick_check: ${dest}: ${out}`);
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

// Core-owned store home: every store default derives from here, so one
// MANIFOLD_DATA_DIR move relocates the whole store. Explicit per-path env
// overrides win over the dataDir derivation for operators who split files
// across volumes. Accepts a pre-resolved `paths` object (same shape as the
// return) so the adapter resolves once and threads it through.
export function resolveStorePaths({ paths = null, dataDir = null, env = process.env } = {}) {
  const dir = paths?.dataDir ?? dataDir ?? resolveDataDir({ env });
  return {
    dataDir: dir,
    runtimeDb: paths?.runtimeDb ?? env.MANIFOLD_RUNTIME_DB ?? runtimeDbPath(dir),
    opsDb: paths?.opsDb ?? env.MANIFOLD_OPS_DB ?? opsDbPath(dir),
    pendingDir: paths?.pendingDir ?? env.MANIFOLD_PENDING_DIR ?? pendingDir(dir),
    locksDir: paths?.locksDir ?? env.MANIFOLD_LOCKS_DIR ?? locksDir(dir),
    pipelinesDir: paths?.pipelinesDir ?? env.MANIFOLD_PIPELINES_DIR ?? pipelinesDir(dir),
    discussSessionsDir: paths?.discussSessionsDir ?? env.MANIFOLD_DISCUSS_SESSIONS_DIR ?? discussSessionsDir(dir),
    discussAudioDir: paths?.discussAudioDir ?? env.MANIFOLD_DISCUSS_AUDIO_DIR ?? discussAudioDir(dir),
    legacyDiscussSessionsDir: paths?.legacyDiscussSessionsDir ?? legacyDiscussSessionsDir(),
    legacyDiscussAudioDir: paths?.legacyDiscussAudioDir ?? legacyDiscussAudioDir(),
  };
}
