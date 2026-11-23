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

// A value that arrived wrapped in matching quotes is a config error, and a
// silent one: the quotes become part of the path, so the process opens (and
// creates) a store whose name merely looks like the intended one, while every
// other reader — dashboard, CLI, telemetry — sees an empty history. Refuse
// loudly instead of stripping them: the environment is the authority on its
// own paths, and a typo there should not be papered over.
function unquotedEnvPath(env, name) {
  const raw = env?.[name];
  if (typeof raw !== "string") return undefined;
  const value = raw.trim();
  const q = value[0];
  if (value.length >= 2 && (q === '"' || q === "'") && value[value.length - 1] === q) {
    throw new Error(
      `manifold: ${name} arrived as a quoted literal — the quotes would become part of the path; unquote it in the environment`,
    );
  }
  return raw;
}

export function resolveDataDir({ env = process.env, home = os.homedir() } = {}) {
  return unquotedEnvPath(env, "MANIFOLD_DATA_DIR") || defaultDataDir(home);
}

export function runtimeDbPath(dataDir) {
  return path.join(dataDir, "runtime.sqlite");
}

export function opsDbPath(dataDir) {
  return path.join(dataDir, "ops.sqlite");
}

export function pipelinesDir(dataDir) {
  return path.join(dataDir, "workspace", ".pipelines");
}

export function mediaAudioDir(dataDir) {
  return path.join(dataDir, "media", "audio");
}

export function harnessMediaAudioDir(dataDir, harness) {
  const safe = String(harness ?? "").replace(/[^A-Za-z0-9._-]+/g, "_");
  if (!safe) throw new Error("manifold: harness is required to resolve the media dir (declare it on the connection)");
  return path.join(mediaAudioDir(dataDir), safe);
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

export function resolveStorePaths({ paths = null, dataDir = null, env = process.env } = {}) {
  const dir = paths?.dataDir ?? dataDir ?? resolveDataDir({ env });
  return {
    dataDir: dir,
    runtimeDb: paths?.runtimeDb ?? unquotedEnvPath(env, "MANIFOLD_RUNTIME_DB") ?? runtimeDbPath(dir),
    opsDb: paths?.opsDb ?? unquotedEnvPath(env, "MANIFOLD_OPS_DB") ?? opsDbPath(dir),
    pipelinesDir: paths?.pipelinesDir ?? unquotedEnvPath(env, "MANIFOLD_PIPELINES_DIR") ?? pipelinesDir(dir),
    mediaAudioDir: paths?.mediaAudioDir ?? unquotedEnvPath(env, "MANIFOLD_MEDIA_AUDIO_DIR") ?? mediaAudioDir(dir),
  };
}
