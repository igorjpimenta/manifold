// state/paths.js — manifold owns its data directory. The store belongs to
// manifold core, not to whichever harness happens to launch it.
//
// Precedence: MANIFOLD_DATA_DIR, then the per-user data directory.
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// Lazy CJS binding (not a static named import) so ESM test mocks of
// node:fs that only stub the functions they intercept keep linking.
const fs = require("node:fs");
const { mkdirSync } = fs;

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

export function pipelinesDir(dataDir) {
  return path.join(dataDir, "workspace", ".pipelines");
}

// Media root for harness-delivered blobs (voice messages today). The store
// home is the root; the harness name namespaces it so two harnesses sharing
// one store never collide. Layout: <dataDir>/media/audio/<harness>/<flow>.
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

// Creates the store home if it is missing. The store lives in one
// core-owned directory: MANIFOLD_DATA_DIR, else the per-user directory.
// A failure to create it is logged, not thrown — boot must not die on a
// store it will report as unusable anyway.
export function ensureDataDir({ dataDir, logger = null } = {}) {
  try {
    mkdirSync(dataDir, { recursive: true });
    return { ok: true };
  } catch (err) {
    logger?.warn?.(`paths: cannot create data dir ${dataDir}: ${err?.message ?? err}`);
    return { ok: false };
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
    pipelinesDir: paths?.pipelinesDir ?? env.MANIFOLD_PIPELINES_DIR ?? pipelinesDir(dir),
    mediaAudioDir: paths?.mediaAudioDir ?? env.MANIFOLD_MEDIA_AUDIO_DIR ?? mediaAudioDir(dir),
  };
}
