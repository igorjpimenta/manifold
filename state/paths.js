import os from "node:os";
import path from "node:path";

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
  return path.join(dataDir, "pipelines");
}

export function mediaAudioDir(dataDir) {
  return path.join(dataDir, "media", "audio");
}

export function harnessMediaAudioDir(dataDir, harness) {
  const safe = String(harness ?? "").replace(/[^A-Za-z0-9._-]+/g, "_");
  if (!safe) throw new Error("manifold: harness is required to resolve the media dir (declare it on the connection)");
  return path.join(mediaAudioDir(dataDir), safe);
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
