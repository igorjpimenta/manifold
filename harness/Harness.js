// storePaths() returns { dataDir, runtimeDb, opsDb, pendingDir, locksDir,
// pipelinesDir }. Each adapter resolves its own root (openclaw:
// ~/.openclaw; opencode: ~/.manifold, never ~/.openclaw) with
// MANIFOLD_{RUNTIME_DB,OPS_DB,PENDING_DIR,LOCKS_DIR,PIPELINES_DIR} env
// overrides. Core takes explicit dirs/dbPaths threaded from the adapter
// scope — it never reads os.homedir itself.
export const HARNESS_METHODS = [
  "spawn",
  "waitForRun",
  "getSessionMessages",
  "emit",
  "registerTool",
  "registerHook",
  "storePaths",
  "sessionKey",
];

export const OPTIONAL_METHODS = ["getSubagent", "registerHttpRoute", "registerService"];

export function assertHarnessShape(h, { what = "harness" } = {}) {
  if (!h || typeof h !== "object") throw new Error(`${what}: harness must be an object`);
  if (typeof h.id !== "string" || h.id.length === 0) {
    throw new Error(`${what}: harness.id must be a non-empty string (declare at construction)`);
  }
  for (const m of HARNESS_METHODS) {
    if (typeof h[m] !== "function") throw new Error(`${what}: harness.${m}() missing`);
  }
  return h;
}

export function createHarnessPort(impl) {
  return assertHarnessShape({ ...impl });
}
