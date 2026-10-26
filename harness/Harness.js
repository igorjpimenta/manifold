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

export const OPTIONAL_METHODS = ["getSubagent"];

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
