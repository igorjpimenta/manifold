// concurrency.js — per-harness concurrency ceiling.
// state/admission-queue-store.js reads as its getLimit(harness) — the pool of
// running flow runs: kind:"child-dispatch" (a subflow child) and
// kind:"flow-start" (a whole flow run, requested by executor.js's start()
// and re-requested on every resume from blocked — see engine/child-resume-
// trigger.js's admitAndSetRunning/flipToRunningAndResume). A run executes
// one node at a time, so this also bounds concurrent agent dispatches.
//
// Each harness declares its own ceiling (via MANIFOLD_CONCURRENCY env,
// X-Manifold-Concurrency header, or plugin config). Limits are stored
// per-harness, not as a global singleton — harness A's limit does not
// overwrite harness B's.

const DEFAULT_MAX_CONCURRENT = 8;

const limits = new Map();

function key(harness) {
  return (typeof harness === "string" && harness.trim() !== "") ? harness.trim() : null;
}

export function configureConcurrency({ harness = null, maxConcurrent: max } = {}) {
  const h = key(harness);
  if (!Number.isInteger(max) || max <= 0) return;
  if (h) limits.set(h, max);
  else limits.set("__default__", max);
}

export function getConcurrencyLimit(harness = null) {
  const h = key(harness);
  if (h && limits.has(h)) return limits.get(h);
  if (limits.has("__default__")) return limits.get("__default__");
  return DEFAULT_MAX_CONCURRENT;
}

export function _resetForTests(max = DEFAULT_MAX_CONCURRENT, harness = null) {
  if (harness === null) {
    limits.clear();
    limits.set("__default__", max);
  } else {
    limits.set(harness, max);
  }
}
