// global-concurrency.js — the single shared concurrency ceiling
// state/admission-queue-store.js reads as its getLimit() — the pool of
// running flow runs: kind:"child-dispatch" (a subflow child) and
// kind:"flow-start" (a whole flow run, requested by executor.js's start()
// and re-requested on every resume from blocked — see engine/child-resume-
// trigger.js's admitAndSetRunning/flipToRunningAndResume). A run executes
// one node at a time, so this also bounds concurrent agent dispatches.
//
// The only per-pipeline escape hatch is `allowConcurrent: false`, and it is
// coarse: a singleton gate in executor.js's start() rejects a run outright if
// another non-terminal run of the same pipelineId exists. There is no way to
// raise or lower the ceiling for one pipeline — a pipeline needing tighter
// serialization than this shared number (e.g. a pipeline colliding on a
// shared external resource) is handled by the pipeline author.

const DEFAULT_MAX_CONCURRENT = 8;

let maxConcurrent = DEFAULT_MAX_CONCURRENT;

// Called once at plugin registration from pluginConfig, if set. A no-op
// (keeps the current value) for anything not a positive integer.
export function configureGlobalConcurrency({ maxConcurrent: max } = {}) {
  if (Number.isInteger(max) && max > 0) maxConcurrent = max;
}

export function getGlobalConcurrencyLimit() {
  return maxConcurrent;
}

// Test-only reset (module state is a singleton; tests need a clean slate).
export function _resetForTests(max = DEFAULT_MAX_CONCURRENT) {
  maxConcurrent = max;
}
