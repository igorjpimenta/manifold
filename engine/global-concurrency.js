// global-concurrency.js — the single shared concurrency ceiling
// state/admission-queue-store.js reads as its getLimit(): the pool of
// running flow runs (kind "child-dispatch" and "flow-start"). A run
// executes one node at a time, so this also bounds concurrent agent
// dispatches. The only per-pipeline escape hatch is `allowConcurrent:
// false` (a coarse singleton gate in start()) — there is no per-pipeline
// raise/lower of this ceiling.

const DEFAULT_MAX_CONCURRENT = 8;

let maxConcurrent = DEFAULT_MAX_CONCURRENT;

export function configureGlobalConcurrency({ maxConcurrent: max } = {}) {
  if (Number.isInteger(max) && max > 0) maxConcurrent = max;
}

export function getGlobalConcurrencyLimit() {
  return maxConcurrent;
}

export function _resetForTests(max = DEFAULT_MAX_CONCURRENT) {
  maxConcurrent = max;
}
