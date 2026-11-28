// admission-promotion-resume.js — wakes a flow once admission-queue-store's
// release()/releaseByFlow()/fillFreeSlots() promotes it from "queued" to
// "admitted". Handles "child-dispatch" (queued subflow child) and "flow-start"
// (queued root flow, or one re-queued on gate resume).
//
// A queued flow has no in-process waiter — a plain flow_runs row that may have
// sat queued for hours across a restart. Waking flips it to 'running' and
// re-enters via resume().
//
// The promoted run is DETACHED from the releaser's stack: the durable part
// (promotion + flip) is awaited, but resume() starts on its own tick. Awaiting
// inline would make the releaser (often mid state-store write) wait for the
// promoted run's next block point, which can promote the next flow inline in
// turn, nesting every queued flow in the first releaser's call.
//
// Gateway readiness: a woken flow may reach an agent node during startup;
// nothing to wait for here — the executor's run-turn gate holds those.

import { flipToRunningAndResume } from "./child-resume-trigger.js";

// On globalThis, not module scope: extensions can load this file via both jiti
// and native ESM in one process, and a CLI's drainDetachedResumes() must see
// tasks scheduled by either instance.
const PENDING_KEY = "__manifold_detached_resumes__";
function pendingResumes() {
  if (!globalThis[PENDING_KEY]) globalThis[PENDING_KEY] = new Set();
  return globalThis[PENDING_KEY];
}

// Starts resume(flowId) on the next tick, in its own call stack. Never throws
// or rejects — failures logged with `context` (caller already returned).
export function scheduleDetachedResume(resume, flowId, { logger, context } = {}) {
  const pending = pendingResumes();
  const task = new Promise((ready) => setImmediate(ready))
    .then(() => resume(flowId))
    .catch((err) => {
      logger?.warn?.(`${context ?? "admission-promotion-resume"}: detached resume [${String(flowId).slice(0, 8)}] failed: ${err?.message ?? err}`);
    })
    .finally(() => pending.delete(task));
  pending.add(task);
  return task;
}

// For short-lived processes (CLI) that must not exit with a detached resume
// pending. Loops because a resumed run can schedule another detached resume.
export async function drainDetachedResumes() {
  const pending = pendingResumes();
  while (pending.size > 0) {
    await Promise.allSettled([...pending]);
  }
}

const RESUMABLE_KINDS = new Set(["child-dispatch", "flow-start"]);

// Guards against driving a promoted row whose kind this engine doesn't admit
// (no flow run to flip).
export function isEngineResumableKind(kind) {
  return RESUMABLE_KINDS.has(kind);
}

// Flips the promoted flow to 'running' (awaited) and schedules its run
// (detached); `resumed: true` means "flipped and scheduled", not "ran".
export async function resumePromotedAdmission({ promoted, runStore, resume, logger, api, context }) {
  if (!promoted || !isEngineResumableKind(promoted.kind)) {
    return { resumed: false, reason: "not_a_resumable_promotion" };
  }
  if (typeof resume !== "function") return { resumed: false, reason: "no_resume_fn" };
  const detachedResume = (flowId) => { scheduleDetachedResume(resume, flowId, { logger, api, context }); };
  // No admissionQueueStore: this row was just PROMOTED, so its admission is
  // already 'admitted' — re-admitting would claim a duplicate flow+kind.
  return flipToRunningAndResume({ parentFlowId: promoted.flowId, runStore, resume: detachedResume, logger, api });
}

// The ONE place every admissionQueueStore.release()/releaseByFlow() call site
// goes through, instead of re-deriving the promoted-then-resume glue.
// `releaseResult` may be an array (a departing flow releases both kinds at
// once — at most one carries a real promotion, first non-null wins). Failures
// log a warning tagged with `context`; no call site checks `result.resumed`.
// Result shape still returned for callers that care.
export async function wakePromotedRelease(releaseResult, { runStore, resume, logger, api, context }) {
  const results = Array.isArray(releaseResult) ? releaseResult : [releaseResult];
  const promoted = results.reduce((found, r) => found ?? r?.promoted ?? null, null);
  if (!promoted || !isEngineResumableKind(promoted.kind)) {
    return { resumed: false, reason: "nothing_to_wake" };
  }
  try {
    const result = await resumePromotedAdmission({ promoted, runStore, resume, logger, api, context });
    if (!result.resumed) {
      logger?.warn?.(`${context}: promoted [${promoted.id.slice(0, 8)}] did not resume: ${result.reason ?? "unknown"}`);
    }
    return result;
  } catch (err) {
    logger?.warn?.(`${context}: resume after promotion [${promoted.id.slice(0, 8)}] failed: ${err?.message ?? err}`);
    return { resumed: false, reason: "threw", error: err?.message ?? String(err) };
  }
}
