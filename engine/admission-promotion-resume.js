// admission-promotion-resume.js — wakes a flow once admission-queue-store's
// release()/releaseByFlow()/fillFreeSlots() promotes it from "queued" to
// "admitted" (a pool slot actually freed). Handles both kinds:
// "child-dispatch" (queued subflow child) and "flow-start" (queued root
// flow, or one re-queued on resume from a gate).
//
// A queued flow has no in-process waiter: it's a plain flow_runs row that
// may have sat queued for hours across a process restart. Waking it means
// flipping the row to 'running' and re-entering the engine via resume().
//
// The promoted run is DETACHED from the releaser's call stack: the durable
// part (promotion + flip) is awaited, but resume() starts on its own tick.
// Awaiting it inline would make the releaser — often a flow inside its own
// state-store write — wait for the promoted run to reach its next block
// point, and that run can promote the next flow inline in turn, nesting
// every queued flow inside the first releaser's call.
//
// Gateway readiness: a woken flow may not have run since a restart, so it
// can reach an agent node during the startup window — nothing to wait for
// here; the executor's run-turn gate holds those dispatches until the
// gateway is ready.

import { flipToRunningAndResume } from "./child-resume-trigger.js";

// On globalThis, not module scope: extensions can load this file through
// both jiti and native ESM in one process, and a CLI's
// drainDetachedResumes() must see tasks scheduled by either instance.
const PENDING_KEY = "__manifold_detached_resumes__";
function pendingResumes() {
  if (!globalThis[PENDING_KEY]) globalThis[PENDING_KEY] = new Set();
  return globalThis[PENDING_KEY];
}

// Starts resume(flowId) on the next tick, in its own call stack. Never
// throws or rejects — failures are logged with `context`, since the caller
// has already returned by the time the run happens.
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

// For short-lived processes (CLI) that must not exit while a detached
// resume is pending. Loops because a resumed run can itself schedule
// another detached resume.
export async function drainDetachedResumes() {
  const pending = pendingResumes();
  while (pending.size > 0) {
    await Promise.allSettled([...pending]);
  }
}

const RESUMABLE_KINDS = new Set(["child-dispatch", "flow-start"]);

// Guards against driving a promoted row whose kind this engine doesn't
// admit — such a row has no flow run to flip.
export function isEngineResumableKind(kind) {
  return RESUMABLE_KINDS.has(kind);
}

// Flips the promoted flow to 'running' (awaited) and schedules its run
// (detached), so `resumed: true` means "flipped and scheduled", not "ran".
export async function resumePromotedAdmission({ promoted, runStore, resume, logger, api, context }) {
  if (!promoted || !isEngineResumableKind(promoted.kind)) {
    return { resumed: false, reason: "not_a_resumable_promotion" };
  }
  if (typeof resume !== "function") return { resumed: false, reason: "no_resume_fn" };
  const detachedResume = (flowId) => { scheduleDetachedResume(resume, flowId, { logger, api, context }); };
  // No admissionQueueStore: this row was just PROMOTED, so its admission is
  // already 'admitted' — re-admitting would claim a duplicate for the same
  // flow+kind.
  return flipToRunningAndResume({ parentFlowId: promoted.flowId, runStore, resume: detachedResume, logger, api });
}

// The ONE place every admissionQueueStore.release()/releaseByFlow() call
// site should go through, instead of each re-deriving the same
// promoted-then-resume glue. `releaseResult` may be an array of several
// (a departing flow releases both kinds at once — at most one carries a
// real promotion, so the first non-null wins). Failures log a warning
// tagged with `context`, so no call site must check `result.resumed`
// itself; the result shape is still returned for callers that care.
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
