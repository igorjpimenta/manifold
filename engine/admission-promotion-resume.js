// admission-promotion-resume.js — wakes a flow once admission-queue-
// store.js's release()/releaseByFlow()/fillFreeSlots() promotes it out of
// "queued" and into "admitted" — i.e. once a slot in the shared pool
// actually frees up. Handles both admission kinds: kind:"child-dispatch" (a
// queued subflow child) and kind:"flow-start" (a queued root flow, or a
// root flow re-queued on resume from a gate — see engine/child-resume-
// trigger.js's admitAndSetRunning/flipToRunningAndResume).
//
// A queued flow/child has no in-process waiter at all: it's a real
// flow_runs row that may have been queued for hours by a process that has
// since restarted. Promoting it means flipping that row to 'running' and
// re-entering the engine via resume(), like any other blocked/queued-flow
// wake in this codebase — engine/child-resume-trigger.js's
// flipToRunningAndResume already does exactly that.
//
// The promoted flow's run is DETACHED from the releaser's call stack
// (scheduleDetachedResume below): the durable part — the admission
// promotion and the flip to 'running' — is still awaited, but resume()
// itself starts on its own tick. Awaiting it inline would make whoever
// called release() — often a flow that just went blocked or terminal inside
// its own state-store write — wait for the promoted flow's whole run to
// reach its next block point, and that run can release and promote the
// next flow inline in turn, nesting every flow in the queue inside the
// first releaser's call.
//
// Gateway readiness: a flow/child woken here may not have run at all since
// the gateway restarted (a queued admission survives a crash same as any
// other flow_runs row), so it can reach an agent-kind node inside the
// gateway's startup window. Nothing to wait for here: resume() is the
// executor's, and every subagent call it makes goes through the executor's
// readiness gate (engine/gateway-subagent.js).

import { flipToRunningAndResume } from "./child-resume-trigger.js";

// On globalThis, not module scope: extensions/manifold/** can load through
// both jiti and native ESM in one process, and a CLI command's
// drainDetachedResumes() must see tasks scheduled by either instance.
const PENDING_KEY = "__manifold_detached_resumes__";
function pendingResumes() {
  if (!globalThis[PENDING_KEY]) globalThis[PENDING_KEY] = new Set();
  return globalThis[PENDING_KEY];
}

// Starts resume(flowId) on the next tick, in its own call stack. Never
// throws and never rejects — a failure is logged tagged with `context`,
// since the caller has already returned by the time the run happens.
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

// For short-lived processes (CLI commands) that must not exit while a
// detached resume is still pending. Loops because a resumed run can itself
// release a slot and schedule another detached resume.
export async function drainDetachedResumes() {
  const pending = pendingResumes();
  while (pending.size > 0) {
    await Promise.allSettled([...pending]);
  }
}

const RESUMABLE_KINDS = new Set(["child-dispatch", "flow-start"]);

// Guards against driving a promoted row whose kind this engine doesn't
// admit (an unknown or legacy kind) — such a row has no flow run to flip.
export function isEngineResumableKind(kind) {
  return RESUMABLE_KINDS.has(kind);
}

// Flips the promoted flow to 'running' (awaited) and schedules its run
// (detached — see this file's header), so `resumed: true` means "flipped
// and scheduled", not "ran".
export async function resumePromotedAdmission({ promoted, runStore, resume, logger, api, context }) {
  if (!promoted || !isEngineResumableKind(promoted.kind)) {
    return { resumed: false, reason: "not_a_resumable_promotion" };
  }
  if (typeof resume !== "function") return { resumed: false, reason: "no_resume_fn" };
  const detachedResume = (flowId) => { scheduleDetachedResume(resume, flowId, { logger, api, context }); };
  // No admissionQueueStore passed here, deliberately — this flow was just
  // PROMOTED, meaning its admission row is already 'admitted'. Asking
  // flipToRunningAndResume to re-admit would claim a second, duplicate
  // entry for the same flow+kind.
  return flipToRunningAndResume({ parentFlowId: promoted.flowId, runStore, resume: detachedResume, logger, api });
}

// wakePromotedRelease — the ONE place every admissionQueueStore.release()/
// releaseByFlow() call site should go through, instead of each
// independently re-deriving the same "if (result.promoted) { await
// resumePromotedAdmission(...).catch(err => logger.warn(...)) }" glue.
//
// `releaseResult` is whatever release()/releaseByFlow() resolved to, or an
// array of several (state-store.js's triggerAdmissionRelease releases both
// flow-start and child-dispatch for the SAME departing flow in one
// Promise.all — at most one of the two can ever carry a real promotion, so
// the first non-null wins). An unsuccessful outcome — resumePromotedAdmission
// resolving {resumed:false}, or throwing outright — logs a warning tagged
// with the caller-supplied `context`, so no call site has to remember to
// check `result.resumed` itself.
//
// Returns resumePromotedAdmission's own result shape ({resumed, reason,
// ...}) so a caller that wants to act on the outcome (e.g. agent/cli.js's
// release-leaked-admission, which prints it for the operator) still can;
// callers that are purely fire-and-forget can ignore the return value.
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
