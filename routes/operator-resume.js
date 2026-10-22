// routes/operator-resume.js — in-process resume for budget-gate approvals.
//
// resumeFlowFromGate is the single implementation behind two callers: POST
// /pipeclaw/resume-from-gate (thin validating wrapper) and the budget
// decide route's in-process call. It re-requests the flow's own admission
// (released the moment it blocked, state-store.js's setWaiting hook) and
// resumes, leaving status:"queued" to the admission cycle when the pool is
// full.
import { admitAndSetRunning } from "../engine/child-resume-trigger.js";

export async function resumeFlowFromGate({
  store, readStore, admissionQueueStore, runStore, executor, logger, api,
  flowId, nodeId, decision,
}) {
  const flow = (await store.get(flowId)) ?? (await readStore?.get(flowId)) ?? null;
  if (!flow) return { ok: false, code: "flow_not_found" };
  const result = await admitAndSetRunning({
    flowId, expectedRevision: flow.revision, lastStep: flow.lastStep, stateJson: flow.stateJson ?? {},
    parentInvocationId: flow.parentInvocationId, store, admissionQueueStore,
    runStore, resume: executor.resume, logger, api,
  });
  if (!result.applied) return { ok: false, code: `setRunning failed: ${result.code}` };
  if (!result.queued) {
    setImmediate(() => {
      executor.resume(flowId).catch((err) => {
        logger.warn(`manifold resume-from-gate: runFlow [${flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
      });
    });
  }
  logger.info(`manifold: resume-from-gate flow=${flowId.slice(0, 8)} node=${nodeId ?? "?"} decision=${decision ?? "?"}${result.queued ? " (queued for admission)" : ""}`);
  return { ok: true, flowId, queued: result.queued };
}
