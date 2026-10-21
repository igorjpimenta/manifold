// routes/operator-resume.js — in-process resume for budget-gate approvals.
//
// resumeFlowFromGate holds the logic POST /pipeclaw/resume-from-gate used
// to serve over HTTP: re-request the flow's own admission (released the
// moment it blocked, state-store.js's setWaiting hook) and resume, leaving
// status:"queued" to the admission cycle when the pool is full. The budget
// decide route calls it directly instead of loopback-fetching the gateway —
// same inputs, same outcomes, no HTTP hop. The route itself stays as a thin
// validating wrapper.
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
        logger.warn(`pipeclaw resume-from-gate: runFlow [${flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
      });
    });
  }
  logger.info(`pipeclaw: resume-from-gate flow=${flowId.slice(0, 8)} node=${nodeId ?? "?"} decision=${decision ?? "?"}${result.queued ? " (queued for admission)" : ""}`);
  return { ok: true, flowId, queued: result.queued };
}
