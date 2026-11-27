// child-resume-trigger.js — wakes a parent flow once every child spawned by
// one subflow-node firing has gone terminal.
//
// Hooked into state/state-store.js's finish/skip/fail/cancel (not this
// engine's persist wrappers): boot/flow-revival.js's orphan-displacement
// path calls store.fail() directly, so wrapper-level hooks would miss waking
// a parent whose child got displaced at boot.
//
// Not wired into the live executor yet (loop-node dispatch not cut over);
// tested against synthetic parent/child pairs.

import { isTerminal } from "./executor-helpers.js";
import { wakePromotedRelease } from "./admission-promotion-resume.js";

// Shared by maybeResumeParent and boot/flow-revival.js's children-pending
// reconciliation pass — the same "is this subflow node's whole fan-out done"
// check, entered from a child walking up or from the parent scanning down at
// boot. Returns `children` too so callers can check per-child outcome
// (subflowOutcomeSatisfied).
export async function areSiblingsAllTerminal({ flowId, nodeId, runStore, nodeExecutionsStore }) {
  const siblingIds = await nodeExecutionsStore.listInvocationIdsForNode(flowId, nodeId);
  if (siblingIds.length === 0) return { allTerminal: false, reason: "no_siblings", children: [] };

  const children = await runStore.listForParentInvocations(siblingIds);
  // Eager creation spawns every child in one pass, but a resume check can race
  // that pass (a sibling invocation with no child row yet) — bail rather than
  // resume against an incomplete fan-out.
  if (children.length !== siblingIds.length) return { allTerminal: false, reason: "not_all_spawned", children: [] };

  const allTerminal = children.every((c) => isTerminal(c.status));
  return { allTerminal, reason: allTerminal ? "ok" : "siblings_pending", children };
}

// A subflow node's `mode` ("any"|"all") decides whether fully-terminal
// children satisfy the fan-out. "any" needs one success (multi-item fan-out,
// per-item failure normal); "all" needs every child to succeed (single-item
// subflow like ENSURE_LOGIN). Unrecognized/missing mode fails open (never
// blocks resume) — defensive; the loader requires a valid mode, so this
// shouldn't happen.
export function subflowOutcomeSatisfied(mode, children) {
  if (mode === "any") return children.some((c) => c.status === "succeeded");
  if (mode === "all") return children.every((c) => c.status === "succeeded");
  return true;
}

// Shared by maybeResumeParent and boot/flow-revival.js. `resume()` is
// runFlow(flowId); runFlow's first check returns immediately if the flow is
// still 'blocked' (guard makes a stray duplicate resume a safe no-op). Every
// other caller of resume() (gates/gate-resolver.js) flips status to 'running'
// via store.setRunning() first — this mirrors that via runStore's raw CAS,
// since this module depends only on runStore/nodeExecutionsStore. A concurrent
// caller racing the flip loses the CAS (revision mismatch) and doesn't resume
// — never a double-dispatch.
//
// Repair propagation: a parent can already be terminal 'failed' here — the
// mode-not-satisfied path fails a parent the moment its fan-out looks
// unsatisfiable, but a later retry of a surviving child can turn it satisfied.
// When repairGuard is supplied (only maybeResumeParent), a 'failed' parent is
// force-unblocked back to 'blocked' first, then the normal casSetRunning flip
// proceeds against the bumped revision. 'succeeded'/'cancelled'/'skipped' are
// never repaired — only 'failed' is eligible.
// admissionKindForFlow: a root flow (no parent) competes as "flow-start"; a
// subflow-spawned child as "child-dispatch". Used by every caller that
// re-requests admission for a flow it's about to resume.
export function admissionKindForFlow(flow) {
  return flow.parentInvocationId ? "child-dispatch" : "flow-start";
}

// Re-requests a flow's own admission before flipping it back to "running"
// through the FULL state-store (not runStore's raw CAS — used by every live
// resume path outside this file: gate-resolver.js, routes/operator.js budget-
// gate resume, routes/operator-retry.js). The flow's admission was released
// when it left "running" (state-store setWaiting/terminal hooks); resuming
// must re-claim a slot. Returns a setRunning/setQueued-shaped result
// ({applied, code}) plus `queued: true` when the pool was full: the flow is
// left at status:"queued" and the caller skips resume-dispatch; the admission
// queue promotes it later.
export async function admitAndSetRunning({ flowId, expectedRevision, lastStep, stateJson, parentInvocationId, store, admissionQueueStore, runStore, resume, logger, api }) {
  const kind = admissionKindForFlow({ parentInvocationId });
  const admission = await admissionQueueStore.admit({ kind, flowId });
  if (admission.status === "queued") {
    const result = await store.setQueued({
      flowId, expectedRevision, lastStep, stateJson,
      waitJson: { kind },
      statusReason: `queued: waiting for a shared admission slot to resume (${kind})`,
    });
    // Claimed a queue position but didn't use it (CAS lost) — release it or
    // it leaks a permanent pool-capacity hole. This admission is 'queued', not
    // 'admitted', so release can't promote anything.
    if (!result.applied) await admissionQueueStore.release(admission.id);
    return { applied: result.applied, queued: true, code: result.code };
  }
  const result = await store.setRunning({ flowId, expectedRevision, lastStep, stateJson });
  if (!result.applied) {
    // Claimed an admitted slot but didn't use it (CAS lost) — release it; this
    // admission WAS 'admitted', so releasing can promote a different flow's
    // queued row — drive wakePromotedRelease or the promoted flow strands.
    const released = await admissionQueueStore.release(admission.id);
    await wakePromotedRelease(released, {
      runStore, resume, logger, api, context: "child-resume-trigger: admitAndSetRunning CAS-loss release",
    });
  }
  return { applied: result.applied, queued: false, code: result.code };
}

// admissionQueueStore, when provided, re-requests the flow's own admission
// before flipping it running (see admitAndSetRunning). Omit for a caller that
// already holds a live admission and is acting on a promotion
// (admission-promotion-resume.js) — passing it there would claim a duplicate.
export async function flipToRunningAndResume({ parentFlowId, runStore, resume, logger, repairGuard, admissionQueueStore, api }) {
  const parentRow = await runStore.get(parentFlowId);
  if (!parentRow) return { resumed: false, reason: "parent_not_found" };

  let expectedRevision = parentRow.revision;

  if (parentRow.status === "failed" && repairGuard) {
    const { nodeExecutionsStore, nodeId, subflowKey, expectedInvocationId } = repairGuard;
    const latest = await nodeExecutionsStore.getLatestInvocationForNode(parentFlowId, nodeId, subflowKey);
    if (!latest || latest.id !== expectedInvocationId) {
      return { resumed: false, reason: "repair_stale_invocation" };
    }
    const unblock = await runStore.forceBlock(parentFlowId, {
      expectedRevision,
      stateJson: parentRow.stateJson,
      statusReason: `repaired: subflow "${nodeId}" outcome later satisfied by a retried child`,
      ownerKey: parentRow.ownerKey,
    });
    if (!unblock.applied) return { resumed: false, reason: `repair_forceBlock_failed:${unblock.code}` };
    expectedRevision += 1;
  }

  let admissionId = null;
  if (admissionQueueStore) {
    const kind = admissionKindForFlow(parentRow);
    const admission = await admissionQueueStore.admit({ kind, flowId: parentFlowId });
    if (admission.status === "queued") {
      const queued = await runStore.casSetQueued(parentFlowId, {
        expectedRevision,
        lastStep: parentRow.lastStep,
        stateJson: parentRow.stateJson,
        waitJson: { kind },
        statusReason: `queued: waiting for a shared admission slot to resume (${kind})`,
      });
      if (!queued.applied) await admissionQueueStore.release(admission.id);
      return { resumed: false, reason: queued.applied ? "queued_for_admission" : `queue_failed:${queued.code}` };
    }
    admissionId = admission.id;
  }

  const flip = await runStore.casSetRunning(parentFlowId, {
    expectedRevision,
    lastStep: parentRow.lastStep,
    stateJson: parentRow.stateJson,
  });
  if (!flip.applied) {
    if (admissionId) {
      const released = await admissionQueueStore.release(admissionId);
      await wakePromotedRelease(released, {
        runStore, resume, logger, api, context: "child-resume-trigger: flipToRunningAndResume CAS-loss release",
      });
    }
    return { resumed: false, reason: `flip_failed:${flip.code}` };
  }

  try {
    await resume(parentFlowId);
    return { resumed: true, parentFlowId };
  } catch (err) {
    logger?.warn?.(`child-resume-trigger: resume(${parentFlowId}) failed: ${err?.message ?? err}`);
    return { resumed: false, reason: "resume_threw", error: err?.message ?? String(err) };
  }
}

// maybeResumeParent: checks whether `flowId` (just went terminal) is a
// subflow-spawned child, and if so whether every sibling under the same
// (parent flowId, nodeId) is terminal. If so and the subflow node's `mode`
// is satisfied, resumes the parent. If mode is NOT satisfied, fails the
// parent directly via failFlow instead of resuming, so a mode-mismatched
// fan-out never silently continues. pipelinesRef optional — if omitted or the
// node/pipeline can't be resolved, mode enforcement is skipped (pre-mode-check
// unconditional resume); only real callers wire it.
// Fire-and-forget by design — callers should not await inline with a CAS write;
// call after, uninstrumented failures logged and swallowed.
// failureStore optional (tests may omit): when provided, a mode-not-satisfied
// failure also gets a real failure_events row (failFlow alone is a bare status
// write with no invocationId/executionId, so it never reaches
// recordEngineFailure otherwise — parent fails with no Inbox entry/retry).
export async function maybeResumeParent({ flowId, runStore, nodeExecutionsStore, pipelinesRef, resume, failFlow, failureStore, admissionQueueStore, logger, api }) {
  if (typeof resume !== "function") return { resumed: false, reason: "no_resume_fn" };

  const row = await runStore.get(flowId);
  if (!row?.parentInvocationId) return { resumed: false, reason: "not_a_child" };

  const invocation = await nodeExecutionsStore.getInvocation(row.parentInvocationId);
  if (!invocation) return { resumed: false, reason: "invocation_not_found" };

  const { allTerminal, reason, children } = await areSiblingsAllTerminal({
    flowId: invocation.flowId, nodeId: invocation.nodeId, runStore, nodeExecutionsStore,
  });
  if (!allTerminal) return { resumed: false, reason };

  const parentFlow = await runStore.get(invocation.flowId);
  if (!parentFlow) return { resumed: false, reason: "parent_not_found" };

  const mode = pipelinesRef?.value?.get(parentFlow.pipelineId)?.nodes?.[invocation.nodeId]?.mode;
  if (mode && !subflowOutcomeSatisfied(mode, children)) {
    if (typeof failFlow !== "function") {
      logger?.warn?.(`child-resume-trigger: subflow "${invocation.nodeId}" mode:"${mode}" not satisfied but no failFlow provided — resuming anyway`);
    } else {
      const statusReason = `subflow "${invocation.nodeId}" (mode:"${mode}") not satisfied — ${mode === "any" ? "every child failed" : "at least one child did not succeed"}`;
      const result = await failFlow({
        flowId: invocation.flowId,
        expectedRevision: parentFlow.revision,
        stateJson: parentFlow.stateJson,
        statusReason,
      });
      if (result?.applied && typeof failureStore?.recordEngineFailure === "function") {
        // invocation above is the CHILD's per-item parentInvocationId, which
        // never gets a flow_node_executions row (spawn-child resolves per-item
        // invocations with subflowKey set; see failure-store computeResolvedAt).
        // The subflow NODE's own dispatch is a different invocation with
        // subflowKey:null — the one routes/runs.js renders a row for. Look it
        // up separately; reusing invocation.id/.subflowKey would record against
        // an invocation with no executions.
        const ownInvocation = await nodeExecutionsStore.getLatestInvocationForNode(
          invocation.flowId, invocation.nodeId, null,
        );
        const latest = await nodeExecutionsStore.getLatestExecutionForNode({
          flowId: invocation.flowId, nodeId: invocation.nodeId, subflowKey: null,
        });
        await failureStore.recordEngineFailure({
          flowId: invocation.flowId,
          pipelineId: parentFlow.pipelineId,
          failureNode: invocation.nodeId,
          message: statusReason,
          state: parentFlow.stateJson,
          invocationId: ownInvocation?.id ?? null,
          executionId: latest?.executionId ?? null,
          errorType: "subflow-mode-not-satisfied",
        });
      }
      return { resumed: false, reason: "mode_not_satisfied", failed: !!result?.applied };
    }
  }

  return flipToRunningAndResume({
    parentFlowId: invocation.flowId,
    runStore,
    resume,
    logger,
    admissionQueueStore,
    api,
    repairGuard: {
      nodeExecutionsStore,
      nodeId: invocation.nodeId,
      subflowKey: invocation.subflowKey,
      expectedInvocationId: row.parentInvocationId,
    },
  });
}
