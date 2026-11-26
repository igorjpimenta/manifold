// child-resume-trigger.js — wakes a parent flow once every child spawned by
// one subflow-node firing has gone terminal.
//
// Hooked into state-store's finish/skip/fail/cancel methods themselves, not
// executor-persist's wrappers: boot/flow-revival's orphan-displacement path
// calls store.fail(...) directly and would bypass a wrapper-level hook.

import { isTerminal } from "./executor-helpers.js";
import { wakePromotedRelease } from "./admission-promotion-resume.js";

// Shared by maybeResumeParent and boot/flow-revival's children-pending
// reconciliation — the same "is this fan-out done" check entered from a child
// walking up or the parent scanning down. Returns `children` so callers can
// also check per-child outcome.
export async function areSiblingsAllTerminal({ flowId, nodeId, runStore, nodeExecutionsStore }) {
  const siblingIds = await nodeExecutionsStore.listInvocationIdsForNode(flowId, nodeId);
  if (siblingIds.length === 0) return { allTerminal: false, reason: "no_siblings", children: [] };

  const children = await runStore.listForParentInvocations(siblingIds);
  // Eager creation spawns every child in one pass, but a resume check can
  // race that pass (sibling invocation exists, child row not yet) — bail
  // rather than resume against an incomplete fan-out.
  if (children.length !== siblingIds.length) return { allTerminal: false, reason: "not_all_spawned", children: [] };

  const allTerminal = children.every((c) => isTerminal(c.status));
  return { allTerminal, reason: allTerminal ? "ok" : "siblings_pending", children };
}

// "any" needs one success (multi-item fan-out where per-item failure is
// normal); "all" needs every child succeeded. An unrecognized mode fails
// open — the loader requires a valid mode, so this is defensive only.
export function subflowOutcomeSatisfied(mode, children) {
  if (mode === "any") return children.some((c) => c.status === "succeeded");
  if (mode === "all") return children.every((c) => c.status === "succeeded");
  return true;
}

// Which admission-pool kind a flow's OWN admission (not any node in it)
// belongs to: root flows compete as "flow-start", subflow-spawned children
// as "child-dispatch".
export function admissionKindForFlow(flow) {
  return flow.parentInvocationId ? "child-dispatch" : "flow-start";
}

// Re-requests the flow's own admission before flipping it running: leaving
// "running" released the slot, so a resume must re-claim one. A full pool
// leaves the flow status:"queued" — the caller must skip its resume-dispatch;
// admission-queue-store's release/promotion resumes it later.
export async function admitAndSetRunning({ flowId, expectedRevision, lastStep, stateJson, parentInvocationId, store, admissionQueueStore, runStore, resume, logger, api }) {
  const kind = admissionKindForFlow({ parentInvocationId });
  const admission = await admissionQueueStore.admit({ kind, flowId });
  if (admission.status === "queued") {
    const result = await store.setQueued({
      flowId, expectedRevision, lastStep, stateJson,
      waitJson: { kind },
      statusReason: `queued: waiting for a shared admission slot to resume (${kind})`,
    });
    // CAS lost after claiming a queue position — give it back or it leaks
    // a permanent invisible pool-capacity hole. This row is 'queued', not
    // 'admitted', so releasing it can't promote anything: no wake needed.
    if (!result.applied) await admissionQueueStore.release(admission.id);
    return { applied: result.applied, queued: true, code: result.code };
  }
  const result = await store.setRunning({ flowId, expectedRevision, lastStep, stateJson });
  if (!result.applied) {
    // CAS lost after claiming an admitted slot — release it, and drive
    // wakePromotedRelease: an admitted release CAN promote another flow,
    // which would otherwise be stranded without anyone to resume it.
    const released = await admissionQueueStore.release(admission.id);
    await wakePromotedRelease(released, {
      runStore, resume, logger, api, context: "child-resume-trigger: admitAndSetRunning CAS-loss release",
    });
  }
  return { applied: result.applied, queued: false, code: result.code };
}

// admissionQueueStore is optional: pass it to re-request admission before
// the flip (slot was released while blocked), omit it when the caller
// already holds a live admission and is acting on a promotion — passing it
// there would claim a duplicate admission for the same flow+kind.
//
// Must flip status to 'running' BEFORE resume(): runFlow returns
// immediately while the flow is still 'blocked' (its guard against stray
// duplicate resumes), so a bare resume() would no-op. The flip is a CAS —
// a concurrent loser simply doesn't resume, never a double-dispatch.
// Repair propagation: the mode-fail path can fail a parent that a later
// child retry then satisfies, so a 'failed' parent with a valid
// repairGuard is force-blocked back first; only 'failed' is repaired —
// succeeded/cancelled/skipped stay final.
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
      // Admitted release CAN promote another flow — drive
      // wakePromotedRelease or the promoted flow is stranded.
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

// A just-terminal child of a subflow node: if every sibling under the same
// (parent flowId, nodeId) is also terminal and the node's `mode` is
// satisfied, resume the parent; if mode is NOT satisfied, fail the parent
// at this node via failFlow so a bad fan-out never continues into its next
// step. `pipelinesRef` optional — if it or the node can't resolve a mode,
// enforcement is skipped and the parent resumes unconditionally.
// Fire-and-forget by design: call it after a CAS write, never inline with
// one; failures are logged and swallowed.
//
// `failureStore` is optional too — when provided, a mode-not-satisfied
// failure also gets a real failure_events row; failFlow alone is a bare
// status write with no invocationId/executionId, so the parent would fail
// with no Inbox entry and no retry button.
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
        // `invocation` is the child's PER-ITEM invocation (subflowKey set),
        // which never gets a flow_node_executions row — the subflow NODE's
        // own dispatch is a different invocation with subflowKey null. Look
        // that one up; recording against the per-item invocation would
        // reference an invocation with no executions.
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
