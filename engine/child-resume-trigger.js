// child-resume-trigger.js — wakes a parent flow once every child spawned by
// one subflow-node firing has reached a terminal state.
//
// Hooked into state/state-store.js's finish/skip/fail/cancel methods
// themselves (not engine/executor-persist.js's wrappers) — boot/flow-
// revival.js's excess-orphan-displacement path calls store.fail(...)
// directly, bypassing the persist wrappers entirely, so hooking at the
// wrapper level would silently miss waking a parent whose child got
// displaced at boot.
//
// Fires for every child terminal transition, whichever path drove it.

import { isTerminal } from "./executor-helpers.js";
import { wakePromotedRelease } from "./admission-promotion-resume.js";

// Shared by maybeResumeParent (below) and boot/flow-revival.js's
// children-pending reconciliation pass — the same "is this subflow node's
// whole fan-out done" check, entered from two different directions (from a
// child walking up, or from the parent scanning down at boot). Returns
// `children` too (not just the boolean) so callers can also check
// per-child outcome — see subflowOutcomeSatisfied below.
export async function areSiblingsAllTerminal({ flowId, nodeId, runStore, nodeExecutionsStore }) {
  const siblingIds = await nodeExecutionsStore.listInvocationIdsForNode(flowId, nodeId);
  if (siblingIds.length === 0) return { allTerminal: false, reason: "no_siblings", children: [] };

  const children = await runStore.listForParentInvocations(siblingIds);
  // Eager creation spawns every child in one pass, but a resume check can
  // race that pass (a sibling invocation exists with no child row yet) —
  // bail rather than resume against an incomplete fan-out.
  if (children.length !== siblingIds.length) return { allTerminal: false, reason: "not_all_spawned", children: [] };

  const allTerminal = children.every((c) => isTerminal(c.status));
  return { allTerminal, reason: allTerminal ? "ok" : "siblings_pending", children };
}

// A subflow node's `mode` ("any" | "all") decides whether its
// now-fully-terminal children satisfy the fan-out, checked once
// areSiblingsAllTerminal confirms every sibling is terminal. "any" only
// needs one success (correct for a genuine multi-item fan-out, where
// per-item failure is normal); "all" needs every child to have succeeded
// (correct for a single-item subflow like ENSURE_LOGIN, where the one
// attempt either worked or the whole point of gating on it failed). An
// unrecognized/missing mode fails open (never blocks the resume) —
// defensive only, the loader requires a valid mode on every subflow node,
// so this should never actually happen.
export function subflowOutcomeSatisfied(mode, children) {
  if (mode === "any") return children.some((c) => c.status === "succeeded");
  if (mode === "all") return children.every((c) => c.status === "succeeded");
  return true;
}

// Shared by maybeResumeParent (below) and boot/flow-revival.js's
// children-pending reconciliation pass. `resume()` is just `runFlow(flowId)`
// (engine/executor.js) — and runFlow's own first check is "if this flow's
// status is still 'blocked', return immediately" (that guard exists so a
// stray/duplicate resume() call on an already-blocked flow is a safe no-op).
// Every OTHER caller of resume() in this codebase (gates/gate-resolver.js's
// decision-commit path) already flips status to 'running' via
// store.setRunning() BEFORE calling resume() — this mirrors that, using the
// lower-level runStore CAS directly since this module only depends on
// runStore/nodeExecutionsStore, not the full state-store. A concurrent
// caller racing the same flip loses the CAS (revision mismatch) and simply
// doesn't resume — never a double-dispatch.
//
// Repair propagation: a parent can already be terminal 'failed' here — the
// mode-not-satisfied path (maybeResumeParent below) fails a parent the
// moment its fan-out looks unsatisfiable, but a later retry of one of the
// surviving children can still turn that same fan-out satisfied afterward.
// When that happens and the caller supplies `repairGuard` (only
// maybeResumeParent does — the boot/flow-revival.js and
// engine/admission-promotion-resume.js callers don't, and get
// `flip_failed:not_terminal`), a 'failed'
// parent is force-unblocked back to 'blocked' first, then the normal
// casSetRunning flip proceeds against the freshly-bumped revision.
// 'succeeded'/'cancelled'/'skipped' are never repaired — only 'failed' is
// eligible (a deliberate business outcome or an operator's cancel must
// stay final).
// Which admission-pool kind a flow's OWN admission (not any node inside
// it) belongs to — a root flow (no parent) competes as "flow-start"; a
// subflow-spawned child competes as "child-dispatch". Shared by every
// caller that needs to re-request admission for a flow it's about to
// resume — this file's own flipToRunningAndResume below, and gates/
// gate-resolver.js's, routes/operator.js's, and routes/operator-retry.js's
// own independent resume sequences (none of which share this file's
// low-level runStore-CAS shape, so they use admitAndSetRunning instead).
export function admissionKindForFlow(flow) {
  return flow.parentInvocationId ? "child-dispatch" : "flow-start";
}

// Re-requests a flow's own admission (see admissionKindForFlow) before
// flipping it back to "running" through the FULL state-store (not
// runStore's raw CAS — used by every live resume path OUTSIDE this file:
// gates/gate-resolver.js, routes/operator.js's budget-gate resume, and
// routes/operator-retry.js). Necessary because the flow's admission was
// released the moment it left "running" (state-store.js's setWaiting/
// terminal hooks) — resuming must re-claim a slot, not assume one is
// still held. Returns a setRunning/setQueued-shaped result
// ({applied, code}) plus `queued: true` when the pool was full: the flow
// is left (or newly set) at status:"queued" instead of resumed, and the
// caller must skip whatever resume-dispatch it would otherwise do —
// admission-queue-store's own release()/promotion cycle resumes it later,
// same as any other queued admission.
export async function admitAndSetRunning({ flowId, expectedRevision, lastStep, stateJson, parentInvocationId, store, admissionQueueStore, runStore, resume, logger, api }) {
  const kind = admissionKindForFlow({ parentInvocationId });
  const admission = await admissionQueueStore.admit({ kind, flowId });
  if (admission.status === "queued") {
    const result = await store.setQueued({
      flowId, expectedRevision, lastStep, stateJson,
      waitJson: { kind },
      statusReason: `queued: waiting for a shared admission slot to resume (${kind})`,
    });
    // Claimed a queue position but didn't end up using it (CAS lost) — a
    // caller's own retry loop will call admit() fresh on its next attempt,
    // so this one must be given back or it leaks a permanent, invisible
    // pool-capacity hole (a row with no flow_runs counterpart ever pointing
    // back at it). This admission's own status is 'queued', not 'admitted',
    // so releasing it can never itself promote anything (admission-queue-
    // store.js's release() only promotes on a genuinely admitted row) — no
    // resumePromotedAdmission call needed here, unlike the setRunning branch
    // below.
    if (!result.applied) await admissionQueueStore.release(admission.id);
    return { applied: result.applied, queued: true, code: result.code };
  }
  const result = await store.setRunning({ flowId, expectedRevision, lastStep, stateJson });
  if (!result.applied) {
    // Claimed a slot but didn't end up using it (CAS lost to a concurrent
    // writer) — give it back rather than leak it. This admission WAS
    // genuinely 'admitted', so releasing it can promote a different flow's
    // queued row from the shared combined pool — drive wakePromotedRelease
    // on that release the same as every other release call site in this
    // codebase, or the promoted flow is stranded exactly like the orphaned-
    // promotion incidents this whole mechanism exists to prevent.
    const released = await admissionQueueStore.release(admission.id);
    await wakePromotedRelease(released, {
      runStore, resume, logger, api, context: "child-resume-trigger: admitAndSetRunning CAS-loss release",
    });
  }
  return { applied: result.applied, queued: false, code: result.code };
}

// `admissionQueueStore`, when provided, re-requests the flow's own
// admission before flipping it running (see admitAndSetRunning above,
// same rationale — just against runStore's raw CAS instead of the full
// state-store, matching this function's existing low-level shape). Omit
// it for a caller that already KNOWS it holds a live admission and is
// just acting on a promotion (e.g. engine/admission-promotion-resume.js) —
// passing it there would claim a SECOND, duplicate admission for the same
// flow+kind.
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
      // This admission WAS genuinely 'admitted' (the branch above only
      // reaches here once admit() returned status:"admitted"), so releasing
      // it can promote a DIFFERENT flow's queued row — drive
      // wakePromotedRelease on that release, same as every other release
      // call site in this codebase, or the promoted flow is stranded
      // exactly like the orphaned-promotion incidents this mechanism exists
      // to prevent.
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

// maybeResumeParent checks whether `flowId` (a flow that just went terminal)
// is a subflow-spawned child, and if so, whether every sibling under the same
// (parent flowId, nodeId) is also terminal. If so — and the subflow node's
// `mode` is satisfied (see subflowOutcomeSatisfied above) —
// resumes the parent. If `mode` is NOT satisfied, fails the parent directly
// at this node via `failFlow` instead of resuming it, so a mode-mismatched
// fan-out never silently continues into its normal next step. `pipelinesRef`
// is optional — if omitted (or the node/pipeline can't be resolved), mode
// enforcement is skipped and behavior is the unconditional
// resume; only real callers wire it, tests may omit it deliberately.
// Fire-and-forget by design — callers should not await this inline with a
// CAS write; call it after, uninstrumented failures are logged and swallowed.
//
// `failureStore` is likewise optional (tests may omit it) — when provided,
// a mode-not-satisfied failure ALSO gets a real failure_events row.
// Without this, `failFlow` alone is a bare status write with no
// invocationId/executionId, so it never reaches recordEngineFailure through
// any other path — the parent flow fails with no Inbox entry and no retry
// button.
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
        // `invocation` above is the CHILD's own parentInvocationId — per
        // spawn-child.js's resolveOrCreateChild, that's the PER-ITEM
        // invocation (subflowKey = this specific item's key), which
        // deliberately never gets a flow_node_executions row at all (see
        // state/failure-store.js's computeResolvedAt comment on the same
        // dead end). The subflow NODE's own dispatch — the one
        // routes/runs.js's buildTreeNodeRows actually renders a row for —
        // is a DIFFERENT invocation, keyed by subflowKey:null. Must look
        // that one up separately; reusing invocation.id/.subflowKey here
        // would silently record against an invocation with no executions.
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
