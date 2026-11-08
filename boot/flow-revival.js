import { completeCancellation } from "../engine/executor-cancel.js";
import { areSiblingsAllTerminal, flipToRunningAndResume, subflowOutcomeSatisfied } from "../engine/child-resume-trigger.js";
import { resolveEdge } from "../engine/executor-helpers.js";
import { wakePromotedRelease, isEngineResumableKind, scheduleDetachedResume } from "../engine/admission-promotion-resume.js";
import { admissionKindForFlow } from "../engine/child-resume-trigger.js";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";

// A promotion (release()'s side effect of flipping a queued row straight to
// 'admitted') is supposed to be immediately followed by wakePromotedRelease
// driving that flow to 'running' — see admission-promotion-resume.js and
// every release()/releaseByFlow() call site in this codebase. But that
// follow-through is still two separate steps (a durable DB write, then an
// async continuation that isn't part of the same transaction): if the
// continuation never runs (a crash in that exact window) or throws in a way
// nothing catches, the row stays 'admitted' forever while its flow sits
// non-terminal and non-running — holding a live slot in the shared pool
// with nothing left to drive it. The terminal-leak sweep above can't catch
// this: it only reconciles rows whose owning flow is already terminal, and
// this flow never gets there — it's stuck exactly where it was queued.
//
// minAgeMs guards against a false positive on a promotion that's
// genuinely still in flight (the continuation hasn't had a chance to run
// yet) — skip anything admitted more recently than that window. Exported
// (not folded into reviveOrphanFlows) so index.js can also run it on a
// periodic timer: reviveOrphanFlows only runs once, at boot, so a leak
// occurring hours into uptime would otherwise go undetected until the next
// restart.
//
// Also reaps admitted rows of kinds the engine doesn't admit — see the
// unknown-kind pass below for why skipping those is what wedged the whole
// pool shut, not a safe no-op.
export async function reconcileStuckAdmissions({ admissionQueueStore, runStore, executor, api, logger, minAgeMs = 30_000 }) {
  if (!admissionQueueStore) return { stuckAdmissionsResumed: 0, stuckAdmissionsFailed: 0, unknownKindReaped: 0 };
  const now = Date.now();
  const admittedRows = await admissionQueueStore.listByStatus("admitted");

  // Reap admitted rows of kinds the engine doesn't admit. Such a row
  // can never be driven: resumePromotedAdmission refuses non-resumable
  // kinds, and every flow transition releases by the flow's CURRENT
  // admission kind, so releaseByFlow never matches such a row either.
  // "Unresumable" therefore makes the row a permanent pool leak, not a safe
  // skip — admit()'s and fillFreeSlots()'s admitted-count queries have no
  // kind filter, so zombie rows count against the ceiling forever and wedge
  // both shut. release() is the reclaim: it frees the slot and, as a side
  // effect, promotes the globally-oldest queued row, which
  // wakePromotedRelease then drives — the freed capacity goes back to work
  // in the same pass instead of idling until some unrelated release.
  // A QUEUED row of such a kind is left here (it holds no slot), but
  // fillFreeSlots() can promote one into 'admitted'; this pass is what
  // reclaims it after that, past the same grace window.
  let unknownKindReaped = 0;
  const resumableRows = [];
  for (const row of admittedRows) {
    if (isEngineResumableKind(row.kind)) { resumableRows.push(row); continue; }
    if (row.admittedAt && now - row.admittedAt < minAgeMs) continue;
    const flow = await runStore.get(row.flowId);
    // A flow still 'running' under the code that admitted this row owns
    // the slot legitimately and releases it through its own transition
    // path; reclaiming the row underneath it would transiently
    // over-admit the pool. Missing or terminal flows are reaped: no live
    // process can release them (the terminal-leak sweep above only runs at
    // boot, and release() is a safe no-op if it already did).
    if (flow?.status === "running") continue;
    try {
      const result = await admissionQueueStore.release(row.id);
      await wakePromotedRelease(result, {
        runStore, resume: executor.resume, logger, api,
        context: `manifold revival: unknown-kind reap [${row.id.slice(0, 8)}]`,
      });
      unknownKindReaped++;
      logger.warn(
        `manifold revival: reaped unknown-kind '${row.kind}' admission [${row.id.slice(0, 8)}] for flow ` +
        `[${row.flowId.slice(0, 8)}] (${flow ? `status=${flow.status}` : "flow_runs row gone"}) — ` +
        `no code path can drive this kind, slot reclaimed`,
      );
    } catch (err) {
      logger.warn(`manifold revival: unknown-kind reap [${row.id.slice(0, 8)}] failed: ${err?.message ?? err}`);
    }
  }

  // Filter down to genuinely stuck candidates first — cheap, fast DB
  // reads, fine to do one at a time. flipToRunningAndResume itself is the
  // slow, unbounded part: it can resolve a flow all the way to a fresh
  // admission request of its own, which — under a congested pool — waits
  // on yet another slot. Driving that sequentially, one candidate at a
  // time, meant a single candidate stuck behind real contention blocked
  // every other candidate behind it in the same pass indefinitely: this
  // sweep exists specifically to recover flows stuck exactly like that, so
  // serializing on one of them defeated its own purpose under load.
  const candidates = [];
  for (const row of resumableRows) {
    if (row.admittedAt && now - row.admittedAt < minAgeMs) continue;
    const flow = await runStore.get(row.flowId);
    // No flow_runs row at all, or already terminal: not this sweep's job —
    // the terminal-leak reconciliation above (boot-only) or a future run of
    // it handles those. This sweep only exists for "admitted but genuinely
    // stuck non-terminal", so a flow already 'running' is also skipped —
    // that's the healthy, expected state.
    if (!flow || flow.status === "running" || TERMINAL_STATUS_SET.has(flow.status)) continue;
    // The row being 'admitted' only means THIS flow's row for THIS kind
    // holds a live slot — it says nothing about what the flow is currently
    // doing: a flow can be 'queued' on a different resource (a cross-process
    // lock) while its slot is legitimately still counted. Force-driving that
    // would resume it on top of its own in-flight wait. The real, narrow
    // signature of the leak this sweep exists for is a flow queued waiting
    // specifically to be admitted for the SAME kind it already holds — a
    // state that can only exist if a promotion's flip-to-running
    // follow-through was dropped, since nothing else produces it.
    if (flow.waitJson?.kind !== row.kind) continue;
    candidates.push(row);
  }

  let stuckAdmissionsResumed = 0;
  let stuckAdmissionsFailed = 0;
  await Promise.all(candidates.map(async (row) => {
    logger.warn(
      `manifold revival: admitted ${row.kind} slot [${row.id.slice(0, 8)}] for flow [${row.flowId.slice(0, 8)}] ` +
      `never resumed (status=queued, admitted ${Math.round((now - (row.admittedAt ?? now)) / 60000)}min ago) — force-driving flipToRunningAndResume`,
    );
    try {
      // No admissionQueueStore passed through: this flow already holds the
      // live admission (that's how it got into `admittedRows` above) —
      // passing it would have flipToRunningAndResume call admit() again,
      // which the resume-admitted CLI command's own header comment already
      // flags as unsafe to do for a flow that already holds the slot.
      const result = await flipToRunningAndResume({ parentFlowId: row.flowId, runStore, resume: executor.resume, logger, api });
      if (result.resumed) {
        stuckAdmissionsResumed++;
      } else {
        stuckAdmissionsFailed++;
        logger.warn(`manifold revival: stuck-admission force-resume for [${row.flowId.slice(0, 8)}] did not apply: ${result.reason ?? "unknown"}`);
      }
    } catch (err) {
      stuckAdmissionsFailed++;
      logger.warn(`manifold revival: stuck-admission force-resume for [${row.flowId.slice(0, 8)}] threw: ${err?.message ?? err}`);
    }
  }));
  return { stuckAdmissionsResumed, stuckAdmissionsFailed, unknownKindReaped };
}

/**
 * Boot-time orphan recovery for flow_runs left in status='running' (crashed
 * mid-flight) or status='queued' (waiting on a resource-lock, or on a
 * shared admission-pool slot — flow-start or child-dispatch — when the
 * process died) after a gateway crash. Called by the manifold-flow-revival
 * service in index.js.
 *
 * Returns a summary object for observability and testing:
 *   { revived: number, revivedAgent: number, queuedForAdmission: number, readmitted: number, recancelled: number, childrenReconciled: number, gatesReconciled: number, admissionLeaksReleased: number, freeSlotsFilled: number }
 */

export async function reviveOrphanFlows({
  all, pipelines, store, logger, executor, createCancelStore,
  runStore, nodeExecutionsStore, maybeResolveFlow, sessionIndex, admissionQueueStore, api,
  // Threaded through to the stuck-promotion reconciliation call below —
  // exists mainly so tests can exercise that pass without waiting out its
  // real-world grace window (a genuine boot's admission rows all predate
  // this process's own start, so the default is trivially satisfied there;
  // a unit test that admits a row moments before calling this function is
  // not so lucky).
  stuckAdmissionMinAgeMs,
}) {
  // ── Complete interrupted cancellations ──────────────────────────────────────
  // store.cancel() writes cancel_requested_at first, then status='cancelled',
  // in two separate SQL statements. A crash between them leaves a flow with
  // cancelRequestedAt set but status still 'running'/'blocked'/'waiting'.
  // Re-cancel those before reviving anything else so they don't land in toRevive.
  // Uses the same completeCancellation path as executor.cancel() — step 1 is
  // already done, so f.revision is already the post-requestCancel value and is
  // passed directly to completeCancellation as the expectedRevision for setRunning.
  //
  // createCancelStore(ownerKey) creates a store bound to the flow's actual ownerKey
  // so that cancelFlowByIdForOwner's equality check passes regardless of which
  // plugin originally created the flow (e.g. a graph-flow-owner flow is
  // rejected by the default manifold-owner store but succeeds with its own key).
  const cancelledByRevival = new Set();
  const terminalStatuses = new Set(["cancelled", "failed", "succeeded"]);
  const cancelStores = new Map();
  function cancelStoreFor(ownerKey) {
    if (!ownerKey || !createCancelStore) return store;
    if (!cancelStores.has(ownerKey)) cancelStores.set(ownerKey, createCancelStore(ownerKey));
    return cancelStores.get(ownerKey);
  }

  for (const f of all) {
    if (f.cancelRequestedAt == null) continue;
    if (terminalStatuses.has(f.status)) continue;
    logger.info(
      `manifold revival: completing interrupted cancel [${f.flowId.slice(0, 8)}] status=${f.status}`,
    );
    await completeCancellation({
      store: cancelStoreFor(f.ownerKey),
      flowId: f.flowId,
      stateJson: f.stateJson,
      lastStep: f.lastStep,
      revision: f.revision,
      logger,
      runStore,
      nodeExecutionsStore,
      reason: "cancel requested before restart, completed on revival",
    });
    cancelledByRevival.add(f.flowId);
  }

  // ── Classify orphans (running AND queued, any wait kind) ───────────────────
  // 'running' orphans: process died mid-flight. 'queued' orphans: process
  // died (or restarted) while the flow was waiting on an internal resource —
  // resource-lock, or the shared flow-start/child-dispatch admission pool.
  // All land here uniformly; the skip-direct-dispatch step
  // below and the revive-survivors step's dispatch don't need to know *why*
  // a row was queued, only whether it wants to run.
  //
  // isAgent decides HOW to revive an agent-kind node: reviveAgentKindOrphan()
  // resumes the SAME deterministic session with REVIVAL_MSG, a continuation
  // prompt, instead of re-running promptBuilder fresh (see engine/
  // revival-messages.js) — and it sends that continuation prompt
  // UNCONDITIONALLY, even to a session with zero prior turns (executor-
  // nodes.js's dispatch: `reviveMessage ?? promptBuilder(...)`, with no
  // branch on whether a prior invocation/session actually exists). So
  // isAgent is only correct when a dispatch attempt genuinely started —
  // sending REVIVAL_MSG as literally the first message to a node that was
  // only ever queued, never dispatched, would be a "continue where you left
  // off" prompt with nothing to continue.
  //
  // For a 'running' orphan, targetNodeId comes from
  // nodeExecutionsStore.findCurrentStep() below, which IS race-free proof a
  // dispatch attempt genuinely entered this node: the nodeExecutions.enter()
  // call creates the invocation row BEFORE executeNode() runs, well before
  // dispatchAgentWithRetry gets to recording a flow_sessions entry
  // (session-index.js) or calling subagent.run() — so currentStep alone is
  // sufficient there, without consulting sessionIndex.hasSessionForNode().
  //
  // The 'running' case must not REQUIRE a session record. The flow_sessions
  // record is written well after enter() (past several awaits — budget gate,
  // assembleState, promptBuilder, dispatch-semaphore acquisition), so a crash
  // in that narrow window leaves a genuinely in-flight agent dispatch with a
  // correct targetNodeId but hasSessionForNode()===false, and requiring it
  // misroutes that dispatch into the generic resolveEdge()/runFlow() path
  // below, which has no node output to route on and fails with "edge <NODE>
  // returned no next node". A running flow with no invocation row yet
  // (findCurrentStep() -> null) is the other case, and there falling through
  // to resolveEdge() IS correct; hasSessionForNode() is what still decides
  // isAgent for it, via the !dispatchGenuinelyEntered branch below.
  //
  // For a 'queued' orphan, targetNodeId instead comes from waitJson.nodeId
  // — set by the resource-lock wait BEFORE it acquires the lock, which only
  // proves the node was ABOUT to be dispatched, not that dispatch began.
  // hasSessionForNode() still gates isAgent there.
  //
  // f.lastStep is the last COMPLETED node, not the one in flight — using it
  // directly as the crashed node's id is wrong. The real target node is:
  //   - queued: f.waitJson.nodeId — the resource-lock wait sets it directly
  //     (executor.js) since a queued flow never got far enough to advance
  //     lastStep at all.
  //   - running: nodeExecutionsStore.findCurrentStep(flowId) — invocation
  //     pre-creation (enter()) happens before executeNode() runs, so
  //     the last invocation's node_id correctly identifies the in-flight node
  //     even across a crash. Falls back to resolveEdge(lastStep) if the query
  //     returns null (first iteration of a brand-new flow with no invocations).
  const orphans = [];
  for (const f of all) {
    if (f.status !== "running" && f.status !== "queued") continue;
    if (cancelledByRevival.has(f.flowId)) continue;
    const pipeline = pipelines.get(f.stateJson?.pipelineId);
    const currentStep = f.status === "running"
      ? await nodeExecutionsStore?.findCurrentStep(f.flowId)
      : null;
    const targetNodeId = f.waitJson?.nodeId
      ?? currentStep
      ?? resolveEdge(pipeline?.edges ?? {}, f.lastStep, f.stateJson, { logger, flowId: f.flowId });
    const nodeKind = pipeline?.nodes?.[targetNodeId]?.kind;
    const dispatchGenuinelyEntered = currentStep != null && targetNodeId === currentStep;
    const hasSession = nodeKind === "agent" && !dispatchGenuinelyEntered && sessionIndex != null
      ? !!(await sessionIndex.hasSessionForNode(f.flowId, targetNodeId))
      : false;
    const isAgent = nodeKind === "agent" && (dispatchGenuinelyEntered || hasSession);
    orphans.push({ flow: f, isAgent, targetNodeId, nodeKind });
  }

  // ── Skip direct dispatch for orphans queued on the shared admission
  // pool ──────────────────────────────────────────────────────────────────
  // The orphan-classification step above classified every non-terminal
  // orphan as 'running' or 'queued'.
  // A 'queued' orphan waiting on kind:"flow-start" or "child-dispatch" is
  // tracked in the DURABLE combined admission pool (state/admission-queue-
  // store.js — the same runtime.sqlite every other table here survives a
  // crash in), which already reflects who was genuinely admitted vs still
  // queued the moment the gateway died. Nothing needs to be re-derived: a
  // live queued flow-start/child-dispatch flow is never dispatched
  // directly either (see engine/executor.js's runFlow() isQueued guard) —
  // the same admission-queue-store release()/promotion cycle that wakes a
  // live queued flow wakes a boot-revived one too, once some other admitted
  // flow elsewhere goes blocked or terminal, no special boot-time action
  // needed. Throttling happens atomically at the moment each flow admits,
  // not as a bulk post-hoc displacement correction here.
  //
  // A 'queued' orphan on waitJson.kind:"agent-dispatch" — not an
  // admission kind (state/admission-queue-store.js's migration deletes its
  // rows) — gave its own run slot back when it queued, so it holds nothing.
  // Re-entering it directly would run it outside the pool; instead it
  // re-requests its own run slot (flipToRunningAndResume with the store:
  // admitted → flipped to running and resumed; pool full → queued on its
  // own kind for the normal promotion cycle).
  //
  // 'queued' orphans on a resource-lock are NOT admission-pool-gated this
  // way — the revive-survivors step below dispatches them directly:
  // re-entering the node they were queued on is how THEY retry (the lock
  // wait lives inside that node's own dispatch path, re-triggered by
  // resolveEdge, not by anything outside this loop).
  const toRevive = [];
  let queuedForAdmission = 0;
  let readmitted = 0;
  for (const o of orphans) {
    const kind = o.flow.waitJson?.kind;
    if (o.flow.status === "queued" && (kind === "flow-start" || kind === "child-dispatch")) {
      logger.info(`manifold revival: [${o.flow.flowId.slice(0, 8)}] left queued for admission (${kind}) — will resume via the normal release/promotion cycle`);
      queuedForAdmission++;
      continue;
    }
    if (o.flow.status === "queued" && kind === "agent-dispatch" && admissionQueueStore && runStore) {
      const result = await flipToRunningAndResume({
        parentFlowId: o.flow.flowId, runStore, admissionQueueStore, logger, api,
        resume: (flowId) => { scheduleDetachedResume(executor.resume, flowId, { logger, api, context: "manifold revival: re-admit" }); },
      });
      logger.info(
        `manifold revival: [${o.flow.flowId.slice(0, 8)}] re-requested its ${admissionKindForFlow(o.flow)} slot — ` +
        `${result.resumed ? "admitted, resuming" : result.reason}`,
      );
      readmitted++;
      continue;
    }
    toRevive.push(o);
  }

  // ── Revive survivors ────────────────────────────────────────────────────────
  let revivedAgent = 0;
  if (toRevive.length === 0) {
    logger.info("manifold revival: no revivable orphan flows at boot");
  } else {
    logger.info(`manifold revival: reviving ${toRevive.length} orphan flow(s) from lastStep`);
    for (const o of toRevive) {
      const flow = o.flow;
      const pipelineId = flow.stateJson?.pipelineId ?? "?";
      logger.info(`manifold revival: [${flow.flowId.slice(0, 8)}] pipeline=${pipelineId} step=${o.targetNodeId}`);
      if (o.isAgent) {
        revivedAgent++;
        // Resumes the SAME session with a continuation prompt. No admission
        // work: the flow's own run slot (flow-start/child-dispatch) is
        // durable across the crash and still held, and the agent call runs
        // inside it like any other node — it's released through the flow's
        // normal lifecycle once the run blocks or finishes.
        executor.reviveAgentKindOrphan(flow, o.targetNodeId)
          .catch((err) => {
            logger.warn(`manifold revival: reviveAgentKindOrphan [${flow.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
          });
      } else {
        executor.runFlow(flow.flowId, "revival").catch((err) => {
          logger.warn(`manifold revival: runFlow [${flow.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
        });
      }
    }
  }

  // ── Children-pending reconciliation ─────────────────────────────────────────
  // The resume-trigger (state-store.js's finish/skip/fail/cancel) fires
  // on the LAST sibling's terminal write — but if the gateway crashes at
  // exactly that moment, the fire can be lost. Re-check every blocked parent
  // directly against its children's current terminal state and resume if
  // already satisfied, the same idempotent-recheck shape gate-resolver.js's
  // maybeResolveFlow already uses for the analogous gate-side problem. No-op
  // when nodeExecutionsStore/runStore aren't wired in (older callers, tests).
  let childrenReconciled = 0;
  if (runStore && nodeExecutionsStore) {
    for (const f of all) {
      if (f.status !== "blocked") continue;
      if (f.waitJson?.kind !== "children-pending") continue;
      const nodeId = f.waitJson?.nodeId;
      if (!nodeId) continue;
      const { allTerminal, children } = await areSiblingsAllTerminal({
        flowId: f.flowId, nodeId, runStore, nodeExecutionsStore,
      });
      if (!allTerminal) continue;
      childrenReconciled++;
      // Same mode check as the live-process path
      // (child-resume-trigger.js's maybeResumeParent) — a parent orphaned
      // mid-resume by a gateway crash must fail (not resume) if its
      // subflow node's mode isn't satisfied, same as it would have if the
      // crash hadn't happened.
      const mode = pipelines?.get?.(f.pipelineId)?.nodes?.[nodeId]?.mode;
      if (mode && !subflowOutcomeSatisfied(mode, children)) {
        logger.info(`manifold revival: failing [${f.flowId.slice(0, 8)}] — subflow "${nodeId}" (mode:"${mode}") not satisfied`);
        await store.fail({
          flowId: f.flowId, expectedRevision: f.revision, stateJson: f.stateJson,
          statusReason: `subflow "${nodeId}" (mode:"${mode}") not satisfied — ${mode === "any" ? "every child failed" : "at least one child did not succeed"}`,
        });
        continue;
      }
      logger.info(`manifold revival: resuming [${f.flowId.slice(0, 8)}] — all children of ${nodeId} terminal`);
      // Must flip status back to 'running' before calling resume() —
      // executor.resume() is just runFlow(), whose own first check returns
      // immediately for a flow still in status='blocked'. See
      // child-resume-trigger.js's flipToRunningAndResume for why.
      await flipToRunningAndResume({ parentFlowId: f.flowId, runStore, resume: executor.resume, logger, admissionQueueStore, api }).catch((err) => {
        logger.warn(`manifold revival: children-pending resume [${f.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
      });
    }
  }

  // ── Stuck-gate-decision reconciliation ──────────────────────────────────────
  // A gate decision is recorded in two steps: (1) write approvalPending[itemId]
  // durably, (2) call maybeResolveFlow → applyResolutionsAndResume, which
  // moves the decision into approvedIds/rejectedIds and resumes the flow.
  // If the process ever dies between those two steps, approvalPending
  // permanently shows a real decision that was never actually applied —
  // the flow stays blocked forever, since nothing else ever re-drives step
  // (2). Same idempotent-recheck shape as the children-pending
  // reconciliation above, applied to the analogous gate-side gap. No-op
  // when maybeResolveFlow isn't wired in (older callers, tests).
  let gatesReconciled = 0;
  if (maybeResolveFlow) {
    for (const f of all) {
      if (f.status !== "blocked") continue;
      const waitKind = f.waitJson?.kind;
      if (waitKind !== "human-gate" && waitKind !== "gate" && waitKind !== "verify-required") continue;
      const items = Array.isArray(f.waitJson?.items) ? f.waitJson.items : [];
      if (items.length === 0) continue;
      const approvalPending = f.stateJson?.approvalPending ?? {};
      const hasUnappliedDecision = items.some(({ itemId }) => {
        const entry = approvalPending[itemId];
        return entry && (entry.decision || entry.sendFailed);
      });
      if (!hasUnappliedDecision) continue;
      logger.info(`manifold revival: [${f.flowId.slice(0, 8)}] has a recorded gate decision never applied — reconciling`);
      gatesReconciled++;
      maybeResolveFlow(f.flowId).catch((err) => {
        logger.warn(`manifold revival: gate reconciliation [${f.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
      });
    }
  }

  // ── Admission-pool leak reconciliation ──────────────────────────────────────
  // A flow_admission_queue row (state/admission-queue-store.js) is only ever
  // supposed to exist while its owning flow is genuinely non-terminal —
  // every terminal transition (finish/fail/cancel) releases it via
  // state-store.js's triggerAdmissionRelease. That release silently no-ops
  // if the caller's state-store was constructed without admissionQueueStore
  // wired in, leaving the flow's row "admitted" forever and shrinking the
  // one shared pool (a single combined ceiling across every
  // flow-start/child-dispatch, gateway-wide) for the rest of the process's
  // uptime. Rather than trust every future release call site
  // to stay correctly wired, sweep for the
  // invariant violation itself on every boot: a terminal flow can never
  // legitimately hold a live admission entry, so any row whose owning flow
  // is already terminal is definitionally a leak, regardless of which bug
  // produced it. statusByFlowId is built from `all` (already loaded above)
  // plus cancelledByRevival (flows this very call just finished cancelling,
  // whose stale status in `all` predates that) — no extra flow_runs reads.
  // Own terminal-status set, not the narrower `terminalStatuses` above
  // (cancelled/failed/succeeded only, scoped to that phase's specific
  // re-cancel check) — this sweep must also catch "skipped" (FILTER's
  // dedup path, run-store.js's own TERMINAL_STATUSES), any of which can
  // equally hold a leaked admission row.
  const admissionTerminalStatuses = new Set(["succeeded", "failed", "cancelled", "skipped"]);
  let admissionLeaksReleased = 0;
  if (admissionQueueStore) {
    const statusByFlowId = new Map(all.map((f) => [f.flowId, f.status]));
    const admissionRows = [
      ...(await admissionQueueStore.listByStatus("admitted")),
      ...(await admissionQueueStore.listByStatus("queued")),
    ];
    for (const row of admissionRows) {
      const owningStatus = cancelledByRevival.has(row.flowId) ? "cancelled" : statusByFlowId.get(row.flowId);
      if (!owningStatus || !admissionTerminalStatuses.has(owningStatus)) continue;
      logger.info(
        `manifold revival: releasing leaked ${row.kind} admission [${row.id.slice(0, 8)}] ` +
        `for already-terminal flow [${row.flowId.slice(0, 8)}] (status=${owningStatus})`,
      );
      try {
        const result = await admissionQueueStore.release(row.id);
        // release() frees this leaked slot and, as a side effect, may
        // promote the globally-oldest queued row into 'admitted' — a
        // DIFFERENT flow than the one this iteration is cleaning up.
        // Every other caller of release()/releaseByFlow() in this codebase
        // drives wakePromotedRelease() on that release, and so must this
        // sweep: calling release() directly and discarding its result would
        // flip the row to 'admitted' with nothing to ever flip the promoted
        // flow's own flow_runs status to 'running' or call runFlow() on it
        // — its admission would look live forever while the flow itself sat
        // frozen exactly where it was queued, permanently shrinking the
        // shared pool by one slot per promotion this sweep caused.
        await wakePromotedRelease(result, {
          runStore, resume: executor.resume, logger, api,
          context: `manifold revival: leak-release promotion [${row.id.slice(0, 8)}]`,
        });
      } catch (err) {
        logger.warn(`manifold revival: admission release [${row.id.slice(0, 8)}] failed: ${err?.message ?? err}`);
      }
      admissionLeaksReleased++;
    }
  }

  // ── Fill free capacity ──────────────────────────────────────────────────────
  // Every release promotes exactly one row into the slot it frees, so the
  // pool never drops below its limit with work queued — except when
  // admitted rows vanish without a release (a crash between a delete and
  // its promotion, or a migration dropping a whole kind). Promote queued
  // rows into whatever capacity is free now, so the pool doesn't idle below
  // its limit until enough unrelated releases happen.
  let freeSlotsFilled = 0;
  if (admissionQueueStore) {
    try {
      const promoted = await admissionQueueStore.fillFreeSlots();
      for (const p of promoted) {
        await wakePromotedRelease({ released: false, promoted: p }, {
          runStore, resume: executor.resume, logger, api,
          context: `manifold revival: free-slot fill [${p.id.slice(0, 8)}]`,
        });
      }
      freeSlotsFilled = promoted.length;
      if (freeSlotsFilled > 0) logger.info(`manifold revival: filled ${freeSlotsFilled} free admission slot(s) from the queue`);
    } catch (err) {
      logger.warn(`manifold revival: free-slot fill failed: ${err?.message ?? err}`);
    }
  }

  // ── Stuck-promotion reconciliation ──────────────────────────────────────────
  // The sweep above only catches leaks on already-terminal flows. Run the
  // non-terminal counterpart too (see reconcileStuckAdmissions's own header)
  // so a boot doesn't just inherit the same "left queued for admission —
  // will resume via the normal release/promotion cycle" hope the process
  // that crashed was already relying on.
  const { stuckAdmissionsResumed, stuckAdmissionsFailed, unknownKindReaped } = await reconcileStuckAdmissions({
    admissionQueueStore, runStore, executor, api, logger,
    ...(stuckAdmissionMinAgeMs != null ? { minAgeMs: stuckAdmissionMinAgeMs } : {}),
  });
  if (unknownKindReaped > 0) {
    logger.warn(`manifold revival: reaped ${unknownKindReaped} unknown-kind admission slot(s) no code path could drive`);
  }

  return {
    revived: toRevive.length, revivedAgent, queuedForAdmission, readmitted, recancelled: cancelledByRevival.size,
    childrenReconciled, gatesReconciled, admissionLeaksReleased, freeSlotsFilled,
    stuckAdmissionsResumed, stuckAdmissionsFailed, unknownKindReaped,
  };
}
