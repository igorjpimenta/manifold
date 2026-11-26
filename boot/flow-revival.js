import { completeCancellation } from "../engine/executor-cancel.js";
import { areSiblingsAllTerminal, flipToRunningAndResume, subflowOutcomeSatisfied } from "../engine/child-resume-trigger.js";
import { resolveEdge } from "../engine/executor-helpers.js";
import { wakePromotedRelease, isEngineResumableKind, scheduleDetachedResume } from "../engine/admission-promotion-resume.js";
import { admissionKindForFlow } from "../engine/child-resume-trigger.js";
import { TERMINAL_STATUS_SET } from "../state/run-store.js";

// A promotion (release()'s side effect of flipping a queued row straight to
// 'admitted') is followed by a separate async continuation that drives the
// flow to 'running' — not the same transaction as the durable write. A crash
// or swallowed throw in that window leaves the row 'admitted' forever with
// nothing to drive it, holding a live pool slot; the terminal-leak sweep
// can't catch it because the flow never terminals. minAgeMs skips promotions
// still in flight. Exported (not folded into reviveOrphanFlows) so the
// periodic timer in server/app.js can rerun it — boot revival runs once.
export async function reconcileStuckAdmissions({ admissionQueueStore, runStore, executor, api, logger, minAgeMs = 30_000 }) {
  if (!admissionQueueStore) return { stuckAdmissionsResumed: 0, stuckAdmissionsFailed: 0, retiredKindReaped: 0 };
  const now = Date.now();
  const admittedRows = await admissionQueueStore.listByStatus("admitted");

  // Admitted rows of kinds this engine can't resume are permanent pool
  // leaks, not safe skips: resumePromotedAdmission refuses non-resumable
  // kinds and releaseByFlow matches the flow's CURRENT admission kind, so
  // nothing can ever drive such a row — while admit()'s and fillFreeSlots()'
  // admitted-count queries have no kind filter, so zombie rows count against
  // the ceiling forever and wedge both shut. release() frees the slot and
  // promotes the oldest queued row, putting the capacity back to work in the
  // same pass. A QUEUED row of such a kind holds no slot and is left alone
  // here; fillFreeSlots() can promote one, and this pass reclaims it after
  // the same grace window.
  let retiredKindReaped = 0;
  const resumableRows = [];
  for (const row of admittedRows) {
    if (isEngineResumableKind(row.kind)) { resumableRows.push(row); continue; }
    if (row.admittedAt && now - row.admittedAt < minAgeMs) continue;
    const flow = await runStore.get(row.flowId);
    // A flow still 'running' owns the slot and releases it through its own
    // transition path; reclaiming underneath it would transiently over-admit
    // the pool. Missing or terminal flows are reaped — no live process can
    // release them, and release() is a safe no-op if the boot sweep did.
    if (flow?.status === "running") continue;
    try {
      const result = await admissionQueueStore.release(row.id);
      await wakePromotedRelease(result, {
        runStore, resume: executor.resume, logger, api,
        context: `manifold revival: retired-kind reap [${row.id.slice(0, 8)}]`,
      });
      retiredKindReaped++;
      logger.warn(
        `manifold revival: reaped retired-kind '${row.kind}' admission [${row.id.slice(0, 8)}] for flow ` +
        `[${row.flowId.slice(0, 8)}] (${flow ? `status=${flow.status}` : "flow_runs row gone"}) — ` +
        `no code path can drive this kind, slot reclaimed`,
      );
    } catch (err) {
      logger.warn(`manifold revival: retired-kind reap [${row.id.slice(0, 8)}] failed: ${err?.message ?? err}`);
    }
  }

  // Filter candidates with cheap sequential reads, then drive them in
  // parallel: flipToRunningAndResume can block unboundedly (it may resolve
  // the flow to a fresh admission request that waits on another slot), so
  // driving candidates one at a time lets a single flow stuck behind real
  // contention stall every other candidate — defeating a sweep that exists
  // to recover exactly those flows under load.
  const candidates = [];
  for (const row of resumableRows) {
    if (row.admittedAt && now - row.admittedAt < minAgeMs) continue;
    const flow = await runStore.get(row.flowId);
    // Missing or terminal flows belong to the boot-only terminal-leak
    // sweep; 'running' is the healthy, expected state.
    if (!flow || flow.status === "running" || TERMINAL_STATUS_SET.has(flow.status)) continue;
    // An 'admitted' row only says this flow holds a slot for THIS kind — it
    // may be legitimately queued on a different resource (a cross-process
    // lock), and force-driving that would resume it atop its own wait. The
    // leak signature is a flow queued to be admitted for the SAME kind it
    // already holds, which only a dropped flip-to-running follow-through can
    // produce.
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
      // No admissionQueueStore passed: this flow already holds the live
      // admission — passing it would have flipToRunningAndResume call
      // admit() again for a slot the flow already owns.
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
  return { stuckAdmissionsResumed, stuckAdmissionsFailed, retiredKindReaped };
}

export async function reviveOrphanFlows({
  all, pipelines, store, logger, executor, createCancelStore,
  runStore, nodeExecutionsStore, maybeResolveFlow, sessionIndex, admissionQueueStore, api,
  // Grace window for reconcileStuckAdmissions below; tests shrink it instead
  // of waiting out the real one (a boot's admission rows all predate this
  // process, so the default is trivially satisfied there).
  stuckAdmissionMinAgeMs,
}) {
  // store.cancel() writes cancel_requested_at first, then status='cancelled',
  // in two separate SQL statements. A crash between them leaves a flow with
  // cancelRequestedAt set but status still 'running'/'blocked'/'waiting'.
  // Re-cancel those before reviving anything else so they don't land in toRevive.
  // f.revision is already the post-requestCancel value — pass it directly to
  // completeCancellation as the expectedRevision for setRunning.
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

  // isAgent decides HOW to revive an agent-kind node: reviveAgentKindOrphan()
  // resumes the SAME session with REVIVAL_MSG, a continuation prompt, sent
  // UNCONDITIONALLY even to a session with zero prior turns (executor-nodes.js:
  // `reviveMessage ?? promptBuilder(...)` with no branch on prior
  // invocations). So isAgent is only correct when a dispatch genuinely
  // started — REVIVAL_MSG as the first-ever message to a node that was only
  // queued would be a "continue where you left off" with nothing to continue.
  //
  // Running: targetNodeId comes from nodeExecutionsStore.findCurrentStep(),
  // race-free proof a dispatch entered the node — nodeExecutions.enter()
  // creates the invocation row BEFORE executeNode() runs, well before
  // flow_sessions is written (session-index.js) or subagent.run() is called.
  // The 'running' case must not REQUIRE a session record: a crash past enter()
  // but before that write leaves correct targetNodeId with
  // hasSessionForNode()===false, and requiring it would misroute the dispatch
  // into resolveEdge()/runFlow(), which fails with "edge <NODE> returned no
  // next node". A running flow with no invocation row yet (findCurrentStep()
  // -> null) correctly falls through to resolveEdge(lastStep), and
  // hasSessionForNode() still decides isAgent for it.
  //
  // Queued: targetNodeId comes from waitJson.nodeId — set by the admission
  // wait before it acquires a slot, proving the node was ABOUT to be
  // dispatched, not that dispatch began — so hasSessionForNode() still gates
  // isAgent there.
  //
  // f.lastStep is the last COMPLETED node, never the one in flight; the real
  // target is waitJson.nodeId (queued) or findCurrentStep() (running, falling
  // back to resolveEdge(lastStep) for a brand-new flow with no invocations).
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

  // A 'queued' orphan on kind:"flow-start" or "child-dispatch" is already
  // tracked in the DURABLE combined admission pool, which reflected who was
  // admitted vs queued the moment the gateway died — nothing to re-derive. A
  // live queued flow of these kinds is never dispatched directly either
  // (executor.js runFlow() isQueued guard): the same release()/promotion
  // cycle that wakes it wakes a boot-revived one once some other flow goes
  // blocked or terminal.
  //
  // A 'queued' orphan on waitJson.kind:"agent-dispatch" — a kind this engine
  // does not admit (its rows are deleted by migration) — gave its run slot
  // back when it queued, so it holds nothing; re-entering it directly would
  // run outside the pool. flipToRunningAndResume makes it re-request its own
  // slot instead (admitted → resumed; pool full → queued for the normal
  // promotion cycle).
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
        // inside it like any other node.
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

  // The resume-trigger (state-store.js's finish/skip/fail/cancel) fires on
  // the LAST sibling's terminal write — a crash at exactly that moment loses
  // the fire. Re-check every blocked parent against its children's current
  // terminal state: same idempotent-recheck shape as gate-resolver.js's
  // maybeResolveFlow. No-op when the stores aren't wired in (older callers,
  // tests).
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
      // Same mode check as child-resume-trigger.js's maybeResumeParent: a
      // parent orphaned mid-resume must fail (not resume) when its subflow
      // node's mode isn't satisfied, as if the crash hadn't happened.
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
      // executor.resume() is runFlow(), whose first check returns immediately
      // for a flow still in status='blocked'.
      await flipToRunningAndResume({ parentFlowId: f.flowId, runStore, resume: executor.resume, logger, admissionQueueStore, api }).catch((err) => {
        logger.warn(`manifold revival: children-pending resume [${f.flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
      });
    }
  }

  // A gate decision is recorded in two steps: write approvalPending[itemId]
  // durably, then maybeResolveFlow → applyResolutionsAndResume moves it into
  // approvedIds/rejectedIds and resumes the flow. A crash between them leaves
  // approvalPending showing a real decision never applied and nothing else
  // re-drives step two — the flow stays blocked forever. Same idempotent
  // recheck as above; no-op when maybeResolveFlow isn't wired in.
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

  // A flow_admission_queue row should exist only while its owning flow is
  // non-terminal — every terminal transition releases it via state-store.js's
  // triggerAdmissionRelease, which silently no-ops if the caller's state-store
  // was built without admissionQueueStore wired in, leaving the row "admitted"
  // forever and shrinking the one gateway-wide shared pool for the rest of
  // uptime. Sweep the invariant itself every boot rather than trust every
  // future release call site: a terminal flow can never legitimately hold a
  // live admission entry, whatever bug produced it. statusByFlowId is built
  // from `all` plus cancelledByRevival (flows just re-cancelled, whose status
  // in `all` is stale). Own terminal-status set — the narrower
  // `terminalStatuses` above (scoped to the re-cancel check) misses "skipped"
  // (run-store.js's TERMINAL_STATUSES), which can equally hold a leak.
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
        // release() may promote a DIFFERENT flow's queued row into the freed
        // slot; every other release() call site drives wakePromotedRelease()
        // on the result, and so must this sweep — discarding it would leave
        // that promotion 'admitted' with nothing to flip its flow to
        // 'running', shrinking the pool by one slot per promotion here.
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

  // Every release promotes exactly one row into the slot it frees, so the
  // pool only idles below its limit when admitted rows vanish without a
  // release (a crash between a delete and its promotion, a migration dropping
  // a whole kind). Promote queued rows into whatever capacity is free now.
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

  // Non-terminal counterpart of the leak sweep above: boot must not just
  // inherit the crashed process's stuck admissions.
  const { stuckAdmissionsResumed, stuckAdmissionsFailed, retiredKindReaped } = await reconcileStuckAdmissions({
    admissionQueueStore, runStore, executor, api, logger,
    ...(stuckAdmissionMinAgeMs != null ? { minAgeMs: stuckAdmissionMinAgeMs } : {}),
  });
  if (retiredKindReaped > 0) {
    logger.warn(`manifold revival: reaped ${retiredKindReaped} retired-kind admission slot(s) no code path could drive`);
  }

  return {
    revived: toRevive.length, revivedAgent, queuedForAdmission, readmitted, recancelled: cancelledByRevival.size,
    childrenReconciled, gatesReconciled, admissionLeaksReleased, freeSlotsFilled,
    stuckAdmissionsResumed, stuckAdmissionsFailed, retiredKindReaped,
  };
}
