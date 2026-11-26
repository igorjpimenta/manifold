import { getNodeExecutionsStore } from "../state/node-executions.js";
import { gateEmitter } from "../events.js";
import { createEmitter } from "../events/emit.js";
import { assembleState } from "../engine/executor-helpers.js";
import { admitAndSetRunning } from "../engine/child-resume-trigger.js";

// Channel-agnostic resolution for gate items — the one write site for every
// decision channel (dashboard POST /approve, MCP manifold_approve, the
// companion ws bridge, automated pipeline writers). All call
// `recordItemDecision`: validate flow → idempotently merge into
// state.approvalPending[itemId] → maybeResolveFlow →
// applyResolutionsAndResume once every item has a decision (or sendFailed
// marker), which calls setRunning + executor.resume so the flow advances.
//
// "human-gate" appears only as the wait-kind discriminator the executor
// stamps; nothing here talks to a delivery channel. The gateway broadcasts
// manifold.gate.opened/closed over WS; each consumer (app bridge, dashboard,
// CLI) decides how to surface it. Channel-specific delivery + cleanup lives
// in that consumer, which calls this factory the same way.
//
// Per-process: the factory closes over a store + executor instance, so the
// gateway and the CLI each spin up their own resolver.
//
// Optional per-item apply hook: a human-gate node can declare
// `apply(ctx, {itemId, decision, editedText, reason})` on itself. When
// `pipelineLookup` is supplied, applyResolutionsAndResume calls it once per
// decided item immediately after that item's decision durably commits. No
// return value is folded back into state — node authors persist their own
// results. `ctx` is the flow-level assembled state, matching PRIME_FILL's
// existing apply(state, item) signature.
const VALID_SOURCES = ["ws", "dashboard", "mcp", "pipeline"];

export function createGateResolver({ api, store, executor, getBroadcast, readStore, runStore, pipelineLookup, nodeExecutionsStore, admissionQueueStore }) {
  const logger = api.logger;
  // Canonical-kind fan-out (events/emit.js): validated envelope,
  // `plugin.`-prefixed wire names, never throws on transport faults.
  const { emit } = createEmitter({ getBroadcast, logger });
  // Injectable for tests; production defaults to the real singleton.
  const nodeExecutions = nodeExecutionsStore ?? getNodeExecutionsStore();

  // Revision-checked state_json write for flows in the DB but not in the
  // in-process Map (post-restart blocked flows). The executor won't resume
  // the flow until it's revived, but the decision IS persisted in
  // runtime.sqlite and is picked up on revival.
  function writeSqlDirect(flowId, expectedRevision, stateJson) {
    if (!runStore) return { applied: false, code: "no_run_store" };
    return runStore.updateStateJsonCAS(flowId, expectedRevision, stateJson);
  }

  // Durable, operator-visible record of an apply() failure — in addition to
  // (not instead of) the warn log. Best-effort single attempt (this already
  // runs on a failure path; if the write itself fails, the warn log is the
  // fallback trail). Written to flow_node_executions so it surfaces through
  // the same errorEntries path as every other failure; branch-mode
  // (subNodeId set) keys the row to that branch.
  async function persistApplyFailureMarker({ flowId, nodeId, subNodeId, itemId, message }) {
    const errMsg = `apply() failed: ${message}`;
    const now = Date.now();
    try {
      const pipelineId = (await store.get(flowId))?.stateJson?.pipelineId ?? null;
      nodeExecutions?.write({
        flowId, pipelineId, nodeId: subNodeId ?? nodeId,
        branchKey: subNodeId ? itemId : null,
        enteredAt: now, exitedAt: now, error: errMsg,
      });
    } catch (err) {
      logger.warn(`manifold [${flowId.slice(0, 8)}] gate-resolver: persistApplyFailureMarker itself failed: ${err?.message ?? err}`);
    }
  }

  // Revision-aware state update, re-reading on conflict (e.g. a poller
  // delivering while a callback resolves). Routes by status:
  //   blocked → setWaiting: preserves waitJson, keeps status pinned at
  //     "blocked" (the canonical resting state for an open gate — despite
  //     the function's name it never writes "waiting").
  //   running → setRunning: used by the eager-delivery + during-running
  //     decision path. The executor's loop workers are still advancing
  //     branches, so we MUST NOT flip status to blocked — that would yank
  //     the rug out from under them and race them onto stale state.
  //     setRunning is a no-op on status; it just persists the new stateJson.
  async function updateApprovalState(flowId, updater) {
    for (let attempt = 0; attempt < 5; attempt++) {
      const flow = (await store.get(flowId)) ?? (await readStore?.get(flowId));
      if (!flow) return { applied: false, code: "flow_not_found" };
      const next = updater(flow.stateJson ?? {});
      let result;
      if (flow.status === "running") {
        result = await store.setRunning({
          flowId,
          expectedRevision: flow.revision,
          lastStep: flow.lastStep,
          stateJson: next,
        });
      } else {
        result = await store.setWaiting({
          flowId,
          expectedRevision: flow.revision,
          lastStep: flow.lastStep,
          stateJson: next,
          waitJson: flow.waitJson,
          statusReason: flow.statusReason,
        });
      }
      if (result.applied) return result;
      // Flow exists in SQL (readStore) but not the in-process Map —
      // store.setWaiting/setRunning return "not_found". Write the decision
      // directly to SQL so it persists for revival; maybeResolveFlow is a
      // no-op for Map-absent flows (it short-circuits on store.get null), so
      // the flow stays blocked until a restart picks up the persisted
      // decision.
      if (result.code === "not_found" && !(await store.get(flowId))) {
        logger.info(`manifold gate-resolver: ${flowId.slice(0, 8)} not in Map; writing decision via sql-direct`);
        return writeSqlDirect(flowId, flow.revision, next);
      }
      if (result.code !== "revision_conflict") return result;
      await new Promise((r) => setTimeout(r, 25 + attempt * 25));
    }
    return { applied: false, code: "retry_exhausted" };
  }

  // Resolve EACH decided item as soon as its decision lands — don't wait for
  // the whole gate batch. Undecided items stay in the waiting set; decided
  // ones get applied + appended to approvalResults, and the executor
  // re-enters the loop node to advance just those branches. Multiple partial
  // passes can fire as decisions trickle in.
  async function maybeResolveFlow(flowId, awaitCompletion = false) {
    const flow = await store.get(flowId);
    if (!flow) return;
    // Gate pauses are always persisted as "blocked" — "waiting" is never
    // actually written anywhere (confirmed repo-wide), so it's not checked.
    if (flow.status !== "blocked") return;
    if (flow.waitJson?.kind !== "human-gate" && flow.waitJson?.kind !== "gate" && flow.waitJson?.kind !== "verify-required") return;

    const items = flow.waitJson?.items ?? [];
    const approvalPending = flow.stateJson?.approvalPending ?? {};
    // Idempotency: an item already reflected in approvalResults[nodeId] for
    // THIS SPECIFIC decision was processed by an earlier partial-resume
    // pass — the executor may re-emit it in the next waitJson (top-level
    // gate nodes whose pendingSelector returns all items, not just
    // undecided ones), and we'd re-fire forever. Compared by decidedAt, not
    // just itemId presence: a branch item (itemId === branchKey) can be
    // legitimately decided again when its branch revisits the SAME
    // human-gate sub-node for a new round (GAP_GATE round 2 after round 1
    // resolved) — approvalResults is append-only and round 1's older entry
    // never goes away, so a blanket itemId-presence check would treat round
    // 2's freshly-recorded entry as already processed forever. Keep the
    // LATEST entry per itemId (last write wins) and skip only when the
    // pending decision IS that already-recorded one.
    const nodeId = flow.waitJson?.nodeId;
    const resultsForNode = (nodeId && Array.isArray(flow.stateJson?.approvalResults?.[nodeId]))
      ? flow.stateJson.approvalResults[nodeId] : [];
    const lastProcessedByItem = new Map();
    for (const r of resultsForNode) {
      if (r?.itemId) lastProcessedByItem.set(r.itemId, r);
    }
    const decided = [];
    for (const { itemId } of items) {
      const entry = approvalPending[itemId];
      if (!entry) continue;
      const alreadyProcessedEntry = lastProcessedByItem.get(itemId);
      if (alreadyProcessedEntry && alreadyProcessedEntry.decidedAt === entry.decidedAt) continue;
      // Skip entries recorded for a different gate: approvalPending is a
      // flat map keyed by itemId, so a later gate reusing the same itemIds
      // (e.g. VERIFY_SUBMIT after PROPOSE) would otherwise auto-resolve off
      // stale decisions. entry.gateNodeId is null for entries written before
      // this guard existed — treat those as unscoped (always eligible).
      if (entry.gateNodeId != null && entry.gateNodeId !== nodeId) continue;
      if (entry.decision) {
        decided.push({ itemId, decision: entry.decision, editedText: entry.editedText, reason: entry.reason, decidedAt: entry.decidedAt });
      } else if (entry.sendFailed) {
        decided.push({ itemId, decision: "reject", reason: `send failed: ${entry.sendError ?? "n/a"}`, decidedAt: entry.decidedAt ?? Date.now() });
      }
    }
    if (decided.length === 0) return;
    await applyResolutionsAndResume(flowId, decided, 0, awaitCompletion);
  }

  // A decision is recorded in two separate CAS writes — (1)
  // recordItemDecision's updateApprovalState durably persists
  // approvalPending[itemId]; (2) this function's own setRunning below moves
  // it into approvedIds/rejectedIds and resumes. If (2)'s retry loop
  // exhausts under sustained revision contention (several items in one gate
  // batch each triggering a maybeResolveFlow pass that races the others'
  // CAS writes), the decision sits recorded-but-unapplied until the next
  // gateway restart — flow-revival.js's stuck-gate-decision reconciliation
  // is a boot-time backstop, not a live one. ROUND_BACKOFF_MS/MAX_ROUNDS
  // give the SAME exhausted call up to 2 more full 5-attempt passes,
  // spaced to let concurrent writers finish, before truly giving up.
  const ROUND_BACKOFF_MS = 500;
  const MAX_ROUNDS = 3;

  // `awaitCompletion` mirrors routes/operator-retry.js's retryFailureBranch
  // param of the same name: the default schedules executor.resume via
  // setImmediate (plus a further round on sustained contention) and returns
  // before either fires — correct for long-lived HTTP callers, fatal for a
  // caller that must observe the resume genuinely finished before returning.
  async function applyResolutionsAndResume(flowId, resolutions, retryRound = 0, awaitCompletion = false) {
    // Revision-aware: re-read on conflict. Appends new decisions to
    // approvalResults[nodeId] so the executor sees the full decision history
    // on each re-entry. approvalPending is left untouched: its entries double
    // as clients' "already-delivered" guard, so dropping them would cause
    // re-delivery on the next setWaiting pass for any gate whose
    // pendingSelector doesn't filter on approvedIds/rejectedIds.
    // recordItemDecision separately enforces per-item idempotency via the
    // entry.decision check, so leaving decided entries in place is safe.
    for (let attempt = 0; attempt < 5; attempt++) {
      const flow = await store.get(flowId);
      if (!flow) return;
      if (flow.status !== "blocked") return;
      const state = flow.stateJson ?? {};
      const approvedIds = [...(state.approvedIds ?? [])];
      const rejectedIds = [...(state.rejectedIds ?? [])];
      const editedTexts = { ...(state.editedTexts ?? {}) };
      const newResultsThisPass = [];
      for (const r of resolutions) {
        if (!r) continue;
        if (r.decision === "approve") {
          if (!approvedIds.includes(r.itemId)) approvedIds.push(r.itemId);
          if (typeof r.editedText === "string") editedTexts[r.itemId] = r.editedText;
        } else {
          if (!rejectedIds.includes(r.itemId)) rejectedIds.push(r.itemId);
        }
        newResultsThisPass.push(r);
      }
      const nodeId = flow.waitJson?.nodeId ?? "gate";
      const prevResults = Array.isArray(state.approvalResults?.[nodeId]) ? state.approvalResults[nodeId] : [];
      const newState = {
        ...state,
        approvedIds,
        rejectedIds,
        editedTexts,
        approvalResults: { ...(state.approvalResults ?? {}), [nodeId]: [...prevResults, ...newResultsThisPass] },
      };
      const items = flow.waitJson?.items ?? [];
      const undecidedCount = items.reduce((n, it) => {
        const e = state.approvalPending?.[it.itemId];
        return n + (e?.decision || e?.sendFailed ? 0 : 1);
      }, 0);
      // Re-requests this flow's own admission (kind:"flow-start" for a
      // root-level gate, "child-dispatch" for a loop-branch gate) — it was
      // released the moment the flow blocked (state-store's setWaiting
      // hook), so resuming past it must re-claim a slot, not assume one is
      // still held. If the pool is full the flow is left "queued" instead of
      // running — the decision itself still commits below; only the actual
      // resume is deferred to admission-queue-store's release/promotion.
      const result = await admitAndSetRunning({
        flowId,
        expectedRevision: flow.revision,
        lastStep: flow.lastStep,
        stateJson: newState,
        parentInvocationId: flow.parentInvocationId,
        store,
        admissionQueueStore,
        runStore,
        resume: executor.resume,
        logger,
        api,
      });
      if (result.applied) {
        logger.info(`manifold [${flowId.slice(0, 8)}] gate-resolver: resolved ${newResultsThisPass.length} (${approvedIds.length} approve, ${rejectedIds.length} reject total)${undecidedCount > 0 ? `; ${undecidedCount} still awaiting` : ""}${result.queued ? "; queued — waiting for a shared admission slot to resume" : "; resuming"}`);
        gateEmitter.emit("gate.closed", { flowId, nodeId });
        // Resume patches for this write come from the WS run listener's
        // per-revision patch — emit no second shape here; clients decode one.
        // Human-gate apply hook: fires per item, immediately after THAT
        // item's decision lands (both a root-level gate and a loop branch's
        // own human-gate sub-node, e.g. PROPOSE's GATE/GAP_GATE). Fired ONLY
        // here, after the CAS write above committed — the retry loop re-runs
        // its in-memory computation on revision_conflict and must stay pure;
        // firing apply() inside it would double-run any real side effect
        // (e.g. a file write) on every retry attempt. No return value is
        // folded back into state — a node that needs to persist something
        // does so itself inside apply (mirrors WRITE, which owns its own
        // file I/O).
        const pipelineNode = pipelineLookup?.(state.pipelineId)?.nodes?.[nodeId];
        if (pipelineNode) {
          // newState only ever carries ENTRY_FIELDS + pipelineId/item/
          // itemIndex/parentItem (executor-persist's toPersistableState
          // allowlist) — a kind:"code" node's own output key (state.DRAFT,
          // state.tbds) is stripped before it reaches flow.stateJson and is
          // only reconstructed via assembleState()'s replay of
          // flow_node_invocations.result_json. apply() needs the same
          // assembly so it sees prior nodes' real output (e.g.
          // state.DRAFT.draftPath), not just entry/bookkeeping fields.
          // Computed once per pass, only when there's an apply() to feed.
          const hasApply = typeof pipelineNode.apply === "function";
          const assembledState = hasApply
            ? await assembleState(flowId, { runStore, nodeExecutionsStore: nodeExecutions, baseState: newState })
            : null;
          for (const r of newResultsThisPass) {
            let gateNode = hasApply ? pipelineNode : null;
            // Dead in practice: a childFlows child's gate is resolved against
            // the CHILD's own flow_runs row, so the flow-level gate node is
            // always right and no per-item sub-node hop is needed. Kept in
            // the signature because persistApplyFailureMarker still takes it
            // and keys its row on it.
            let subNodeId = null;
            let applyCtx = assembledState;
            if (typeof gateNode?.apply !== "function") continue;
            // The matching waitJson item — carries requestPayload.layout,
            // where a gate-UI field edit (persistRevisedMeta) actually lands.
            // `r` only carries the decision + optional editedText, never the
            // edited metadata fields, so apply() needs this to see what the
            // operator actually changed.
            const matchedItem = items.find((it) => it?.itemId === r.itemId);
            try {
              await gateNode.apply(applyCtx, r, matchedItem);
            } catch (err) {
              const errMessage = err?.message ?? String(err);
              logger.warn(`manifold [${flowId.slice(0, 8)}] gate-resolver: apply(${nodeId}, ${r.itemId}) threw: ${errMessage}`);
              // Beyond the warn: without this durable marker the decision
              // still commits (the item is approved/rejected either way) but
              // if apply()'s own side effect failed — a file-lock timeout, a
              // disk error — the operator would see "approved" and assume
              // done with no way to know it didn't land anywhere.
              await persistApplyFailureMarker({ flowId, nodeId, subNodeId, itemId: r.itemId, message: errMessage });
            }
          }
        }
        // Queued for admission — the decision durably committed above; the
        // flow itself isn't running yet. admission-queue-store's own
        // release/promotion cycle resumes it once a slot frees.
        if (result.queued) return;
        const resumeAndReconsume = async () => {
          try {
            await executor.resume(flowId);
            // Race: decisions can land while the flow is `running` (between
            // setRunning above and the executor's next setWaiting). Those
            // writes go to approvalPending but maybeResolveFlow
            // short-circuits on non-blocked status. After resume() returns,
            // the flow has either re-blocked (more items to process) or
            // terminated; in the blocked case, re-fire to consume decisions
            // that arrived mid-flight.
            await maybeResolveFlow(flowId, awaitCompletion);
          } catch (err) {
            logger.warn(`manifold gate-resolver: resume [${flowId.slice(0, 8)}] error: ${err?.message ?? err}`);
          }
        };
        if (awaitCompletion) await resumeAndReconsume();
        else setImmediate(resumeAndReconsume);
        return;
      }
      if (result.code !== "revision_conflict") {
        logger.warn(`manifold gate-resolver: setRunning failed for ${flowId.slice(0, 8)}: ${result.code}`);
        return;
      }
      await new Promise((r) => setTimeout(r, 25 + attempt * 25));
    }
    if (retryRound < MAX_ROUNDS - 1) {
      const delayMs = ROUND_BACKOFF_MS * (retryRound + 1);
      logger.warn(
        `manifold gate-resolver: setRunning failed for ${flowId.slice(0, 8)}: retry_exhausted ` +
        `(round ${retryRound + 1}/${MAX_ROUNDS}); scheduling another round in ${delayMs}ms`
      );
      const runNextRound = () => applyResolutionsAndResume(flowId, resolutions, retryRound + 1, awaitCompletion).catch((err) => {
        logger.warn(`manifold gate-resolver: scheduled retry round for ${flowId.slice(0, 8)} threw: ${err?.message ?? err}`);
      });
      if (awaitCompletion) {
        await new Promise((r) => setTimeout(r, delayMs));
        await runNextRound();
      } else {
        setTimeout(runNextRound, delayMs);
      }
      return;
    }
    // All live rounds exhausted — the decision is still durably recorded in
    // approvalPending (written before this function ever ran), so nothing
    // is lost. boot/flow-revival.js's stuck-gate-decision reconciliation
    // pass will pick this up and apply it on the next restart; this is the
    // loud, discoverable trail for why that backstop had to fire, not a
    // starting point for further live retries (avoids retrying forever
    // against a genuinely wedged flow).
    logger.warn(
      `manifold gate-resolver: setRunning failed for ${flowId.slice(0, 8)}: retry_exhausted after ${MAX_ROUNDS} rounds — ` +
      `decision remains durably recorded in approvalPending; will self-heal via flow-revival.js's stuck-gate-decision reconciliation pass on next restart`
    );
  }

  // Shared write path for every channel resolving a gate item.
  // Returns { ok, status, code? }: "already-decided" (idempotent no-op),
  // "applied" (decision recorded, consolidator triggered), or ok=false with
  // code carrying the failure reason.
  async function recordItemDecision({ flowId, itemId, decision, source, editedText, reason, awaitCompletion = false }) {
    if (!flowId || typeof flowId !== "string") {
      return { ok: false, code: "missing_flow_id" };
    }
    if (!itemId || typeof itemId !== "string") {
      return { ok: false, code: "missing_item_id" };
    }
    if (!["approve", "reject", "edit"].includes(decision)) {
      return { ok: false, code: "invalid_decision" };
    }
    if (decision === "edit" && typeof editedText !== "string") {
      return { ok: false, code: "edit_requires_text" };
    }
    // "edit" is caller-facing sugar for "approve, with modified text" — an
    // edit IS an approval, no distinct third resolution state. Normalize
    // here so storage/idempotency/applyResolutionsAndResume only ever see
    // approve|reject; editedText still carries through either spelling.
    if (decision === "edit") decision = "approve";
    if (!source || !VALID_SOURCES.includes(source)) {
      return { ok: false, code: "invalid_source" };
    }

    const flow = (await store.get(flowId)) ?? (await readStore?.get(flowId));
    if (!flow) return { ok: false, code: "flow_not_found" };
    if (flow.status !== "blocked" && flow.status !== "running") {
      return { ok: false, code: "flow_not_awaiting" };
    }
    // Idempotency first — a decision already recorded for this itemId (any
    // source, any earlier call) short-circuits ok:true. This MUST run
    // before the item-membership validation below: after the executor
    // consumes a decision the itemId drops out of waitJson.items, so
    // re-calling resolve (audit retry, double-tap, channel race) would
    // otherwise 409 with item_not_in_wait even though the desired end state
    // is already in place. Idempotent retries are no-ops, not failures.
    const existing = flow.stateJson?.approvalPending?.[itemId];
    if (existing?.decision) {
      // A branch item's identity (itemId === branchKey) is shared across
      // every visit to a human-gate sub-node — a branch only ever awaits one
      // thing at a time, so GAP_GATE's round 2 reuses round 1's exact itemId.
      // A blanket "already decided" here would silently swallow round 2's
      // answer forever: nothing else in this file clears
      // approvalPending[itemId], by design — client dismiss-signal checks
      // depend on it surviving past consumption. This guard runs BEFORE the
      // membership validation below, so it cannot use membership to tell the
      // two cases apart: a re-presented gate carries a fresh waitJson.items
      // entry, but nothing has consulted that by here. An existing
      // approvalPending entry is therefore always a genuine duplicate.
      logger.info(
        `manifold [${flowId.slice(0, 8)}] gate-resolver: ${itemId} already decided (${existing.decision} via ${existing.source ?? "?"}); ignoring duplicate from ${source}`
      );
      return { ok: true, status: "already-decided" };
    }

    // Membership: blocked validates against waitJson.items (canonical batch
    // list); running never validates — childFlows gates always surface via
    // waitJson when blocked, so a running flow has no mid-batch items.
    let validItem = false;
    if (flow.status === "blocked") {
      if (flow.waitJson?.kind !== "human-gate" && flow.waitJson?.kind !== "gate" && flow.waitJson?.kind !== "verify-required") {
        return { ok: false, code: "flow_not_gate" };
      }
      const items = Array.isArray(flow.waitJson?.items) ? flow.waitJson.items : [];
      validItem = items.some((it) => it?.itemId === itemId);
    } else {
      validItem = false;
    }
    if (!validItem) {
      return { ok: false, code: "item_not_in_wait" };
    }

    // Gate that owns this decision, so maybeResolveFlow can skip entries
    // recorded for an earlier gate with the same itemId (PROPOSE items
    // leaking into VERIFY_SUBMIT).
    const gateNodeId = flow.waitJson?.nodeId ?? null;

    const writeResult = await updateApprovalState(flowId, (s) => ({
      ...s,
      approvalPending: {
        ...(s.approvalPending ?? {}),
        [itemId]: {
          ...(s.approvalPending?.[itemId] ?? {}),
          decision,
          source,
          decidedAt: Date.now(),
          gateNodeId,
          ...(typeof editedText === "string" ? { editedText } : {}),
          ...(typeof reason === "string" ? { reason } : {}),
        },
      },
    }));
    if (!writeResult.applied) {
      logger.warn(`manifold gate-resolver: persist decision failed for ${flowId.slice(0, 8)}:${itemId}: ${writeResult.code}`);
      return { ok: false, code: writeResult.code ?? "persist_failed" };
    }
    logger.info(`manifold [${flowId.slice(0, 8)}] gate-resolver: ${itemId} → ${decision} (source=${source})`);

    await maybeResolveFlow(flowId, awaitCompletion);
    // Canonical gate.resolved: wire name is the kind (`plugin.gate.resolved`).
    try { await emit("gate.resolved", { flowId, itemId, decision, resolvedAt: Date.now() }); } catch {}
    return { ok: true, status: "applied" };
  }

  return {
    recordItemDecision,
    maybeResolveFlow,
    updateApprovalState,
  };
}
