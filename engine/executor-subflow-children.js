// executor-subflow-children.js — real child-flow dispatch for a
// `kind:"subflow"` node.
//
// Deliberately small: a childFlows
// subflow's items are ordinary flow_runs children driven by the exact same
// executor.js/runFlow() every other flow uses — this module's only job is
// resolving items → spawning/finding each child → checking whether the
// whole fan-out is done. No data-shape adapter, no calling-convention
// translation: a childFlows subflow's `.flow.nodes`/`.flow.edges` are
// validated (engine/loader.js) as plain root-level nodes, so by the time
// this runs they already speak the same convention as the rest of the
// engine.
//
// node.concurrency: a boolean PARTICIPATION switch, not a per-node number.
// true (default, including when unset) means this
// subflow's children draw from the single shared combined admission pool
// (state/admission-queue-store.js, kind:"child-dispatch" — the same pool
// flow-start draws from); false opts a node out entirely (unbounded —
// including any agent nodes its children run, since those execute inside
// the child's own run with no admission of their own), for a fan-out cheap
// enough that throttling it would only add latency for no protective
// benefit. The shared pool is deliberately not
// scoped to any one pipeline, node, or kind, since the resource actually
// being protected (host CPU/memory, a shared account session, agent worker
// processes) is shared by literally everything.

import { resolveOrCreateChild } from "./spawn-child.js";
import { areSiblingsAllTerminal } from "./child-resume-trigger.js";
import { makeNeedsCtx } from "./needs-resolver.js";

// Above the shared admission cap (engine/global-concurrency.js's default of
// 8) since this batch size only bounds cheap
// row-bookkeeping (resolveOrCreateChild, store.get/setQueued), not the
// actual resource-consuming work that cap protects; low enough to avoid a
// thundering-herd write burst against the one shared WAL file.
const DISPATCH_BATCH_SIZE = 10;

// Bounded-concurrency map, error-isolated per item — Promise.allSettled per
// batch, not Promise.all, so one item's rejection can never affect its
// batch-mates or later batches (matches this file's own former per-item
// try/catch granularity). No general-purpose equivalent exists elsewhere in
// this codebase; small enough to keep local to this one caller rather than
// add a dependency or a new shared module.
async function runInBatches(items, batchSize, fn) {
  const results = new Array(items.length);
  for (let start = 0; start < items.length; start += batchSize) {
    const batch = items.slice(start, start + batchSize);
    const settled = await Promise.allSettled(batch.map((item, i) => fn(item, start + i)));
    for (let i = 0; i < settled.length; i++) results[start + i] = settled[i];
  }
  return results;
}

// subflowKey is node.itemKey when the pipeline declares one (e.g. CONNECT),
// else the item's own id/listingId.
function deriveSubflowKey(node, item) {
  if (node.itemKey) return item?.[node.itemKey];
  return item?.id ?? item?.listingId ?? String(item);
}

// A child's initialState is deliberately NOT a full clone of the parent's
// state. Any node inside the subflow that genuinely needs a value from an
// ancestor flow declares it explicitly via `needs:`
// (engine/needs-resolver.js), resolved lazily at read time against that
// ancestor's own resultJson/entry fields — not inherited wholesale at
// spawn time.
function buildChildInitialState(state, item, itemIndex) {
  // Captured before spawn — so a single-item retry-cascade (selector: () =>
  // [{id:"only"}], structuring a code -> infra-recovery -> agent-fallback
  // chain, not a real per-item fan-out) can still reach the real
  // parent-branch item its inner nodes need — without this, `item` below
  // silently replaces the real listing/candidate/conversation object with
  // the cascade's own placeholder, and any inner node reading
  // `state.item.id`/`.jobId` for the real target id gets the literal string
  // "only" instead. A real fan-out's own item is unaffected either way —
  // parentItem is a pure addition.
  const parentItem = state?.item;
  return { item, itemIndex, parentItem };
}

export async function dispatchSubflowChildren({ node, nodeId, flowId, state, store, runStore, nodeExecutionsStore, maybeBudgetGate, runFlow, logger, api, lineage, pipelinesRef, admissionQueueStore }) {
  const _lineage = lineage ?? api?.runtime?.lineage ?? null;
  // "needs:" resolution — a subflow node's own selector can declare .needs and call
  // ctx.needs(varName) to reach an ancestor flow, same lazy/opt-in
  // mechanism code and agent nodes get. A rejected/not-ready need throws
  // NodeBlocked here, propagating straight through (no local try/catch in
  // this function) to executor.js's own NodeBlocked handling, unchanged.
  const items = await Promise.resolve(node.selector(state, { needs: makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef }) }));
  if (!Array.isArray(items) || items.length === 0) {
    logger?.info?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: 0 items, skipping (childFlows)`);
    return { stateUpdate: {} };
  }

  // Skip the subflow-entry gate for a cascade-shape node (flow.kind:
  // "cascade"). A cascade's own code leg frequently
  // resolves the item for free (edge routes straight to "__end__" without ever touching the
  // agent leg — e.g. a cached login), so gating here, before that code leg
  // even runs, produces a false-positive hold on the common case. Real
  // spend stays fully covered: the agent leg (e.g. ENSURE_LOGIN_AGENT), if
  // and when the cascade actually reaches it, is independently gated by
  // dispatchAgentEntry's own maybeBudgetGate call. A real multi-item
  // childFlows fan-out (e.g. SCORE, buildScoreLoopNode) has no flow.kind at
  // all, so it's unaffected by this guard and keeps its existing
  // subflow-entry gate.
  if (node.flow?.kind !== "cascade") {
    const gate = await maybeBudgetGate({
      flowId, nodeId, node,
      pipelineId: state?.pipelineId ?? null,
      pipelineName: state?.pipelineId ?? null,
      itemCount: items.length,
      items,
    });
    if (gate.skip) return { stateUpdate: {} };
    if (gate.pause) return { blockedInfo: gate.blockedInfo };
  }

  const childPipelineId = `${state?.pipelineId ?? ""}::${nodeId}`;

  // Pre-filter valid items up front (same empty-subflowKey skip as before)
  // so the batched phases below work off a clean list.
  const validItems = [];
  for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
    const item = items[itemIndex];
    const subflowKey = deriveSubflowKey(node, item);
    if (subflowKey == null || subflowKey === "") {
      logger?.warn?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: item ${itemIndex} produced an empty subflowKey, skipping`);
      continue;
    }
    validItems.push({ item, itemIndex, subflowKey: String(subflowKey) });
  }

  // Race-safe row resolution, batched with bounded concurrency.
  // resolveOrCreateChild's race-safety (spawn-child.js) is per-identity
  // (flowId, nodeId, subflowKey), backed by a unique index — it was never
  // dependent on THIS loop processing items one at a time; concurrent
  // calls for DIFFERENT identities have nothing to race over.
  //
  // node.dispatchGuard: optional async (item, subflowKey, { flowId }) => boolean,
  // declared by a pipeline that needs cross-flow dedup beyond what
  // node.selector's own upstream filtering can guarantee — the selector's
  // list is decided once per parent-flow visit, so two parent flows racing
  // through the same selector logic can both decide to dispatch the same
  // subflowKey before either commits. Threaded through to
  // findOrCreateInvocation (state/node-executions.js), which evaluates it
  // inside the same write-queue turn as the actual insert, so the second
  // caller's guard always sees the first caller's already-committed row.
  const resolved = await runInBatches(validItems, DISPATCH_BATCH_SIZE, (v) =>
    resolveOrCreateChild({
      flowId,
      pipelineId: state?.pipelineId ?? null,
      nodeId,
      subflowKey: v.subflowKey,
      childPipelineId,
      initialState: buildChildInitialState(state, v.item, v.itemIndex),
      store,
      runStore,
      nodeExecutionsStore,
      guard: node.dispatchGuard ? () => node.dispatchGuard(v.item, v.subflowKey, { flowId }) : undefined,
    }).then((r) => ({ ...v, ...r }))
  );

  const childFlowIds = [];
  const created = [];
  for (let i = 0; i < resolved.length; i++) {
    const settled = resolved[i];
    const v = validItems[i];
    if (settled.status === "rejected") {
      logger?.warn?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: failed to spawn child for item ${v.itemIndex} (subflowKey=${v.subflowKey}): ${settled.reason?.message ?? settled.reason}`);
      continue;
    }
    const { childFlowId, created: isNew, guarded } = settled.value;
    if (guarded) {
      logger?.info?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: item ${v.itemIndex} (subflowKey=${v.subflowKey}) rejected by dispatchGuard, skipping`);
      continue;
    }
    childFlowIds.push(childFlowId);
    // Eager creation dispatches once; an already-existing child was already
    // dispatched on a prior visit to this subflow node (or is legitimately
    // mid-flight/blocked on its own gate) — re-triggering it here would be
    // redundant, not incorrect (runFlow is documented idempotent/
    // re-entrant), so it's simply skipped.
    if (isNew) created.push({ ...v, childFlowId });
  }

  // Real atomic admission per new child, via the shared combined
  // pool (state/admission-queue-store.js). Each admit() call is itself one
  // atomic DB transaction — count-then-claim happens inside a single
  // BEGIN IMMEDIATE, so concurrent callers (another subflow node, another
  // pipeline, another process) admit or queue against the TRUE live count
  // every time.
  //
  // node.concurrency (see this file's header): true (default)
  // participates in the shared pool; false opts out entirely (unadmitted,
  // always dispatched).
  //
  // Only "running" counts as occupying a slot — a child "blocked" on its
  // own human gate consumes nothing while it waits, so it must NOT occupy
  // one; state-store.js's setWaiting releases this admission the moment
  // that happens (see triggerAdmissionRelease there), or a gated child
  // could starve the pool for hours. The exact same rule applies to a ROOT
  // flow's own admission too (kind:"flow-start") — the shared admission
  // pool treats every kind identically.
  const toQueue = [];
  const toDispatch = [];
  for (const c of created) {
    // Register the child's own trigger edge (flow:<childFlowId> ->
    // flow:<flowId>) BEFORE scheduling its run, so the resolver can walk
    // straight through to the parent's already-resolved root (e.g. the cron
    // that triggered the top-level fan-out) instead of dead-ending —
    // without this, cost attributed to the child's nodes collapses into an
    // unlinked "unknown" bucket. recordTrigger is async (it resolves the
    // root, then upserts session_lineage), so it is awaited: the runFlow
    // scheduling further down is setImmediate, which fires on the next tick
    // and would otherwise let the child's first node resolve lineage before
    // this edge lands.
    await _lineage?.recordTrigger?.({
      flowId: c.childFlowId,
      pipelineId: childPipelineId,
      parentSessionKey: `flow:${flowId}`,
    });

    if (node.concurrency === false) {
      toDispatch.push(c);
      continue;
    }
    const admission = await admissionQueueStore.admit({ kind: "child-dispatch", flowId: c.childFlowId, nodeId });
    (admission.status === "queued" ? toQueue : toDispatch).push(c);
  }

  // Dispatch: setImmediate is already non-blocking fire-and-forget — no
  // batching needed, this was never part of the serialization problem.
  for (const c of toDispatch) {
    setImmediate(() => {
      runFlow(c.childFlowId, "fresh").catch((err) => {
        logger?.warn?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: child ${c.childFlowId.slice(0, 8)} runFlow failed: ${err?.message ?? err}`);
      });
    });
  }

  // Queue: store.get() + store.setQueued() are genuinely awaited DB calls —
  // batch these too, same reason as the resolution step above.
  await runInBatches(toQueue, DISPATCH_BATCH_SIZE, async (c) => {
    const row = await store.get(c.childFlowId);
    if (row) {
      await store.setQueued({
        flowId: c.childFlowId,
        expectedRevision: row.revision,
        lastStep: row.lastStep,
        stateJson: row.stateJson,
        waitJson: { kind: "child-dispatch" },
        statusReason: `queued: waiting for a shared admission slot (child of node ${nodeId})`,
      });
    }
  }).then((settled) => {
    for (let i = 0; i < settled.length; i++) {
      if (settled[i].status === "rejected") {
        const c = toQueue[i];
        logger?.warn?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: failed to queue child ${c.childFlowId.slice(0, 8)} (item ${c.itemIndex}): ${settled[i].reason?.message ?? settled[i].reason}`);
      }
    }
  });

  const { allTerminal } = await areSiblingsAllTerminal({ flowId, nodeId, runStore, nodeExecutionsStore });
  if (allTerminal) {
    return { stateUpdate: {} };
  }
  return {
    stateUpdate: {},
    waitJson: { kind: "children-pending", nodeId, childFlowIds },
    reentrant: true,
  };
}
