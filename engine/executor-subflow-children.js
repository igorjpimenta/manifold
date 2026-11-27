// executor-subflow-children.js — real child-flow dispatch for a
// `kind:"subflow"` node.
//
// Children are ordinary flow_runs driven by the same executor.js/runFlow()
// as every other flow; this module only resolves items → spawns/finds each
// child → checks whether the whole fan-out is done. No data-shape adapter or
// calling-convention translation: childFlows subflow `.flow.nodes`/`.flow.edges`
// are validated (engine/loader.js) as plain root-level nodes.
//
// node.concurrency: a boolean PARTICIPATION switch, not a per-node number.
// true (default, including unset) means this subflow's children draw from the
// single shared combined admission pool (state/admission-queue-store.js,
// kind:"child-dispatch" — same pool flow-start draws from); false opts a node
// out entirely (unbounded, including agent nodes its children run, since those
// execute inside the child's own run with no admission of their own), for a
// fan-out cheap enough that throttling adds only latency. The shared pool is
// deliberately not scoped to any one pipeline/node/kind: the protected
// resource (host CPU/memory, shared gateway Chrome, agent worker processes)
// is shared by everything.

import { resolveOrCreateChild } from "./spawn-child.js";
import { areSiblingsAllTerminal } from "./child-resume-trigger.js";
import { makeNeedsCtx } from "./needs-resolver.js";

// Above the shared admission cap (engine/global-concurrency.js default 8):
// this batch size bounds cheap row-bookkeeping (resolveOrCreateChild,
// store.get/setQueued), not the resource-consuming work the cap protects; low
// enough to avoid a thundering-herd write burst against the one shared WAL.
const DISPATCH_BATCH_SIZE = 10;

// Bounded-concurrency map, error-isolated per item — Promise.allSettled per
// batch so one item's rejection can't affect batch-mates or later batches.
// No general-purpose equivalent exists elsewhere; small enough to keep local.
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
// state. Ancestor values a node needs are declared via `needs:` and resolved
// lazily at read time against that ancestor's resultJson/entry fields — not
// inherited wholesale at spawn time.
function buildChildInitialState(state, item, itemIndex) {
  // Captured before spawn so a single-item retry-cascade (selector: () =>
  // [{id:"only"}], structuring code -> infra-recovery -> agent-fallback)
  // can still reach the real parent-branch item its inner nodes need —
  // without this, `item` replaces the real listing/candidate/conversation
  // object with the cascade's placeholder, and inner nodes reading
  // state.item.id/jobId get the literal "only". A real fan-out's own item is
  // unaffected (parentItem is a pure addition).
  const parentItem = state?.item;
  return { item, itemIndex, parentItem };
}

export async function dispatchSubflowChildren({ node, nodeId, flowId, state, store, runStore, nodeExecutionsStore, maybeBudgetGate, runFlow, logger, api, lineage, pipelinesRef, admissionQueueStore }) {
  const _lineage = lineage ?? api?.runtime?.lineage ?? null;
  // "needs:" resolution — a subflow node's own selector can declare .needs and
  // call ctx.needs(varName) to reach an ancestor flow, same lazy/opt-in
  // mechanism code and agent nodes get. A rejected/not-ready need throws
  // NodeBlocked here, propagating to executor.js's NodeBlocked handling.
  const items = await Promise.resolve(node.selector(state, { needs: makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef }) }));
  if (!Array.isArray(items) || items.length === 0) {
    logger?.info?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: 0 items, skipping (childFlows)`);
    return { stateUpdate: {} };
  }

  // Skip the subflow-entry budget gate for a cascade-shape node (flow.kind:
  // "cascade" — code-then-agent-fallback migration, e.g. ENSURE_LOGIN/
  // SUBMIT/SEND_REPLY). A cascade's code leg frequently resolves the item for
  // free (edge routes to "__end__" without touching the agent leg), so gating
  // before that leg runs produces a false-positive hold. Real spend stays
  // covered: the agent leg, if reached, is independently gated by
  // dispatchAgentEntry's own maybeBudgetGate. A real multi-item childFlows
  // fan-out has no flow.kind, so it keeps its subflow-entry gate.
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

  // Pre-filter valid items up front so the batched phases work off a clean list.
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
  // (flowId, nodeId, subflowKey) via a unique index — never dependent on
  // this loop processing items serially; concurrent calls for different
  // identities have nothing to race over.
  //
  // node.dispatchGuard: optional async (item, subflowKey, { flowId }) => boolean,
  // for cross-flow dedup beyond what node.selector's own upstream filtering
  // guarantees — the selector's list is decided once per parent-flow visit,
  // so two parent flows racing through the same selector can both decide to
  // dispatch the same subflowKey before either commits. Threaded through to
  // findOrCreateInvocation (state/node-executions.js), which evaluates it
  // inside the same write-queue turn as the insert, so the second caller's
  // guard always sees the first caller's already-committed row.
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
    // dispatched on a prior visit (or is mid-flight/blocked on its own gate) —
    // re-triggering is redundant, not incorrect (runFlow is idempotent/
    // re-entrant), so skip.
    if (isNew) created.push({ ...v, childFlowId });
  }

  // Real atomic admission per new child via the shared combined pool. Each
  // admit() is one atomic DB transaction — count-then-claim inside a single
  // BEGIN IMMEDIATE, so concurrent callers admit or queue against the true
  // live count every time.
  //
  // node.concurrency (see header): true (default) participates in the shared
  // pool; false opts out entirely (unadmitted, always dispatched).
  //
  // Only "running" counts as occupying a slot — a child "blocked" on its own
  // human gate consumes nothing while it waits, so it must NOT occupy one;
  // state-store.js's setWaiting releases this admission the moment that
  // happens (triggerAdmissionRelease), else a gated child could starve the
  // pool for hours. Same rule for a ROOT flow's own admission (flow-start).
  const toQueue = [];
  const toDispatch = [];
  for (const c of created) {
    // Register the child's trigger edge (flow:<childFlowId> -> flow:<flowId>)
    // BEFORE scheduling its run, so the resolver walks straight through to the
    // parent's already-resolved root instead of dead-ending — without this,
    // cost attributed to the child's nodes collapses into an unlinked
    // "unknown" bucket. recordTrigger is async (resolves root, then upserts
    // session_lineage), so await it: runFlow scheduling below is setImmediate,
    // which fires next tick and would otherwise let the child's first node
    // resolve lineage before this edge lands.
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

  // Dispatch: setImmediate is already non-blocking fire-and-forget.
  for (const c of toDispatch) {
    setImmediate(() => {
      runFlow(c.childFlowId, "fresh").catch((err) => {
        logger?.warn?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: child ${c.childFlowId.slice(0, 8)} runFlow failed: ${err?.message ?? err}`);
      });
    });
  }

  // Queue: store.get()+store.setQueued() are awaited DB calls — batch them.
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
