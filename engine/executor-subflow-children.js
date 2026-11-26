// executor-subflow-children.js — real child-flow dispatch for a
// `kind:"subflow"` node. Children are ordinary flow_runs driven by the same
// runFlow() as every other flow; this module only resolves items →
// spawn/find each child → checks whether the fan-out is done.
//
// node.concurrency is a boolean PARTICIPATION switch, not a per-node
// number: true (default) draws from the single shared admission pool
// (kind:"child-dispatch", same pool flow-start uses); false opts out
// entirely (unbounded, including any agent nodes its children run). The
// pool is deliberately unscoped — the resource it protects (host CPU/mem,
// shared sessions, workers) is shared by everything.

import { resolveOrCreateChild } from "./spawn-child.js";
import { areSiblingsAllTerminal } from "./child-resume-trigger.js";
import { makeNeedsCtx } from "./needs-resolver.js";

// Above the shared admission cap (default 8): this batch only bounds cheap
// row-bookkeeping, not the resource that cap protects; low enough to avoid
// a thundering-herd write burst against the one shared WAL file.
const DISPATCH_BATCH_SIZE = 10;

// Bounded-concurrency map, error-isolated per item: allSettled (not
// Promise.all) so one rejection can't affect its batch-mates or later
// batches.
async function runInBatches(items, batchSize, fn) {
  const results = new Array(items.length);
  for (let start = 0; start < items.length; start += batchSize) {
    const batch = items.slice(start, start + batchSize);
    const settled = await Promise.allSettled(batch.map((item, i) => fn(item, start + i)));
    for (let i = 0; i < settled.length; i++) results[start + i] = settled[i];
  }
  return results;
}

function deriveSubflowKey(node, item) {
  if (node.itemKey) return item?.[node.itemKey];
  return item?.id ?? item?.listingId ?? String(item);
}

// A child's initialState deliberately does NOT clone the parent's state —
// nodes inside the subflow that need an ancestor value declare it via
// `needs:`, resolved lazily at read time.
function buildChildInitialState(state, item, itemIndex) {
  // Captured before spawn: a single-item retry-cascade (selector returning
  // a placeholder like {id:"only"} for a code → recovery → fallback chain)
  // replaces `item` with that placeholder, so inner nodes reading
  // state.item.id for the real target would get the literal string —
  // parentItem keeps the real parent-branch object reachable. Pure
  // addition; a real fan-out's own item is unaffected.
  const parentItem = state?.item;
  return { item, itemIndex, parentItem };
}

export async function dispatchSubflowChildren({ node, nodeId, flowId, state, store, runStore, nodeExecutionsStore, maybeBudgetGate, runFlow, logger, api, lineage, pipelinesRef, admissionQueueStore }) {
  const _lineage = lineage ?? api?.runtime?.lineage ?? null;
  const items = await Promise.resolve(node.selector(state, { needs: makeNeedsCtx({ node, flowId, runStore, nodeExecutionsStore, pipelinesRef }) }));
  if (!Array.isArray(items) || items.length === 0) {
    logger?.info?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: 0 items, skipping (childFlows)`);
    return { stateUpdate: {} };
  }

  // Skip the subflow-entry budget gate for cascade-shape nodes: a cascade's
  // code leg often resolves the item for free (edge routes straight to
  // __end__ without reaching the agent leg), so gating here would
  // false-hold the common case. Real spend stays covered — the agent leg,
  // if reached, is gated by dispatchAgentEntry's own maybeBudgetGate.
  // Non-cascade fan-outs have no flow.kind and keep this gate.
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

  // Batched with bounded concurrency: resolveOrCreateChild's race-safety is
  // per-identity (flowId, nodeId, subflowKey) behind a unique index, so
  // concurrent calls for DIFFERENT identities have nothing to race over.
  //
  // node.dispatchGuard: optional async (item, subflowKey, { flowId }) =>
  // boolean, for cross-flow dedup beyond what node.selector's once-per-visit
  // filtering can guarantee — two parent flows can both decide to dispatch
  // the same subflowKey before either commits. Threaded to
  // findOrCreateInvocation, which evaluates it inside the same write-queue
  // turn as the insert, so the second caller's guard always sees the
  // first's already-committed row.
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
    // Only newly created children dispatch here — an existing one was
    // already dispatched on a prior visit (possibly mid-flight/blocked);
    // re-triggering would be redundant.
    if (isNew) created.push({ ...v, childFlowId });
  }

  // Atomic admission per new child: admit() is one BEGIN IMMEDIATE
  // transaction, so concurrent callers admit/queue against the true live
  // count. node.concurrency === false opts out (unadmitted, always
  // dispatched). Only "running" occupies a slot — a child blocked on a
  // human gate must not starve the pool for hours, so state-store's
  // setWaiting releases its admission the moment it blocks (same rule for
  // root flow-start admissions).
  const toQueue = [];
  const toDispatch = [];
  for (const c of created) {
    // Register the child's trigger edge BEFORE scheduling its run, and
    // await it: runFlow below is setImmediate, so without the await the
    // child's first node could resolve lineage before this edge lands and
    // its cost would collapse into an unlinked "unknown" bucket.
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

  for (const c of toDispatch) {
    setImmediate(() => {
      runFlow(c.childFlowId, "fresh").catch((err) => {
        logger?.warn?.(`manifold [${flowId.slice(0, 8)}] ${nodeId}: child ${c.childFlowId.slice(0, 8)} runFlow failed: ${err?.message ?? err}`);
      });
    });
  }

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
