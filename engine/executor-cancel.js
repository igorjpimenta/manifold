// Shared cancel-completion logic for executor.cancel() and flow-revival.js.
//
// Precondition: cancelRequestedAt is already written on the flow — this
// completes a cancellation, it does not request one.
//
// Cancel does NOT interrupt a kind:"agent" node mid-dispatch: only the
// status flips, the in-flight LLM call runs to natural completion, and its
// result is discarded by a harmless CAS rejection against the
// already-terminal flow.

import { getNodeExecutionsStore } from "../state/node-executions.js";

const TERMINAL_STATUSES = new Set(["succeeded", "failed", "cancelled", "skipped"]);

// Recursively cancels every non-terminal child of every childFlows subflow
// node this flow has children under — without it, cancelling a parent
// would leave live children (and their in-flight agent sessions) running.
async function cascadeCancelChildren({ store, flowId, cfg, logger, nodeExecutions, runStore }) {
  if (!runStore || !nodeExecutions) return;
  let subflowNodeIds;
  try {
    subflowNodeIds = await nodeExecutions.listSubflowNodeIdsForFlow(flowId);
  } catch {
    return;
  }
  if (!subflowNodeIds || subflowNodeIds.length === 0) return;

  // Every cascaded child has exactly one unambiguous cause, so its reason
  // is always worth recording rather than left null.
  const childReason = `parent flow ${flowId.slice(0, 8)} was cancelled`;

  for (const nodeId of subflowNodeIds) {
    const invocationIds = await nodeExecutions.listInvocationIdsForNode(flowId, nodeId);
    const children = await runStore.listForParentInvocations(invocationIds);
    for (const child of children) {
      if (TERMINAL_STATUSES.has(child.status)) continue;
      try {
        await store.requestCancel({ flowId: child.flowId });
        await completeCancellation({
          store, flowId: child.flowId, stateJson: child.stateJson,
          lastStep: child.lastStep, revision: child.revision, cfg, logger,
          nodeExecutionsStore: nodeExecutions, runStore, reason: childReason,
        });
      } catch (err) {
        logger?.warn?.(`completeCancellation: cascade-cancel of child [${child.flowId.slice(0, 8)}] failed: ${err?.message ?? err}`);
      }
    }
  }
}

export async function completeCancellation({ store, flowId, stateJson, lastStep, revision, cfg = {}, logger = null, nodeExecutionsStore, runStore, reason = null }) {
  const nodeExecutions = nodeExecutionsStore ?? getNodeExecutionsStore();

  // Best-effort: a cascade failure (or a caller that passes no runStore)
  // must not block this flow's own terminal status flip below.
  try {
    await cascadeCancelChildren({ store, flowId, cfg, logger, nodeExecutions, runStore });
  } catch (err) {
    logger?.warn?.(`completeCancellation: cascadeCancelChildren(${flowId.slice(0, 8)}) failed: ${err?.message ?? err}`);
  }

  const result = await store.cancel({ flowId, cfg, reason });
  return result;
}
