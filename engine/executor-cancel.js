// Shared cancel-completion logic for both executor.cancel() and flow-revival.js.
//
// Precondition: cancelRequestedAt is already written on the flow (step 1 done).
//
// Sequence:
//   step 2d — cascade-cancel non-terminal childFlows children (Phase 2),
//             recursive: a child's own childFlows subflow cascades too.
//   step 3  — store.cancel() → writes status='cancelled' to DB
//
// KNOWN GAP (not fixed here — CANCEL_SESSION_ABORT.design.md): neither step
// actually interrupts a `kind:"agent"` node that's currently mid-dispatch,
// root or subflow. Cancel only flips `flow_runs.status`; the in-flight LLM
// call keeps running to natural completion and its result is silently
// discarded once it tries to persist against an already-terminal flow (a
// harmless CAS rejection, no corruption — but no abort, no record of what
// the agent did in the meantime either). See the design doc above for what
// a real fix would need.

import { getNodeExecutionsStore } from "../state/node-executions.js";

const TERMINAL_STATUSES = new Set(["succeeded", "failed", "cancelled", "skipped"]);

// Phase 2: find every non-terminal childFlows child of `flowId` and cancel
// each one — recursively, so a child that itself has a childFlows subflow
// cascades further down. Without this, cancelling a parent would leave live
// children (and any in-flight agent session they're running — e.g. an
// actual outbound send) completely untouched. Walks every subflow node this
// flow has real children under
// (nodeExecutionsStore.listSubflowNodeIdsForFlow), not just one — a flow
// can have more than one childFlows subflow node.
async function cascadeCancelChildren({ store, flowId, cfg, logger, nodeExecutions, runStore }) {
  if (!runStore || !nodeExecutions) return;
  let subflowNodeIds;
  try {
    subflowNodeIds = await nodeExecutions.listSubflowNodeIdsForFlow(flowId);
  } catch {
    return;
  }
  if (!subflowNodeIds || subflowNodeIds.length === 0) return;

  // The reason is unambiguous here (unlike the top-level cancel, where the
  // caller may or may not have one) — every cascaded child is cancelled for
  // exactly one cause, so it's always worth recording rather than left NULL.
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
  // Injectable for tests (defaults to the real singleton in production) —
  // same DI pattern as engine/executor.js.
  const nodeExecutions = nodeExecutionsStore ?? getNodeExecutionsStore();

  // Step 2d: cascade-cancel non-terminal childFlows children, if any — see
  // cascadeCancelChildren's own comment. Best-effort: a failure here (or a
  // caller that doesn't pass runStore, e.g. older call sites) shouldn't
  // block this flow's own terminal status flip (step 3) below.
  try {
    await cascadeCancelChildren({ store, flowId, cfg, logger, nodeExecutions, runStore });
  } catch (err) {
    logger?.warn?.(`completeCancellation: cascadeCancelChildren(${flowId.slice(0, 8)}) failed: ${err?.message ?? err}`);
  }

  // Step 3: flip flow status to 'cancelled'.
  const result = await store.cancel({ flowId, cfg, reason });
  return result;
}
