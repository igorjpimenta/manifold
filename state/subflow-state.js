// subflow-state.js — read a flow's subflow children grouped by groupId.
// A subflow node's items are real `flow_runs` children (one child per
// item, linked via parent_invocation_id).

import { replayResults } from "../engine/result-replay.js";

async function normalizeChildAsSubflowRow(child, invocation, nodeExecutionsStore) {
  const isBlockedOnGate = child.status === "blocked" && child.waitJson != null;
  const results = (await nodeExecutionsStore?.listResultsForFlow?.(child.flowId)) ?? [];
  return {
    // invocation.nodeId, not a distinct groupId field — every caller
    // addresses groups by node id, and group_id and node_id are always
    // identical for a real subflow-child invocation.
    groupId: invocation.nodeId,
    subflowKey: invocation.subflowKey,
    // 'blocked' folds into "running": a pending gate is signaled via
    // `awaiting` alone. 'queued' passes through unchanged — clients
    // render it distinctly (self-resolving internal-resource wait).
    status: child.status === "blocked" ? "running" : child.status,
    step: child.lastStep,
    startedAt: child.createdAt,
    updatedAt: child.updatedAt,
    doneAt: (child.status === "succeeded" || child.status === "skipped") ? child.endedAt : null,
    failedAt: child.status === "failed" ? child.endedAt : null,
    cancelledAt: child.status === "cancelled" ? child.endedAt : null,
    // Replays the child's completed resultJson rows onto its entry-only
    // stateJson, same merge assembleState() uses for the top-level case —
    // imported from result-replay.js specifically to avoid a circular
    // import (assembleState() itself calls getAllSubflowGroups).
    data: replayResults(child.stateJson ?? {}, results),
    awaiting: isBlockedOnGate
      ? { requestPayload: child.waitJson, requestedAt: child.updatedAt, subNodeId: child.currentStep ?? null }
      : null,
    // Additive: per-branch rows never carried a reason, and `awaiting` is
    // reserved for gate waits — a queued child's reason (e.g. "queued for
    // admission") had nowhere else to surface.
    statusReason: child.statusReason ?? null,
    _display: null,
    activeSessionKey: null,
    flowId: child.flowId,
  };
}


async function getMigratedLoopGroup({ flowId, groupId, runStore, nodeExecutionsStore }) {
  const invocationIds = (await nodeExecutionsStore?.listInvocationIdsForNode?.(flowId, groupId)) ?? [];
  if (invocationIds.length === 0) return null;

  const children = (await runStore?.listForParentInvocations?.(invocationIds)) ?? [];
  // A genuine subflow-child invocation always has a matching flow_runs
  // child (dispatchLoopChildren never creates one without spawning the
  // other). Non-empty invocations with no children are historical timing
  // rows that merely share a node_id — return null so the caller sees an
  // empty group.
  if (children.length === 0) return null;
  const group = {};
  for (const child of children) {
    const invocation = await nodeExecutionsStore.getInvocation(child.parentInvocationId);
    if (!invocation?.subflowKey) continue;
    group[invocation.subflowKey] = await normalizeChildAsSubflowRow(child, invocation, nodeExecutionsStore);
  }
  return group;
}

export async function getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore } = {}) {
  const groups = {};
  const groupIds = (await nodeExecutionsStore?.listSubflowNodeIdsForFlow?.(flowId)) ?? [];
  for (const groupId of groupIds) {
    const migrated = await getMigratedLoopGroup({ flowId, groupId, runStore, nodeExecutionsStore });
    groups[groupId] = migrated ?? {};
  }
  return groups;
}

export async function getSubflowGroup({ flowId, groupId, runStore, nodeExecutionsStore } = {}) {
  const migrated = await getMigratedLoopGroup({ flowId, groupId, runStore, nodeExecutionsStore });
  return migrated ?? {};
}
