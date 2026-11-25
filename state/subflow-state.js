// subflow-state.js — read a flow's subflow children grouped by groupId.
//
// A subflow node's items are real `flow_runs` children (one child per
// item, linked via parent_invocation_id — see engine/spawn-child.js).
// Both functions return the `{ [subflowKey]: <row> }` /
// `{ [groupId]: { [subflowKey]: <row> } }` nested shape every caller
// (routes/runs.js, dashboard, engine assembleState) reads — the row
// shape is defined in normalizeChildAsSubflowRow below.

import { replayResults } from "../engine/result-replay.js";

// Maps one flow_runs child row (+ its invocation) into the grouped-row
// shape callers read. Fields with no direct equivalent (_display,
// activeSessionKey) default to null — real values arrive per-pipeline as
// a caller demonstrates it needs them.
//
// `data`: replays that child's own completed resultJson rows onto its
// entry-only stateJson, same merge engine-helpers.js's assembleState()
// uses for the top-level case — imported from result-replay.js
// specifically to avoid a circular import (assembleState() itself calls
// getAllSubflowGroups, defined in this file). Async; callers below already
// run inside an async loop.
async function normalizeChildAsSubflowRow(child, invocation, nodeExecutionsStore) {
  const isBlockedOnGate = child.status === "blocked" && child.waitJson != null;
  const results = (await nodeExecutionsStore?.listResultsForFlow?.(child.flowId)) ?? [];
  return {
    // invocation.nodeId, not a distinct groupId field. Kept as the
    // "groupId" key because every caller of
    // getAllSubflowGroups/getSubflowGroup addresses groups by node id —
    // group_id and node_id are always identical for a real
    // subflow-child invocation.
    groupId: invocation.nodeId,
    subflowKey: invocation.subflowKey,
    // 'blocked' folds into "running": a pending gate is signaled via
    // `awaiting` alone, and clients render `awaiting` distinctly rather
    // than a status value. 'queued' passes through unchanged — clients
    // already render it distinctly (self-resolving internal-resource
    // wait, not a gate).
    status: child.status === "blocked" ? "running" : child.status,
    step: child.lastStep,
    startedAt: child.createdAt,
    updatedAt: child.updatedAt,
    doneAt: (child.status === "succeeded" || child.status === "skipped") ? child.endedAt : null,
    failedAt: child.status === "failed" ? child.endedAt : null,
    cancelledAt: child.status === "cancelled" ? child.endedAt : null,
    data: replayResults(child.stateJson ?? {}, results),
    awaiting: isBlockedOnGate
      ? { requestPayload: child.waitJson, requestedAt: child.updatedAt, subNodeId: child.currentStep ?? null }
      : null,
    // Additive — the run-level equivalent of this field (routes/runs.js's
    // RunSummary/RunDetail.statusReason) already reaches clients;
    // per-branch rows never carried it, which is why a queued child's
    // reason (e.g. "queued for admission")
    // had nowhere to surface once `awaiting` was reserved for gate waits.
    statusReason: child.statusReason ?? null,
    _display: null,
    activeSessionKey: null,
    // Additive — the row's own flow_runs id, for callers that want to
    // navigate to it directly rather than treat it as an inline data
    // blob.
    flowId: child.flowId,
  };
}


// Returns the child-model group as `{ [subflowKey]: row }`, or null if this
// group has no real children yet.
async function getMigratedLoopGroup({ flowId, groupId, runStore, nodeExecutionsStore }) {
  // groupId here is always a subflow node's own id (see the field comment in
  // normalizeChildAsSubflowRow above) — listInvocationIdsForNode takes a
  // nodeId param; passing groupId straight through is correct.
  const invocationIds = (await nodeExecutionsStore?.listInvocationIdsForNode?.(flowId, groupId)) ?? [];
  if (invocationIds.length === 0) return null;

  const children = (await runStore?.listForParentInvocations?.(invocationIds)) ?? [];
  // A genuine childFlows subflow-child invocation always has a matching
  // flow_runs child — dispatchLoopChildren never creates an invocation
  // without also spawning one (engine/executor-subflow-children.js). If
  // invocationIds is non-empty but children comes back empty, these
  // invocations aren't real subflow children at all — they're historical
  // timing rows that happen to share a node_id with this "group"
  // (flow_node_invocations has no group_id, and subflow_key IS NOT NULL
  // alone can't distinguish this specific case). Return null
  // so the caller sees an empty group.
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
