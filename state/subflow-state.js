// subflow-state.js — read a flow's subflow data, grouped by groupId.
//
// A subflow node's items are real `flow_runs` children (one child per item,
// linked via parent_invocation_id — see engine/spawn-child.js). If
// `nodeExecutionsStore.listInvocationIdsForNode` finds ANY child-spawn
// invocation for a group, that group is treated as fully materialised (even
// if a child row is momentarily missing mid-creation) — a group's rows are
// never half old and half new, which would be more confusing than a narrow
// race window.
//
// Both functions return the nested shape callers are written against:
// `{ [subflowKey]: <row> }` / `{ [groupId]: { [subflowKey]: <row> } }`.
// See normalizeChildAsSubflowRow below for the row's shape.

import { replayResults } from "../engine/result-replay.js";

// Maps one flow_runs child row (+ its invocation) into the subflow row
// shape callers read. Fields with no equivalent yet (_display,
// activeSessionKey, per-subflow errors) default to empty/null.
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
    // invocation.nodeId, not a distinct groupId field (flow_node_invocations
    // has no group_id). Kept as the "groupId" key here to match the row
    // shape every caller of getAllSubflowGroups/getSubflowGroup expects —
    // group_id and node_id were always identical for a real subflow-child
    // invocation.
    groupId: invocation.nodeId,
    subflowKey: invocation.subflowKey,
    // 'blocked' folds into "running": a pending gate is signaled via
    // `awaiting` alone, not a distinct status value.
    // 'queued' is passed through unchanged — clients render it distinctly
    // (self-resolving admission wait, not a gate).
    status: child.status === "blocked" ? "running" : child.status,
    step: child.lastStep,
    startedAt: child.createdAt,
    updatedAt: child.updatedAt,
    doneAt: (child.status === "succeeded" || child.status === "skipped") ? child.endedAt : null,
    failedAt: child.status === "failed" ? child.endedAt : null,
    cancelledAt: child.status === "cancelled" ? child.endedAt : null,
    data: replayResults(child.stateJson ?? {}, results),
    errors: [],
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
    // The child's own flow_runs id, for callers that want to navigate to it
    // directly rather than treat it as an inline data blob.
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
  // pre-Milestone-9 sub-node timing rows that happen to share a node_id
  // with this "group" (a real risk since flow_node_invocations.group_id
  // was dropped — subflow_key IS NOT NULL alone can't distinguish this
  // specific case). Return null so the caller sees an empty group.
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
