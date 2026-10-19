// subflow-state.js — read branch/subflow data for a flow, grouped by groupId.
//
// Dual-mode (Phase 2):
// a subflow node's items may live as `flow_branches` rows (old model) OR
// as real `flow_runs` children (new model, one child per item, linked via
// parent_invocation_id — see engine/spawn-child.js). Per group, exactly one
// model applies — a cutover takes a whole subflow node at once, never
// mixing sources within one group. If
// `nodeExecutionsStore.listInvocationIdsForNode` finds ANY child-spawn
// invocation for a group, that group is treated as fully migrated (even
// if a child row is momentarily missing mid-creation) — old and new data
// are never blended for the same group, which would be more confusing
// than a narrow race window.
//
// Both functions still return the same `{ [subflowKey]: <row> } `/
// `{ [groupId]: { [subflowKey]: <row> } }` nested shape pre-Phase-2 callers
// were already written against (field renamed branchKey -> subflowKey —
// the old-model path below is normalized to the same field name so every
// caller sees one consistent shape regardless of which model actually
// answered) — see normalizeChildAsSubflowRow below for the new-model
// row's shape, kept as close to branchService's mapBranchRow as the two
// models allow.

import { replayResults } from "../engine/result-replay.js";

// Maps one flow_runs child row (+ its invocation) into the old
// flow_branches row shape. Fields with no clean new-model equivalent yet
// (_display, activeSessionKey, per-subflow errors) default to empty/null —
// real values arrive per-pipeline as each one's cutover lands and a real
// caller demonstrates it needs them.
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
    // invocation.nodeId, not a distinct groupId field (dropped from
    // flow_node_invocations once every subflow node became childFlows-
    // implicit). Kept as the "groupId" key here to match the old-model
    // row shape every caller of getAllSubflowGroups/getSubflowGroup already
    // expects — group_id and node_id were always identical for a real
    // subflow-child invocation, so this is not a behavior change.
    groupId: invocation.nodeId,
    subflowKey: invocation.subflowKey,
    // 'blocked' folds into "running" to match the old model, where status
    // stayed 'running' for the whole branch lifetime and a pending gate
    // was signaled via `awaiting` alone, not a distinct status value.
    // 'queued' is passed through unchanged (Phase 4) — it has no old-model
    // analogue to preserve, and iOS's StatusPill/BranchDetailModel already
    // render it distinctly (self-resolving internal-resource wait, not a
    // gate).
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
    // RunSummary/RunDetail.statusReason) was already reaching iOS;
    // per-branch rows never carried it, which is why a queued child's
    // reason (e.g. "waiting for cross-process lock browser:linkedin")
    // had nowhere to surface once `awaiting` was reserved for gate waits.
    statusReason: child.statusReason ?? null,
    _display: null,
    activeSessionKey: null,
    // Additive — not on the old shape. The new-model row's own flow_runs
    // id, for callers that want to navigate to it directly rather than
    // treat it as an inline data blob.
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
