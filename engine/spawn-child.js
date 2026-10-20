// spawn-child.js — Phase 2 branch/flow unification: race-safe identity +
// creation for a subflow-item child flow.

// resolveOrCreateChild finds or creates the flow_node_invocations row
// identifying "nodeId processing item subflowKey", then finds or creates
// the flow_runs child that invocation should own.
//
// Race safety comes entirely from findOrCreateInvocation's insert-then-
// reselect pattern (state/node-executions.js) backed by the invocation
// table's unique index — two concurrent callers for the same identity tuple
// always agree on one invocation id, so at most one of them ever wins the
// "does a child exist yet" check and creates one. That identity tuple
// includes flowId, though — it says nothing about two DIFFERENT parent
// flows independently deciding to dispatch the same subflowKey (e.g. two
// freelance_freelancer-proposal scans both fanning out SCORE for the same
// listing). `guard`, when supplied, is threaded straight through to
// findOrCreateInvocation to close exactly that gap — see its own comment
// for why running it inside that call's write-queue turn makes it
// race-safe across flowIds, not just within one.
export async function resolveOrCreateChild({
  flowId,
  pipelineId = null,
  nodeId,
  subflowKey,
  childPipelineId,
  initialState,
  store,
  runStore,
  nodeExecutionsStore,
  guard,
}) {
  const invocationId = await nodeExecutionsStore.findOrCreateInvocation({
    flowId, pipelineId, nodeId, subflowKey, guard,
  });
  if (invocationId == null) {
    return { invocationId: null, childFlowId: null, created: false, guarded: true };
  }

  const existing = await runStore.getByParentInvocationId(invocationId);
  if (existing) {
    return { invocationId, childFlowId: existing.flowId, created: false };
  }

  const child = await store.create({
    pipelineId: childPipelineId,
    initialState,
    parentInvocationId: invocationId,
  });
  // Re-query to find the actual winner of the concurrent-insert race.
  // With async clients, multiple callers can pass the existence check
  // before any of them persists, so only the one that won the UNIQUE
  // constraint has a real row. The winner is always findable via the index.
  const winner = await runStore.getByParentInvocationId(invocationId);
  return { invocationId, childFlowId: winner?.flowId ?? child.flowId, created: winner?.flowId === child.flowId };
}
