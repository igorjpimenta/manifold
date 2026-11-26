// spawn-child.js — race-safe identity + creation for a subflow-item child flow.

// Race safety rests on findOrCreateInvocation's insert-then-reselect backed
// by the invocation table's unique index: concurrent callers for one
// identity tuple agree on one invocation id, so only one ever creates a
// child. That tuple includes flowId, so it can't stop two DIFFERENT parent
// flows dispatching the same subflowKey — `guard` closes that gap inside
// findOrCreateInvocation's write-queue turn, race-safe across flowIds.
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
  // Re-query: with async clients several callers can pass the existence
  // check before anyone persists, so only the UNIQUE-constraint winner has
  // a real row — and that winner is always findable via the index.
  const winner = await runStore.getByParentInvocationId(invocationId);
  return { invocationId, childFlowId: winner?.flowId ?? child.flowId, created: winner?.flowId === child.flowId };
}
