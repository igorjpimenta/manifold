// Owner kind "invocation" for the cross-process lock (lock.js): a lock
// held on behalf of one node invocation stays held until that invocation is
// resolved, however long it runs. Resolved means either:
//   - flow_node_invocations.result_written_at is set (the node finished and
//     wrote its result — set even for a genuinely null result), or
//   - the invocation's flow has ended (ended_at set: succeeded, failed,
//     skipped, cancelled) — covers a node that failed without a result.
// A store read that fails, or finds no row, counts as unresolved: waiting
// longer is recoverable, taking a live holder's lock is not. A holder whose
// process died is reaped by lock.js's PID check before this is ever asked.

export function createInvocationOwnerResolver({ nodeExecutionsStore, runStore }) {
  return {
    async isResolved(holder) {
      const invocationId = holder?.owner?.id;
      if (!invocationId) return false;
      const invocation = await nodeExecutionsStore.getInvocation(invocationId);
      if (!invocation) return false;
      if (invocation.resultWrittenAt != null) return true;
      const run = await runStore.get(invocation.flowId);
      return run?.endedAt != null;
    },
    describe(owner) {
      const node = owner?.nodeId ?? "node";
      const flow = owner?.flowId ? `flow ${String(owner.flowId).slice(0, 8)}, ` : "";
      return `${node} (${flow}invocation ${String(owner?.id ?? "?").slice(0, 8)})`;
    },
  };
}
