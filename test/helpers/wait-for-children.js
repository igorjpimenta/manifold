
const DEFAULT_TIMEOUT_MS = 10_000;
const POLL_INTERVAL_MS = 10;


async function poll(predicate, description, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await predicate();
    if (value) return value;
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${description}`);
    }
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

export async function waitForChildFlowIds(
  nodeExecutionsStore,
  runStore,
  parentFlowId,
  nodeId,
  expectedCount,
  timeoutMs = DEFAULT_TIMEOUT_MS,
) {
  return poll(async () => {
    const invocationIds = await nodeExecutionsStore.listInvocationIdsForNode(parentFlowId, nodeId);
    const children = await runStore.listForParentInvocations(invocationIds);
    return children.length >= expectedCount ? children.map((c) => c.flowId) : null;
  }, `${parentFlowId} to spawn ${expectedCount} ${nodeId} child flow(s)`);
}

export async function waitForRunRow(runStore, flowId, predicate, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return poll(async () => {
    const row = await runStore.get(flowId);
    return row && predicate(row) ? row : null;
  }, `run store row ${flowId} to match`, timeoutMs);
}


export function waitForTerminalStatus(runStore, flowId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return waitForRunRow(
    runStore,
    flowId,
    (row) => ["succeeded", "failed", "skipped", "cancelled"].includes(row.status),
    timeoutMs,
  );
}
