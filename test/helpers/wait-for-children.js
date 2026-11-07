// wait-for-children.js — wait for the child flows a parent spawns, and for
// those children to reach a terminal state.
//
// The subflow-children e2e suites used a fixed sleep (`flush()`) between
// `runFlow()` and the assertions about the spawned children. That sleep is
// short on a loaded CI runner — the assertion reads the run store before the
// child's row exists, or before its status was written — and long elsewhere.
// Bumping the constant only moves the threshold.
//
// These helpers poll the same run store the assertions read, so the wait and
// the assertion can never disagree about what "done" means.

const DEFAULT_TIMEOUT_MS = 10_000;
const POLL_INTERVAL_MS = 10;

/** Poll `predicate` until it returns a truthy value, or fail naming the condition. */
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

/**
 * The child flow ids a parent spawned for `nodeId`, waiting until
 * `expectedCount` of them exist.
 *
 * The invocation row and the child's own run row are written by the dispatch
 * that follows the parent's step, so reading them right after `runFlow()`
 * can race that write.
 */
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

/**
 * The run store row for `flowId`, once it matches `predicate`.
 *
 * Polls the store the assertions read, so a status the row has not caught up
 * with yet is never mistaken for a terminal one.
 */
export async function waitForRunRow(runStore, flowId, predicate, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return poll(async () => {
    const row = await runStore.get(flowId);
    return row && predicate(row) ? row : null;
  }, `run store row ${flowId} to match`, timeoutMs);
}

/** The run store row for `flowId`, once its status is terminal. */
export function waitForTerminalStatus(runStore, flowId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return waitForRunRow(
    runStore,
    flowId,
    (row) => ["succeeded", "failed", "skipped", "cancelled"].includes(row.status),
    timeoutMs,
  );
}
