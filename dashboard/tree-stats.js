// Small, dependency-free home for stats derived from the GET
// /manifold/runs/:flowId/tree response shape (routes/runs.js's
// buildTreeNodeRows) — kept out of server.js so it stays importable by unit
// tests without pulling in device.js's `openclaw` package dependency.

// deriveSystemSummary's stepsExecuted. Sum of executionCount across every
// top-level invocation row, not tree.length — a node re-entered via
// retry/recheck gets more than one execution under the SAME invocation row.
export function stepsExecutedFromTree(tree) {
  if (!Array.isArray(tree)) return 0;
  return tree.reduce((sum, row) => sum + (row?.executionCount ?? 0), 0);
}
