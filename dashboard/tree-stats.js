// Stats over the GET /manifold/runs/:flowId/tree row shape, split out of
// server.js so unit tests can import them without device.js's `openclaw`
// dependency.

// Sum of executionCount across top-level invocation rows — not tree.length:
// a node re-entered via retry/recheck shares one invocation row.
export function stepsExecutedFromTree(tree) {
  if (!Array.isArray(tree)) return 0;
  return tree.reduce((sum, row) => sum + (row?.executionCount ?? 0), 0);
}
