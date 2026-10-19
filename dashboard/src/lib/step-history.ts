/** Build a Set<"from→to"> of edges that were actually traversed, given an
 *  ordered list of visited node ids (GraphView derives this from GET
 *  /pipeclaw/runs/:flowId/tree's items). Walks adjacent pairs in the
 *  history; the first transition is always __start__ → history[0]. */
export function buildTracedEdges(history: string[] | null | undefined): Set<string> {
  const edges = new Set<string>();
  if (!history || history.length === 0) return edges;
  edges.add(`__start__→${history[0]}`);
  for (let i = 1; i < history.length; i++) {
    edges.add(`${history[i - 1]}→${history[i]}`);
  }
  return edges;
}
