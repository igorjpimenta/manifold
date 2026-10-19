// pipeline-graph.js — serialize a loaded pipeline definition into UI-safe JSON.
//
// The in-memory pipeline def holds function values (run/promptBuilder/apply for
// nodes; conditional edge resolvers). We strip those before shipping over HTTP
// — the browser only needs topology and labels. For conditional edges, we
// regex-scan the function's source for quoted node ids so the dashboard can
// render every possible branch instead of just one.

const NODE_ID_RE = /["'`]([A-Za-z_][A-Za-z0-9_]*|__end__|__start__)["'`]/g;

export function serializePipeline(pipeline) {
  if (!pipeline) return null;

  const nodeIds = new Set(Object.keys(pipeline.nodes ?? {}));
  const nodes = [
    { id: "__start__", kind: "start" },
    ...Object.entries(pipeline.nodes ?? {}).map(([id, node]) => ({
      id,
      kind: node.kind,
      ...(node.agent ? { agent: node.agent } : {}),
      ...(node.groupId ? { groupId: node.groupId } : {}),
      ...(node.concurrency ? { concurrency: node.concurrency } : {}),
      ...(node.waitKind ? { waitKind: node.waitKind } : {}),
      ...(node.kind === "subflow" && node.flow ? {
        flow: serializeFlow(node.flow, id, node.groupId ?? id),
      } : {}),
    })),
    { id: "__end__", kind: "end" },
  ];

  const edges = [];
  for (const [from, edge] of Object.entries(pipeline.edges ?? {})) {
    if (typeof edge === "string") {
      edges.push({ from, to: edge, conditional: false });
      continue;
    }
    if (typeof edge === "function") {
      const hints = pipeline.edgeHints?.[from];
      if (Array.isArray(hints) && hints.length > 0) {
        for (const to of hints) {
          edges.push({ from, to, conditional: true, via: null });
        }
      } else {
        const src = edge.toString();
        const targets = extractTargets(src, nodeIds);
        if (targets.length === 0) {
          edges.push({ from, to: "?", conditional: true, via: shorten(src) });
        } else {
          for (const to of targets) {
            edges.push({ from, to, conditional: true, via: shorten(src) });
          }
        }
      }
    }
  }

  return {
    id: pipeline.id,
    goal: pipeline.goal ?? null,
    file: pipeline._filepath ?? null,
    dir: pipeline._dir ?? null,
    nodes,
    edges,
    // Optional logical groupings the dashboard renders as compound parents
    // around their member nodes.
    groups: pipeline.groups ?? null,
    // Declared dashboard views — opt-in extra surfaces rendered by
    // the generic <DeclaredView> dispatcher on PipelineDetail. The
    // serializer ships the declarations themselves (not the computed
    // view data); the data comes from /api/stats/pipelines/:id/views.
    // Stored as `null` for pipelines that don't declare any so the
    // type stays stable across pipelines.
    views: Array.isArray(pipeline.views) && pipeline.views.length > 0
      ? pipeline.views
      : null,
  };
}

function extractTargets(src, knownIds) {
  const found = new Set();
  let m;
  NODE_ID_RE.lastIndex = 0;
  while ((m = NODE_ID_RE.exec(src)) !== null) {
    const id = m[1];
    if (id === "__start__" || id === "__end__" || knownIds.has(id)) found.add(id);
  }
  return [...found];
}

function shorten(src) {
  const oneLine = src.replace(/\s+/g, " ").trim();
  return oneLine.length > 200 ? oneLine.slice(0, 197) + "..." : oneLine;
}

/**
 * Serialize a subflow node's inner pipeline ("flow") so the dashboard can
 * render it as an inset graph. Shape mirrors the top-level serialization
 * but keeps the namespace local (no clash with parent node ids).
 */
function serializeFlow(flow, parentNodeId, groupId) {
  const flowNodeIds = new Set(Object.keys(flow.nodes ?? {}));
  const nodes = [
    { id: "__start__", kind: "start" },
    ...Object.entries(flow.nodes ?? {}).map(([id, node]) => ({
      id,
      kind: node.kind,
      ...(node.agent ? { agent: node.agent } : {}),
      ...(node.skill ? { skill: node.skill } : {}),
    })),
    { id: "__end__", kind: "end" },
  ];
  const edges = [];
  for (const [from, edge] of Object.entries(flow.edges ?? {})) {
    if (typeof edge === "string") {
      edges.push({ from, to: edge, conditional: false });
      continue;
    }
    if (typeof edge === "function") {
      const src = edge.toString();
      const targets = extractTargets(src, flowNodeIds);
      if (targets.length === 0) {
        edges.push({ from, to: "?", conditional: true, via: shorten(src) });
      } else {
        for (const to of targets) {
          edges.push({ from, to, conditional: true, via: shorten(src) });
        }
      }
    }
  }
  return { parentNodeId, groupId, nodes, edges };
}
