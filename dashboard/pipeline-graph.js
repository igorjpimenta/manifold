// pipeline-graph.js — serialize a loaded pipeline definition into UI-safe JSON.
// Function values (run/promptBuilder/apply, edge resolvers) are stripped before
// shipping; conditional edges are regex-scanned for quoted node ids so the
// dashboard renders every possible branch.

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
    groups: pipeline.groups ?? null,
    // Declarations only — computed data comes from
    // /api/stats/pipelines/:id/views. null when none, so the type is stable.
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
