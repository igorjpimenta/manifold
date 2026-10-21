// run-patch.js — pure RunSummary-shaped patch builders for the flow-state
// WS events (manifold.run.started/updated/ended payloads).
//
// Each function takes already-fetched data (no async DB calls inside) so the
// caller can batch DB work and reuse results across multiple items in one
// event tick.
export function prettifyPipelineId(pipelineId) {
  if (typeof pipelineId !== "string" || !pipelineId) return "Pipeline";
  return pipelineId.replace(/^([a-z]+)_(.+)$/, "$1: $2").replace(/-/g, " ");
}

export function pipelineMetaFor(pipelineId, { pipelineLookup } = {}) {
  const declared = (pipelineLookup && pipelineLookup(pipelineId)) || {};
  return {
    id: pipelineId,
    displayName: declared.displayName ?? prettifyPipelineId(pipelineId),
    emoji:       declared.emoji       ?? "📊",
    sfSymbol:    declared.sfSymbol    ?? "tray.fill",
  };
}

// Builds a RunSummary-shaped patch from a flow row (flowId, stateJson,
// waitJson, lastStep, status, createdAt/updatedAt/endedAt).
export function buildRunPatch(flow, now, { pipelineLookup } = {}) {
  const state = flow.stateJson ?? {};
  const display = state._display ?? {};
  const pipelineId = state.pipelineId ?? null;
  const pipelineMeta = pipelineId ? pipelineMetaFor(pipelineId, { pipelineLookup }) : null;
  return {
    flowId: flow.flowId,
    pipelineId,
    status: flow.status,
    updatedAtMs: flow.updatedAt ?? now,
    endedAtMs: flow.endedAt ?? null,
    createdAtMs: flow.createdAt ?? null,
    statusReason: flow.waitJson?.summary ?? null,
    displayTitle: display.title ?? null,
    displayMetrics: Array.isArray(display.metrics) ? display.metrics : null,
    currentNodeId: flow.lastStep ?? null,
    pipelineDisplayName: pipelineMeta?.displayName ?? null,
    pipelineEmoji: pipelineMeta?.emoji ?? null,
  };
}
