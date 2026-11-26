// result-replay.js — the pure merge step shared by assembleState()
// (executor-helpers.js) and subflow-state.js's per-child `data` field.
// Split into its own module to avoid a circular import: assembleState
// already imports getAllSubflowGroups from subflow-state.js, so
// subflow-state.js can't import assembleState back. No _subflowGroups
// nesting happens here — callers needing a child's own nested groups call
// assembleState() on that child's flowId.
export function replayResults(baseState, results) {
  let state = { ...baseState };
  for (const r of results ?? []) {
    if (r.resultWrittenAt == null) continue;
    let update;
    try { update = JSON.parse(r.resultJson ?? "null"); } catch { continue; }
    if (update && typeof update === "object" && !Array.isArray(update)) {
      state = { ...state, ...update };
    }
  }
  return state;
}
