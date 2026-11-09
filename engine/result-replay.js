// result-replay.js — the pure merge step shared by assembleState()
// (executor-helpers.js) and subflow-state.js's per-child `data` field.
//
// Split out on its own specifically to avoid a circular import:
// executor-helpers.js's assembleState() already imports getAllSubflowGroups
// from subflow-state.js, so subflow-state.js can't import assembleState()
// back — but it DOES need this exact same "replay a flow's own completed
// resultJson rows onto a base object" logic for its own `data` field
// (state/subflow-state.js's normalizeChildAsSubflowRow).
// No _subflowGroups nesting happens here — a child's OWN nested subflow
// groups aren't part of "this child's own top-level output"; a caller
// that needs those calls assembleState() on that child's flowId directly.
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
