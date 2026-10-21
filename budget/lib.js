// Pure logic extracted from budget/index.js for unit testing.
// No openclaw SDK or SQLite imports — safe to require in Node --test.

export function deserialize(row) {
  return {
    id:         row.id,
    label:      row.label,
    agent:      row.agent,
    agentEmoji: row.agent_emoji,
    trigger:    row.trigger,
    costLow:    row.cost_low,
    costHigh:   row.cost_high,
    brief:      row.brief ?? null,
    modelChain:     row.model_chain ? JSON.parse(row.model_chain) : [],
    pipeline:       row.pipeline ?? null,
    node:           row.node ?? null,
    tokenEstimate:  row.token_estimate ?? null,
    status:         row.status,
    createdAt:  row.created_at,
    expiresAt:  row.expires_at ?? null,
    decidedAt:  row.decided_at ?? null,
    decision:   row.decision ?? null,
    pipelineId:   row.pipeline_id ?? null,
    pipelineArgs: row.pipeline_args ? JSON.parse(row.pipeline_args) : null,
    flowId:       row.flow_id ?? null,
    kind:         row.kind ?? "node",
  };
}

// Canonical day source for the budget gate. Local, not UTC: the gate is a
// human control, so an operator locking spend at 23:30 means locked through
// their own midnight.
//
// Every gate_state read and write must derive the day from here. A reader on
// a different clock than the writer fails open, and the two only diverge
// during part of the day, so the mismatch is invisible to a daytime test run.
export function todayLocal(now = new Date()) {
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
}

export function isGateLocked(row, nowIso) {
  if (!row) return false;
  const today = nowIso ?? todayLocal();
  return Boolean(row.locked && row.day >= today);
}

export function resolveNodeDecision(row) {
  if (!row) return null;
  if (row.status === "pending") return "pending";
  if (row.decision === "approve") return "approve";
  if (row.decision === "reject") return "reject";
  return null;
}

export function classifyDecisionStatus(decision) {
  if (decision === "approve") return "approved";
  if (decision === "reject")  return "rejected";
  return null;
}
