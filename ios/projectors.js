// Projectors — pure functions that map internal openclaw objects (pipeclaw
// flows, agent sessions, etc.) into the Live Activity payload shapes defined
// in CONTRACT.md.
//
// Each projector returns a `start`, `update`, or `end` message ready for the
// relay. The synthetic helpers (`synth*`) generate fixture messages used by
// the test HTTP endpoint (`/pipeclaw/ios/test/emit`).

function nowMs() { return Date.now(); }

// ─── AgentApproval ──────────────────────────────────────────────────────────

// Composite activityId convention:
//   needs-info block      → "agent-approval:<flowId>"        (legacy — no
//                            live pipeline can produce a needs-info wait
//                            anymore; itemId always empty on new activities)
//   gate item    → "agent-approval:<flowId>:<itemId>"
// The intent router (intents.js) parses both forms; iOS reconstructs
// the same string from {flowId, itemId} when firing Approve/Deny so the
// gateway can correlate back to the originating activity.
function agentApprovalActivityId(flowId, itemId) {
  return itemId ? `agent-approval:${flowId}:${itemId}` : `agent-approval:${flowId}`;
}

export function startAgentApproval({ flow, agentMeta, pipelineMeta }) {
  const itemId = typeof flow.itemId === "string" && flow.itemId ? flow.itemId : "";
  return {
    type: "live_activity.start",
    activityId: agentApprovalActivityId(flow.flowId, itemId),
    activityType: "AgentApproval",
    // Prefer when the flow actually entered its waiting state, so the
    // iPhone's "Nm ago" reflects real age rather than "just now"
    // every time it polls /live-activity/state. Fall back to nowMs
    // for WS-pushed starts where the caller didn't thread it
    // through (broadcast call sites without flow context).
    emittedAtMs: flow.blockedAtMs ?? nowMs(),
    attributes: {
      flowId: flow.flowId,
      itemId,
      // This item's own real flow_runs.flow_id when it's a subflow item. Null
      // for root-level (non-subflow) gates and legacy flow_branches groups.
      childFlowId: flow.childFlowId ?? null,
      // The subflow group this item belongs to (dual-mode-safe — see
      // state/subflow-state.js's getAllSubflowGroups), e.g. "PRIME_FILL"
      // for a batch of <TBD> fills. Null for gates that aren't subflow
      // items. Lets iOS cluster sibling items into one deck instead of
      // treating them as unrelated Inbox rows.
      groupId: flow.groupId ?? null,
      pipelineId: pipelineMeta?.id ?? "unknown",
      pipelineDisplayName: pipelineMeta?.displayName ?? pipelineMeta?.id ?? "unknown",
      pipelineEmoji: pipelineMeta?.emoji ?? "📊",
      pipelineSfSymbol: pipelineMeta?.sfSymbol ?? "tray.fill",
      agentEmoji: agentMeta?.emoji ?? "🤖",
      agentName: agentMeta?.name ?? "Agent",
    },
    contentState: {
      nodeId: flow.nodeId ?? null,
      reason: flow.reason ?? "needs input",
      needed: flow.needed ?? null,
      hint: flow.hint ?? null,
      // Per-gate discuss opt-in (V2). discussSupported=true means the
      // pipeline's gate node declared a `discuss: { agent, ... }` block;
      // iOS renders the Discuss link only when true. discussAgent is
      // the human-readable label of the chat agent (Hermes / Herculano
      // / etc) — distinct from the AgentApproval attributes' agentName
      // (which reflects the flow's controller, not necessarily the
      // chat target).
      discussSupported: flow.discussSupported === true,
      discussAgent: typeof flow.discussAgent === "string" ? flow.discussAgent : null,
      // True when the operator has already opened the discuss chat for
      // this gate (file-existence probe in poller-helpers.js). iOS uses this
      // to lift the gate into the Inbox's "Open conversations"
      // section, separate from the pipeline-grouped list.
      discussActive: flow.discussActive === true,
      // Mtime of the discuss session JSONL — proxy for "last activity
      // on this conversation" (operator send or agent reply, whichever
      // was latest). iOS prefers this for the ConversationCard age
      // label so the displayed time reflects the chat, not when the
      // gate first fired. Null when no session exists yet.
      lastDiscussMs: typeof flow.lastDiscussMs === "number" ? flow.lastDiscussMs : null,
      // True when the pending-store sidecar exists for this gate —
      // Cyrano proposed something (body and/or frontmatter) that the
      // operator hasn't applied yet. Drives the "Apply pending" badge
      // on the conversation card so the operator knows there's an
      // actionable proposal waiting, not just a chat history to re-read.
      applyPending: flow.applyPending === true,
      // Ordered list of typed renderable slots — the only thing iOS
      // iterates to draw a gate. Every gate declares this itself via
      // approvalRequestBuilder's returned `layout` (engine/executor-
      // helpers.js's validateGateLayout refuses to persist a waitJson
      // without it). Always an array, never null — iOS treats absent /
      // empty as "nothing to render". The `?? []` here only guards
      // pre-existing open gates from before that check landed.
      layout: Array.isArray(flow.layout) ? flow.layout : [],
      deadlineMs: flow.deadlineMs ?? null,
      gateKind: flow.gateKind ?? null,
    },
    staleAfterMs: 300_000,
  };
}

export function updateAgentApproval({ flowId, itemId, content }) {
  return {
    type: "live_activity.update",
    activityId: agentApprovalActivityId(flowId, itemId),
    emittedAtMs: nowMs(),
    contentState: content,
  };
}

export function endAgentApproval({ flowId, itemId, dismissalPolicy = "after:30s", finalState }) {
  return {
    type: "live_activity.end",
    activityId: agentApprovalActivityId(flowId, itemId),
    emittedAtMs: nowMs(),
    dismissalPolicy,
    ...(finalState ? { finalState } : {}),
  };
}

// ─── AgentRunning ───────────────────────────────────────────────────────────

// Activity surfaced while a pipeclaw node is dispatching work to a named
// agent (kind=agent with an `agent` field). Read-only — no buttons.
// Correlation key is `agent-running:<flowId>:<nodeId>` so a flow whose
// lastStep changes (and lands on a NEW agent node) gets a fresh activity
// rather than confusing the old one with a re-pointed identity.

export function startAgentRunning({ flow, agentMeta, pipelineMeta, nodeMeta, subflows }) {
  return {
    type: "live_activity.start",
    activityId: `agent-running:${flow.flowId}:${flow.nodeId}`,
    activityType: "AgentRunning",
    emittedAtMs: nowMs(),
    attributes: {
      flowId: flow.flowId,
      nodeId: flow.nodeId,
      pipelineId: pipelineMeta?.id ?? "unknown",
      pipelineDisplayName: pipelineMeta?.displayName ?? pipelineMeta?.id ?? "unknown",
      pipelineEmoji: pipelineMeta?.emoji ?? "📊",
      agentEmoji: agentMeta?.emoji ?? "🤖",
      agentName: agentMeta?.name ?? "Agent",
      startedAtMs: flow.startedAtMs ?? nowMs(),
    },
    contentState: {
      nodeLabel: flow.nodeLabel ?? flow.nodeId,
      skill: nodeMeta?.skill ?? null,
      kind: nodeMeta?.kind ?? null,
      // Populated only for subflow parents — aggregated subflow state so
      // the iOS widget can render "🔨 3 · 📨 1 · ⏸ 2 awaiting".
      subflows: subflows ?? null,
    },
    staleAfterMs: 600_000,
  };
}

export function updateAgentRunning({ flowId, nodeId, content }) {
  return {
    type: "live_activity.update",
    activityId: `agent-running:${flowId}:${nodeId}`,
    emittedAtMs: nowMs(),
    contentState: content,
  };
}

export function endAgentRunning({ flowId, nodeId, dismissalPolicy = "after:5s" }) {
  return {
    type: "live_activity.end",
    activityId: `agent-running:${flowId}:${nodeId}`,
    emittedAtMs: nowMs(),
    dismissalPolicy,
  };
}

// ─── FailureEvent ──────────────────────────────────────────────────────────
//
// One-shot event fired by the live-bridge (delivered to iOS over WS)
// when a new row lands in `~/.openclaw/flows/failures.sqlite` (pipeclaw's
// failure-store records these at terminal __end__ transitions). Unlike
// live_activity.* events, FailureEvent doesn't keep an open activity — iOS
// EventConsumer reads the `type: "failure_event.start"` payload and hands
// it to NotificationsBridge to schedule a local notification with a
// deep-link into the Failures section. No matching `failure_event.end` —
// the row's ack/expiry is managed by the failures section itself.

export function startFailureEvent({ row }) {
  return {
    type: "failure_event.start",
    emittedAtMs: typeof row.failed_at === "number" ? row.failed_at : nowMs(),
    failureId: row.id,
    flowId: row.flow_id,
    pipelineId: row.pipeline_id,
    branchItemId: row.branch_item_id ?? null,
    failureNode: row.failure_node ?? null,
    errorType: row.error_type,
    failureSummary: row.failure_summary,
    isRetryable: Boolean(row.branch_item_id && row.failure_node),
  };
}

// ─── Synthetic fixtures (test-only) ─────────────────────────────────────────

export function synthAgentApproval({ seed = "test", phase = "start", flowId } = {}) {
  const id = flowId ?? `synthetic-${seed}`;
  const fakeFlow = {
    flowId: id,
    nodeId: "human-gate",
    reason: "Synthetic test event from live-activity-bridge",
    needed: ["budget", "deadline"],
    hint: "This is a fixture; no real flow is blocked.",
  };
  if (phase === "start") {
    return startAgentApproval({
      flow: fakeFlow,
      agentMeta: { emoji: "📨", name: "Hermes (test)" },
      pipelineMeta: { id: "synthetic-pipeline", displayName: "Synthetic Test Pipeline" },
    });
  }
  if (phase === "update") {
    return updateAgentApproval({
      flowId: id,
      content: { nodeId: "human-gate", reason: "Updated reason (synthetic)", needed: ["deadline"] },
    });
  }
  return endAgentApproval({ flowId: id, dismissalPolicy: "immediate" });
}
