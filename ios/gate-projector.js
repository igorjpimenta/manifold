// gate-projector.js — pure projection helpers for live_activity.* messages.
//
// Each function takes already-fetched data (no async DB calls inside) so the
// caller can batch DB work (getAllSubflowGroups, getSubflowGroup) and reuse
// across multiple items in one event tick.
//
// Used by both live-bridge.js (event-push path) and the startup seed in
// live-bridge's start() (one-shot scan of active flows on gateway boot).
import {
  startAgentApproval,
  startAgentRunning,
  updateAgentRunning,
} from "./projectors.js";
import {
  prettifyPipelineId,
  parallelGroupContaining,
  awaitingSubNodeId,
  readStampedDiscussSession,
  discussSessionExists,
  discussSessionMtime,
  applyPendingExists,
} from "./poller-helpers.js";

export function pipelineMetaFor(pipelineId, { pipelineLookup } = {}) {
  const declared = (pipelineLookup && pipelineLookup(pipelineId)) || {};
  return {
    id: pipelineId,
    displayName: declared.displayName ?? prettifyPipelineId(pipelineId),
    emoji:       declared.emoji       ?? "📊",
    sfSymbol:    declared.sfSymbol    ?? "tray.fill",
  };
}

// Builds a RunSummary-shaped patch (pipeclaw.run.started/updated/ended's
// payload, and the shape live-bridge.js's snapshot() now uses for the
// reconciliation path too) from a flow row (index.js's store.get()/
// live-bridge.js's runStore rows — both produce the same field set: flowId,
// stateJson, waitJson, lastStep, status, createdAt/updatedAt/endedAt).
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

// Build the byAgent summary from an already-fetched subflow group object
// (result of getSubflowGroup). Returns { byAgent, awaiting, total } or null.
export function buildSubflowAggregateSummary(subflowRows, { nodeAgentLookup, agentLookup, pipelineId, parentNodeId } = {}) {
  if (!subflowRows || typeof subflowRows !== "object") return null;
  const rows = Object.values(subflowRows);
  if (rows.length === 0) return null;
  const parent = nodeAgentLookup?.(pipelineId, parentNodeId);
  const byAgentMap = new Map();
  let awaiting = 0;
  let total = 0;
  for (const subflow of rows) {
    total++;
    if (subflow.awaiting) { awaiting++; continue; }
    const step = subflow.step;
    if (!step) continue;
    const subNode = parent?.flowNodes?.[step];
    const agentId = typeof subNode?.agent === "string" ? subNode.agent : null;
    if (!agentId) continue;
    const prev = byAgentMap.get(agentId);
    if (prev) { prev.count++; }
    else {
      const meta = agentLookup?.(agentId) ?? {};
      byAgentMap.set(agentId, {
        agentId, agentName: meta.name ?? agentId, agentEmoji: meta.emoji ?? "🤖",
        subNode: step, count: 1,
      });
    }
  }
  const byAgent = [...byAgentMap.values()].sort((a, b) => b.count - a.count);
  return { byAgent, awaiting, total };
}

export function agentRunningContent(nodeMeta, subflows) {
  return {
    nodeLabel: nodeMeta?.nodeLabel ?? null,
    skill:     nodeMeta?.skill     ?? null,
    kind:      nodeMeta?.kind      ?? null,
    subflows:  subflows             ?? null,
  };
}

// Project a gate item into a live_activity.start AgentApproval message.
// allSubflowGroups: the result of getAllSubflowGroups({ flowId, runStore, nodeExecutionsStore }).
export function projectGateItem(flow, item, allSubflowGroups, {
  pipelineLookup, agentLookup, nodeAgentLookup, nodeDiscussLookup,
} = {}) {
  const pipelineId = flow.stateJson?.pipelineId ?? "unknown";
  const responder = flow.waitJson?.responder || "main";
  const agentMeta = (agentLookup && agentLookup(responder)) || { emoji: "🤖", name: "Agent" };
  const rp = item?.requestPayload ?? {};
  const parallelParent = parallelGroupContaining(allSubflowGroups, item.itemId);
  const childFlowId = parallelParent
    ? (allSubflowGroups[parallelParent]?.[item.itemId]?.flowId ?? null)
    : null;
  const gateLookupNodeId = awaitingSubNodeId(allSubflowGroups, item.itemId)
    ?? flow.waitJson?.nodeId ?? parallelParent ?? flow.lastStep;
  const rootGateNodeId = !parallelParent ? (flow.waitJson?.nodeId ?? null) : null;
  const deckOptedIn = Boolean(
    nodeAgentLookup?.(pipelineId, gateLookupNodeId)?.deckPresentation === true
    || (parallelParent && nodeAgentLookup?.(pipelineId, parallelParent)?.deckPresentation === true)
    || (rootGateNodeId && nodeAgentLookup?.(pipelineId, rootGateNodeId)?.deckPresentation === true),
  );
  const deckGroupKey = (parallelParent && gateLookupNodeId && gateLookupNodeId !== parallelParent)
    ? `${parallelParent}::${gateLookupNodeId}`
    : (parallelParent ?? rootGateNodeId);
  const discussMeta = nodeDiscussLookup
    ? nodeDiscussLookup(pipelineId, gateLookupNodeId)
    : null;
  const stamp = discussMeta?.agent
    ? readStampedDiscussSession(flow.flowId, gateLookupNodeId, item.itemId)
    : null;
  const effectiveDiscussAgent = stamp?.agent ?? discussMeta?.agent ?? null;
  let discussAgentLabel = null;
  if (effectiveDiscussAgent) {
    const m = agentLookup?.(effectiveDiscussAgent);
    if (m) {
      discussAgentLabel = m.emoji ? `${m.emoji} ${m.name ?? effectiveDiscussAgent}` : (m.name ?? effectiveDiscussAgent);
    } else {
      discussAgentLabel = effectiveDiscussAgent;
    }
  }
  return startAgentApproval({
    flow: {
      flowId: flow.flowId,
      itemId: item.itemId,
      childFlowId,
      groupId:          deckOptedIn ? deckGroupKey : null,
      nodeId:           gateLookupNodeId,
      reason:           rp.title ?? `Approve ${item.itemId}?`,
      needed:           null,
      hint:             rp.description ?? null,
      discussSupported: Boolean(discussMeta?.agent),
      discussAgent:     discussAgentLabel,
      discussActive:    discussSessionExists(flow, item.itemId, discussMeta?.agent, gateLookupNodeId),
      lastDiscussMs:    discussSessionMtime(flow, item.itemId, discussMeta?.agent, gateLookupNodeId),
      applyPending:     applyPendingExists(flow.flowId, item.itemId),
      layout:           Array.isArray(rp.layout) ? rp.layout : [],
      // Sole source of truth: flow_node_invocations.invoked_at for this
      // exact gate node (threaded in by live-bridge.js as
      // flow.gateInvokedAtMs), the exact moment the gate NODE was
      // dispatched — written eagerly at invocation start, independent of
      // whether the node later blocks, and reused (not re-stamped) across
      // a partial-decision re-entry, since the engine only creates a new
      // invocation row on a genuinely fresh node entry (executor.js's
      // isFreshNodeEntry). Deliberately no fallback chain here: flow.
      // createdAt/updatedAt were tried and discarded — createdAt reflects
      // the whole FLOW's creation and can predate the gate node's actual
      // dispatch by however long earlier steps took to run; updatedAt is
      // bumped by ANY write to the flow row (including an unrelated field/
      // draft edit via persistRevisedMeta/persistRevisedDraft), silently
      // resetting an item's "waiting since" clock to "now" on every edit.
      blockedAtMs:      flow.gateInvokedAtMs ?? null,
      gateKind:         flow.waitJson?.kind ?? null,
    },
    agentMeta,
    pipelineMeta: pipelineMetaFor(pipelineId, { pipelineLookup }),
  });
}


// Project a node.executing event into a live_activity.start AgentRunning message.
export function projectAgentRunning(flowId, nodeId, stateJson, nodeMeta, subflows, {
  pipelineLookup, agentLookup,
} = {}) {
  const pipelineId = stateJson?.pipelineId ?? "unknown";
  const headerAgentId = _pickHeaderAgent(nodeMeta, subflows);
  const agentMeta = (agentLookup && agentLookup(headerAgentId)) || { emoji: "🤖", name: headerAgentId };
  return startAgentRunning({
    flow: {
      flowId,
      nodeId:       nodeMeta.nodeLabel ?? nodeId,
      nodeLabel:    nodeMeta.nodeLabel ?? nodeId,
      startedAtMs:  Date.now(),
    },
    agentMeta,
    pipelineMeta: pipelineMetaFor(pipelineId, { pipelineLookup }),
    nodeMeta,
    subflows,
  });
}

export function projectAgentRunningUpdate(flowId, nodeId, nodeMeta, subflows) {
  return updateAgentRunning({
    flowId,
    nodeId: nodeMeta.nodeLabel ?? nodeId,
    content: agentRunningContent(nodeMeta, subflows),
  });
}

function _pickHeaderAgent(nodeMeta, subflows) {
  if (subflows?.byAgent?.length) {
    const top = [...subflows.byAgent].sort((a, b) => b.count - a.count)[0];
    if (top?.agentId) return top.agentId;
  }
  return nodeMeta?.agent ?? "main";
}
