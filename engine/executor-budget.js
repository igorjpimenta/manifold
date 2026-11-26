import { nodeCallStats } from "../state/node-cost.js";

export function createBudgetGate({ getApprovalQueue, logger, nodeCallStats: nodeCallStatsFn = nodeCallStats }) {
  async function maybeBudgetGate({ flowId, nodeId, pipelineId, pipelineName, node, itemCount = 1, items = [] }) {
    const aq = getApprovalQueue?.();
    if (!aq || typeof aq.isGateLocked !== "function") return { proceed: true };

    // isGateLocked() awaits its own store attachment in place rather than
    // throwing a "not ready" error — a throw from here is a genuine
    // failure and must propagate, never be swallowed as "not locked"
    // (which would fail open past the gate).
    const locked = await aq.isGateLocked();
    if (!locked) return { proceed: true };

    const decision = typeof aq.lookupNodeDecision === "function"
      ? await aq.lookupNodeDecision({ flowId, nodeId })
      : null;
    if (decision === "approve") return { proceed: true };
    if (decision === "reject")  return { skip: true };
    if (decision === "pending") {
      return { pause: true, blockedInfo: budgetGateBlockedInfo(nodeId, null) };
    }

    const estimate = await estimateNodeCost(nodeCallStatsFn, node, itemCount, pipelineId, nodeId);
    const itemLabel = formatNodeLabel(nodeId, node, itemCount);
    const brief = formatNodeBrief(node, items, itemCount);
    let itemId = null;
    try {
      itemId = await aq.enqueueNodeApproval({
        flowId,
        nodeId,
        pipeline: pipelineName,
        pipelineId,
        label: itemLabel,
        agent: node?.agent ?? "manifold",
        agentEmoji: agentEmojiFor(node?.agent),
        trigger: "cron",
        brief,
        costLow: estimate.low,
        costHigh: estimate.high,
        modelChain: [],
        tokenEstimate: null,
      });
      logger?.info?.(`manifold [${flowId.slice(0,8)}] ${nodeId}: budget gate locked — queued item=${itemId} (~$${estimate.low.toFixed(4)}-$${estimate.high.toFixed(4)})`);
    } catch (err) {
      logger?.warn?.(`manifold [${flowId.slice(0,8)}] ${nodeId}: enqueue failed: ${err?.message ?? err}`);
    }
    return { pause: true, blockedInfo: budgetGateBlockedInfo(nodeId, itemId) };
  }

  return { maybeBudgetGate };
}

function budgetGateBlockedInfo(_nodeId, _itemId) {
  return {
    _kind: "budget-gate-approval",
    reason: "daily budget cap reached — node held for operator approval",
    hint: "approve or reject from your companion app",
  };
}

async function estimateNodeCost(nodeCallStatsFn, _node, itemCount, pipelineId, nodeId) {
  const stats = await nodeCallStatsFn(pipelineId, nodeId);
  if (stats) {
    return { low: stats.p25 * itemCount, high: stats.p75 * itemCount };
  }
  return { low: 0.001 * itemCount, high: 0.05 * itemCount };
}

function formatNodeLabel(nodeId, _node, itemCount) {
  if (itemCount > 1) return `${nodeId} × ${itemCount}`;
  return nodeId;
}

function formatNodeBrief(_node, items, itemCount) {
  if (!items?.length) return `${itemCount} call(s) awaiting approval`;
  const preview = items.slice(0, 3).map((it) => it?.id ?? it?.listingId ?? it?.title ?? String(it)).join(", ");
  return `${itemCount} item(s): ${preview}${items.length > 3 ? "…" : ""}`;
}

// MANIFOLD_AGENT_EMOJI ("<agent>=<emoji>,...") opts an agent into its own
// marker; the same neutral one otherwise.
function agentEmojiFor(agent) {
  const configured = String(process.env.MANIFOLD_AGENT_EMOJI ?? "")
    .split(",")
    .map((pair) => pair.split("="))
    .find(([name]) => name?.trim() === agent);
  return configured?.[1]?.trim() || "🤖";
}
