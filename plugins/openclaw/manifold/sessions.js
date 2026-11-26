import path from "node:path";
import { readFileSync } from "node:fs";

function authPaths(agentId) {
  const home = process.env.HOME || process.env.USERPROFILE || "/root";
  return [
    path.join(home, `.openclaw/agents/${agentId}/agent/auth-state.json`),
    path.join(home, ".openclaw/agents/main/agent/auth-state.json"),
  ];
}

// Gateway model resolution for an agent. Dynamic import keeps the
// openclaw SDK out of core's static graph; throws propagate to the route's
// catch (500).
export async function resolveAgentEffectiveModelPrimary(config, agentId) {
  const { resolveAgentEffectiveModelPrimary } = await import("openclaw/plugin-sdk/agent-runtime");
  return resolveAgentEffectiveModelPrimary(config, agentId);
}

function usageStatsOf(agentId) {
  for (const fp of authPaths(agentId)) {
    try {
      const st = JSON.parse(readFileSync(fp, "utf8"));
      return st?.usageStats ?? null;
    } catch {}
  }
  return null;
}

// True when the agent's (or main's fallback) auth-state disables the provider
// until a future timestamp. First parseable file wins; never throws.
export function isProviderCooldown(agentId, provider) {
  try {
    const stats = usageStatsOf(agentId);
    if (!stats) return false;
    for (const [k, v] of Object.entries(stats)) {
      if (k.startsWith(`${provider}:`) && typeof v?.disabledUntil === "number" && Date.now() < v.disabledUntil) return true;
    }
    return false;
  } catch {
    return false;
  }
}

// Harness-native id for a gate discuss thread: core never mints discuss ids,
// it asks the harness. Deterministic in (flow, gate, item) so concurrent
// first-turns converge on one thread. Segments are sanitized and bounded —
// the harness keeps one transcript file per session, named after the id.
function safe(s) {
  // Lowercase: the gateway normalizes session keys to lowercase, so an id with
  // an uppercase segment (node ids are SCREAMING_CASE) reaches tools in a
  // different case than the dispatch recorded it, and every hand-off write
  // then misses the row the engine is waiting on.
  return String(s ?? "").toLowerCase().replace(/[^A-Za-z0-9._-]+/g, "_").slice(0, 80) || "_";
}

export function newDiscussSessionId({ agentId, flowId, gateNodeId, itemId }) {
  return `agent:${safe(agentId)}:discuss:${String(flowId ?? "").slice(0, 8)}-${safe(gateNodeId)}-${safe(itemId)}`;
}

// Harness-native id for one agent-node dispatch; core never mints dispatch
// ids either. Deterministic in (agent, flow, node) so a retry and a boot
// revival re-attach to the same session — tool-handoff rows, the session
// index and the lineage ledger all key off it. The `manifold:` segment keeps
// these out of the cron/subagent/discuss namespaces (telemetry/grammar.js
// classifies it back to the flow).
export function newNodeSessionId({ agentId, flowId, nodeId }) {
  return `agent:${safe(agentId)}:manifold:${String(flowId ?? "").slice(0, 8)}-${safe(nodeId)}`;
}
