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
// catch (500), same as the inline import this replaces.
export async function resolveAgentEffectiveModelPrimary(config, agentId) {
  const { resolveAgentEffectiveModelPrimary } = await import("openclaw/plugin-sdk/agent-runtime");
  return resolveAgentEffectiveModelPrimary(config, agentId);
}

function usageStatsOf(agentId) {
  for (const fp of authPaths(agentId)) {
    try {
      const st = JSON.parse(readFileSync(fp, "utf8"));
      return st?.usageStats ?? null;
    } catch {
      // Missing/unparseable file falls through to the next candidate path.
    }
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

// Harness-native id for a gate discuss thread. Core never mints discuss
// ids — it asks the harness, which owns the session namespace, and passes
// the value through verbatim. Same `agent:<agentId>:…` family as the
// cron/subagent/explicit ids (see telemetry/grammar.js), so history and
// transcript lookups resolve the agent straight from the key.
// Deterministic in (flow, gate, item): the thread's coordinates identify
// it, so concurrent first-turns converge on one thread instead of forking.
// The variable segments are sanitized for filesystem use — the harness
// keeps one transcript file per session, named after the id.
export function newDiscussSessionId({ agentId, flowId, gateNodeId, itemId }) {
  const safe = (s) => String(s ?? "").replace(/[^A-Za-z0-9._-]+/g, "_").slice(0, 80) || "_";
  return `agent:${safe(agentId)}:discuss:${String(flowId ?? "").slice(0, 8)}-${safe(gateNodeId)}-${safe(itemId)}`;
}

// Raw JSONL text. Throws on unreadable files — the route's outer catch
// already handles that as a child-session skip, same as before.
export function readSessionFileSync(file) {
  return readFileSync(file, "utf8");
}
