import path from "node:path";
import os from "node:os";
import { existsSync, mkdirSync, writeFileSync, readFileSync, readdirSync } from "node:fs";

function agentsRoot() {
  return path.join(os.homedir(), ".openclaw", "agents");
}

function sessionsDir(agentId) {
  return path.join(agentsRoot(), agentId, "sessions");
}

function authPaths(agentId) {
  const home = process.env.HOME || process.env.USERPROFILE || "/root";
  return [
    path.join(home, `.openclaw/agents/${agentId}/agent/auth-state.json`),
    path.join(home, ".openclaw/agents/main/agent/auth-state.json"),
  ];
}

// Session-file path for an agent's JSONL transcript. Pure join, never throws
// on string input — callers validate agentId/sessionId before reaching here.
// Harness-internal: the harness names its own file id `sessionId`.
export function resolveSessionFile(agentId, sessionId) {
  return path.join(sessionsDir(agentId), `${sessionId}.jsonl`);
}

// Transcript file for a manifold session (the index KEY). Core hands over
// its single `session` handle; the harness owns the index, so the entry
// for that key names the file — the key itself is never a filename.
export function resolveSessionFileForKey(agentId, session) {
  try {
    const index = readSessionsIndex(agentId);
    const entry = index?.[session];
    if (entry?.sessionFile) return entry.sessionFile;
    const sessionId = entry?.sessionId;
    if (typeof sessionId === "string" && sessionId) {
      return path.join(sessionsDir(agentId), `${sessionId}.jsonl`);
    }
  } catch {
    // Unreadable index falls through to the conventional path below.
  }
  // A session is an opaque harness string, so it is never used as a path
  // segment: only the last segment, sanitized, may name a file.
  const leaf = String(session ?? "").split(":").pop().replace(/[^A-Za-z0-9._-]+/g, "_");
  return path.join(sessionsDir(agentId), `${leaf}.jsonl`);
}

// existsSync-shaped predicate: false on any filesystem error, never throws.
export function sessionFileExists(file) {
  try {
    return existsSync(file);
  } catch {
    return false;
  }
}

// mkdir -p + empty-file touch. Throws on failure — the discuss route catches
// and downgrades to a warn, same as the inline code this replaces.
export function ensureSessionFile(file) {
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, "", { flag: "w" });
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

// Agent ids with a sessions registry on disk. Unreadable root yields [] —
// the route treats that as "no child sessions", same as before.
export function listSessionAgents() {
  try {
    const root = agentsRoot();
    return readdirSync(root).filter((d) => {
      try {
        return existsSync(path.join(root, d, "sessions", "sessions.json"));
      } catch {
        return false;
      }
    });
  } catch {
    return [];
  }
}

// Parsed sessions.json registry for an agent, or null when missing/unparseable.
// Per-agent read failures are skipped by the route, never fatal.
export function readSessionsIndex(agentId) {
  try {
    return JSON.parse(readFileSync(path.join(sessionsDir(agentId), "sessions.json"), "utf8"));
  } catch {
    return null;
  }
}

// Child transcript path: registry-recorded sessionFile, else the conventional
// <sessionId>.jsonl derived from the trailing segment of the child key.
export function resolveChildSessionFile(agentId, entry, childKey) {
  return entry?.sessionFile
    ?? path.join(sessionsDir(agentId), `${entry?.sessionId ?? childKey.split(":").pop()}.jsonl`);
}

// Raw JSONL text. Throws on unreadable files — the route's outer catch
// already handles that as a child-session skip, same as before.
export function readSessionFileSync(file) {
  return readFileSync(file, "utf8");
}
