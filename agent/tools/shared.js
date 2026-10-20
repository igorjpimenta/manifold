// shared.js — helpers used by more than one pipeclaw agent tool.

import { getSharedResolver } from "../../telemetry/index.js";

export function toolErr(message) {
  return {
    isError: true,
    content: [{ type: "text", text: message }],
  };
}

// Whitelist of frontmatter fields propose_draft_meta accepts. Anything
// else returns a tool error — keeps Cyrano from drifting into
// pipeline-internal fields (status / approval-token / job-id /
// source-listing / drafted-at) which the executor owns.
export const META_FIELD_WHITELIST = new Set(["budget", "timeframe", "language", "duration", "period"]);

// Detects a numeric range like "R$ 2.600 - 3.500" or "2600 a 3500" before a
// caller's naive "strip everything but digits" parse gets a chance to run.
// That strip step (used for budget/period fields so agents don't have to
// pre-clean "R$ 6.000"-style currency formatting) also removes the range
// separator itself, concatenating both numbers into one nonsense value
// (e.g. "26003500") that still passes a finite/positive check downstream.
// Reject range inputs explicitly instead of silently picking (or mangling) a
// number.
const NUMERIC_RANGE_PATTERN = /\d[\d.,]*\s*(?:-|–|—|~|\ba\b|\bto\b)\s*\d[\d.,]*/i;

export function looksLikeNumericRange(raw) {
  return NUMERIC_RANGE_PATTERN.test(String(raw ?? ""));
}

export function normalizePendingFields(fields) {
  if (!fields || typeof fields !== "object") return fields;
  const out = { ...fields };
  if (typeof out.budget === "string") {
    const n = parseInt(out.budget, 10);
    if (!isNaN(n)) out.budget = n;
  }
  if (typeof out.period === "string") {
    const n = parseInt(out.period, 10);
    if (!isNaN(n)) out.period = n;
  }
  return out;
}

// Resolves { flowId, itemId } for a propose_draft / propose_draft_meta call
// that omitted them (or passed values that don't match a real flow) — the
// documented Cyrano protocol ("you don't need flowId/itemId, the gateway
// resolves the gate context automatically") depends on this.
//
// `ctx` is the per-invocation factory-form tool context (see
// pipeclaw-start.js) — `ctx.sessionKey` is the CALLING agent's own session,
// not its spawner's. Backed by the telemetry lineage resolver's persisted
// parent/child spawn graph (session_lineage table): the discuss session's
// row carries flowId/itemId (stamped once, on its first turn, via
// recordSpawn in routes/discuss.js); a subagent spawned from it (e.g.
// Cyrano) has no item context of its own, but its lineage row's
// parent_session_key points at the discuss session, so resolve() walks up
// and inherits flowId/itemId from there. Race-free — unlike a shared
// flowId-only-keyed registry, each spawn edge is its own persisted row.
export async function resolveGateContext({ ctx }) {
  const sessionKey = ctx?.sessionKey;
  if (!sessionKey) return null;
  const resolver = getSharedResolver();
  if (!resolver) return null;
  let row;
  // resolver.resolve() returns a Promise and must be awaited here: without
  // the await, `row` is a pending Promise (always truthy but fieldless), so
  // `row?.flow_id` is undefined and this function returns null for any
  // session that needs auto-resolve (e.g. a detached subagent like Cyrano,
  // without its own explicit flowId/itemId).
  try { row = await resolver.resolve(sessionKey); } catch { return null; }
  if (!row?.flow_id || !row?.item_key) return null;
  return { flowId: row.flow_id, itemId: row.item_key, pipelineId: row.pipeline_id ?? null };
}

// Resolves { flowId } for a tool call from a plain kind:"agent" node
// dispatch (e.g. submit_score from SCORE_LLM) — these sessions never get an
// itemKey stamped on their lineage row (executor-nodes.js's recordSpawn
// call for a plain agent dispatch only passes {childSessionKey, flowId,
// nodeId, pipelineId, agentId}, no itemId), so row.item_key is always SQL
// NULL for them. resolveGateContext's stricter check (requires BOTH
// flow_id and item_key) would wrongly return null even though flow_id is
// present and valid — this helper only requires flow_id.
export async function resolveFlowIdOnly({ ctx }) {
  const sessionKey = ctx?.sessionKey;
  if (!sessionKey) return null;
  const resolver = getSharedResolver();
  if (!resolver) return null;
  let row;
  try { row = await resolver.resolve(sessionKey); } catch { return null; }
  if (!row?.flow_id) return null;
  return { flowId: row.flow_id, nodeId: row.node_id ?? null, pipelineId: row.pipeline_id ?? null };
}
