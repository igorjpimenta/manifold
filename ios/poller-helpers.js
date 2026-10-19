// Pure and filesystem helpers for gate-projector.js — no closure state, no relay deps.
import os from "node:os";
import path from "node:path";
import { existsSync, statSync, readFileSync } from "node:fs";

export function prettifyPipelineId(pipelineId) {
  if (typeof pipelineId !== "string" || !pipelineId) return "Pipeline";
  return pipelineId.replace(/^([a-z]+)_(.+)$/, "$1: $2").replace(/-/g, " ");
}

// A subflowKey (job id) can persist across a pipeline's successive subflow
// stages — e.g. the same listing gets its own row under group_id "scored"
// and later, independently, under "PROPOSE". Prefer the row that's
// currently awaiting a decision over an earlier, already-finished stage
// that happens to share the same key. `allGroups` is the dual-mode-safe
// `{ [groupId]: { [subflowKey]: row } }` shape from
// state/subflow-state.js's getAllSubflowGroups — searches every group,
// since the caller doesn't know in advance which one (if any) has itemId.
function activeSubflowRow(allGroups, itemId) {
  if (!allGroups || !itemId) return null;
  let found = null;
  for (const [groupId, group] of Object.entries(allGroups)) {
    const row = group?.[itemId];
    if (!row) continue;
    if (row.awaiting) return { groupId, row };
    if (!found) found = { groupId, row };
  }
  return found;
}

// Find the subflow group (parent node id) that contains a given itemId as a
// subflow key. Fallback for gateLookupNodeId during the brief running window
// between a partial gate decision and the next waitJson re-emit. Pure —
// caller supplies the flow's dual-mode-safe subflow groups (see
// state/subflow-state.js's getAllSubflowGroups) rather than this function
// reaching for a store itself.
export function parallelGroupContaining(allGroups, itemId) {
  return activeSubflowRow(allGroups, itemId)?.groupId ?? null;
}

// The sub-node a subflow item is CURRENTLY awaiting a decision at (e.g.
// "GAP_GATE" nested inside PROPOSE's flow.nodes) — distinct from
// parallelGroupContaining's groupId, which is the parent subflow node
// ("PROPOSE"). When a subflow node batches multiple items sitting at
// different sub-nodes into one waitJson, waitJson.nodeId can only describe
// the batch as a whole (the parent subflow node id) — per-node config like
// deckPresentation lives on the specific awaiting sub-node, which only this
// reads.
export function awaitingSubNodeId(allGroups, itemId) {
  return activeSubflowRow(allGroups, itemId)?.row?.awaiting?.subNodeId ?? null;
}

// Read the discuss session sidecar at
//   ~/.openclaw/discuss-sessions/<flowId8>__<gateNodeId>__<itemId>.json
// Returns { agent, sessionId } locked at creation time, or null.
export function readStampedDiscussSession(flowId, gateNodeId, itemId) {
  if (!flowId || !gateNodeId || !itemId) return null;
  const safe = (s) => String(s).replace(/[^A-Za-z0-9._-]+/g, "_");
  const sidecar = path.join(
    os.homedir(), ".openclaw", "discuss-sessions",
    `${String(flowId).slice(0, 8)}__${safe(gateNodeId)}__${safe(itemId)}.json`
  );
  try {
    const obj = JSON.parse(readFileSync(sidecar, "utf8"));
    if (typeof obj?.agent === "string" && typeof obj?.sessionId === "string") return obj;
  } catch {}
  return null;
}

// True when the operator has already opened the chat for this gate
// (detected by stat-ing the discuss session JSONL).
export function discussSessionExists(flow, itemId, discussAgent, gateNodeId) {
  if (!itemId || !gateNodeId) return false;
  const flowId = flow?.flowId ?? "";
  const flow8 = flowId.slice(0, 8);
  if (!flow8) return false;
  const stamp = readStampedDiscussSession(flowId, gateNodeId, itemId);
  const agent = stamp?.agent ?? discussAgent;
  if (!agent) return false;
  const sessionId = stamp?.sessionId ?? `gf-discuss-${flow8}-${gateNodeId}-${itemId}`;
  const sessionFile = path.join(
    os.homedir(), ".openclaw", "agents", agent, "sessions", `${sessionId}.jsonl`
  );
  try { return existsSync(sessionFile); } catch { return false; }
}

// Returns the session JSONL's mtime in unix ms, or null — iOS reads this as
// the "last activity" timestamp on the conversation card.
export function discussSessionMtime(flow, itemId, discussAgent, gateNodeId) {
  if (!itemId || !gateNodeId) return null;
  const flowId = flow?.flowId ?? "";
  const flow8 = flowId.slice(0, 8);
  if (!flow8) return null;
  const stamp = readStampedDiscussSession(flowId, gateNodeId, itemId);
  const agent = stamp?.agent ?? discussAgent;
  if (!agent) return null;
  const sessionId = stamp?.sessionId ?? `gf-discuss-${flow8}-${gateNodeId}-${itemId}`;
  const sessionFile = path.join(
    os.homedir(), ".openclaw", "agents", agent, "sessions", `${sessionId}.jsonl`
  );
  try {
    const st = statSync(sessionFile);
    return st.mtimeMs ? Math.round(st.mtimeMs) : null;
  } catch { return null; }
}

// True when a propose_draft / propose_draft_meta sidecar exists for (flow,item).
// Mirrors the file-naming convention in pending-store.js#fileNameFor.
export function applyPendingExists(flowId, itemId) {
  if (!flowId || !itemId) return false;
  const safe = (s) => String(s).replace(/[^A-Za-z0-9._-]+/g, "_");
  const fileName = `${safe(flowId)}__${safe(itemId)}.json`;
  const sidecar = path.join(os.homedir(), ".openclaw", "discuss-pending", fileName);
  try { return existsSync(sidecar); } catch { return false; }
}
