// Discuss session sidecar read/write.
// Each gate discuss session is identified by (flowId, gateNodeId, itemId).
// The sidecar locks the agent id + session id at first-turn creation time so
// that pipeline config changes don't orphan an in-progress conversation.
import path from "node:path";
import os from "node:os";
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";

export const DISCUSS_SESSIONS_DIR = path.join(os.homedir(), ".openclaw", "discuss-sessions");

export function _discussSidecarPath(flowId, gateNodeId, itemId) {
  const safe = (s) => String(s).replace(/[^A-Za-z0-9._-]+/g, "_");
  return path.join(DISCUSS_SESSIONS_DIR, `${String(flowId).slice(0, 8)}__${safe(gateNodeId)}__${safe(itemId)}.json`);
}

export function stampDiscussSession(flowId, gateNodeId, itemId, agentId, sessionId) {
  try {
    mkdirSync(DISCUSS_SESSIONS_DIR, { recursive: true });
    writeFileSync(_discussSidecarPath(flowId, gateNodeId, itemId), JSON.stringify({ agent: agentId, sessionId }), "utf8");
  } catch {}
}

export function readDiscussSession(flowId, gateNodeId, itemId) {
  try {
    const raw = readFileSync(_discussSidecarPath(flowId, gateNodeId, itemId), "utf8");
    const obj = JSON.parse(raw);
    if (typeof obj?.agent === "string" && typeof obj?.sessionId === "string") return obj;
  } catch {}
  return null;
}
