// Discuss session sidecar read/write.
// Each gate discuss session is identified by (flowId, gateNodeId, itemId).
// The sidecar locks the agent id + session id at first-turn creation time so
// that pipeline config changes don't orphan an in-progress conversation.
import path from "node:path";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { resolveStorePaths } from "../state/paths.js";

// Store-home sidecar dir, resolved per call so a MANIFOLD_DATA_DIR change
// between import and first use still relocates the store.
export function discussSessionsDir() {
  return resolveStorePaths().discussSessionsDir;
}

// Read-only fallback for sidecars written under the harness home. Writes
// always go to discussSessionsDir() above; reads consult the legacy dir
// (state/paths.js) only when the store-home file is absent.
function fallbackSessionsDir() {
  return resolveStorePaths().legacyDiscussSessionsDir;
}

function sidecarFileName(flowId, gateNodeId, itemId) {
  const safe = (s) => String(s).replace(/[^A-Za-z0-9._-]+/g, "_");
  return `${String(flowId).slice(0, 8)}__${safe(gateNodeId)}__${safe(itemId)}.json`;
}

export function _discussSidecarPath(flowId, gateNodeId, itemId) {
  return path.join(discussSessionsDir(), sidecarFileName(flowId, gateNodeId, itemId));
}

function _fallbackSidecarPath(flowId, gateNodeId, itemId) {
  return path.join(fallbackSessionsDir(), sidecarFileName(flowId, gateNodeId, itemId));
}

function parseSidecar(raw) {
  try {
    const obj = JSON.parse(raw);
    if (typeof obj?.agent === "string" && typeof obj?.sessionId === "string") return obj;
  } catch {}
  return null;
}

function readSidecarFile(sidecarPath) {
  try {
    return parseSidecar(readFileSync(sidecarPath, "utf8"));
  } catch {}
  return null;
}

export function stampDiscussSession(flowId, gateNodeId, itemId, agentId, sessionId) {
  try {
    mkdirSync(discussSessionsDir(), { recursive: true });
    writeFileSync(_discussSidecarPath(flowId, gateNodeId, itemId), JSON.stringify({ agent: agentId, sessionId }), "utf8");
  } catch {}
}

export function readDiscussSession(flowId, gateNodeId, itemId) {
  const sidecarPath = _discussSidecarPath(flowId, gateNodeId, itemId);
  if (existsSync(sidecarPath)) return readSidecarFile(sidecarPath);
  return readSidecarFile(_fallbackSidecarPath(flowId, gateNodeId, itemId));
}
