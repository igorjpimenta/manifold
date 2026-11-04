// Per-(flow, item) on-disk pending proposal store. Survives gateway
// restart so the operator can cold-open a client Inbox and still see
// the "Apply as draft" stripe for a proposal Cyrano made before the
// app was opened (or before the gateway last bounced).
//
// One file per (flowId, itemId) under the store home's discuss-pending/.
// File shape:
//   { flowId, itemId, text?: string, fields?: {...}, updatedAt: ms }
//
// Lifecycle:
//   * `setPendingText`           — write/overwrite the body proposal
//   * `mergePendingFields`       — shallow-merge frontmatter fields
//   * `readPending`              — single source of truth for "what's
//                                   currently waiting on the operator"
//   * `clearPending`             — apply-draft success removes the file
//
// JSON write is best-effort synchronous via writeFileSync to keep the
// tool-execute path simple; payloads are sub-kilobyte so latency is a
// non-issue.

import { mkdirSync, readFileSync, writeFileSync, unlinkSync, existsSync } from "node:fs";
import path from "node:path";
import { resolveStorePaths } from "./db.js";

function defaultPendingDir() {
  return resolveStorePaths().pendingDir;
}

export function createPendingStore(pendingDir) {
  function ensureDir() {
    try { mkdirSync(pendingDir, { recursive: true }); } catch {}
  }

  // Sanitize the (flowId, itemId) pair into a single safe filename.
  // flowId is a UUID; itemId can be a long slug. Replace any character
  // outside [A-Za-z0-9._-] with "_" so the result is always a valid
  // filename on macOS/Linux.
  function fileNameFor(flowId, itemId) {
    const safe = (s) => String(s).replace(/[^A-Za-z0-9._-]+/g, "_");
    return `${safe(flowId)}__${safe(itemId)}.json`;
  }

  function filePath(flowId, itemId) {
    return path.join(pendingDir, fileNameFor(flowId, itemId));
  }

  function readPending(flowId, itemId) {
    if (!flowId || !itemId) return null;
    const p = filePath(flowId, itemId);
    if (!existsSync(p)) return null;
    try {
      const raw = readFileSync(p, "utf-8");
      const obj = JSON.parse(raw);
      if (!obj || typeof obj !== "object") return null;
      return obj;
    } catch {
      return null;
    }
  }

  function writePending(flowId, itemId, payload) {
    ensureDir();
    const p = filePath(flowId, itemId);
    writeFileSync(p, JSON.stringify(payload, null, 2));
  }

  function setPendingText(flowId, itemId, text, draftId = null) {
    const prior = readPending(flowId, itemId) ?? { flowId, itemId };
    // draftId is forward-compat for gates that ship multiple draft
    // slots in their layout (subject + body, etc.). Stored alongside
    // text so apply-draft can route to the right slot once that lands.
    // For today's single-draft gates, omit → null → apply-draft writes
    // the prose body unchanged.
    const next = {
      ...prior,
      flowId,
      itemId,
      text,
      draftId: draftId ?? prior.draftId ?? null,
      updatedAt: Date.now(),
    };
    writePending(flowId, itemId, next);
    return next;
  }

  function mergePendingFields(flowId, itemId, fields) {
    const prior = readPending(flowId, itemId) ?? { flowId, itemId };
    const mergedFields = { ...(prior.fields ?? {}), ...fields };
    const next = { ...prior, flowId, itemId, fields: mergedFields, updatedAt: Date.now() };
    writePending(flowId, itemId, next);
    return next;
  }

  function clearPending(flowId, itemId) {
    if (!flowId || !itemId) return;
    const p = filePath(flowId, itemId);
    try { unlinkSync(p); } catch {}
  }

  return { readPending, setPendingText, mergePendingFields, clearPending, _dir: pendingDir };
}

// Default singleton over the core-owned pending dir, resolved lazily at
// first call (not import) so MANIFOLD_* env set after import still applies.
let _default = null;
function _store() {
  if (!_default) _default = createPendingStore(defaultPendingDir());
  return _default;
}
export function readPending(...args) { return _store().readPending(...args); }
export function setPendingText(...args) { return _store().setPendingText(...args); }
export function mergePendingFields(...args) { return _store().mergePendingFields(...args); }
export function clearPending(...args) { return _store().clearPending(...args); }
