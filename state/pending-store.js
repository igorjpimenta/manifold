// Pending operator proposal store: one row per (flow, item) in the
// pending_proposals table of runtime.sqlite. Survives gateway restart so
// the operator can cold-open a client Inbox and still see the "Apply as
// draft" stripe for a proposal made before the app was opened (or before
// the gateway last bounced).
//
// Lifecycle:
//   * `setPendingText`           — write/overwrite the body proposal
//   * `mergePendingFields`       — shallow-merge frontmatter fields
//   * `readPending`              — single source of truth for "what's
//                                   currently waiting on the operator"
//   * `clearPending`             — apply-draft success removes the row
//
// Synchronous better-sqlite3 on purpose: routes call these inline while
// shaping a response, so the API stays sync and every caller keeps
// working unchanged. Payloads are sub-kilobyte so latency is a non-issue.

import Database from "better-sqlite3";
import { resolveStorePaths } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

function parseFields(raw) {
  if (raw == null) return undefined;
  try {
    const v = JSON.parse(raw);
    if (v && typeof v === "object" && !Array.isArray(v)) return v;
  } catch {}
  return undefined;
}

// One shared handle per file: opening a second better-sqlite3 connection
// to a live runtime.sqlite would compete for the file lock instead of
// sharing it, so stores keyed to the same path reuse one connection.
const _handles = new Map(); // dbPath -> Database

function openShared(dbPath) {
  let db = _handles.get(dbPath);
  if (!db || !db.open) {
    migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
    db = new Database(dbPath);
    db.exec("PRAGMA journal_mode = WAL;");
    db.exec("PRAGMA busy_timeout = 30000;");
    _handles.set(dbPath, db);
  }
  return db;
}

export function createPendingStore(arg) {
  const dbPath = typeof arg === "string" ? arg : (arg?.dbPath ?? defaultDbPath());
  const db = openShared(dbPath);

  const _select = db.prepare(
    "SELECT flow_id, item_id, text, fields_json, draft_id, updated_at FROM pending_proposals WHERE flow_id = ? AND item_id = ?",
  );
  // Text writes leave fields_json alone and field merges leave
  // text/draft_id alone, so either order of arrival converges on one row.
  const _upsertText = db.prepare(
    "INSERT INTO pending_proposals (flow_id, item_id, text, fields_json, draft_id, updated_at) VALUES (?, ?, ?, ?, ?, ?) " +
    "ON CONFLICT(flow_id, item_id) DO UPDATE SET text = excluded.text, draft_id = excluded.draft_id, updated_at = excluded.updated_at",
  );
  const _upsertFields = db.prepare(
    "INSERT INTO pending_proposals (flow_id, item_id, text, fields_json, draft_id, updated_at) VALUES (?, ?, ?, ?, ?, ?) " +
    "ON CONFLICT(flow_id, item_id) DO UPDATE SET fields_json = excluded.fields_json, updated_at = excluded.updated_at",
  );
  const _delete = db.prepare("DELETE FROM pending_proposals WHERE flow_id = ? AND item_id = ?");

  function rowToPending(row) {
    if (!row) return null;
    const out = { flowId: row.flow_id, itemId: row.item_id, updatedAt: row.updated_at };
    // Absent columns stay absent (not null): callers branch on
    // `typeof pending.text === "string"` and on fields being an object.
    if (row.text != null) out.text = row.text;
    const fields = parseFields(row.fields_json);
    if (fields !== undefined) out.fields = fields;
    out.draftId = row.draft_id ?? null;
    return out;
  }

  function readPending(flowId, itemId) {
    if (!flowId || !itemId) return null;
    try {
      return rowToPending(_select.get(flowId, itemId));
    } catch {
      return null;
    }
  }

  function setPendingText(flowId, itemId, text, draftId = null) {
    const prior = readPending(flowId, itemId);
    // draftId routes apply-draft to the right slot on multi-draft gates;
    // a null arg keeps the prior slot, defaulting to null on first write.
    const nextDraftId = draftId ?? prior?.draftId ?? null;
    const now = Date.now();
    _upsertText.run(flowId, itemId, text, prior?.fields !== undefined ? JSON.stringify(prior.fields) : null, nextDraftId, now);
    return readPending(flowId, itemId);
  }

  function mergePendingFields(flowId, itemId, fields) {
    const prior = readPending(flowId, itemId);
    const merged = { ...(prior?.fields ?? {}), ...fields };
    const now = Date.now();
    _upsertFields.run(flowId, itemId, prior?.text ?? null, JSON.stringify(merged), prior?.draftId ?? null, now);
    return readPending(flowId, itemId);
  }

  function clearPending(flowId, itemId) {
    if (!flowId || !itemId) return;
    try { _delete.run(flowId, itemId); } catch {}
  }

  function close() {
    if (_handles.get(dbPath) === db) _handles.delete(dbPath);
    try { db.close(); } catch {}
  }

  return { readPending, setPendingText, mergePendingFields, clearPending, close };
}

// Default singleton over the core-owned runtime.sqlite, resolved lazily at
// first call (not import) so MANIFOLD_* env set after import still applies.
let _default = null;
function _store() {
  if (!_default) _default = createPendingStore();
  return _default;
}
export function readPending(...args) { return _store().readPending(...args); }
export function setPendingText(...args) { return _store().setPendingText(...args); }
export function mergePendingFields(...args) { return _store().mergePendingFields(...args); }
export function clearPending(...args) { return _store().clearPending(...args); }
