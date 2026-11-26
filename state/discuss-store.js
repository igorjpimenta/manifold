// discuss-store.js — authoritative server-side state for every discuss
// conversation: one row per (flowId, itemId) in SQLite, so a client can
// rebuild full state from a single GET after any reconnect, restart, or
// missed SSE event. Correctness does not depend on event delivery.

import { randomUUID } from "node:crypto";
import { eq, and, inArray, isNotNull } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "./db.js";
import { discussState } from "./schema.js";
import path from "node:path";
import os from "node:os";
import { mkdirSync } from "node:fs";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

function tryParse(s, fallback) {
  if (s == null) return fallback;
  try { return JSON.parse(s); } catch { return fallback; }
}

export function createDiscussStore(opts = {}) {
  const _p = _createDiscussStoreImpl(opts);
  return new Proxy({}, { get(_, k) {
    if (k === "then" || typeof k === "symbol") return undefined;
    if (k === "ready") return () => _p.then(() => {});
    return (...a) => _p.then(s => { if (typeof s[k] !== "function") throw new TypeError(`discuss-store: ${String(k)} is not a function`); return s[k](...a); });
  } });
}

async function _createDiscussStoreImpl({ dbPath = defaultDbPath(), logger } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  const _where = (flowId, itemId) => and(eq(discussState.flowId, flowId), eq(discussState.itemId, itemId));

  async function ensureRow(flowId, itemId) {
    await withSharedWrite(dbPath, () =>
      db.insert(discussState).values({ flowId, itemId, updatedAt: Date.now() }).onConflictDoNothing()
    );
  }

  async function getRow(flowId, itemId) {
    const rows = await db.select().from(discussState).where(_where(flowId, itemId));
    return rows[0] ?? null;
  }

  async function getState(flowId, itemId) {
    const row = await getRow(flowId, itemId);
    if (!row) return null;
    return {
      messages:     tryParse(row.messagesJson, []),
      turnStatus:   row.turnStatus    ?? "idle",
      activeTurnId: row.activeTurnId  ?? null,
      pending:      normalizePending(tryParse(row.pendingJson, null), row.updatedAt),
      components:   tryParse(row.componentsJson, []),
      updatedAt:    row.updatedAt,
    };
  }

  async function appendMessage(flowId, itemId, msg) {
    await ensureRow(flowId, itemId);
    const row = await getRow(flowId, itemId);
    const messages = tryParse(row?.messagesJson, []);
    messages.push({
      id:            msg.id            ?? randomUUID(),
      turnId:        msg.turnId        ?? null,
      role:          msg.role,
      text:          msg.text          ?? null,
      audioKey:      msg.audioKey      ?? null,
      proposedDraft: msg.proposedDraft ?? null,
      proposedMeta:  msg.proposedMeta  ?? null,
      error:         msg.error         ?? null,
      ts:            msg.ts            ?? Date.now(),
    });
    await withSharedWrite(dbPath, () =>
      db.update(discussState)
        .set({ messagesJson: JSON.stringify(messages), updatedAt: Date.now() })
        .where(_where(flowId, itemId))
    );
  }

  async function stampMessageError(flowId, itemId, turnId, errorText) {
    await ensureRow(flowId, itemId);
    const row = await getRow(flowId, itemId);
    const messages = tryParse(row?.messagesJson, []);
    let idx = -1;
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === "operator" && (!turnId || messages[i].turnId === turnId)) {
        idx = i; break;
      }
    }
    if (idx !== -1) {
      messages[idx] = { ...messages[idx], error: errorText };
      await withSharedWrite(dbPath, () =>
        db.update(discussState)
          .set({ messagesJson: JSON.stringify(messages), updatedAt: Date.now() })
          .where(_where(flowId, itemId))
      );
    }
  }

  async function setTurnStatus(flowId, itemId, status, activeTurnId = null) {
    await ensureRow(flowId, itemId);
    await withSharedWrite(dbPath, () =>
      db.update(discussState)
        .set({ turnStatus: status, activeTurnId: activeTurnId ?? null, updatedAt: Date.now() })
        .where(_where(flowId, itemId))
    );
  }

  // pending_json's own updatedAt is the proposal's timestamp — the row's
  // updated_at also moves on every message append, so it cannot stand in
  // for it. Blobs written before the blob carried its own timestamp fall
  // back to the row's last activity, the earliest moment the proposal
  // could have existed.
  function normalizePending(blob, rowUpdatedAt) {
    if (!blob || typeof blob !== "object" || Array.isArray(blob)) return null;
    const out = {
      draftId: blob.draftId ?? null,
      updatedAt: typeof blob.updatedAt === "number" ? blob.updatedAt : Number(rowUpdatedAt),
    };
    if (typeof blob.text === "string") out.text = blob.text;
    if (blob.fields && typeof blob.fields === "object" && !Array.isArray(blob.fields)) out.fields = blob.fields;
    return out;
  }

  async function readPending(flowId, itemId) {
    if (!flowId || !itemId) return null;
    const row = await getRow(flowId, itemId);
    if (!row) return null;
    return normalizePending(tryParse(row.pendingJson, null), row.updatedAt);
  }

  // Merges with any existing fields so a later text write doesn't wipe
  // earlier field writes, and keeps the prior draft slot when the caller
  // names none — the slot picks which draft apply-draft commits on
  // multi-draft gates.
  async function setPending(flowId, itemId, text, draftId = null) {
    await ensureRow(flowId, itemId);
    const row = await getRow(flowId, itemId);
    const existing = tryParse(row?.pendingJson, {}) ?? {};
    const next = {
      ...existing,
      text,
      draftId: draftId ?? existing.draftId ?? null,
      updatedAt: Date.now(),
    };
    await withSharedWrite(dbPath, () =>
      db.update(discussState)
        .set({ pendingJson: JSON.stringify(next), updatedAt: Date.now() })
        .where(_where(flowId, itemId))
    );
  }

  async function mergePendingFields(flowId, itemId, fields) {
    await ensureRow(flowId, itemId);
    const row = await getRow(flowId, itemId);
    const existing = tryParse(row?.pendingJson, {}) ?? {};
    const next = {
      ...existing,
      fields: { ...(existing.fields ?? {}), ...fields },
      updatedAt: Date.now(),
    };
    await withSharedWrite(dbPath, () =>
      db.update(discussState)
        .set({ pendingJson: JSON.stringify(next), updatedAt: Date.now() })
        .where(_where(flowId, itemId))
    );
  }

  async function clearPending(flowId, itemId) {
    await withSharedWrite(dbPath, () =>
      db.update(discussState)
        .set({ pendingJson: null, updatedAt: Date.now() })
        .where(_where(flowId, itemId))
    );
  }

  async function setComponent(flowId, itemId, component) {
    if (!component?.kind) return;
    await ensureRow(flowId, itemId);
    const row = await getRow(flowId, itemId);
    const current = tryParse(row?.componentsJson, []);
    const next = [
      ...current.filter((c) => c.kind !== component.kind),
      { ...component, ts: component.ts ?? Date.now() },
    ];
    await withSharedWrite(dbPath, () =>
      db.update(discussState)
        .set({ componentsJson: JSON.stringify(next), updatedAt: Date.now() })
        .where(_where(flowId, itemId))
    );
  }

  async function getComponents(flowId, itemId) {
    const row = await getRow(flowId, itemId);
    return tryParse(row?.componentsJson, []);
  }

  async function getBulkSummaries(flowIds) {
    if (!flowIds || flowIds.length === 0) return {};
    const rows = await db.select({ flowId: discussState.flowId, itemId: discussState.itemId })
      .from(discussState)
      .where(and(inArray(discussState.flowId, flowIds), isNotNull(discussState.pendingJson)));
    const result = {};
    for (const row of rows) {
      if (!result[row.flowId]) result[row.flowId] = { pendingItemIds: [] };
      result[row.flowId].pendingItemIds.push(row.itemId);
    }
    return result;
  }

  // Transcript listing builds its discuss entries from these rows so both
  // directions of every dialog-door turn come from this store, keyed by
  // (flowId, itemId) — never from harness session files.
  async function listForFlow(flowId) {
    if (!flowId) return [];
    const rows = await db.select().from(discussState)
      .where(eq(discussState.flowId, flowId));
    const entries = rows.map((row) => ({
      itemId: row.itemId,
      messages: tryParse(row.messagesJson, []),
      turnStatus: row.turnStatus ?? "idle",
      updatedAt: Number(row.updatedAt),
    }));
    return entries.sort((a, b) => a.updatedAt - b.updatedAt);
  }

  return {
    getState,
    getBulkSummaries,
    listForFlow,
    appendMessage,
    stampMessageError,
    setTurnStatus,
    setPending,
    readPending,
    mergePendingFields,
    clearPending,
    setComponent,
    getComponents,
    close() { try { client.close(); } catch { /* ignore */ } },
  };
}
