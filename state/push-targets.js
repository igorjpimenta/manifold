// push-targets.js — per-harness URL push targets.
//
// One row per harness id: the URL manifold POSTs harness lifecycle events
// to (agent.dispatched, gate.opened, push.test), plus optional bearer and
// timeout. Written by autoregisterPushTarget (harness/openclaw.js); read by
// resolvePushTarget (harness/push.js) on every notify. Same sqlite +
// migrate-on-open shape as node-health-store.js.

import { eq } from "drizzle-orm";
import { mkdirSync } from "node:fs";
import path from "node:path";
import { getSharedDb, resolveStorePaths } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { pushTargets } from "./schema.js";

export async function createPushTargetsStore({ logger, dbPath = resolveStorePaths().runtimeDb } = {}) {
  void logger;
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  function toPublic(row) {
    if (!row) return null;
    return {
      harness: row.harness,
      url: row.url,
      bearer: row.bearer ?? null,
      timeoutMs: row.timeoutMs ?? null,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    };
  }

  async function get(harness) {
    if (!harness) return null;
    const rows = await db.select().from(pushTargets).where(eq(pushTargets.harness, harness)).limit(1);
    return toPublic(rows[0] ?? null);
  }

  // Idempotent: same harness re-registers without duplicating.
  async function upsert(harness, { url, bearer = null, timeoutMs = null } = {}) {
    if (!harness || typeof url !== "string" || url.trim() === "") {
      throw new Error("push-targets upsert requires harness and url");
    }
    const now = Date.now();
    await db.insert(pushTargets).values({
      harness,
      url: url.trim(),
      bearer: bearer ?? null,
      timeoutMs: Number.isInteger(timeoutMs) ? timeoutMs : null,
      createdAt: now,
      updatedAt: now,
    }).onConflictDoUpdate({
      target: pushTargets.harness,
      set: { url: url.trim(), bearer: bearer ?? null, timeoutMs: Number.isInteger(timeoutMs) ? timeoutMs : null, updatedAt: now },
    });
    return get(harness);
  }

  async function getAll() {
    return (await db.select().from(pushTargets)).map(toPublic);
  }

  async function remove(harness) {
    if (!harness) return { removed: 0 };
    const r = await db.delete(pushTargets).where(eq(pushTargets.harness, harness));
    return { removed: r.rowsAffected ?? 0 };
  }

  function close() {
    client.close();
  }

  return { upsert, get, getAll, remove, close };
}
