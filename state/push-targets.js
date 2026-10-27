// state/push-targets.js — per-harness push (webhook) targets. Core-owned
// delivery table: when an agent turn is dispatched or a human gate opens,
// core POSTs the event payload here so the harness can wake the receiver
// instead of waiting for a poll tick.
//
// Registration is a deliberate admin act (adapter autoregister or operator),
// one row per harness. Delivery never fails the run: bounded timeout,
// network/HTTP errors become a warning. No target = no-op.
import { eq } from "drizzle-orm";
import { mkdirSync } from "node:fs";
import path from "node:path";
import { getSharedDb, resolveStorePaths, withSharedWrite } from "./db.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "./migrate.js";
import { pushTargets } from "./schema.js";

function defaultDbPath() {
  return resolveStorePaths().runtimeDb;
}

export async function createPushTargetsStore({ logger, dbPath = defaultDbPath() } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);

  async function upsert({ harness, url, bearer = null, timeoutMs = null }) {
    if (!harness || !url) throw new Error("push-targets: harness and url are required");
    const now = Date.now();
    return withSharedWrite(dbPath, () =>
      db.insert(pushTargets).values({
        harness, url, bearer, timeoutMs, createdAt: now, updatedAt: now,
      }).onConflictDoUpdate({
        target: pushTargets.harness,
        set: { url, bearer, timeoutMs, updatedAt: now },
      }));
  }

  async function get(harness) {
    if (!harness) return null;
    const rows = await db.select().from(pushTargets).where(eq(pushTargets.harness, harness)).limit(1);
    return rows[0] ?? null;
  }

  async function getAll() {
    return db.select().from(pushTargets);
  }

  async function remove(harness) {
    if (!harness) return { removed: 0 };
    const r = await withSharedWrite(dbPath, () =>
      db.delete(pushTargets).where(eq(pushTargets.harness, harness)));
    return { removed: r.rowsAffected ?? 0 };
  }

  function close() {
    client.close();
  }

  return { upsert, get, getAll, remove, close };
}
