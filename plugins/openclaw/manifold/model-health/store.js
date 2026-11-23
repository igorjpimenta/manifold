// model-health/store.js — durable state for the model-routing circuit breaker.
//
// Storage: manifold's runtime DB (resolveStorePaths().runtimeDb) — the same
// file and shared connection every manifold store uses, so the breaker's rows
// and the engine's cannot end up in two files. It is deliberately a SEPARATE
// table rather than a `kind` column on node_health: a model entry is either a
// terminal block (until = NULL, lifted only by a probe) or a rate-limit
// quarantine carrying its own deadline, while a node entry has no deadline at
// all. One `until` column would mean two different things.
//
// Deliberately NOT persisted: the breaker's failure buckets. They hold
// timestamps inside a failureWindowMs window, so every entry is stale by the
// time a restart could read it back.

import { mkdirSync } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { eq, and, inArray } from "drizzle-orm";
import { getSharedDb, withSharedWrite, resolveStorePaths } from "../../../../state/db.js";
import { enqueue, flush as flushBatchWriter } from "../../../../state/batch-writer.js";
import { modelHealth } from "../../../../state/schema.js";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "../../../../state/migrate.js";

// The table itself comes from manifold's runtime migration. This index does
// not: put() relies on it to collapse two concurrent trips on the same ref
// into one row.
const MODEL_HEALTH_INDEX_DDL = `CREATE UNIQUE INDEX IF NOT EXISTS idx_model_health_one_active
  ON model_health (ref)
  WHERE status IN ('blocked', 'quarantined')`;

const toRow = (ref, e, strike, now) => ({
  id: randomUUID(),
  ref,
  status: e.until == null ? "blocked" : "quarantined",
  reason: e.reason,
  downSince: e.downSince,
  untilMs: e.until ?? null,
  probing: e.probing ? 1 : 0,
  strikeLevel: strike?.level ?? 0,
  strikeEndedAt: strike?.endedAt ?? null,
  createdAt: e.downSince,
  updatedAt: now,
});

export async function createModelHealthStore({ logger, dbPath = resolveStorePaths().runtimeDb } = {}) {
  mkdirSync(path.dirname(dbPath), { recursive: true });
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const { db, client } = await getSharedDb(dbPath);
  await withSharedWrite(dbPath, () => client.execute(MODEL_HEALTH_INDEX_DDL));

  // Write-behind, like the engine's other stores: the caller is a synchronous
  // hook that must not await, and a lost trailing write only costs one
  // quarantine that the next failure re-trips anyway.
  function put(ref, entry, strike) {
    if (!ref) return;
    enqueue(dbPath, db.insert(modelHealth).values(toRow(ref, entry, strike, Date.now())).onConflictDoNothing());
  }

  function remove(ref) {
    if (!ref) return;
    enqueue(dbPath, db.delete(modelHealth).where(eq(modelHealth.ref, ref)));
  }

  function clearAll() {
    enqueue(dbPath, db.delete(modelHealth));
  }

  // Drops quarantines whose deadline passed while the process was down, so a
  // stale row cannot outlive its own expiry and block a model forever. A
  // terminal block is never dropped — it has no deadline and only a probe or
  // an explicit clear() lifts it.
  async function load(now = Date.now()) {
    const rows = await db.select().from(modelHealth)
      .where(inArray(modelHealth.status, ["blocked", "quarantined"]));
    const live = [];
    const expired = [];
    for (const r of rows) {
      const e = {
        downSince: r.downSince,
        reason: r.reason,
        until: r.untilMs,
        probing: !!r.probing,
      };
      if (e.until != null && now >= e.until) {
        expired.push(r.ref);
      } else {
        live.push({
          ref: r.ref,
          entry: e,
          strike: r.strikeLevel > 0 || r.strikeEndedAt != null
            ? { level: r.strikeLevel, endedAt: r.strikeEndedAt }
            : null,
        });
      }
    }
    if (expired.length) {
      await withSharedWrite(dbPath, () =>
        db.delete(modelHealth).where(inArray(modelHealth.ref, expired)),
      );
      logger?.info?.(
        `model-health: dropped ${expired.length} quarantine row(s) whose deadline passed while down`,
      );
    }
    return live;
  }

  async function flush() {
    await flushBatchWriter(dbPath);
  }

  return { put, remove, clearAll, load, flush, and };
}
