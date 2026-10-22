// state/migrate.js — snapshot migrate-on-open (winch src/db.js shape, but
// libsql-native: a second driver writing under live libsql handles leaves
// them stale (observed as SQLITE_IOERR_SHORT_READ under concurrency), so
// snapshots apply through a short-lived libsql client instead — same
// driver family as every other access, same WAL + busy_timeout posture.
//
// Schema DDL lives in state/schema.js (runtime.sqlite) and
// state/schema.ops.js (ops.sqlite); `npm run db:generate` snapshots them
// into .migrations/*/<n>.sql from it. This applies pending snapshots at
// open — AFTER each store's own createMigrator chain, never before: the
// legacy chain owns all pre-existing databases outright.
//
// Journal compatibility with drizzle-kit is exact, so `npm run db:migrate`
// and this runner agree on what is applied: the same
// __drizzle_migrations(id, hash, created_at) rows, hash = sha256 of the
// snapshot file, created_at = the journal entry's `when`, applied in
// journal order inside one transaction, only entries newer than the newest
// recorded row (mirrors SQLiteSyncDialect.migrate line for line).
//
// The 0000 baseline files carry hand-added IF NOT EXISTS on every CREATE
// (one-time anchor edit — drizzle-kit emits plain CREATE). That makes
// this a strict no-op on databases the legacy chain just shaped, while
// future snapshots (pure additive ALTERs the chain never mentions) apply
// normally. Never hand-edit 0001+.
import path from "node:path";
import { existsSync, mkdirSync, readFileSync, readdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { createClient } from "@libsql/client";
import { withSharedWrite } from "./db.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.join(HERE, "..");

export const RUNTIME_MIGRATIONS_FOLDER = path.join(REPO_ROOT, ".migrations", "runtime");
export const OPS_MIGRATIONS_FOLDER = path.join(REPO_ROOT, ".migrations", "ops");

function readJournal(migrationsFolder) {
  const journalPath = path.join(migrationsFolder, "meta", "_journal.json");
  if (!existsSync(journalPath)) {
    throw new Error(`migrate: no meta/_journal.json in ${migrationsFolder}`);
  }
  return JSON.parse(readFileSync(journalPath, "utf8"));
}

export async function migrateDatabase({ dbPath, migrationsFolder, logger } = {}) {
  const dir = path.dirname(dbPath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const journal = readJournal(migrationsFolder);
  const entries = [...journal.entries].sort((a, b) => a.idx - b.idx);
  const snapshots = entries.map((entry) => {
    const file = path.join(migrationsFolder, `${entry.tag}.sql`);
    if (!existsSync(file)) {
      throw new Error(`migrate: snapshot file missing: ${file}`);
    }
    const sql = readFileSync(file, "utf8");
    return {
      tag: entry.tag,
      when: entry.when,
      hash: createHash("sha256").update(sql).digest("hex"),
      statements: sql
        .split("--> statement-breakpoint")
        .map((s) => s.trim())
        .filter((s) => s.length > 0),
    };
  });

  const client = createClient({ url: `file:${dbPath}` });
  try {
    await client.execute("PRAGMA busy_timeout = 30000");
    await client.execute(
      `CREATE TABLE IF NOT EXISTS __drizzle_migrations (
         id SERIAL PRIMARY KEY,
         hash text NOT NULL,
         created_at numeric
       )`,
    );
    const applied = await client.execute(
      "SELECT id, hash, created_at FROM __drizzle_migrations ORDER BY created_at DESC LIMIT 1",
    );
    const last = applied.rows[0];
    const pending = snapshots.filter(
      (s) => !last || Number(last.created_at) < s.when,
    );
    if (pending.length === 0) return { applied: [] };
    // Serialized with every other in-process writer on this file: snapshot
    // application must never interleave with live traffic on the shared
    // connection the way overlapping createMigrator runs would.
    return await withSharedWrite(dbPath, async () => {
      await client.execute("BEGIN");
      try {
        const appliedTags = [];
        for (const snapshot of pending) {
          for (const stmt of snapshot.statements) {
            await client.execute(stmt);
          }
          await client.execute({
            sql: `INSERT INTO __drizzle_migrations ("hash", "created_at") VALUES(?, ?)`,
            args: [snapshot.hash, snapshot.when],
          });
          appliedTags.push(snapshot.tag);
        }
        await client.execute("COMMIT");
        logger?.info?.(`migrate: applied [${appliedTags.join(", ")}] at ${dbPath}`);
        return { applied: appliedTags };
      } catch (err) {
        try { await client.execute("ROLLBACK"); } catch {}
        throw err;
      }
    });
  } finally {
    client.close();
  }
}
