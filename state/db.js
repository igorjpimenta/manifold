import { createClient } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import * as schema from './schema.js';
export { resolveStorePaths } from './paths.js';

// Returns { db, client } where:
//   db     — Drizzle ORM instance; use for typed queries
//   client — raw @libsql/client handle; use for raw SQL the ORM can't express
//
// readonly opens skip WAL/autocheckpoint/busy_timeout — those are writer concerns.
// WAL mode on the write connection already allows concurrent reads without extra config.
//
// `readonly` is a PRAGMA choice, not a read-only open: libsql's local client
// rejects `?mode=ro` (URL_PARAM_NOT_SUPPORTED), so every handle here can write.
// A closed handle therefore checkpoints and unlinks the -wal/-shm, which breaks
// any other connection (in this process or another) still holding them. Use
// getSharedDb() for anything opened while the gateway runs; close() only at
// process shutdown or on a per-test file that nobody else shares.
export async function openDb(dbPath, { readonly = false } = {}) {
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute("PRAGMA foreign_keys = ON");
  if (!readonly) {
    await client.execute("PRAGMA busy_timeout = 30000");
    await client.execute("PRAGMA journal_mode = WAL");
    await client.execute("PRAGMA wal_autocheckpoint = 200");
  }
  const db = drizzle(client, { schema });
  return { db, client };
}

// Process-wide, path-keyed connection cache: every state/*.js store module
// shares one @libsql/client connection per file rather than each opening its
// own, since separate connections to the same runtime.sqlite all separately
// compete for that one file's write lock (busy_timeout=30000 above) instead
// of sharing a single connection the way SQLite's own single-writer model
// expects. Keyed by `${dbPath}::${readonly}` (not bare dbPath) so a
// readonly-scoped caller and
// a writer-scoped caller against the same file get distinct connections —
// readonly opens skip the writer PRAGMAs above and shouldn't be conflated
// with the writer's.
const _sharedDbCache = new Map(); // key -> Promise<{ db, client }>

// Self-healing: several stores' close()/test-cleanup paths call client.close()
// directly on what is now a SHARED connection (e.g. run-store.test.js's
// "opening same dbPath twice" idempotency check, session-index.test.js's
// "persists across re-open of same DB file") — a real pattern, not just a
// test artifact: a store can legitimately be closed and later reopened
// against the same path. Wrapping close() to evict the cache entry first
// means the next getSharedDb() call for that key opens a genuinely fresh
// connection instead of handing back a dead one that throws CLIENT_CLOSED
// on first use. The tradeoff: closing a shared connection closes it for
// every other store currently sharing it too — acceptable here because nothing
// in production calls close() on a live store mid-process (it's a boot-time-
// open, run-until-exit lifecycle); close() is exercised by test cleanup and
// isolated per-test dbPaths, which don't share cache keys with each other.
export function getSharedDb(dbPath, { readonly = false } = {}) {
  const key = `${dbPath}::${readonly}`;
  let entry = _sharedDbCache.get(key);
  if (!entry) {
    entry = openDb(dbPath, { readonly }).then(({ db, client }) => {
      const originalClose = client.close.bind(client);
      client.close = (...args) => {
        _sharedDbCache.delete(key);
        return originalClose(...args);
      };
      return { db, client };
    });
    _sharedDbCache.set(key, entry);
  }
  return entry;
}

export function isSqliteBusy(err) {
  return String(err?.code ?? err?.message ?? "").includes("SQLITE_BUSY");
}

// Per-dbPath in-process write queue — the same serialize()/withWriteTx()
// idiom admission-queue-store.js uses, generalized here so every store
// sharing a connection via getSharedDb() gets it for free instead of each
// one reimplementing its own copy. (Its own test suite drives 30 genuinely
// concurrent admit() callers through it.)
//
// Keyed by bare dbPath, not the readonly-qualified getSharedDb() cache key
// — SQLITE_BUSY contention is a property of the underlying FILE, not of
// which connection variant issued the call, so a write on the writer
// connection and one on a readonly connection to the same file still need
// to serialize against each other here.
const _writeQueues = new Map(); // dbPath -> Promise (queue tail)

async function _withRetry(fn, maxAttempts) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt < maxAttempts - 1 && isSqliteBusy(err)) {
        await new Promise((r) => setTimeout(r, 2 + Math.floor(Math.random() * 8)));
        continue;
      }
      throw err;
    }
  }
}

// Runs `fn()` serialized against every other withSharedWrite() call for the
// same dbPath, retrying the whole call on SQLITE_BUSY. Never awaits the
// previous queue occupant's *result* — `.then(attempt, attempt)` runs the
// next attempt whether the prior one resolved or rejected, so one caller's
// failure can never poison the queue for callers after it.
//
// `fn` must be safe to re-run from scratch on a BUSY retry: either a single
// statement (atomic on its own — SQLite's own per-statement guarantee) or a
// caller-owned client.transaction() block (nothing durable commits until
// tx.commit(), so a retry before that point has zero partial effect). Do
// NOT pass multiple independent statements with no enclosing transaction —
// a BUSY on the second would silently re-run the first too.
export function withSharedWrite(dbPath, fn, { maxAttempts = 200 } = {}) {
  const prevTail = _writeQueues.get(dbPath) ?? Promise.resolve();
  const attempt = () => _withRetry(fn, maxAttempts);
  const result = prevTail.then(attempt, attempt);
  _writeQueues.set(dbPath, result.then(() => {}, () => {}));
  return result;
}
