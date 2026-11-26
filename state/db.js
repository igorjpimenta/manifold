import { createClient } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import * as schema from './schema.js';
export { resolveStorePaths } from './paths.js';

// Returns { db, client }: db is the Drizzle ORM instance, client the raw
// @libsql/client handle for SQL the ORM can't express. readonly opens
// skip WAL/autocheckpoint/busy_timeout — writer concerns; WAL on the
// write connection already allows concurrent reads.
//
// `readonly` is a PRAGMA choice, not a read-only open: libsql's local
// client rejects `?mode=ro`, so every handle here can write. A closed
// handle checkpoints and unlinks the -wal/-shm, breaking any other
// connection still holding them — use getSharedDb() while the gateway
// runs; close() only at process shutdown or on a per-test file nobody
// else shares.
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

// Process-wide, path-keyed connection cache: every state/*.js store
// shares one @libsql/client connection per file — separate connections
// to the same runtime.sqlite all compete for that one file's write lock
// instead of sharing a single connection the way SQLite's single-writer
// model expects. Keyed by `${dbPath}::${readonly}` (not bare dbPath) so a
// readonly-scoped and a writer-scoped caller against the same file get
// distinct connections — readonly opens skip the writer PRAGMAs and
// shouldn't be conflated with the writer's.
const _sharedDbCache = new Map(); // key -> Promise<{ db, client }>

// Self-healing: stores' close()/test-cleanup paths call client.close()
// directly on what is now a SHARED connection — a store can legitimately
// be closed and later reopened against the same path. Wrapping close() to
// evict the cache entry first means the next getSharedDb() opens a fresh
// connection instead of handing back a dead one that throws CLIENT_CLOSED.
// Tradeoff: closing a shared connection closes it for every other store
// sharing it too — acceptable because production close() is a boot-open,
// run-until-exit lifecycle; it's exercised by test cleanup and isolated
// per-test dbPaths, which don't share cache keys with each other.
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

// Per-dbPath in-process write queue, shared by every store that routes
// through getSharedDb(). Keyed by bare dbPath, not the readonly-qualified
// getSharedDb() cache key — SQLITE_BUSY is a property of the underlying
// FILE, so a write on the writer connection and one on a readonly
// connection to the same file still serialize against each other here.
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

// Runs `fn()` serialized against every other withSharedWrite() call for
// the same dbPath, retrying the whole call on SQLITE_BUSY. Never awaits
// the previous queue occupant's *result* — `.then(attempt, attempt)` runs
// the next attempt whether the prior one resolved or rejected, so one
// caller's failure can never poison the queue for callers after it.
//
// `fn` must be safe to re-run from scratch on a BUSY retry: either a
// single statement (atomic on its own) or a caller-owned
// client.transaction() block (nothing durable commits until tx.commit()).
// Do NOT pass multiple independent statements with no enclosing
// transaction — a BUSY on the second would silently re-run the first too.
export function withSharedWrite(dbPath, fn, { maxAttempts = 200 } = {}) {
  const prevTail = _writeQueues.get(dbPath) ?? Promise.resolve();
  const attempt = () => _withRetry(fn, maxAttempts);
  const result = prevTail.then(attempt, attempt);
  _writeQueues.set(dbPath, result.then(() => {}, () => {}));
  return result;
}
