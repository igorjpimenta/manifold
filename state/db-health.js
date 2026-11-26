// db-health.js — synchronous per-file reachability probes for the boot
// gate and health endpoint. Synchronous better-sqlite3 on purpose:
// register() must stay synchronous, and a probe is one indexed read. A
// missing file reports missing WITHOUT creating it (better-sqlite3
// creates on open — a probe must never materialize databases).
import { existsSync } from "node:fs";
import Database from "better-sqlite3";

export function checkDb(dbPath) {
  const started = Date.now();
  if (!existsSync(dbPath)) {
    return { ok: false, latencyMs: Date.now() - started, error: `missing: ${dbPath}` };
  }
  let db = null;
  try {
    db = new Database(dbPath, { readonly: true });
    db.prepare("SELECT 1").get();
    return { ok: true, latencyMs: Date.now() - started };
  } catch (err) {
    return { ok: false, latencyMs: Date.now() - started, error: err?.message ?? String(err), code: err?.code };
  } finally {
    try { db?.close(); } catch {}
  }
}
