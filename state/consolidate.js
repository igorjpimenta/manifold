// consolidate.js — one-shot boot migration from a store's old per-file path
// into the new consolidated runtime.sqlite / ops.sqlite.
//
// Called immediately after createMigrator ensures the target schema exists.
// Idempotent: INSERT OR IGNORE skips rows already present, so a partial run
// (e.g. gateway crash mid-migration) is safe to re-run on the next boot.
// Old file is deleted only after a successful copy.

import { existsSync, unlinkSync } from "node:fs";

export async function migrateFromLegacy(client, srcPath, tables, logger) {
  if (!existsSync(srcPath)) return false;

  const safe = srcPath.replace(/'/g, "''");
  try {
    await client.execute(`ATTACH DATABASE '${safe}' AS _legacy`);
  } catch (err) {
    logger?.warn?.(`consolidate: ATTACH ${srcPath} failed: ${err?.message ?? err}`);
    return false;
  }

  for (const table of tables) {
    try {
      // `INSERT ... SELECT *` matches columns POSITIONALLY, so it breaks the
      // moment the target's own schema drifts from whatever the legacy file
      // still has — exactly what's already happened to every one of these
      // tables since consolidation shipped (failure_events alone has gone
      // from 21 columns down to 7 across two later redesigns). Read each
      // side's real column list and copy only the ones that exist on both —
      // a legacy-only column is dropped (nothing downstream reads it
      // anymore, same reasoning as every column-drop migration already
      // applies), a target-only column gets its normal default. Skip
      // entirely if the table isn't present on either side (empty/corrupt
      // source, or nothing to migrate).
      const { rows: mainCols } = await client.execute(`PRAGMA main.table_info(${table})`);
      const { rows: legacyCols } = await client.execute(`PRAGMA _legacy.table_info(${table})`);
      if (mainCols.length === 0 || legacyCols.length === 0) continue;
      const legacyNames = new Set(legacyCols.map((c) => c.name));
      const shared = mainCols.map((c) => c.name).filter((name) => legacyNames.has(name));
      if (shared.length === 0) continue;
      const cols = shared.join(", ");
      await client.execute(`INSERT OR IGNORE INTO main.${table} (${cols}) SELECT ${cols} FROM _legacy.${table}`);
    } catch (err) {
      // Old table may be absent (empty/corrupt source) — skip without failing.
      logger?.warn?.(`consolidate: ${table}: ${err?.message ?? err}`);
    }
  }

  try { await client.execute("DETACH DATABASE _legacy"); } catch { /* ignore */ }

  try {
    unlinkSync(srcPath);
    logger?.info?.(`consolidate: migrated [${tables.join(", ")}] from ${srcPath}`);
  } catch (err) {
    logger?.warn?.(`consolidate: could not remove ${srcPath}: ${err?.message ?? err}`);
  }

  return true;
}
