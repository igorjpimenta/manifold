// createMigrator(client, { name, migrations })
//
// Unified schema migration runner for all manifold SQLite stores.
// Designed to work with a shared DB file (Phase 1 consolidation) or a
// per-store file — the `name` discriminator keeps version rows separate.
//
// Each migration is a SQL string OR an async function (client) => void.
// Index 0 → version 1, index 1 → version 2, etc.
//
// Each migration runs in a transaction. On SQLITE_BUSY the runner retries
// once before surfacing the error. Any other throw rolls back and rethrows.
//
// Returns { currentVersion } after all pending migrations are applied.

function splitSql(sql) {
  return sql.split(';').map(s => s.trim()).filter(Boolean);
}

export async function createMigrator(client, { name, migrations }) {
  await client.execute(
    `CREATE TABLE IF NOT EXISTS _schema_version (
       store      TEXT    NOT NULL,
       version    INTEGER NOT NULL,
       applied_at INTEGER NOT NULL,
       PRIMARY KEY (store, version)
     )`
  );

  const { rows } = await client.execute({
    sql: `SELECT COALESCE(MAX(version), 0) AS v FROM _schema_version WHERE store = ?`,
    args: [name],
  });
  const current = Number(rows[0]?.v ?? 0);

  for (let i = current; i < migrations.length; i++) {
    const version = i + 1;
    const migration = migrations[i];
    let attempt = 0;
    while (true) {
      try {
        await client.execute("BEGIN");
        try {
          if (typeof migration === "function") {
            await migration(client);
          } else {
            for (const sql of splitSql(migration)) {
              await client.execute(sql);
            }
          }
          await client.execute({
            sql: `INSERT INTO _schema_version(store, version, applied_at) VALUES (?, ?, ?)`,
            args: [name, version, Date.now()],
          });
          await client.execute("COMMIT");
        } catch (err) {
          try { await client.execute("ROLLBACK"); } catch {}
          throw err;
        }
        break;
      } catch (err) {
        if (attempt === 0 && String(err?.message ?? "").includes("SQLITE_BUSY")) {
          attempt++;
          continue;
        }
        throw new Error(`[migrator] ${name} v${version} failed: ${err?.message ?? err}`);
      }
    }
  }

  const { rows: rows2 } = await client.execute({
    sql: `SELECT COALESCE(MAX(version), 0) AS v FROM _schema_version WHERE store = ?`,
    args: [name],
  });
  return { currentVersion: Number(rows2[0]?.v ?? 0) };
}
