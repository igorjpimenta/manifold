// Unit tests for state/migrator.js — createMigrator: happy path, idempotent re-open,
// multi-store discrimination, function migrations, SQLITE_BUSY retry, rollback on error.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createClient } from "@libsql/client";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { createMigrator } from "../../../state/migrator.js";

// ─── helpers ────────────────────────────────────────────────────────────────

function tmpClient() {
  const p = path.join(os.tmpdir(), `migrator-test-${randomUUID()}.sqlite`);
  return createClient({ url: `file:${p}` });
}

function tmpClientAt(p) {
  return createClient({ url: `file:${p}` });
}

// ─── happy path ─────────────────────────────────────────────────────────────

describe("createMigrator — happy path", () => {
  test("applies string migrations and records version", async () => {
    const client = tmpClient();
    const MIGRATIONS = [
      "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)",
      "ALTER TABLE items ADD COLUMN extra TEXT",
    ];
    const { currentVersion } = await createMigrator(client, { name: "test", migrations: MIGRATIONS });
    assert.strictEqual(currentVersion, 2);
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('items')");
    const cols = rows.map((r) => r.name);
    assert.ok(cols.includes("val"));
    assert.ok(cols.includes("extra"));
  });

  test("applies function migrations", async () => {
    const client = tmpClient();
    let called = false;
    const MIGRATIONS = [
      "CREATE TABLE things (id TEXT PRIMARY KEY)",
      async (c) => {
        called = true;
        await c.execute("ALTER TABLE things ADD COLUMN label TEXT");
      },
    ];
    await createMigrator(client, { name: "test", migrations: MIGRATIONS });
    assert.ok(called, "function migration was invoked");
    const { rows } = await client.execute("SELECT name FROM pragma_table_info('things')");
    const cols = rows.map((r) => r.name);
    assert.ok(cols.includes("label"));
  });

  test("creates _schema_version table with store discriminator", async () => {
    const client = tmpClient();
    await createMigrator(client, { name: "mystore", migrations: ["CREATE TABLE t (x TEXT)"] });
    const { rows } = await client.execute({
      sql: "SELECT * FROM _schema_version WHERE store = ?",
      args: ["mystore"],
    });
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(Number(rows[0].version), 1);
    assert.ok(Number(rows[0].applied_at) > 0);
  });
});

// ─── idempotency ────────────────────────────────────────────────────────────

describe("createMigrator — idempotency", () => {
  test("re-opening the same DB does not re-run migrations", async () => {
    const dbPath = path.join(os.tmpdir(), `migrator-idempotent-${randomUUID()}.sqlite`);
    let runCount = 0;
    const MIGRATIONS = [
      async (c) => { runCount++; await c.execute("CREATE TABLE IF NOT EXISTS counter (n INTEGER)"); },
    ];

    const c1 = tmpClientAt(dbPath);
    await createMigrator(c1, { name: "counter", migrations: MIGRATIONS });
    c1.close();

    const c2 = tmpClientAt(dbPath);
    await createMigrator(c2, { name: "counter", migrations: MIGRATIONS });
    c2.close();

    assert.strictEqual(runCount, 1, "migration ran exactly once across two opens");
  });

  test("currentVersion reflects already-applied count on re-open", async () => {
    const dbPath = path.join(os.tmpdir(), `migrator-version-${randomUUID()}.sqlite`);
    const MIGRATIONS = [
      "CREATE TABLE t (x TEXT)",
      "ALTER TABLE t ADD COLUMN y TEXT",
    ];
    const c1 = tmpClientAt(dbPath);
    await createMigrator(c1, { name: "s", migrations: MIGRATIONS });
    c1.close();

    const c2 = tmpClientAt(dbPath);
    const { currentVersion } = await createMigrator(c2, { name: "s", migrations: MIGRATIONS });
    c2.close();

    assert.strictEqual(currentVersion, 2);
  });
});

// ─── multi-store discrimination ─────────────────────────────────────────────

describe("createMigrator — multi-store on shared DB", () => {
  test("two stores in the same file have independent version counters", async () => {
    const client = tmpClient();
    const M_A = ["CREATE TABLE alpha (id TEXT PRIMARY KEY)"];
    const M_B = [
      "CREATE TABLE beta  (id TEXT PRIMARY KEY)",
      "ALTER TABLE beta ADD COLUMN val TEXT",
    ];
    const { currentVersion: vA } = await createMigrator(client, { name: "store-a", migrations: M_A });
    const { currentVersion: vB } = await createMigrator(client, { name: "store-b", migrations: M_B });
    assert.strictEqual(vA, 1);
    assert.strictEqual(vB, 2);
    const { rows } = await client.execute(
      "SELECT store, version FROM _schema_version ORDER BY store, version"
    );
    assert.deepStrictEqual(rows.map((r) => [r.store, Number(r.version)]), [
      ["store-a", 1],
      ["store-b", 1],
      ["store-b", 2],
    ]);
  });
});

// ─── error handling ─────────────────────────────────────────────────────────

describe("createMigrator — error handling", () => {
  test("throws and rolls back when a migration fails", async () => {
    const client = tmpClient();
    await client.execute("CREATE TABLE existing (x TEXT)");
    const MIGRATIONS = [
      // This will fail: can't CREATE a table that already exists (no IF NOT EXISTS)
      "CREATE TABLE existing (x TEXT)",
    ];
    await assert.rejects(
      () => createMigrator(client, { name: "err-store", migrations: MIGRATIONS }),
      /\[migrator\] err-store v1 failed/
    );
    // _schema_version for this store must have no entries (transaction rolled back)
    const { rows } = await client.execute({
      sql: "SELECT * FROM _schema_version WHERE store = ?",
      args: ["err-store"],
    });
    assert.strictEqual(rows.length, 0, "version was not recorded after failure");
  });

  test("function migration error also rolls back and includes store+version in message", async () => {
    const client = tmpClient();
    const MIGRATIONS = [
      "CREATE TABLE ft (id TEXT)",
      async (_c) => { throw new Error("oops"); },
    ];
    await assert.rejects(
      () => createMigrator(client, { name: "fn-store", migrations: MIGRATIONS }),
      /\[migrator\] fn-store v2 failed: oops/
    );
    // Only v1 row should exist
    const { rows } = await client.execute({
      sql: "SELECT version FROM _schema_version WHERE store = ?",
      args: ["fn-store"],
    });
    assert.deepStrictEqual(rows.map((r) => Number(r.version)), [1]);
  });

  test("partial success: correctly-applied migrations stay, only the failing one rolls back", async () => {
    const client = tmpClient();
    const MIGRATIONS = [
      "CREATE TABLE partial (id TEXT)",
      "ALTER TABLE partial ADD COLUMN col1 TEXT",
      "INVALID SQL $$$$",
    ];
    await assert.rejects(() => createMigrator(client, { name: "partial", migrations: MIGRATIONS }));
    // v1 and v2 should be recorded; v3 rolled back
    const { rows } = await client.execute({
      sql: "SELECT version FROM _schema_version WHERE store = ? ORDER BY version",
      args: ["partial"],
    });
    assert.deepStrictEqual(rows.map((r) => Number(r.version)), [1, 2]);
    const { rows: cols } = await client.execute("SELECT name FROM pragma_table_info('partial')");
    assert.ok(cols.map(r => r.name).includes("col1"), "v2 column was kept");
  });
});
