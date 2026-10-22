/**
 * Unit tests for state/migrate.js + schema parity.
 *
 * Verifies:
 *   - Fresh DBs migrate to the full expected table set (runtime + ops),
 *     with a journal row; reruns are idempotent.
 *   - migrateDatabase() after the legacy createMigrator chain is a no-op
 *     success (ordering coexistence — the chain shapes, snapshots confirm).
 *   - Schema parity: every table the real store boot produces matches the
 *     drizzle def column-for-column. A new inline column without a schema.js
 *     update (or vice versa) fails here — that is the whole point.
 */
import { test, describe, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { getTableConfig } from "drizzle-orm/sqlite-core";
import {
  migrateDatabase,
  RUNTIME_MIGRATIONS_FOLDER,
  OPS_MIGRATIONS_FOLDER,
} from "../../../state/migrate.js";
import * as runtimeSchema from "../../../state/schema.js";
import * as opsSchema from "../../../state/schema.ops.js";

const silentLogger = { info: () => {}, warn: () => {}, error: () => {} };

function tmpDb(name) {
  const dir = path.join(os.tmpdir(), `migrate-test-${randomUUID()}`);
  fs.mkdirSync(dir, { recursive: true });
  return { dir, dbPath: path.join(dir, `${name}.sqlite`) };
}

function liveTables(dbPath) {
  const db = new Database(dbPath, { readonly: true });
  try {
    return db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
      .all()
      .map((r) => r.name);
  } finally {
    db.close();
  }
}

function liveColumns(dbPath, table) {
  const db = new Database(dbPath, { readonly: true });
  try {
    return db.prepare(`PRAGMA table_info(${table})`).all().map((r) => r.name).sort();
  } finally {
    db.close();
  }
}

function defColumns(tableDef) {
  return Object.values(getTableConfig(tableDef).columns)
    .map((c) => c.name)
    .sort();
}

// Map of drizzle table name -> def, per schema module (excludes nothing;
// every def must materialize on a fresh migrate).
function defEntries(schemaModule) {
  const out = [];
  for (const [key, v] of Object.entries(schemaModule)) {
    try {
      const cfg = getTableConfig(v);
      if (cfg?.name) out.push([key, v]);
    } catch {
      // Not a table def (constants, helpers) — skipped, never asserted.
    }
  }
  return out;
}

// ── fresh migrate ────────────────────────────────────────────────────────────

describe("migrateDatabase — fresh databases", () => {
  let dir = null;
  beforeEach(() => { dir = path.join(os.tmpdir(), `migrate-fresh-${randomUUID()}`); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  test("runtime: creates every def table + journal; rerun is a no-op", async () => {
    const dbPath = path.join(dir, "runtime.sqlite");
    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER, logger: silentLogger });

    const tables = liveTables(dbPath);
    for (const [key, def] of defEntries(runtimeSchema)) {
      assert.ok(tables.includes(getTableConfig(def).name), `table missing: ${key}`);
    }

    const db = new Database(dbPath, { readonly: true });
    const journal = db.prepare("SELECT COUNT(*) AS n FROM __drizzle_migrations").get();
    db.close();
    assert.ok(journal.n >= 1, "journal must record applied snapshots");

    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER, logger: silentLogger });
    const db2 = new Database(dbPath, { readonly: true });
    const journal2 = db2.prepare("SELECT COUNT(*) AS n FROM __drizzle_migrations").get();
    db2.close();
    assert.equal(journal2.n, journal.n, "rerun must not append journal rows");
  });

  test("ops: creates every def table + journal", async () => {
    const dbPath = path.join(dir, "ops.sqlite");
    await migrateDatabase({ dbPath, migrationsFolder: OPS_MIGRATIONS_FOLDER, logger: silentLogger });

    const tables = liveTables(dbPath);
    for (const [, def] of defEntries(opsSchema)) {
      assert.ok(tables.includes(getTableConfig(def).name), `table missing: ${getTableConfig(def).name}`);
    }
  });

  test("creates missing parent directories", async () => {
    const dbPath = path.join(dir, "nested", "deep", "runtime.sqlite");
    await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER, logger: silentLogger });
    assert.ok(fs.existsSync(dbPath));
  });
});

// ── snapshot hygiene ─────────────────────────────────────────────────────────
// Forward-safety invariants for every present and future snapshot file:
// snapshots are the sole schema path, applied to whatever shape a file is
// in, so every CREATE must be IF NOT EXISTS. DROP COLUMN and RENAME have
// no IF EXISTS form in SQLite — they require manual review, so they fail
// here by design. ADD COLUMN is allowed for genuinely new columns.

describe("snapshots — forward safety", () => {
  const folders = [RUNTIME_MIGRATIONS_FOLDER, OPS_MIGRATIONS_FOLDER];

  function snapshotSql() {
    const out = [];
    for (const folder of folders) {
      for (const file of fs.readdirSync(folder)) {
        if (!file.endsWith(".sql")) continue;
        const body = fs.readFileSync(path.join(folder, file), "utf8");
        for (const stmt of body.split("--> statement-breakpoint")) {
          const line = stmt.trim().replace(/\s+/g, " ");
          if (line) out.push(`${file}: ${line.slice(0, 120)}`);
        }
      }
    }
    return out;
  }

  test("every CREATE is IF NOT EXISTS", () => {
    for (const stmt of snapshotSql()) {
      if (/^CREATE (TABLE|INDEX|UNIQUE INDEX) /i.test(stmt.replace(/^[^:]+: /, ""))) {
        assert.ok(
          /IF NOT EXISTS/i.test(stmt),
          `bare CREATE must be guarded: ${stmt}`,
        );
      }
    }
  });

  test("no DROP COLUMN or RENAME without manual review", () => {
    for (const stmt of snapshotSql()) {
      assert.ok(
        !/DROP COLUMN|RENAME (COLUMN|TO)/i.test(stmt.replace(/^[^:]+: /, "")),
        `DROP/RENAME needs manual review (no IF EXISTS form in SQLite): ${stmt}`,
      );
    }
  });
});

// ── snapshots are the sole schema path ───────────────────────────────────────

describe("migrateDatabase — snapshots only, no chain", () => {
  test("opening a store creates the full schema with no _schema_version table", async () => {
    const { dbPath } = tmpDb("snapshots-only");
    const { createRunStore } = await import("../../../state/run-store.js");
    const store = await createRunStore({ dbPath, logger: silentLogger });
    await store.close?.().catch(() => {});

    const tables = liveTables(dbPath);
    assert.ok(tables.includes("flow_runs"));
    assert.ok(!tables.includes("_schema_version"), "no legacy journal on snapshot-built DBs");
  });
});

// ── schema parity guard ──────────────────────────────────────────────────────

describe("schema parity — live boot vs drizzle defs", () => {
  // Boots every store through its real open path, then asserts the
  // produced columns equal the def columns exactly. Add a column to a
  // def without generating (or vice versa) and this fails — that is the
  // whole point.
  //
  // One tmp file PER STORE (same precedent as the engine e2e tests):
  // Proxy-lazy factories resolve await instantly while construction races
  // in the background, so each boot below polls for its store's tables
  // before the assertions run. Bounded waits, fail loudly on timeout —
  // never blind sleeps. One tmp file PER STORE (same precedent as the
  // engine e2e tests).
  test("runtime tables match defs column-for-column", async () => {
    const { dbPath: runsDb } = tmpDb("parity-runs");
    const { dbPath: execDb } = tmpDb("parity-exec");
    const { dbPath: healthDb } = tmpDb("parity-health");
    const { dbPath: sessionDb } = tmpDb("parity-session");
    const { dbPath: failureDb } = tmpDb("parity-failure");
    const { dbPath: discussDb } = tmpDb("parity-discuss");
    const { dbPath: registryDb } = tmpDb("parity-registry");
    const { dbPath: admissionDb } = tmpDb("parity-admission");
    const { dbPath: eventsDb } = tmpDb("parity-events");
    const W = "../../../";

    async function bootAndWait(dbPath, tables, fn) {
      await fn();
      const deadline = Date.now() + 15000;
      for (;;) {
        let seen = [];
        try {
          const db = new Database(dbPath, { readonly: true });
          try {
            seen = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name);
          } finally {
            db.close();
          }
        } catch { seen = []; }
        if (tables.every((t) => seen.includes(t))) return;
        if (Date.now() > deadline) assert.fail(`tables [${tables}] never materialized in ${dbPath} (saw [${seen}])`);
        await new Promise((r) => setTimeout(r, 100));
      }
    }

    const { createRunStore } = await import(`${W}state/run-store.js`);
    const runStore = await createRunStore({ dbPath: runsDb, logger: silentLogger });
    await bootAndWait(runsDb, ["flow_runs"], async () => {});

    const { createNodeExecutionsStore } = await import(`${W}state/node-executions.js`);
    await bootAndWait(execDb, ["flow_node_invocations", "flow_node_executions", "gate_records"], async () => { createNodeExecutionsStore(execDb); });

    const { createNodeHealthStore } = await import(`${W}state/node-health-store.js`);
    await bootAndWait(healthDb, ["node_health"], async () => {
      await createNodeHealthStore({ dbPath: healthDb, logger: silentLogger });
    });

    const { createSessionIndex } = await import(`${W}state/session-index.js`);
    await bootAndWait(sessionDb, ["flow_sessions"], async () => {
      await createSessionIndex({ dbPath: sessionDb, logger: silentLogger });
    });

    const { createFailureStore } = await import(`${W}state/failure-store.js`);
    await bootAndWait(failureDb, ["failure_events"], async () => {
      createFailureStore({
        dbPath: failureDb,
        logger: silentLogger,
        nodeExecutionsStore: { listResultsForFlow: async () => [] },
      });
    });

    const { createDiscussStore } = await import(`${W}state/discuss-store.js`);
    await bootAndWait(discussDb, ["discuss_state"], async () => {
      createDiscussStore({ dbPath: discussDb, logger: silentLogger });
    });

    const { createPipelineRegistryStore } = await import(`${W}state/pipeline-registry.js`);
    await bootAndWait(registryDb, ["pipeline_versions", "pipeline_node_versions"], async () => {
      createPipelineRegistryStore(registryDb);
    });

    const { createAdmissionQueueStore } = await import(`${W}state/admission-queue-store.js`);
    await bootAndWait(admissionDb, ["flow_admission_queue"], async () => {
      createAdmissionQueueStore({ dbPath: admissionDb, logger: silentLogger, getLimit: () => 8 });
    });

    const { createEventBus } = await import(`${W}dashboard/event-log.js`);
    const bus = createEventBus({ store: { list: async () => [] }, logger: silentLogger, dbPath: eventsDb });
    await bus.start?.();
    await bootAndWait(eventsDb, ["flow_events"], async () => {});

    const files = [runsDb, execDb, healthDb, sessionDb, failureDb, discussDb, registryDb, admissionDb, eventsDb];
    const tablesByFile = new Map(files.map((f) => [f, liveTables(f)]));
    const allTables = [...new Set([...tablesByFile.values()].flat())].filter(
      (t) => !["__drizzle_migrations", "sqlite_sequence"].includes(t),
    );
    const expected = new Set(
      defEntries(runtimeSchema).map(([, def]) => getTableConfig(def).name),
    );
    await bus.stop?.();
    await runStore.close?.().catch(() => {});

    const tableFile = (name) => files.find((f) => tablesByFile.get(f).includes(name));
    assert.deepEqual(
      new Set(allTables),
      expected,
      `live tables must equal def tables exactly (extra=${allTables.filter((t) => !expected.has(t))}, missing=${[...expected].filter((t) => !allTables.includes(t))})`,
    );
    for (const [, def] of defEntries(runtimeSchema)) {
      const name = getTableConfig(def).name;
      assert.deepEqual(
        liveColumns(tableFile(name), name),
        defColumns(def),
        `columns of ${name} must match the def exactly`,
      );
    }
  }, { timeout: 120000 });
});
