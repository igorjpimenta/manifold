// The ops copies of the moved tables are dropped only after runtime holds
// every row. These tests pin the two guards: the canonical file names, and
// the per-table row count that keeps local writes when an older binary is
// still appending to ops.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import Database from "better-sqlite3";
import { dropMovedOpsTables } from "../../../state/drop-moved-ops.js";

const MOVED = ["queue_items", "session_lineage", "cost_by_call"];

function pair() {
  const dir = mkdtempSync(path.join(os.tmpdir(), `manifold-ops-drop-${randomUUID()}-`));
  return { dir, ops: path.join(dir, "ops.sqlite"), runtime: path.join(dir, "runtime.sqlite") };
}

function createTables(file, { rows = 1 } = {}) {
  const db = new Database(file);
  for (const t of MOVED) db.exec(`CREATE TABLE ${t} (id TEXT PRIMARY KEY)`);
  for (const t of MOVED) {
    for (let i = 0; i < rows; i++) db.prepare(`INSERT INTO ${t} (id) VALUES (?)`).run(`${t}-${i}`);
  }
  db.close();
}

function tables(file) {
  const db = new Database(file, { readonly: true });
  try {
    return db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name);
  } finally { db.close(); }
}

describe("dropMovedOpsTables", () => {
  test("drops the three tables when runtime holds their rows", () => {
    const { ops, runtime } = pair();
    createTables(ops, { rows: 2 });
    createTables(runtime, { rows: 2 });
    const out = dropMovedOpsTables(ops);
    assert.deepEqual(out.dropped.sort(), [...MOVED].sort());
    const left = tables(ops);
    for (const t of MOVED) assert.ok(!left.includes(t), `${t} should be gone`);
  });

  test("keeps a table whose ops copy has rows runtime does not", () => {
    const { ops, runtime } = pair();
    createTables(ops, { rows: 3 });
    createTables(runtime, { rows: 1 });
    const out = dropMovedOpsTables(ops);
    assert.deepEqual(out.dropped, []);
    assert.deepEqual(out.skipped.sort(), [...MOVED].sort());
    assert.ok(tables(ops).includes("queue_items"));
  });

  test("does nothing without a canonical runtime sibling", () => {
    const { dir, ops } = pair();
    createTables(ops);
    const out = dropMovedOpsTables(path.join(dir, "other-ops.sqlite"));
    assert.equal(out.reason, "not-canonical");
    assert.ok(tables(ops).includes("queue_items"));
  });

  test("is a no-op once the tables are gone", () => {
    const { ops, runtime } = pair();
    createTables(runtime);
    new Database(ops).close();
    const out = dropMovedOpsTables(ops);
    assert.deepEqual(out.dropped, []);
    assert.deepEqual(out.skipped, []);
  });
});
