// The three lineage/cost tables moved to runtime.sqlite so their flow_id and
// pipeline_id can carry FKs. These tests cover the one-time copy that runs
// inside migrateDatabase(): rows arrive, historical orphans lose only the
// dangling pointer, and a second call is a no-op.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { createClient } from "@libsql/client";
import { migrateDatabase, OPS_MIGRATIONS_FOLDER, RUNTIME_MIGRATIONS_FOLDER } from "../../../state/migrate.js";
import { mergeOpsTables } from "../../../state/ops-merge.js";

function tmpPair() {
  const dir = mkdtempSync(path.join(os.tmpdir(), `manifold-ops-merge-${randomUUID()}-`));
  return { dir, runtimeDb: path.join(dir, "runtime.sqlite"), opsDb: path.join(dir, "ops.sqlite") };
}

async function seedOps(opsDb) {
  await migrateDatabase({ dbPath: opsDb, migrationsFolder: OPS_MIGRATIONS_FOLDER });
  const client = createClient({ url: `file:${opsDb}` });
  await client.execute({
    sql: `INSERT INTO queue_items (id, label, agent, created_at, pipeline_id, flow_id)
          VALUES (?, ?, ?, ?, ?, ?)`,
    args: ["q1", "SCORE", "scout", 1000, "pipe_a", "flow-1"],
  });
  await client.execute({
    sql: `INSERT INTO queue_items (id, label, agent, created_at, pipeline_id, flow_id)
          VALUES (?, ?, ?, ?, ?, ?)`,
    args: ["q2", "DRAFT", "scout", 1001, "pipe_a", "flow-gone"],
  });
  await client.execute({
    sql: `INSERT INTO session_lineage
            (child_session_key, root_session_key, kind, root_kind, first_seen_at, source, pipeline_id, flow_id)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    args: ["agent:a", "agent:a", "agent", "agent", 1000, "hook", "pipe_a", "flow-1"],
  });
  await client.execute({
    sql: `INSERT INTO cost_by_call (session_id, msg_id, ts, cost_total, pipeline_id, flow_id)
          VALUES (?, ?, ?, ?, ?, ?)`,
    args: ["s1", "m1", 1000, 0.5, "pipe_a", "flow-1"],
  });
  client.close();
}

describe("mergeOpsTables — one-time copy into runtime.sqlite", () => {
  test("copies all three tables, keeps valid pointers, nulls a dangling one, and is idempotent", async () => {
    const { runtimeDb, opsDb } = tmpPair();

    // Canonical basename is what the merge guard requires. Migrate before the
    // ops file exists, then seed the parents the surviving pointers name.
    assert.equal(path.basename(runtimeDb), "runtime.sqlite");
    await migrateDatabase({ dbPath: runtimeDb, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
    const client = createClient({ url: `file:${runtimeDb}` });
    await client.execute({ sql: "INSERT INTO pipelines (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)", args: ["pipe_a", "pipe_a", 1, 1] });
    await client.execute({ sql: "INSERT INTO flow_runs (flow_id, status, created_at, updated_at) VALUES (?, ?, ?, ?)", args: ["flow-1", "running", 1, 1] });

    await seedOps(opsDb);
    const result = mergeOpsTables({ runtimeDbPath: runtimeDb });
    assert.equal(result.merged, true);

    for (const [table, expected] of [["cost_by_call", 1], ["session_lineage", 1]]) {
      const { rows } = await client.execute(`SELECT COUNT(*) AS n FROM ${table}`);
      assert.equal(rows[0].n, expected, `${table} rows copied`);
    }
    const { rows: queued } = await client.execute("SELECT id, flow_id, pipeline_id FROM queue_items ORDER BY id");
    assert.equal(queued.length, 2);
    assert.equal(queued[0].flow_id, "flow-1", "a pointer with a real flow row survives");
    assert.equal(queued[0].pipeline_id, "pipe_a");
    assert.equal(queued[1].flow_id, null, "flow-gone has no flow_runs row; only the pointer is dropped");
    assert.equal(queued[1].pipeline_id, "pipe_a", "pipeline pointers survive");

    const again = mergeOpsTables({ runtimeDbPath: runtimeDb });
    assert.equal(again.merged, false);
    assert.equal(again.reason, "already-populated");
    const { rows: after } = await client.execute("SELECT COUNT(*) AS n FROM queue_items");
    assert.equal(after[0].n, 2);
    client.close();
  });

  test("does not treat a non-canonical runtime path as a merge candidate", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), `manifold-ops-merge-${randomUUID()}-`));
    const result = mergeOpsTables({ runtimeDbPath: path.join(dir, "alt-runtime.sqlite") });
    assert.equal(result.merged, false);
    assert.equal(result.reason, "not-canonical");
  });
});
