// Attribution keys — item, node name, harness turn run — are materialized in
// three registries so the columns that name them can carry FKs. Tests cover
// the rejection, the writer guard, and the migration backfill.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { createClient } from "@libsql/client";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "../../../state/migrate.js";
import { ensureSessionKeys } from "../../../state/session-keys.js";
import {
  ensureFlowItems, ensureTurnRuns, ensurePipelineNodes, backfillAttribution,
} from "../../../state/attribution-registries.js";

const SILENCE = "agent:test";

async function freshDb() {
  const dir = mkdtempSync(path.join(os.tmpdir(), `manifold-attr-${randomUUID()}-`));
  const dbPath = path.join(dir, "runtime.sqlite");
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute("PRAGMA foreign_keys = ON");
  await client.execute({
    sql: "INSERT INTO flow_runs (flow_id, status, created_at, updated_at) VALUES (?, ?, ?, ?)",
    args: ["flow-1", "running", 1, 1],
  });
  await client.execute({
    sql: "INSERT INTO pipelines (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
    args: ["pipe-1", "pipe-1", 1, 1],
  });
  await ensureSessionKeys(client, [SILENCE]);
  return client;
}

async function insertLineage(client, patch = {}) {
  const row = {
    child: SILENCE, parent: null, root: SILENCE,
    pipeline: null, node: null, flow: null, item: null, run: null, ...patch,
  };
  return client.execute({
    sql: `INSERT INTO session_lineage
            (child_session_key, parent_session_key, root_session_key, kind, root_kind,
             pipeline_id, node_id, flow_id, item_key, run_id, first_seen_at, source)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
    args: [row.child, row.parent, row.root, "agent", "agent",
           row.pipeline, row.node, row.flow, row.item, row.run, 1, "hook"],
  });
}

describe("attribution registries", () => {
  test("item, node and turn keys are rejected until the writer registers them", async () => {
    const client = await freshDb();

    await assert.rejects(() => insertLineage(client, { flow: "flow-1", item: "item-1" }), /FOREIGN KEY/);
    await ensureFlowItems(client, [["flow-1", "item-1"]]);
    await insertLineage(client, { flow: "flow-1", item: "item-1" });

    await assert.rejects(
      () => insertLineage(client, { pipeline: "pipe-1", node: "NODE_X", child: "agent:node" }),
      /FOREIGN KEY/,
    );
    await ensurePipelineNodes(client, [["pipe-1", "NODE_X"]]);
    await ensureSessionKeys(client, ["agent:node"]);
    await insertLineage(client, { pipeline: "pipe-1", node: "NODE_X", child: "agent:node" });

    await assert.rejects(() => insertLineage(client, { run: "run-1", child: "agent:run" }), /FOREIGN KEY/);
    await ensureTurnRuns(client, ["run-1"]);
    await ensureSessionKeys(client, ["agent:run"]);
    await insertLineage(client, { run: "run-1", child: "agent:run" });

    const { rows } = await client.execute("SELECT COUNT(*) AS n FROM session_lineage");
    assert.equal(rows[0].n, 3);
    client.close();
  });

  test("a partial composite pointer (flow without item) is legal and unchecked", async () => {
    const client = await freshDb();
    await insertLineage(client, { flow: "flow-1" });
    const { rows } = await client.execute("SELECT COUNT(*) AS n FROM session_lineage");
    assert.equal(rows[0].n, 1);
    client.close();
  });

  test("node health and queue rows carry the same node-name FK", async () => {
    const client = await freshDb();
    await assert.rejects(() => client.execute({
      sql: `INSERT INTO node_health (id, pipeline_id, node_id, status, reason, created_at, updated_at)
            VALUES (?, ?, ?, 'blocked', 'x', 1, 1)`,
      args: ["h1", "pipe-1", "NODE_X"],
    }), /FOREIGN KEY/);
    await ensurePipelineNodes(client, [["pipe-1", "NODE_X"]]);
    await client.execute({
      sql: `INSERT INTO node_health (id, pipeline_id, node_id, status, reason, created_at, updated_at)
            VALUES (?, ?, ?, 'blocked', 'x', 1, 1)`,
      args: ["h1", "pipe-1", "NODE_X"],
    });
    client.close();
  });

  test("backfill registers branch keys and the keys existing rows already name", async () => {
    const client = await freshDb();
    // v7 no longer creates flow_branches; old files still carry the branch
    // keys item attribution was written against.
    await client.execute(`CREATE TABLE flow_branches (
      flow_id TEXT NOT NULL, group_id TEXT NOT NULL, branch_key TEXT NOT NULL,
      started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      PRIMARY KEY (flow_id, group_id, branch_key))`);
    await client.execute("PRAGMA foreign_keys = OFF");
    await client.execute({
      sql: `INSERT INTO flow_branches (flow_id, group_id, branch_key, started_at, updated_at)
            VALUES (?, ?, ?, ?, ?)`,
      args: ["flow-1", "g", "branch-9", 1, 1],
    });
    await insertLineage(client, { flow: "flow-1", item: "branch-9", run: "run-1", child: "agent:old" });
    await client.execute({
      sql: `INSERT INTO session_lineage
              (child_session_key, root_session_key, kind, root_kind, pipeline_id, node_id, first_seen_at, source)
            VALUES (?, ?, 'agent', 'agent', ?, ?, 1, 'hook')`,
      args: ["agent:old-noded", "agent:old-noded", "pipe-1", "NODE_OLD"],
    });
    await client.execute("PRAGMA foreign_keys = ON");
    await ensureSessionKeys(client, ["agent:old", "agent:old-noded"]);

    await backfillAttribution(client);

    const items = (await client.execute("SELECT item_key FROM flow_items ORDER BY item_key")).rows.map((r) => r.item_key);
    assert.deepEqual(items, ["branch-9"]);
    const runs = (await client.execute("SELECT run_id FROM turn_runs")).rows.map((r) => r.run_id);
    assert.deepEqual(runs, ["run-1"]);
    const nodes = (await client.execute("SELECT node_name FROM pipeline_nodes ORDER BY node_name")).rows.map((r) => r.node_name);
    assert.deepEqual(nodes, ["NODE_OLD"]);
    const { rows: bad } = await client.execute("PRAGMA foreign_key_check");
    assert.equal(bad.length, 0);
    client.close();
  });
});
