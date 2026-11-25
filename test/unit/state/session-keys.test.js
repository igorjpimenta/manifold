// The sessions registry is what turns session-key pointers into FKs. These
// tests cover both halves: writers must register a key before referencing
// it, and the migration backfill must cover keys already in the file.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { createClient } from "@libsql/client";
import { migrateDatabase, RUNTIME_MIGRATIONS_FOLDER } from "../../../state/migrate.js";
import { backfillSessionKeys, ensureSessionKeys } from "../../../state/session-keys.js";

async function freshDb() {
  const dir = mkdtempSync(path.join(os.tmpdir(), `manifold-sessions-${randomUUID()}-`));
  const dbPath = path.join(dir, "runtime.sqlite");
  await migrateDatabase({ dbPath, migrationsFolder: RUNTIME_MIGRATIONS_FOLDER });
  const client = createClient({ url: `file:${dbPath}` });
  await client.execute("PRAGMA foreign_keys = ON");
  return client;
}

describe("sessions registry", () => {
  test("a session-key pointer is rejected until the writer registers the key", async () => {
    const client = await freshDb();
    const insertSession = (key) => client.execute({
      sql: "INSERT INTO flow_sessions (id, flow_id, session_key, spawned_at) VALUES (?, ?, ?, ?)",
      args: [randomUUID(), "flow-1", key, 1],
    });
    await client.execute({
      sql: "INSERT INTO flow_runs (flow_id, status, created_at, updated_at) VALUES (?, ?, ?, ?)",
      args: ["flow-1", "running", 1, 1],
    });

    await assert.rejects(() => insertSession("agent:unknown"), /FOREIGN KEY/);

    await ensureSessionKeys(client, ["agent:unknown"]);
    await insertSession("agent:unknown");
    const { rows } = await client.execute("SELECT COUNT(*) AS n FROM sessions");
    assert.equal(rows[0].n, 1);
    client.close();
  });

  test("backfill registers every key already referenced by rows", async () => {
    const client = await freshDb();
    // Rows referencing keys absent from the registry can exist while FK
    // enforcement is off; insert them that way, then let the backfill fix
    // the key set the way migrateDatabase does after applying snapshots.
    await client.execute("PRAGMA foreign_keys = OFF");
    await client.execute({
      sql: `INSERT INTO session_lineage
              (child_session_key, parent_session_key, root_session_key, kind, root_kind, first_seen_at, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
      args: ["agent:child", "agent:parent", "agent:root", "agent", "agent", 1, "hook"],
    });
    await client.execute({
      sql: `INSERT INTO cost_by_call (session_id, msg_id, ts, cost_total, session_key, root_session_key)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: ["s1", "m1", 1, 0.1, "agent:cost", "agent:root"],
    });
    await client.execute("PRAGMA foreign_keys = ON");

    await backfillSessionKeys(client);

    const { rows } = await client.execute("SELECT session_key FROM sessions ORDER BY session_key");
    assert.deepEqual(rows.map((r) => r.session_key), ["agent:child", "agent:cost", "agent:parent", "agent:root"]);
    const { rows: bad } = await client.execute("PRAGMA foreign_key_check");
    assert.equal(bad.length, 0);
    client.close();
  });

  test("backfill is a no-op on a second pass", async () => {
    const client = await freshDb();
    await client.execute("PRAGMA foreign_keys = OFF");
    await client.execute({
      sql: "INSERT INTO flow_sessions (id, flow_id, session_key, spawned_at) VALUES (?, ?, ?, ?)",
      args: ["s1", "flow-1", "agent:a", 1],
    });
    await client.execute("PRAGMA foreign_keys = ON");
    await backfillSessionKeys(client);
    await backfillSessionKeys(client);
    const { rows } = await client.execute("SELECT COUNT(*) AS n FROM sessions");
    assert.equal(rows[0].n, 1);
    client.close();
  });
});
