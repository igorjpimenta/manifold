import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { createSessionIndex } from "../../../state/session-index.js";
import { createRunStore } from "../../../state/run-store.js";

function tmpDb() {
  return path.join(os.tmpdir(), `gf-test-${randomUUID()}.sqlite`);
}

async function makeIndex(overrides = {}) {
  const dbPath = tmpDb();
  const idx = await createSessionIndex({ dbPath, ...overrides });
  const runStore = await createRunStore({ dbPath });
  const seed = async (flowId) => {
    if (typeof flowId === "string") await runStore.insert({ flowId, status: "running" });
  };
  return new Proxy(idx, {
    get(target, key) {
      const value = target[key];
      if (key === "record") {
        return async (args = {}) => { await seed(args.flowId); return value.call(target, args); };
      }
      if (key === "recordThread") {
        return async (args = {}) => { await seed(args.flowId); return value.call(target, args); };
      }
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

describe("createSessionIndex — DB setup", () => {
  test("creates the DB, table, and indexes without throwing", async () => {
    const idx = await makeIndex();
    assert.ok(idx._db, "exposes _db");
    const { rows } = await idx._db.execute("SELECT * FROM flow_sessions LIMIT 1");
    assert.ok(Array.isArray(rows));
    idx.close();
  });

  test("schema is idempotent — opening twice on the same file does not throw", async () => {
    const dbPath = tmpDb();
    const i1 = await createSessionIndex({ dbPath });
    i1.close();
    const i2 = await createSessionIndex({ dbPath });
    i2.close();
  });

  test("all expected columns exist after open", async () => {
    const idx = await makeIndex();
    const { rows: info } = await idx._db.execute("PRAGMA table_info(flow_sessions)");
    const cols = info.map((r) => r.name);
    for (const col of ["id", "flow_id", "session_key", "node_id", "branch_item_id", "spawned_at"]) {
      assert.ok(cols.includes(col), `column ${col} should exist`);
    }
    idx.close();
  });
});

describe("record() — no-op when required fields missing", () => {
  test("record() is a no-op when flowId is missing", async () => {
    const idx = await makeIndex();
    await idx.record({ session: "sess-abc" });
    const { rows } = await idx._db.execute("SELECT * FROM flow_sessions");
    assert.equal(rows.length, 0);
    idx.close();
  });

  test("record() is a no-op when session is missing", async () => {
    const idx = await makeIndex();
    await idx.record({ flowId: "flow-abc" });
    const { rows } = await idx._db.execute("SELECT * FROM flow_sessions");
    assert.equal(rows.length, 0);
    idx.close();
  });

  test("record() is a no-op when both flowId and session are missing", async () => {
    const idx = await makeIndex();
    await idx.record({});
    const { rows } = await idx._db.execute("SELECT * FROM flow_sessions");
    assert.equal(rows.length, 0);
    idx.close();
  });

  test("record() is a no-op when called with no arguments", async () => {
    const idx = await makeIndex();
    await idx.record();
    const { rows } = await idx._db.execute("SELECT * FROM flow_sessions");
    assert.equal(rows.length, 0);
    idx.close();
  });
});

describe("record() — successful insertion", () => {
  test("inserts a row with correct flowId, session, nodeId, branchItemId, and a numeric spawned_at", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    const session = `sess-${randomUUID()}`;
    const nodeId = "SOME_NODE";
    const branchItemId = "item-42";

    const before = Date.now();
    await idx.record({ flowId, session, nodeId, branchItemId });
    const after = Date.now();

    const { rows } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 1);
    const row = rows[0];
    assert.equal(row.flow_id, flowId);
    assert.equal(row.session_key, session);
    assert.equal(row.node_id, nodeId);
    assert.equal(row.branch_item_id, branchItemId);
    assert.equal(typeof Number(row.spawned_at), "number");
    assert.ok(Number(row.spawned_at) >= before, "spawned_at should be >= time before call");
    assert.ok(Number(row.spawned_at) <= after, "spawned_at should be <= time after call");
    idx.close();
  });

  test("nodeId defaults to null when not provided", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-1" });
    const { rows } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].node_id, null);
    idx.close();
  });

  test("branchItemId defaults to null when not provided", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-2" });
    const { rows } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].branch_item_id, null);
    idx.close();
  });

  test("generates a unique UUID primary key per call", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-A" });
    await idx.record({ flowId, session: "sess-B" });
    const { rows } = await idx._db.execute({
      sql: "SELECT id FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 2);
    assert.notEqual(rows[0].id, rows[1].id);
    const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    assert.match(rows[0].id, uuidRe);
    assert.match(rows[1].id, uuidRe);
    idx.close();
  });
});

describe("record() — deduplication via UNIQUE(flow_id, session_key)", () => {
  test("recording the same (flowId, session) pair twice is a no-op (INSERT OR IGNORE)", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    const session = "sess-dup";
    await idx.record({ flowId, session, nodeId: "NODE_A" });
    await idx.record({ flowId, session, nodeId: "NODE_B" });
    const { rows } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 1, "only one row should exist");
    assert.equal(rows[0].node_id, "NODE_A", "first insert wins; second is ignored");
    idx.close();
  });

  test("same session with different flowId creates two separate rows", async () => {
    const idx = await makeIndex();
    const session = "shared-sess";
    const flowId1 = randomUUID();
    const flowId2 = randomUUID();
    await idx.record({ flowId: flowId1, session });
    await idx.record({ flowId: flowId2, session });
    const { rows: all } = await idx._db.execute(
      "SELECT flow_id FROM flow_sessions ORDER BY spawned_at ASC"
    );
    assert.equal(all.length, 2);
    const ids = all.map((r) => r.flow_id);
    assert.ok(ids.includes(flowId1));
    assert.ok(ids.includes(flowId2));
    idx.close();
  });

  test("record() does not throw on duplicate — no-op is silent", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await assert.doesNotReject(async () => {
      await idx.record({ flowId, session: "dup-sess" });
      await idx.record({ flowId, session: "dup-sess" });
    });
    idx.close();
  });
});

describe("close()", () => {
  test("close() on the real instance does not throw", async () => {
    const idx = await makeIndex();
    assert.doesNotThrow(() => idx.close());
  });

  test("calling close() twice does not throw", async () => {
    const idx = await makeIndex();
    idx.close();
    assert.doesNotThrow(() => idx.close());
  });
});

// Non-SQLite content: mkdir succeeds, client open throws → shim path.
function makeCorruptDbPath() {
  const filePath = path.join(os.tmpdir(), `gf-corrupt-${randomUUID()}.sqlite`);
  writeFileSync(filePath, "THIS IS NOT SQLITE \x00\x01\x02");
  return filePath;
}

describe("no-op shim — DB open failure", () => {
  test("returns a shim when the DB file contains invalid SQLite content", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    assert.equal(typeof shim.record, "function");
    assert.equal(typeof shim.close, "function");
  });

  test("no-op shim record() does not throw", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    await assert.doesNotReject(() => shim.record({ flowId: "f", session: "s" }));
  });

  test("no-op shim record() with no args does not throw", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    await assert.doesNotReject(() => shim.record());
  });

  test("no-op shim record() with partial args does not throw", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    await assert.doesNotReject(() => shim.record({}));
  });

  test("no-op shim close() does not throw", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    assert.doesNotThrow(() => shim.close());
  });

  test("no-op shim exposes no _db property", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    assert.equal(shim._db, undefined);
  });
});

describe("logger integration", () => {
  test("does not throw when no logger is provided (default)", async () => {
    const idx = await makeIndex();
    await assert.doesNotReject(() => idx.record({ flowId: "f1", session: "s1" }));
    idx.close();
  });

  test("accepts a logger object with warn method", async () => {
    const warnings = [];
    const logger = { warn: (msg) => warnings.push(msg) };
    const idx = await makeIndex({ logger });
    await assert.doesNotReject(() => idx.record({ flowId: "f2", session: "s2" }));
    idx.close();
    assert.equal(warnings.length, 0);
  });
});

describe("hasSessionForNode()", () => {
  test("returns false when no record exists for the (flowId, nodeId) pair", async () => {
    const idx = await makeIndex();
    assert.equal(await idx.hasSessionForNode(randomUUID(), "SOME_NODE"), false);
    idx.close();
  });

  test("returns true once record() has been called for that exact (flowId, nodeId) pair", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-1", nodeId: "SCORE_LLM" });
    assert.equal(await idx.hasSessionForNode(flowId, "SCORE_LLM"), true);
    idx.close();
  });

  test("returns false for the same flowId but a different nodeId", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-1", nodeId: "SCORE_LLM" });
    assert.equal(await idx.hasSessionForNode(flowId, "OTHER_NODE"), false);
    idx.close();
  });

  test("returns false for the same nodeId but a different flowId", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-1", nodeId: "SCORE_LLM" });
    assert.equal(await idx.hasSessionForNode(randomUUID(), "SCORE_LLM"), false);
    idx.close();
  });

  test("returns false when flowId or nodeId is missing", async () => {
    const idx = await makeIndex();
    assert.equal(await idx.hasSessionForNode(null, "NODE"), false);
    assert.equal(await idx.hasSessionForNode("flow-1", null), false);
    assert.equal(await idx.hasSessionForNode(), false);
    idx.close();
  });

  test("a record with nodeId=null does not satisfy a hasSessionForNode lookup for a real nodeId", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-1" });
    assert.equal(await idx.hasSessionForNode(flowId, "SOME_NODE"), false);
    idx.close();
  });

  test("no-op shim's hasSessionForNode() returns false without throwing", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    await assert.doesNotReject(async () => {
      const result = await shim.hasSessionForNode("flow-1", "NODE");
      assert.equal(result, false);
    });
  });
});

describe("flowIdForSessionKey()", () => {
  test("returns null when no record exists for that session key", async () => {
    const idx = await makeIndex();
    assert.equal(await idx.flowIdForSessionKey("sess-unknown"), null);
    idx.close();
  });

  test("returns the flowId once record() has been called for that session key", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "agent:scorer:subagent:mf-abcdef12-score_llm", nodeId: "SCORE_LLM" });
    assert.equal(await idx.flowIdForSessionKey("agent:scorer:subagent:mf-abcdef12-score_llm"), flowId);
    idx.close();
  });

  test("returns null when the session key is missing", async () => {
    const idx = await makeIndex();
    assert.equal(await idx.flowIdForSessionKey(null), null);
    assert.equal(await idx.flowIdForSessionKey(), null);
    idx.close();
  });

  test("no-op shim's flowIdForSessionKey() returns null without throwing", async () => {
    const shim = await createSessionIndex({ dbPath: makeCorruptDbPath() });
    await assert.doesNotReject(async () => {
      const result = await shim.flowIdForSessionKey("sess-1");
      assert.equal(result, null);
    });
  });
});

describe("record persistence roundtrip", () => {
  test("multiple sessions for one flow are all persisted", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "s1", nodeId: "NODE_1" });
    await idx.record({ flowId, session: "s2", nodeId: "NODE_2" });
    await idx.record({ flowId, session: "s3", nodeId: "NODE_3" });
    const { rows } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 3);
    const keys = rows.map((r) => r.session_key);
    assert.ok(keys.includes("s1"));
    assert.ok(keys.includes("s2"));
    assert.ok(keys.includes("s3"));
    idx.close();
  });

  test("sessions from different flows are isolated", async () => {
    const idx = await makeIndex();
    const flowA = randomUUID();
    const flowB = randomUUID();
    await idx.record({ flowId: flowA, session: "sa1" });
    await idx.record({ flowId: flowA, session: "sa2" });
    await idx.record({ flowId: flowB, session: "sb1" });

    const { rows: rowsA } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowA],
    });
    const { rows: rowsB } = await idx._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowB],
    });
    assert.equal(rowsA.length, 2);
    assert.equal(rowsB.length, 1);
    assert.equal(rowsB[0].session_key, "sb1");
    idx.close();
  });

  test("persists across re-open of same DB file", async () => {
    const dbPath = tmpDb();
    const idx1 = await createSessionIndex({ dbPath });
    const flowId = randomUUID();
    const runs = await createRunStore({ dbPath });
    await runs.insert({ flowId, status: "running" });
    await idx1.record({ flowId, session: "persistent-sess", nodeId: "N1" });
    idx1.close();

    const idx2 = await createSessionIndex({ dbPath });
    const { rows } = await idx2._db.execute({
      sql: "SELECT * FROM flow_sessions WHERE flow_id = ?",
      args: [flowId],
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].session_key, "persistent-sess");
    assert.equal(rows[0].node_id, "N1");
    idx2.close();
  });
});

describe("listForFlow()", () => {
  test("returns this flow's rows in spawn order with stored attribution", async () => {
    const idx = await makeIndex();
    const flowId = randomUUID();
    await idx.record({ flowId, session: "sess-b", nodeId: "NODE_B" });
    await idx.record({ flowId, session: "sess-a", nodeId: "NODE_A", branchItemId: "item-1" });
    await idx.record({ flowId: "other-flow", session: "sess-x", nodeId: "NODE_X" });
    const rows = await idx.listForFlow(flowId);
    assert.equal(rows.length, 2);
    const bySession = new Map(rows.map((r) => [r.session, r]));
    assert.equal(bySession.get("sess-b").nodeId, "NODE_B");
    assert.equal(bySession.get("sess-b").branchItemId, null);
    assert.equal(bySession.get("sess-a").branchItemId, "item-1");
    assert.ok(typeof bySession.get("sess-a").spawnedAt === "number");
    idx.close();
  });

  test("unknown flow returns an empty list", async () => {
    const idx = await makeIndex();
    assert.deepEqual(await idx.listForFlow(randomUUID()), []);
    idx.close();
  });
});

describe("recordThread / findThread", () => {
  test("findThread returns null when no turn ran yet", async () => {
    const idx = await makeIndex();
    assert.equal(await idx.findThread("flow-1", "GATE", "item-1"), null);
    idx.close();
  });

  test("recordThread then findThread round-trips agent + session", async () => {
    const idx = await makeIndex();
    await idx.recordThread({ flowId: "flow-1", gateNodeId: "GATE", itemId: "item-1", agentId: "scout", session: "agent:scout:t1" });
    assert.deepEqual(
      await idx.findThread("flow-1", "GATE", "item-1"),
      { session: "agent:scout:t1", agentId: "scout" },
    );
    idx.close();
  });

  test("recordThread upserts on (flow, gate, item) — one row per thread", async () => {
    const idx = await makeIndex();
    await idx.recordThread({ flowId: "flow-1", gateNodeId: "GATE", itemId: "item-1", agentId: "a1", session: "k1" });
    await idx.recordThread({ flowId: "flow-1", gateNodeId: "GATE", itemId: "item-1", agentId: "a2", session: "k2" });
    assert.deepEqual(
      await idx.findThread("flow-1", "GATE", "item-1"),
      { session: "k2", agentId: "a2" },
    );
    const { rows } = await idx._db.execute({
      sql: "SELECT * FROM discuss_threads WHERE flow_id = ?",
      args: ["flow-1"],
    });
    assert.equal(rows.length, 1);
    idx.close();
  });
});
