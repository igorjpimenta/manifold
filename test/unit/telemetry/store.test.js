// Unit tests for telemetry/store.js — openStore, upsertLineage/getLineage
// (unchanged, stays immediate), and insertCost's deferred-through-batch-
// writer behavior. No prior coverage existed for this file before this
// suite.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { openStore } from "../../../telemetry/store.js";
import { _resetForTests } from "../../../state/batch-writer.js";

function tmpDb() {
  return path.join(os.tmpdir(), `gf-telemetry-${randomUUID()}.sqlite`);
}

function makeCostRow(overrides = {}) {
  return {
    session_id: overrides.session_id ?? randomUUID(),
    msg_id: overrides.msg_id ?? randomUUID(),
    ts: overrides.ts ?? Date.now(),
    provider: "openrouter",
    model: "deepseek/deepseek-v4-flash",
    cost_total: 0.05,
    ...overrides,
  };
}

describe("telemetry/store.js — openStore", () => {
  test("opens the DB and creates the schema without throwing", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    assert.equal(await store.getLineage(randomUUID()), null);
    store.close();
    _resetForTests(dbPath);
  });
});

describe("telemetry/store.js — upsertLineage / getLineage (unchanged, immediate)", () => {
  test("a row is visible immediately after upsertLineage, no flush needed", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const childKey = randomUUID();

    await store.upsertLineage({
      child_session_key: childKey,
      root_session_key: childKey,
      kind: "root",
      root_kind: "cron",
      source: "live",
    });

    const row = await store.getLineage(childKey);
    assert.ok(row, "row should be visible with no flush — upsertLineage stays immediate");
    assert.equal(row.child_session_key, childKey);

    store.close();
    _resetForTests(dbPath);
  });
});

describe("telemetry/store.js — insertCost (deferred via batch-writer)", () => {
  test("a cost row is not visible until flush() runs", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const row = makeCostRow();

    store.insertCost(row);
    const { rows: before } = await store._client.execute({
      sql: "SELECT * FROM cost_by_call WHERE session_id = ? AND msg_id = ?",
      args: [row.session_id, row.msg_id],
    });
    assert.equal(before.length, 0, "row must not be visible before flush");

    await store.flush();
    const { rows: after } = await store._client.execute({
      sql: "SELECT * FROM cost_by_call WHERE session_id = ? AND msg_id = ?",
      args: [row.session_id, row.msg_id],
    });
    assert.equal(after.length, 1, "row must be visible after flush");
    assert.equal(after[0].session_id, row.session_id);
    assert.equal(Number(after[0].cost_total), row.cost_total);

    store.close();
    _resetForTests(dbPath);
  });

  test("multiple insertCost calls in one window batch into a single flush", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const sessionId = randomUUID();
    const rows = [
      makeCostRow({ session_id: sessionId, msg_id: "m1" }),
      makeCostRow({ session_id: sessionId, msg_id: "m2" }),
      makeCostRow({ session_id: sessionId, msg_id: "m3" }),
    ];
    for (const row of rows) store.insertCost(row);

    await store.flush();
    const { rows: got } = await store._client.execute({
      sql: "SELECT msg_id FROM cost_by_call WHERE session_id = ? ORDER BY msg_id",
      args: [sessionId],
    });
    assert.deepEqual(got.map((r) => r.msg_id), ["m1", "m2", "m3"]);

    store.close();
    _resetForTests(dbPath);
  });
});

describe("telemetry/store.js — injected flowsDbPath", () => {
  test("openStore accepts a missing flows DB (attach skipped silently)", async () => {
    const { openStore: open2 } = await import("../../../telemetry/store.js");
    const dbPath = tmpDb();
    const store = await open2({ dbPath, flowsDbPath: path.join(os.tmpdir(), `gf-nope-${randomUUID()}.sqlite`) });
    assert.equal(await store.getLineage(randomUUID()), null);
    store.close();
    _resetForTests(dbPath);
  });

  test("MANIFOLD_OPS_DB env relocates the default DB path", async () => {
    const modPath = `../../../telemetry/store.js?envreloc-${randomUUID()}`;
    const dbPath = tmpDb();
    const prev = process.env.MANIFOLD_OPS_DB;
    process.env.MANIFOLD_OPS_DB = dbPath;
    try {
      const { openStore: open3 } = await import(modPath);
      const store = await open3({});
      await store.upsertLineage({
        child_session_key: "k1",
        root_session_key: "k1",
        kind: "root",
        root_kind: "cron",
        source: "live",
      });
      assert.ok(await store.getLineage("k1"));
      store.close();
    } finally {
      if (prev === undefined) delete process.env.MANIFOLD_OPS_DB;
      else process.env.MANIFOLD_OPS_DB = prev;
      _resetForTests(dbPath);
    }
  });
});
