// backfill with stubbed gateway sources: no ~/.openclaw reads, real tmp ops DB.
import { test, describe } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { openStore } from "../../../telemetry/store.js";
import { runBackfill } from "../../../telemetry/backfill.js";
import { _resetForTests } from "../../../state/batch-writer.js";

function tmpDb() {
  return path.join(os.tmpdir(), `gf-backfill-${randomUUID()}.sqlite`);
}

function headRow(key, id) {
  return JSON.stringify({ sessionKey: key, sessionId: id, ts: new Date().toISOString() });
}

function stubResolver() {
  return {
    resolve: async (key, { source } = {}) => ({
      child_session_key: key,
      parent_session_key: null,
      root_session_key: key,
      kind: "explicit",
      root_kind: "explicit",
      root_label: key,
      agent_id: "a",
      pipeline_id: null, flow_id: null, node_id: null, item_key: null,
      first_seen_at: Date.now(),
      source: source ?? "backfill",
    }),
    walk: async (key) => [{ key, kind: "explicit", rootKey: key, rootKind: "explicit", rootLabel: key }],
  };
}

describe("telemetry/backfill.js — stubbed sources", () => {
  test("writes lineage rows for walked trajectories", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const key = `agent:test:explicit:${randomUUID()}`;
    const sources = {
      listTrajectoryPairs: () => [{ trajPath: "/t/a.traj", sessionFile: "/t/a.jsonl" }],
      readTrajectoryHead: () => headRow(key, "sid-1"),
      readSessionText: () => null,
    };
    const stats = await runBackfill({ store, resolver: stubResolver(), sources });
    assert.equal(stats.sessionsScanned, 1);
    assert.equal(stats.lineageWritten, 1);
    assert.ok(await store.getLineage(key));
    store.close();
    _resetForTests(dbPath);
  });

  test("skips existing rows without force, rewrites with force", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const key = `agent:test:explicit:${randomUUID()}`;
    const sources = {
      listTrajectoryPairs: () => [{ trajPath: "/t/a.traj", sessionFile: "/t/a.jsonl" }],
      readTrajectoryHead: () => headRow(key, "sid-1"),
      readSessionText: () => null,
    };
    await runBackfill({ store, resolver: stubResolver(), sources });
    const again = await runBackfill({ store, resolver: stubResolver(), sources });
    assert.equal(again.lineageSkipped, 1);
    assert.equal(again.lineageWritten, 0);
    const forced = await runBackfill({ store, resolver: stubResolver(), sources, force: true });
    assert.equal(forced.lineageWritten, 1);
    store.close();
    _resetForTests(dbPath);
  });

  test("includeCost extracts assistant cost rows from stubbed session text", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const key = `agent:test:explicit:${randomUUID()}`;
    const msg = JSON.stringify({
      type: "assistant",
      id: "m1",
      timestamp: new Date().toISOString(),
      message: {
        role: "assistant", provider: "openrouter", model: "x/y",
        usage: { input: 10, output: 5, cost: { total: 0.001 } },
      },
    });
    const sources = {
      listTrajectoryPairs: () => [{ trajPath: "/t/a.traj", sessionFile: "/t/a.jsonl" }],
      readTrajectoryHead: () => headRow(key, "sid-9"),
      readSessionText: () => `${msg}\n`,
    };
    const stats = await runBackfill({ store, resolver: stubResolver(), sources, includeCost: true });
    assert.equal(stats.costWritten, 1);
    await store.flush();
    const { rows } = await store._client.execute({
      sql: "SELECT * FROM cost_by_call WHERE session_key = ?",
      args: [key],
    });
    assert.equal(rows.length, 1);
    store.close();
    _resetForTests(dbPath);
  });

  test("unreadable agents dir returns empty stats without throwing", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const sources = {
      listTrajectoryPairs: () => { throw new Error("nope"); },
      readTrajectoryHead: () => null,
      readSessionText: () => null,
    };
    const stats = await runBackfill({ store, resolver: stubResolver(), sources, logger: { warn: () => {} } });
    assert.equal(stats.sessionsScanned, 0);
    store.close();
    _resetForTests(dbPath);
  });

  test("no injected sources returns empty stats without throwing", async () => {
    const dbPath = tmpDb();
    const store = await openStore({ dbPath });
    const stats = await runBackfill({ store, resolver: stubResolver(), sources: null, logger: { warn: () => {} } });
    assert.equal(stats.sessionsScanned, 0);
    store.close();
    _resetForTests(dbPath);
  });
});
