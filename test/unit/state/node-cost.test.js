
import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { createClient } from "@libsql/client";
import { createNodeCostReader } from "../../../state/node-cost.js";

const TEST_DB_DIR = path.join(os.tmpdir(), `gf-node-cost-${randomUUID()}`);
const TEST_DB_PATH = path.join(TEST_DB_DIR, "lineage.sqlite");
mkdirSync(TEST_DB_DIR, { recursive: true });

const { nodeCallStats } = createNodeCostReader({ dbPath: TEST_DB_PATH });

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS cost_by_call (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT    NOT NULL,
    node_id     TEXT    NOT NULL,
    ts          INTEGER NOT NULL,
    cost_total  REAL    NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_cbc_pipe_node_ts
    ON cost_by_call (pipeline_id, node_id, ts)`,
];

let _testClient = null;

async function getTestClient() {
  if (!_testClient) {
    _testClient = createClient({ url: `file:${TEST_DB_PATH}` });
    for (const sql of SCHEMA) {
      await _testClient.execute(sql);
    }
  }
  return _testClient;
}

async function insertRows(pipelineId, nodeId, costs, { daysAgo = 1 } = {}) {
  const client = await getTestClient();
  const ts = Date.now() - daysAgo * 86_400_000;
  for (const cost of costs) {
    await client.execute({
      sql: "INSERT INTO cost_by_call (pipeline_id, node_id, ts, cost_total) VALUES (?, ?, ?, ?)",
      args: [pipelineId, nodeId, ts, cost],
    });
  }
}

describe("null argument guards (short-circuit before DB)", () => {
  test("returns null when pipelineId is undefined", async () => {
    assert.equal(await nodeCallStats(undefined, "NODE_A"), null);
  });

  test("returns null when pipelineId is empty string", async () => {
    assert.equal(await nodeCallStats("", "NODE_A"), null);
  });

  test("returns null when pipelineId is null", async () => {
    assert.equal(await nodeCallStats(null, "NODE_A"), null);
  });

  test("returns null when nodeId is undefined", async () => {
    assert.equal(await nodeCallStats("pipe_x", undefined), null);
  });

  test("returns null when nodeId is empty string", async () => {
    assert.equal(await nodeCallStats("pipe_x", ""), null);
  });

  test("returns null when nodeId is null", async () => {
    assert.equal(await nodeCallStats("pipe_x", null), null);
  });

  test("returns null when both pipelineId and nodeId are falsy", async () => {
    assert.equal(await nodeCallStats(null, null), null);
  });
});

describe("DB unavailability", () => {
  test("returns null when lineage.sqlite is missing", async () => {
    const missing = path.join(os.tmpdir(), `gf-missing-${randomUUID()}.sqlite`);
    const { nodeCallStats: ncsMissing } = createNodeCostReader({ dbPath: missing });
    const result = await ncsMissing("pipe_x", "NODE_A");
    assert.equal(result, null);
  });
});

before(async () => {
  await getTestClient();
});

describe("minSamples threshold", () => {
  test("returns null when row count is below default minSamples (3)", async () => {
    await insertRows("pipe_ms", "NODE_BELOW", [0.001, 0.002]);
    assert.equal(await nodeCallStats("pipe_ms", "NODE_BELOW"), null);
  });

  test("returns stats when row count exactly meets default minSamples (3)", async () => {
    await insertRows("pipe_ms", "NODE_EXACT3", [0.001, 0.002, 0.003]);
    const r = await nodeCallStats("pipe_ms", "NODE_EXACT3");
    assert.ok(r !== null);
    assert.equal(r.sampleSize, 3);
  });

  test("returns null when row count is below custom minSamples", async () => {
    await insertRows("pipe_ms", "NODE_CUSTOM", [1, 2, 3, 4]);
    assert.equal(await nodeCallStats("pipe_ms", "NODE_CUSTOM", { minSamples: 5 }), null);
  });

  test("returns stats when row count exactly meets custom minSamples", async () => {
    await insertRows("pipe_ms", "NODE_CUSTOM5", [1, 2, 3, 4, 5]);
    const r = await nodeCallStats("pipe_ms", "NODE_CUSTOM5", { minSamples: 5 });
    assert.ok(r !== null);
    assert.equal(r.sampleSize, 5);
  });

  test("minSamples=1 allows a single row", async () => {
    await insertRows("pipe_ms", "NODE_ONE", [0.05]);
    const r = await nodeCallStats("pipe_ms", "NODE_ONE", { minSamples: 1 });
    assert.ok(r !== null);
    assert.equal(r.sampleSize, 1);
    assert.equal(r.p25, 0.05);
    assert.equal(r.p50, 0.05);
    assert.equal(r.p75, 0.05);
  });

  test("minSamples=2 returns null for 1 row", async () => {
    await insertRows("pipe_ms", "NODE_ONE2", [0.07]);
    assert.equal(await nodeCallStats("pipe_ms", "NODE_ONE2", { minSamples: 2 }), null);
  });

  test("minSamples=2 returns stats for 2 rows", async () => {
    await insertRows("pipe_ms", "NODE_DUO", [0.05, 0.10]);
    const r = await nodeCallStats("pipe_ms", "NODE_DUO", { minSamples: 2 });
    assert.ok(r !== null);
    assert.equal(r.sampleSize, 2);
  });
});

describe("percentile calculations — odd-length dataset (5 values)", () => {
  before(async () => {
    await insertRows("pipe_odd", "NODE_ODD5", [3, 1, 5, 2, 4]);
  });

  test("p25 = 2 (exact integer index)", async () => {
    assert.equal((await nodeCallStats("pipe_odd", "NODE_ODD5")).p25, 2);
  });

  test("p50 = 3 (exact integer index)", async () => {
    assert.equal((await nodeCallStats("pipe_odd", "NODE_ODD5")).p50, 3);
  });

  test("p75 = 4 (exact integer index)", async () => {
    assert.equal((await nodeCallStats("pipe_odd", "NODE_ODD5")).p75, 4);
  });

  test("sampleSize = 5", async () => {
    assert.equal((await nodeCallStats("pipe_odd", "NODE_ODD5")).sampleSize, 5);
  });
});

describe("percentile calculations — even-length dataset (4 values)", () => {
  before(async () => {
    await insertRows("pipe_even", "NODE_EVEN4", [40, 10, 30, 20]);
  });

  test("p25 interpolates → 17.5", async () => {
    assert.equal((await nodeCallStats("pipe_even", "NODE_EVEN4")).p25, 17.5);
  });

  test("p50 interpolates → 25", async () => {
    assert.equal((await nodeCallStats("pipe_even", "NODE_EVEN4")).p50, 25);
  });

  test("p75 interpolates → 32.5", async () => {
    assert.equal((await nodeCallStats("pipe_even", "NODE_EVEN4")).p75, 32.5);
  });

  test("sampleSize = 4", async () => {
    assert.equal((await nodeCallStats("pipe_even", "NODE_EVEN4")).sampleSize, 4);
  });
});

describe("percentile — exact integer index returns value without interpolation", () => {
  before(async () => {
    await insertRows("pipe_exact", "NODE_EXACT", [0, 50, 100]);
  });

  test("p50 hits exact integer index → 50 (no interpolation)", async () => {
    assert.equal((await nodeCallStats("pipe_exact", "NODE_EXACT")).p50, 50);
  });

  test("p25 interpolates → 25", async () => {
    assert.equal((await nodeCallStats("pipe_exact", "NODE_EXACT")).p25, 25);
  });

  test("p75 interpolates → 75", async () => {
    assert.equal((await nodeCallStats("pipe_exact", "NODE_EXACT")).p75, 75);
  });
});

describe("sinceDays cutoff", () => {
  test("default sinceDays=30 includes rows from 29 days ago", async () => {
    await insertRows("pipe_since", "NODE_29D", [1, 2, 3], { daysAgo: 29 });
    const r = await nodeCallStats("pipe_since", "NODE_29D");
    assert.ok(r !== null, "rows within 30d should produce stats");
    assert.equal(r.sampleSize, 3);
  });

  test("default sinceDays=30 excludes rows older than 30 days", async () => {
    await insertRows("pipe_since", "NODE_31D", [1, 2, 3], { daysAgo: 31 });
    const r = await nodeCallStats("pipe_since", "NODE_31D");
    assert.equal(r, null, "rows older than 30d should be excluded");
  });

  test("sinceDays=7 excludes rows from 8 days ago", async () => {
    await insertRows("pipe_since", "NODE_8D", [1, 2, 3], { daysAgo: 8 });
    const r = await nodeCallStats("pipe_since", "NODE_8D", { sinceDays: 7 });
    assert.equal(r, null, "rows older than 7d should be excluded with sinceDays=7");
  });

  test("sinceDays=7 includes rows from 6 days ago", async () => {
    await insertRows("pipe_since", "NODE_6D", [1, 2, 3], { daysAgo: 6 });
    const r = await nodeCallStats("pipe_since", "NODE_6D", { sinceDays: 7 });
    assert.ok(r !== null);
    assert.equal(r.sampleSize, 3);
  });

  test("sinceDays=90 includes rows from 60 days ago", async () => {
    await insertRows("pipe_since", "NODE_60D", [1, 2, 3], { daysAgo: 60 });
    const r = await nodeCallStats("pipe_since", "NODE_60D", { sinceDays: 90 });
    assert.ok(r !== null);
    assert.equal(r.sampleSize, 3);
  });
});

describe("sampleSize reflects actual row count", () => {
  test("sampleSize = 10 for 10 rows", async () => {
    await insertRows("pipe_sz", "NODE_10", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const r = await nodeCallStats("pipe_sz", "NODE_10");
    assert.equal(r.sampleSize, 10);
  });

  test("sampleSize = 3 for minimum 3 rows", async () => {
    await insertRows("pipe_sz", "NODE_3", [0.01, 0.02, 0.03]);
    const r = await nodeCallStats("pipe_sz", "NODE_3");
    assert.equal(r.sampleSize, 3);
  });
});

describe("result shape", () => {
  test("result has exactly p25, p50, p75, sampleSize keys", async () => {
    await insertRows("pipe_shape", "NODE_SHAPE", [1, 2, 3, 4, 5]);
    const r = await nodeCallStats("pipe_shape", "NODE_SHAPE");
    const keys = Object.keys(r).sort();
    assert.deepEqual(keys, ["p25", "p50", "p75", "sampleSize"]);
  });

  test("all percentile values are finite numbers", async () => {
    await insertRows("pipe_shape", "NODE_FINITE", [0.001, 0.002, 0.003]);
    const r = await nodeCallStats("pipe_shape", "NODE_FINITE");
    assert.ok(Number.isFinite(r.p25), "p25 must be finite");
    assert.ok(Number.isFinite(r.p50), "p50 must be finite");
    assert.ok(Number.isFinite(r.p75), "p75 must be finite");
  });

  test("p25 <= p50 <= p75 for any non-trivial dataset", async () => {
    await insertRows("pipe_shape", "NODE_ORDER", [5, 10, 15, 20, 25]);
    const r = await nodeCallStats("pipe_shape", "NODE_ORDER");
    assert.ok(r.p25 <= r.p50, "p25 must be <= p50");
    assert.ok(r.p50 <= r.p75, "p50 must be <= p75");
  });
});

describe("pipeline_id and node_id isolation", () => {
  test("rows for pipe_a are not visible when querying pipe_b", async () => {
    await insertRows("pipe_iso_a", "NODE_ISO", [1, 2, 3, 4, 5]);
    const r = await nodeCallStats("pipe_iso_b", "NODE_ISO");
    assert.equal(r, null);
  });

  test("rows for node_a are not visible when querying node_b", async () => {
    await insertRows("pipe_iso_shared", "NODE_ISO_A", [1, 2, 3, 4, 5]);
    const r = await nodeCallStats("pipe_iso_shared", "NODE_ISO_B");
    assert.equal(r, null);
  });
});
